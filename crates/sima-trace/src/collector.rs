//! The collector: the single thread that funnels events into the journal
//! and out to the run's observer.
//!
//! Components that emit hold an [`Emitter`] — a cloneable channel handle —
//! and the one collector thread drains the channel. For each event it stamps
//! the wall clock, appends the record's line through the [`DurableSink`],
//! and then hands the record to the observer. Event arrival order across
//! threads is nondeterministic, which is correct: the journal is
//! observational and excluded from every equality criterion.

use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{Scope, ScopedJoinHandle};

use sima_core::Result;

use crate::event::Event;
use crate::record::Record;

/// The durable side of the collector: where each record's line lands before
/// the observer sees it. The boundary that keeps this crate below the store —
/// the store implements it for its journal writer.
pub trait DurableSink: Send {
    /// Appends one line durably; the collector stops on the first error.
    fn append_line(&mut self, line: &str) -> Result<()>;

    /// Appends every line of `lines` durably, as one batch.
    ///
    /// The collector drains what is queued and hands it here in one call, so a
    /// sink whose durability costs a syscall per call pays it once for the
    /// batch rather than once per line. The default appends one at a time, so a
    /// sink with nothing to gain from batching implements nothing.
    ///
    /// The guarantee is per batch: on return every line is durable, and on
    /// error nothing after the failing line is written. The collector hands the
    /// observer only what this returned `Ok` for.
    fn append_lines(&mut self, lines: &[String]) -> Result<()> {
        for line in lines {
            self.append_line(line)?;
        }
        Ok(())
    }
}

/// A cloneable emission handle wrapping the collector's channel sender.
#[derive(Clone)]
pub struct Emitter {
    events: Sender<Event>,
}

impl Emitter {
    /// Sends an event to the collector, ignoring a closed channel: the
    /// receiver closes only after a journal fault, which the collector's
    /// join already surfaces, so reporting the drop here would be
    /// double-reporting.
    pub fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }
}

/// An emitter over a caller-owned channel, for a consumer that drains the
/// events itself — a test observing raw emissions, or an in-process pipeline
/// that is not the collector. The fire-and-forget contract is unchanged: a
/// dropped receiver drops the event silently.
impl From<Sender<Event>> for Emitter {
    fn from(events: Sender<Event>) -> Emitter {
        Emitter { events }
    }
}

/// A record consumer the collector thread calls: the run's observer, borrowed
/// for the scope the collector runs in. `Sync` because the collector thread
/// calls it while the caller's thread holds the same reference.
pub type Observer<'a> = &'a (dyn Fn(&Record) + Sync);

/// A running collector: the event channel plus the collector thread's join
/// handle. Dropping every emitter ends the channel; the thread then drains
/// the remaining events and returns.
///
/// Ordering guarantee: the journal write for an event happens before the
/// observer sees it, and the observer sees records in journal order, from
/// one calling thread.
pub struct Collector<'scope> {
    events: Sender<Event>,
    handle: ScopedJoinHandle<'scope, Result<()>>,
}

impl<'scope> Collector<'scope> {
    /// Spawns the collector thread owning `sink` into `scope` — a scoped
    /// thread, so `observer` may borrow from the caller. The thread stamps
    /// and appends each received event's line and hands the record to the
    /// observer, until every emitter is dropped, then returns — carrying the
    /// first append or encoding failure, which the caller surfaces at
    /// [`shutdown`](Self::shutdown).
    pub fn spawn<'env, S>(
        scope: &'scope Scope<'scope, 'env>,
        sink: S,
        observer: Observer<'env>,
    ) -> Collector<'scope>
    where
        S: DurableSink + 'scope,
    {
        let (events, rx) = mpsc::channel();
        let handle = scope.spawn(move || drain(sink, rx, observer));
        Collector { events, handle }
    }

    /// A cloneable emission handle for a component that emits events.
    pub fn emitter(&self) -> Emitter {
        Emitter {
            events: self.events.clone(),
        }
    }

    /// Drops the collector's own sender and joins the collector thread. Once
    /// every emitter has also dropped, the thread returns; its result — an
    /// append or encoding fault, or a thread panic — surfaces here.
    pub fn shutdown(self) -> Result<()> {
        drop(self.events);
        match self.handle.join() {
            Ok(result) => result,
            // A panic in the collector thread is a bug, not a domain outcome:
            // resume unwinding so it surfaces rather than being swallowed. This
            // preserves the meaning of the Err vocabulary: every Err a caller
            // receives is an expected, describable fault it can act on, while a
            // bug arrives as an abnormal death, so a supervising caller can
            // distinguish retry-after-fixing-the-environment from
            // the-code-is-wrong.
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

/// Stamps and appends the queued events' lines and then hands their records to
/// the observer, in order, until the channel closes. Stops at the first append
/// or encoding failure, returning it; remaining events are dropped —
/// unappended events never reach the observer — and their emitters' sends
/// become no-ops.
///
/// Each iteration takes one blocking event and then everything else already
/// queued behind it, appends the batch, and only then walks the observer. The
/// journal write for an event still happens before the observer sees it, which
/// is the guarantee; what changes is that a burst of events costs one
/// durability barrier instead of one per event, which is what caps a run's task
/// throughput at the sink's own rate.
///
/// A crash mid-batch loses the events the sink had not made durable, which the
/// observational-journal contract already permits: a journal is not a
/// write-ahead log, and the store's own records are what a resume derives the
/// frontier from.
fn drain<S: DurableSink>(mut sink: S, rx: Receiver<Event>, observer: Observer) -> Result<()> {
    // One clock, read on this thread at append time — remote events are
    // stamped on arrival. A batch's records are stamped as they are taken, so
    // the stamps stay in the order the events arrived.
    while let Ok(first) = rx.recv() {
        let mut records = vec![Record::stamped(first)];
        records.extend(rx.try_iter().map(Record::stamped));
        let lines = records
            .iter()
            .map(Record::to_line)
            .collect::<Result<Vec<String>>>()?;
        sink.append_lines(&lines)?;
        for record in &records {
            observer(record);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::thread;

    use sima_core::Error;

    use super::*;
    use crate::Level;

    /// A sink recording the size of every batch it was handed, so a test can
    /// see how many durability barriers a burst of events cost.
    struct BatchSink {
        batches: Arc<Mutex<Vec<usize>>>,
        lines: Arc<Mutex<Vec<String>>>,
    }

    impl DurableSink for BatchSink {
        fn append_line(&mut self, line: &str) -> Result<()> {
            self.batches.lock().expect("batch lock").push(1);
            self.lines.lock().expect("line lock").push(line.to_string());
            Ok(())
        }

        fn append_lines(&mut self, lines: &[String]) -> Result<()> {
            self.batches.lock().expect("batch lock").push(lines.len());
            self.lines
                .lock()
                .expect("line lock")
                .extend(lines.iter().cloned());
            Ok(())
        }
    }

    #[test]
    fn queued_events_share_one_durability_barrier() {
        // A run committing tasks faster than one fsync each is capped at the
        // disk's fsync rate however many workers it has. The collector takes
        // one blocking event and everything queued behind it, so a burst costs
        // one barrier rather than one per event.
        //
        // The events are queued before the collector starts, so what it drains
        // on its first pass is all of them.
        let batches = Arc::new(Mutex::new(Vec::new()));
        let lines = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = std::sync::mpsc::channel();
        for i in 0..16 {
            tx.send(Event::Diagnostic {
                level: Level::Info,
                source: "test".to_string(),
                message: format!("event {i}"),
                worker: None,
                host: None,
                task: None,
            })
            .expect("queue an event");
        }
        drop(tx);
        let sink = BatchSink {
            batches: Arc::clone(&batches),
            lines: Arc::clone(&lines),
        };
        drain(sink, rx, &|_| {}).expect("the drain succeeds");

        let batches = batches.lock().expect("batch lock");
        assert_eq!(
            batches.iter().sum::<usize>(),
            16,
            "every event is appended: {batches:?}"
        );
        assert!(
            batches.len() < 16,
            "a queued burst shares its barriers: {batches:?}"
        );
        assert_eq!(lines.lock().expect("line lock").len(), 16);
    }

    #[test]
    fn the_observer_sees_what_the_journal_holds_in_the_order_it_holds_it() {
        // The guarantee the batching had to preserve: an event is journaled
        // before the observer sees it, and the observer's order is the
        // journal's. A batch appends first and walks the observer after, so
        // both halves still hold across one.
        let lines = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = std::sync::mpsc::channel();
        for i in 0..8 {
            tx.send(Event::Diagnostic {
                level: Level::Info,
                source: "test".to_string(),
                message: format!("event {i}"),
                worker: None,
                host: None,
                task: None,
            })
            .expect("queue an event");
        }
        drop(tx);
        let sink = BatchSink {
            batches: Arc::new(Mutex::new(Vec::new())),
            lines: Arc::clone(&lines),
        };
        let observed = Arc::clone(&seen);
        drain(sink, rx, &move |record: &Record| {
            observed
                .lock()
                .expect("seen lock")
                .push(record.to_line().expect("a line"));
        })
        .expect("the drain succeeds");
        assert_eq!(
            *seen.lock().expect("seen lock"),
            *lines.lock().expect("line lock")
        );
    }

    /// A sink that logs each appended line into the shared trace.
    struct LogSink {
        trace: Arc<Mutex<Vec<String>>>,
    }

    impl DurableSink for LogSink {
        fn append_line(&mut self, line: &str) -> Result<()> {
            self.trace
                .lock()
                .expect("lock trace")
                .push(format!("append {line}"));
            Ok(())
        }
    }

    /// A sink that fails every append.
    struct FailingSink;

    impl DurableSink for FailingSink {
        fn append_line(&mut self, _line: &str) -> Result<()> {
            Err(Error::Validation("sink refuses the line".to_string()))
        }
    }

    fn queued(task_byte: &str) -> Event {
        Event::Queued {
            task: task_byte.repeat(32),
        }
    }

    #[test]
    fn append_precedes_the_observer_and_records_arrive_in_journal_order() -> Result<()> {
        let trace: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let trace_for_observer = Arc::clone(&trace);
        let observer = move |record: &Record| {
            trace_for_observer
                .lock()
                .expect("lock trace")
                .push(format!("observed {:?}", record.event));
        };
        thread::scope(|scope| -> Result<()> {
            let sink = LogSink {
                trace: Arc::clone(&trace),
            };
            let collector = Collector::spawn(scope, sink, &observer);
            let emitter = collector.emitter();
            emitter.emit(queued("ab"));
            emitter.emit(queued("cd"));
            drop(emitter);
            collector.shutdown()
        })?;
        let entries = trace.lock().expect("lock trace").clone();
        assert_eq!(entries.len(), 4, "{entries:?}");
        // The guarantee, stated over the whole trace rather than as a strict
        // alternation: whether the two events shared a batch or not is timing,
        // but an event's append always precedes its own observation, and both
        // sequences follow send order.
        let position = |prefix: &str, task: &str| {
            entries
                .iter()
                .position(|entry| entry.starts_with(prefix) && entry.contains(&task.repeat(32)))
                .unwrap_or_else(|| panic!("{prefix}{task} is missing from {entries:?}"))
        };
        for task in ["ab", "cd"] {
            assert!(
                position("append ", task) < position("observed ", task),
                "the journal write for {task} precedes its observation: {entries:?}"
            );
        }
        assert!(
            position("append ", "ab") < position("append ", "cd"),
            "appends follow send order: {entries:?}"
        );
        assert!(
            position("observed ", "ab") < position("observed ", "cd"),
            "observations follow send order: {entries:?}"
        );
        Ok(())
    }

    #[test]
    fn every_line_and_record_is_stamped() -> Result<()> {
        let trace: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen: Arc<Mutex<Vec<Record>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_by_observer = Arc::clone(&seen);
        let observer = move |record: &Record| {
            seen_by_observer
                .lock()
                .expect("lock records")
                .push(record.clone());
        };
        thread::scope(|scope| -> Result<()> {
            let sink = LogSink {
                trace: Arc::clone(&trace),
            };
            let collector = Collector::spawn(scope, sink, &observer);
            collector.emitter().emit(queued("ab"));
            collector.shutdown()
        })?;
        // The observer saw the stamped record.
        let records = seen.lock().expect("lock records").clone();
        assert_eq!(records.len(), 1);
        assert!(records[0].ts_ms > 0, "{records:?}");
        // The journal line carries the same stamp.
        let entries = trace.lock().expect("lock trace").clone();
        let line = entries[0].strip_prefix("append ").expect("an append entry");
        assert_eq!(Record::from_line(line)?, records[0]);
        Ok(())
    }

    #[test]
    fn an_emitter_over_a_caller_owned_channel_delivers_to_its_receiver() {
        let (tx, rx) = mpsc::channel();
        let emitter = Emitter::from(tx);
        emitter.emit(queued("ab"));
        drop(emitter);
        let received: Vec<Event> = rx.into_iter().collect();
        assert_eq!(received, [queued("ab")]);
    }

    #[test]
    fn emitting_after_the_collector_thread_exited_is_silent() {
        thread::scope(|scope| {
            // The failing sink makes the collector thread return on the first
            // event, dropping the receiver while this emitter stays alive.
            let collector = Collector::spawn(scope, FailingSink, &|_| {});
            let emitter = collector.emitter();
            emitter.emit(queued("ab"));
            // Joining succeeds despite the live emitter: the thread already
            // exited on the append failure.
            let result = collector.shutdown();
            assert!(matches!(result, Err(Error::Validation(_))), "{result:?}");
            // The channel is closed; the event drops without panic or error.
            emitter.emit(queued("cd"));
        });
    }

    #[test]
    fn an_append_failure_surfaces_at_shutdown_and_stops_the_hand_off() {
        let seen: Arc<Mutex<Vec<Record>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_by_observer = Arc::clone(&seen);
        let observer = move |record: &Record| {
            seen_by_observer
                .lock()
                .expect("lock records")
                .push(record.clone());
        };
        let result = thread::scope(|scope| {
            let collector = Collector::spawn(scope, FailingSink, &observer);
            let emitter = collector.emitter();
            emitter.emit(queued("ab"));
            emitter.emit(queued("cd"));
            drop(emitter);
            collector.shutdown()
        });
        assert!(matches!(result, Err(Error::Validation(_))), "{result:?}");
        // Unappended events never reach the observer.
        assert!(seen.lock().expect("lock records").is_empty());
    }
}
