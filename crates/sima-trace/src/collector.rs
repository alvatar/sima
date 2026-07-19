//! The collector: the single thread that funnels events into the journal
//! and out to subscribers.
//!
//! Components that emit hold an [`Emitter`] — a cloneable channel handle —
//! and the one collector thread drains the channel. For each event it stamps
//! the wall clock, appends the record's line through the [`DurableSink`],
//! and then hands the record to each subscriber. Event arrival order across
//! threads is nondeterministic, which is correct: the journal is
//! observational and excluded from every equality criterion.

use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{Scope, ScopedJoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use sima_core::Result;

use crate::event::Event;
use crate::record::Record;

/// The durable side of the collector: where each record's line lands before
/// any subscriber sees it. The seam that keeps this crate below the store —
/// the store implements it for its journal writer.
pub trait DurableSink: Send {
    /// Appends one line durably; the collector stops on the first error.
    fn append_line(&mut self, line: &str) -> Result<()>;
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

/// A running collector: the event channel plus the collector thread's join
/// handle. Dropping every emitter ends the channel; the thread then drains
/// the remaining events and returns.
///
/// Ordering guarantee: the journal write for an event happens before any
/// subscriber sees it, and subscribers see records in journal order, from
/// one calling thread.
pub struct Collector<'scope> {
    events: Sender<Event>,
    handle: ScopedJoinHandle<'scope, Result<()>>,
}

impl<'scope> Collector<'scope> {
    /// Spawns the collector thread owning `sink` into `scope` — a scoped
    /// thread, so subscribers may borrow from the caller. The thread stamps
    /// and appends each received event's line and hands the record to each
    /// subscriber in declaration order, until every emitter is dropped, then
    /// returns — carrying the first append or encoding failure, which the
    /// caller surfaces at [`shutdown`](Self::shutdown).
    pub fn spawn<'env, S>(
        scope: &'scope Scope<'scope, 'env>,
        sink: S,
        subscribers: &'env [&'env (dyn Fn(&Record) + Sync)],
    ) -> Collector<'scope>
    where
        S: DurableSink + 'scope,
    {
        let (events, rx) = mpsc::channel();
        let handle = scope.spawn(move || drain(sink, rx, subscribers));
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

/// Stamps and appends each event's line and then hands the record to each
/// subscriber, until the channel closes. Stops at the first append or
/// encoding failure, returning it; remaining events are dropped — unappended
/// events never reach a subscriber — and their emitters' sends become no-ops.
fn drain<S: DurableSink>(
    mut sink: S,
    rx: Receiver<Event>,
    subscribers: &[&(dyn Fn(&Record) + Sync)],
) -> Result<()> {
    for event in rx {
        // One clock, read on this thread at append time — remote events are
        // stamped on arrival. A clock before the epoch stamps zero.
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since_epoch| since_epoch.as_millis() as u64);
        let record = Record {
            ts_ms: Some(ts_ms),
            event,
        };
        sink.append_line(&record.to_line()?)?;
        for subscriber in subscribers {
            subscriber(&record);
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
    fn append_precedes_subscribers_and_subscribers_run_in_declaration_order() -> Result<()> {
        let trace: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let trace_a = Arc::clone(&trace);
        let trace_b = Arc::clone(&trace);
        let first = move |record: &Record| {
            let event = record.event.clone();
            trace_a
                .lock()
                .expect("lock trace")
                .push(format!("first {:?}", event));
        };
        let second = move |record: &Record| {
            let event = record.event.clone();
            trace_b
                .lock()
                .expect("lock trace")
                .push(format!("second {:?}", event));
        };
        let subscribers: [&(dyn Fn(&Record) + Sync); 2] = [&first, &second];
        thread::scope(|scope| -> Result<()> {
            let sink = LogSink {
                trace: Arc::clone(&trace),
            };
            let collector = Collector::spawn(scope, sink, &subscribers);
            let emitter = collector.emitter();
            emitter.emit(queued("ab"));
            emitter.emit(queued("cd"));
            drop(emitter);
            collector.shutdown()
        })?;
        let entries = trace.lock().expect("lock trace").clone();
        assert_eq!(entries.len(), 6, "{entries:?}");
        // Per event: journal append first, then each subscriber in order;
        // events in send order.
        for (i, task_byte) in ["ab", "cd"].iter().enumerate() {
            let chunk = &entries[i * 3..i * 3 + 3];
            assert!(chunk[0].starts_with("append "), "{chunk:?}");
            assert!(chunk[0].contains(&task_byte.repeat(32)), "{chunk:?}");
            assert!(chunk[1].starts_with("first "), "{chunk:?}");
            assert!(chunk[2].starts_with("second "), "{chunk:?}");
        }
        Ok(())
    }

    #[test]
    fn every_line_and_record_is_stamped() -> Result<()> {
        let trace: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen: Arc<Mutex<Vec<Record>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_by_subscriber = Arc::clone(&seen);
        let subscriber = move |record: &Record| {
            seen_by_subscriber
                .lock()
                .expect("lock records")
                .push(record.clone());
        };
        let subscribers: [&(dyn Fn(&Record) + Sync); 1] = [&subscriber];
        thread::scope(|scope| -> Result<()> {
            let sink = LogSink {
                trace: Arc::clone(&trace),
            };
            let collector = Collector::spawn(scope, sink, &subscribers);
            collector.emitter().emit(queued("ab"));
            collector.shutdown()
        })?;
        // The subscriber saw the stamped record.
        let records = seen.lock().expect("lock records").clone();
        assert_eq!(records.len(), 1);
        assert!(records[0].ts_ms.is_some(), "{records:?}");
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
            let collector = Collector::spawn(scope, FailingSink, &[]);
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
    fn an_append_failure_surfaces_at_shutdown_and_stops_the_fan_out() {
        let seen: Arc<Mutex<Vec<Record>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_by_subscriber = Arc::clone(&seen);
        let subscriber = move |record: &Record| {
            seen_by_subscriber
                .lock()
                .expect("lock records")
                .push(record.clone());
        };
        let subscribers: [&(dyn Fn(&Record) + Sync); 1] = [&subscriber];
        let result = thread::scope(|scope| {
            let collector = Collector::spawn(scope, FailingSink, &subscribers);
            let emitter = collector.emitter();
            emitter.emit(queued("ab"));
            emitter.emit(queued("cd"));
            drop(emitter);
            collector.shutdown()
        });
        assert!(matches!(result, Err(Error::Validation(_))), "{result:?}");
        // Unappended events never reach a subscriber.
        assert!(seen.lock().expect("lock records").is_empty());
    }
}
