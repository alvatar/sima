//! The journal sink: the single thread that owns the run's [`JournalWriter`].
//!
//! `JournalWriter::append` takes `&mut self`, so exactly one thread may write a
//! run's journal. Workers, the watchdog, and the driver emit events over an
//! `mpsc` channel; this one thread drains it, appends each line, and then
//! invokes the caller's observer with the typed event — so the observer sees
//! typed events, in journal order, from one calling thread. Event arrival
//! order across threads is nondeterministic, which is correct: the journal is
//! observational and excluded from every equality criterion.

use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{Scope, ScopedJoinHandle};

use sima_core::Result;
use sima_store::JournalWriter;

use crate::event::LifecycleEvent;

/// A running journal sink: the cloneable event sender plus the writer thread's
/// join handle. Dropping every sender ends the channel; the thread then drains
/// the remaining events and returns.
pub(crate) struct JournalSink<'scope> {
    events: Sender<LifecycleEvent>,
    handle: ScopedJoinHandle<'scope, Result<()>>,
}

impl<'scope> JournalSink<'scope> {
    /// Spawns the writer thread owning `writer` into `scope` — a scoped
    /// thread, so `observer` may borrow from the caller. The thread appends
    /// each received event's line and hands the event to the observer, until
    /// every sender is dropped, then returns — carrying the first append or
    /// encoding failure, which the caller surfaces as the run's
    /// infrastructure fault at join.
    pub(crate) fn spawn<'env>(
        scope: &'scope Scope<'scope, 'env>,
        writer: JournalWriter,
        observer: &'env (dyn Fn(&LifecycleEvent) + Sync),
    ) -> JournalSink<'scope> {
        let (events, rx) = mpsc::channel();
        let handle = scope.spawn(move || drain(writer, rx, observer));
        JournalSink { events, handle }
    }

    /// A clone of the event sender for a worker, the watchdog, or the driver.
    pub(crate) fn sender(&self) -> Sender<LifecycleEvent> {
        self.events.clone()
    }

    /// Drops the sink's own sender and joins the writer thread. Once every
    /// other sender has also dropped, the thread returns; its result — an
    /// append or encoding fault, or a thread panic — surfaces here.
    pub(crate) fn shutdown(self) -> Result<()> {
        drop(self.events);
        match self.handle.join() {
            Ok(result) => result,
            // A panic in the writer thread is a bug, not a domain outcome:
            // resume unwinding so it surfaces rather than being swallowed.
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

/// Appends each event's line and then invokes the observer with the event,
/// until the channel closes. Stops at the first append or encoding failure,
/// returning it; remaining events are dropped — unappended events never reach
/// the observer — and their senders' sends become no-ops.
fn drain(
    mut writer: JournalWriter,
    rx: Receiver<LifecycleEvent>,
    observer: &(dyn Fn(&LifecycleEvent) + Sync),
) -> Result<()> {
    for event in rx {
        writer.append(&event.to_line()?)?;
        observer(&event);
    }
    Ok(())
}

/// Sends an event to the sink, ignoring a closed channel: the receiver closes
/// only after a journal fault, which the sink's join already surfaces, so a
/// dropped event here would be double-reporting.
pub(crate) fn emit(events: &Sender<LifecycleEvent>, event: LifecycleEvent) {
    let _ = events.send(event);
}
