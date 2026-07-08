//! [`RunControl`]: the caller's handles into a running search.

use std::sync::atomic::AtomicBool;

use crate::event::LifecycleEvent;

/// The caller's handles into a running search: an observer invoked with
/// each lifecycle event and an interrupt flag the driver polls to wind
/// the run down.
pub struct RunControl<'a> {
    /// Invoked with each event on the journal-sink thread, immediately
    /// after the event's line is appended: typed events, in journal
    /// order, from one calling thread.
    pub observer: &'a (dyn Fn(&LifecycleEvent) + Sync),
    /// Level-triggered wind-down request. Once set, the driver stops
    /// handing out tasks; in-flight attempts finish and commit, and the
    /// run returns [`RunOutcome::Interrupted`](crate::RunOutcome::Interrupted)
    /// with no manifest written, leaving the store resumable.
    pub interrupt: &'a AtomicBool,
}

impl RunControl<'_> {
    /// A control nobody holds: the observer ignores every event and the
    /// interrupt flag is never set. The handle for callers that drive a
    /// run without live observation.
    pub fn detached() -> RunControl<'static> {
        static NEVER: AtomicBool = AtomicBool::new(false);
        RunControl {
            observer: &|_| {},
            interrupt: &NEVER,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn detached_is_inert() {
        let control = RunControl::detached();
        // The observer accepts events without effect, and the flag never
        // requests a wind-down.
        (control.observer)(&LifecycleEvent::Queued {
            task: "00".repeat(32),
        });
        assert!(!control.interrupt.load(Ordering::Relaxed));
    }
}
