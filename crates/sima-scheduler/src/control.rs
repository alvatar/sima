//! [`RunControl`]: the caller's handles into a running search.

use std::sync::atomic::AtomicBool;

use sima_trace::Observer;

/// The caller's handles into a running search: an observer invoked with
/// each journal record and an interrupt flag the driver polls to wind
/// the run down.
pub struct RunControl<'a> {
    /// Invoked with each record on the collector thread, immediately
    /// after the record's line is appended: typed records, in journal
    /// order, from one calling thread.
    pub observer: Observer<'a>,
    /// Level-triggered wind-down request. Once set, the driver stops
    /// handing out tasks; in-flight attempts finish and commit, and the
    /// run returns [`RunOutcome::Interrupted`](crate::RunOutcome::Interrupted)
    /// with no manifest written, leaving the store resumable.
    pub interrupt: &'a AtomicBool,
}

impl RunControl<'_> {
    /// A control nobody holds: the observer ignores every record and the
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

    use sima_trace::{Event, Record};

    use super::*;

    #[test]
    fn detached_is_inert() {
        let control = RunControl::detached();
        // The observer accepts records without effect, and the flag never
        // requests a wind-down.
        (control.observer)(&Record {
            ts_ms: 0,
            event: Event::Queued {
                task: "00".repeat(32),
            },
        });
        assert!(!control.interrupt.load(Ordering::Relaxed));
    }
}
