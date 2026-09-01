//! [`SearchControl`]: the caller's handles into a running search.

use std::sync::atomic::AtomicBool;

use sima_trace::{Emitter, Observer};

/// A hook invoked once, on the driver thread, with a clone of the search's
/// emitter as the search starts — immediately after the collector spawns. It
/// carries the emitter to a caller that emits alongside the search — the rental
/// supervisor — without a scheduler edge to that caller: the closure is opaque.
pub type StartHook<'a> = &'a (dyn Fn(Emitter) + Send + Sync);

/// The caller's handles into a running search: an observer invoked with
/// each journal record and an interrupt flag the driver polls to wind
/// the search down.
pub struct SearchControl<'a> {
    /// Invoked with each record on the collector thread, immediately
    /// after the record's line is appended: typed records, in journal
    /// order, from one calling thread.
    pub observer: Observer<'a>,
    /// Level-triggered wind-down request. Once set, the driver stops
    /// handing out tasks; in-flight attempts finish and commit, and the
    /// search returns [`SearchOutcome::Interrupted`](crate::SearchOutcome::Interrupted)
    /// with no manifest written, leaving the store resumable.
    pub interrupt: &'a AtomicBool,
    /// Invoked once with the search's emitter when the collector spawns, or
    /// `None` for a caller that emits nothing of its own.
    pub on_start: Option<StartHook<'a>>,
}

impl SearchControl<'_> {
    /// A control nobody holds: the observer ignores every record, the
    /// interrupt flag is never set, and no start hook fires. The handle for
    /// callers that drive a search without live observation.
    pub fn detached() -> SearchControl<'static> {
        static NEVER: AtomicBool = AtomicBool::new(false);
        SearchControl {
            observer: &|_| {},
            interrupt: &NEVER,
            on_start: None,
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
        let control = SearchControl::detached();
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
