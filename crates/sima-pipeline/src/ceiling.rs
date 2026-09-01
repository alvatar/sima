//! The search's own wall-clock ceiling: `[budget] max_wall_clock_ms`, enforced
//! wherever the search executes.
//!
//! A search that states a ceiling interrupts itself when it elapses, through the
//! flag `SIGINT` sets — so what follows is the wind-down that already exists:
//! the attempts in flight are abandoned, their checkpoints stand, and the
//! store is left resumable.
//!
//! **The clock starts with this execution, not with the search.** A resumed search
//! gets a fresh ceiling, and so does each session of a migrated one. The
//! ceiling therefore bounds unattended computing per launch, which is what a
//! search nobody is watching needs: it ends on its own.
//!
//! **A search has a ceiling only where no bill searches against its time.** A local
//! search and a machine of yours keep one; a rented destination is never sent the
//! key, because a rental bills by the hour rather than by use and a search stopped
//! early there saves nothing while leaving a machine that bills and computes
//! nothing. Which forms carry the key is decided where the far config is
//! synthesized; what this module does is keep the ceiling a config states.

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use sima_core::Result;
use sima_model::SearchId;
use sima_scheduler::{Event, Level};
use sima_store::Store;
use sima_trace::{Collector, Observer};

use crate::rental::StopSignal;

/// Runs `body` under `limit`, and reports whether the ceiling fired.
///
/// A `limit` of `None` is a search stating no ceiling: `body` searches with nothing
/// watching it and the answer is always `false`. Otherwise a thread waits out
/// what is left of the ceiling and raises `interrupt` when it elapses; `body`
/// returning wakes it, so a search that finishes first costs one parked thread.
pub(crate) fn under_ceiling<T>(
    limit: Option<Duration>,
    interrupt: &AtomicBool,
    body: impl FnOnce() -> T,
) -> (T, bool) {
    let Some(limit) = limit else {
        return (body(), false);
    };
    let deadline = Instant::now() + limit;
    let done = StopSignal::new();
    thread::scope(|scope| {
        let watch = scope.spawn(|| {
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    interrupt.store(true, Ordering::Relaxed);
                    return true;
                }
                // A wait that ends on the signal is a search that finished inside
                // its ceiling; one that ends on the timeout comes back around
                // to a deadline that has passed.
                if done.wait(left) {
                    return false;
                }
            }
        });
        let out = body();
        done.raise();
        (out, watch.join().expect("the ceiling thread joins"))
    })
}

/// Journals why the search interrupted, once it has.
///
/// It is appended after the search rather than emitted as the ceiling fires
/// because the search's emitter belongs to the collector the scheduler owns:
/// holding a clone of it outside that scope would keep the collector from
/// joining, and so keep the search from ever returning. The record is written
/// through the same collector boundary every other event crosses, so the
/// operator's view sees it too.
pub(crate) fn report_ceiling(
    store: &Store,
    search: &SearchId,
    observer: Observer<'_>,
    limit: Duration,
) -> Result<()> {
    let writer = store.journal_writer(search)?;
    thread::scope(|scope| {
        let collector = Collector::spawn(scope, writer, observer);
        collector.emitter().emit(Event::Diagnostic {
            level: Level::Warn,
            source: "budget".to_string(),
            message: format!(
                "the search reached the {}ms max_wall_clock_ms its [budget] states and wound \
                 itself down",
                limit.as_millis()
            ),
            worker: None,
            host: None,
            task: None,
        });
        collector.shutdown()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_that_finishes_inside_its_ceiling_is_never_interrupted() {
        let interrupt = AtomicBool::new(false);
        let (out, fired) = under_ceiling(Some(Duration::from_secs(30)), &interrupt, || 7);
        assert_eq!(out, 7);
        assert!(!fired, "the ceiling did not elapse");
        assert!(
            !interrupt.load(Ordering::Relaxed),
            "nothing was asked to end"
        );
    }

    #[test]
    fn a_ceiling_that_elapses_raises_the_flag_the_run_winds_down_on() {
        let interrupt = AtomicBool::new(false);
        let (out, fired) = under_ceiling(Some(Duration::from_millis(20)), &interrupt, || {
            // The body watches the same flag a search's driver watches.
            while !interrupt.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1));
            }
            "wound down"
        });
        assert_eq!(out, "wound down");
        assert!(fired, "the ceiling elapsed");
    }

    #[test]
    fn a_run_stating_no_ceiling_is_watched_by_nothing() {
        let interrupt = AtomicBool::new(false);
        let (out, fired) = under_ceiling(None, &interrupt, || 1);
        assert_eq!(out, 1);
        assert!(!fired);
        assert!(!interrupt.load(Ordering::Relaxed));
    }
}
