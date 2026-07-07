//! The watchdog: it detects lease overruns and reports them.
//!
//! A memory-safe runtime has no safe forced thread termination — killing a
//! thread mid-execution would leave locks held and memory half-mutated — so
//! forced preemption requires process isolation and arrives with the
//! subprocess worker. Here the watchdog delivers detection only: it scans the
//! lease table and emits one `TaskOverran` event per lease that outruns its
//! soft deadline, never touching the lease or the worker.

use std::collections::HashSet;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use sima_model::TaskKey;

use crate::driver::{Coord, Stop};
use crate::event::LifecycleEvent;
use crate::journal_sink::emit;

/// Scans the lease table on an interval, reporting each overrun once, until the
/// run winds down and the pool has drained.
pub(crate) fn watchdog_loop(coord: &Coord, timeout: Duration, events: &Sender<LifecycleEvent>) {
    // Scan several times per timeout; a small floor keeps a tiny timeout from
    // spinning.
    let interval = (timeout / 4).max(Duration::from_millis(1));
    // Leases already reported, keyed by (task, attempt) so a later attempt of
    // the same task can overrun and report afresh.
    let mut reported: HashSet<(TaskKey, u32)> = HashSet::new();
    // The scan, the exit check, and the wait share one continuous guard: every
    // notify_all site takes this mutex first, so a terminal wakeup landing
    // between the check and the wait cannot be lost.
    let mut state = coord.lock();
    loop {
        let now = Instant::now();
        // Detection only: read the lease table, never mutate it. Emitting under
        // the lock is safe here — the channel is unbounded so the send never
        // blocks, and overruns are rare.
        for (key, lease) in &state.leases {
            let elapsed = now.duration_since(lease.leased_at);
            if elapsed > timeout && reported.insert((*key, lease.attempt)) {
                emit(
                    events,
                    LifecycleEvent::TaskOverran {
                        task: key.to_string(),
                        worker: lease.worker.0,
                        elapsed_ms: elapsed.as_millis() as u64,
                    },
                );
            }
        }
        if !matches!(state.stop, Stop::Running) && state.leases.is_empty() {
            return;
        }
        // Sleep for the interval, waking early on any state change.
        state = coord
            .idle
            .wait_timeout(state, interval)
            .unwrap_or_else(|p| p.into_inner())
            .0;
    }
}
