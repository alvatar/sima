//! [`Lease`]: one worker's in-memory hold on a task for one attempt.

use std::time::Instant;

use sima_contracts::WorkerId;

/// A worker's hold on a task while it evaluates one attempt. Leases live in
/// memory only: a process death drops them all, and resume re-derives the
/// frontier from the store. The watchdog derives expiry from the lease's
/// age against the configured timeout; in-process execution cannot be
/// preempted, so nothing enforces it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Lease {
    /// The worker holding the task.
    pub(crate) worker: WorkerId,
    /// The zero-based attempt this lease covers.
    pub(crate) attempt: u32,
    /// When the attempt was leased; the watchdog reports the lease expired
    /// once its age exceeds the configured timeout.
    pub(crate) leased_at: Instant,
}
