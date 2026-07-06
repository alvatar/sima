//! [`Lease`]: one worker's in-memory hold on a task for one attempt.

use std::time::Instant;

use sima_contracts::WorkerId;

/// A worker's hold on a task while it evaluates one attempt. Leases live in
/// memory only: a process death drops them all, and resume re-derives the
/// frontier from the store. The `deadline` is a soft target the watchdog reads
/// for overrun detection; in-process execution cannot be preempted, so nothing
/// enforces it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Lease {
    /// The worker holding the task.
    pub(crate) worker: WorkerId,
    /// The zero-based attempt this lease covers.
    pub(crate) attempt: u32,
    /// When the attempt should have finished; past it, the watchdog reports an
    /// overrun.
    pub(crate) deadline: Instant,
}
