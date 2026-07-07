//! [`RunStatus`]: a run's observable state, computed from its journal.

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_scheduler::LifecycleEvent;
use sima_store::Store;

/// A run's observable state, computed from its journal alone.
#[derive(Debug)]
pub struct RunStatus {
    /// The run the status describes.
    pub run: RunId,
    /// The run's task count, from the latest `RunStarted` — resume appends
    /// a fresh segment per orchestration, and each restates the count.
    pub tasks: usize,
    /// Committed tasks, summed across the whole journal: a task never
    /// commits twice, so the sum over resume segments stays a task count.
    pub committed: usize,
    /// Retry events across the whole journal.
    pub retried: usize,
    /// Rejection events across the whole journal.
    pub rejected: usize,
    /// Infrastructure-fault events across the whole journal.
    pub faulted: usize,
    /// Lease-expiry reports across the whole journal.
    pub lease_expired: usize,
    /// The run's current state.
    pub state: RunState,
}

/// The state the journal's last run-level event decides. A journal ending
/// mid-run reads as in progress: a dead orchestrator is indistinguishable
/// from a live one by the journal alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunState {
    /// A segment started and no run-level event closed it.
    InProgress,
    /// The run finalized; its manifest is written.
    Finalized,
    /// A definitive candidate failure ended the run.
    Failed {
        /// The failing task's key, as journaled.
        task: String,
        /// Why it failed.
        reason: String,
    },
    /// The caller interrupted the run; the store is resumable.
    Interrupted,
}

/// Computes `run`'s status from its journal in `store`. A run never
/// started in this store is [`Error::Validation`]; a journal line that
/// fails to parse is [`Error::Corruption`].
pub fn status(store: &Store, run: &RunId) -> Result<RunStatus> {
    let lines = store.journal(run)?;
    if lines.is_empty() {
        return Err(Error::Validation(format!(
            "run {run} was never started in this store"
        )));
    }
    let mut report = RunStatus {
        run: *run,
        tasks: 0,
        committed: 0,
        retried: 0,
        rejected: 0,
        faulted: 0,
        lease_expired: 0,
        state: RunState::InProgress,
    };
    // One pass over the journal: counters sum across every resume segment,
    // and the run-level events overwrite the state so the last one decides.
    for line in &lines {
        let event = LifecycleEvent::from_line(line)
            .map_err(|e| Error::Corruption(format!("journal of run {run}: {e}")))?;
        match event {
            LifecycleEvent::RunStarted { tasks, .. } => {
                report.tasks = tasks;
                report.state = RunState::InProgress;
            }
            LifecycleEvent::Committed { .. } => report.committed += 1,
            LifecycleEvent::Retried { .. } => report.retried += 1,
            LifecycleEvent::Rejected { .. } => report.rejected += 1,
            LifecycleEvent::Faulted { .. } => report.faulted += 1,
            LifecycleEvent::LeaseExpired { .. } => report.lease_expired += 1,
            LifecycleEvent::RunFinalized { .. } => report.state = RunState::Finalized,
            LifecycleEvent::RunFailed { task, reason, .. } => {
                report.state = RunState::Failed { task, reason };
            }
            LifecycleEvent::RunInterrupted { .. } => report.state = RunState::Interrupted,
            LifecycleEvent::Queued { .. }
            | LifecycleEvent::Leased { .. }
            | LifecycleEvent::Failed { .. } => {}
        }
    }
    Ok(report)
}
