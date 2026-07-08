//! Terminal rendering: one plain line per meaningful lifecycle event, and
//! the status block. Ids render short — the first twelve hex characters —
//! since a run's journal names them consistently.

use std::sync::atomic::{AtomicUsize, Ordering};

use sima_pipeline::{LifecycleEvent, RunState, RunStatus};

/// How many hex characters of an id a progress line shows.
const SHORT: usize = 12;

/// The leading `SHORT` characters of a journaled id.
fn short(id: &str) -> &str {
    &id[..id.len().min(SHORT)]
}

/// Progress rendering over a run's event stream: prints one line per
/// meaningful event. Called from the journal-sink thread, one event at a
/// time, in journal order; the counters give the `committed k/n` running
/// count.
pub struct Progress {
    /// The run's task count, from `RunStarted`.
    tasks: AtomicUsize,
    /// Commits seen so far.
    committed: AtomicUsize,
}

impl Progress {
    /// A progress renderer with no events seen yet.
    pub fn new() -> Progress {
        Progress {
            tasks: AtomicUsize::new(0),
            committed: AtomicUsize::new(0),
        }
    }

    /// Prints the line `event` warrants, if any. `Queued` and `Leased` are
    /// bookkeeping and stay silent.
    pub fn event(&self, event: &LifecycleEvent) {
        match event {
            LifecycleEvent::RunStarted { tasks, .. } => {
                self.tasks.store(*tasks, Ordering::Relaxed);
                println!("started: {tasks} tasks");
            }
            LifecycleEvent::Committed { task, .. } => {
                let k = self.committed.fetch_add(1, Ordering::Relaxed) + 1;
                let n = self.tasks.load(Ordering::Relaxed);
                println!("committed {k}/{n}  {}", short(task));
            }
            LifecycleEvent::Retried { task, next_attempt } => {
                println!("retrying {} (attempt {next_attempt})", short(task));
            }
            LifecycleEvent::Rejected { task, reason, .. } => {
                println!("rejected {}: {reason}", short(task));
            }
            LifecycleEvent::Failed {
                task,
                attempt,
                reason,
                ..
            } => {
                println!("failed {} (attempt {attempt}): {reason}", short(task));
            }
            LifecycleEvent::Faulted { task, error, .. } => {
                println!("fault {}: {error}", short(task));
            }
            LifecycleEvent::LeaseExpired {
                task, elapsed_ms, ..
            } => {
                println!("lease expired {} ({elapsed_ms} ms)", short(task));
            }
            LifecycleEvent::RunFinalized { committed, .. } => {
                println!("finalized: {committed} tasks committed");
            }
            LifecycleEvent::RunFailed { task, reason, .. } => {
                println!("run failed on {}: {reason}", short(task));
            }
            LifecycleEvent::RunInterrupted { .. } => {
                println!("interrupted: store resumable, re-run to continue");
            }
            LifecycleEvent::Queued { .. } | LifecycleEvent::Leased { .. } => {}
        }
    }
}

/// Renders the status block, one aligned `name  value` line per field.
pub fn status_block(status: &RunStatus) -> String {
    let state = match &status.state {
        RunState::InProgress => "in progress".to_string(),
        RunState::Finalized => "finalized".to_string(),
        RunState::Failed { task, reason } => {
            format!("failed on {}: {reason}", short(task))
        }
        RunState::Interrupted => "interrupted".to_string(),
    };
    format!(
        "run            {}\n\
         state          {state}\n\
         tasks          {}\n\
         committed      {}\n\
         retried        {}\n\
         rejected       {}\n\
         faulted        {}\n\
         lease expired  {}",
        status.run,
        status.tasks,
        status.committed,
        status.retried,
        status.rejected,
        status.faulted,
        status.lease_expired,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_truncates_long_ids_and_keeps_short_ones() {
        assert_eq!(short(&"ab".repeat(32)), "abababababab");
        assert_eq!(short("abcd"), "abcd");
    }

    #[test]
    fn the_status_block_names_every_field() {
        let status = RunStatus {
            run: sima_model::RunId::from_hash(sima_core::hash_bytes(b"a run to render")),
            tasks: 3,
            committed: 2,
            retried: 1,
            rejected: 0,
            faulted: 0,
            lease_expired: 0,
            state: RunState::InProgress,
            occupancy: std::collections::BTreeMap::new(),
        };
        let block = status_block(&status);
        for field in [
            "run",
            "state",
            "tasks",
            "committed",
            "retried",
            "rejected",
            "faulted",
            "lease expired",
        ] {
            assert!(block.contains(field), "missing {field}: {block}");
        }
        assert!(block.contains("in progress"));
    }
}
