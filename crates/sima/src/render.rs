//! Terminal rendering: one plain line per meaningful lifecycle event, and
//! the status block. Ids render short — the first twelve hex characters —
//! since a run's journal names them consistently.

use std::sync::atomic::{AtomicUsize, Ordering};

use sima_pipeline::{LifecycleEvent, RunState, RunStatus};

/// How many hex characters of an id a progress line shows.
const SHORT: usize = 12;

/// The leading `SHORT` characters of a journaled id.
pub fn short(id: &str) -> &str {
    &id[..id.len().min(SHORT)]
}

/// Renders `event` to the one line it warrants, or `None` for the `Queued`
/// and `Leased` bookkeeping events. `committed`/`tasks` supply the running
/// `committed k/n` count a commit line shows. The single source of the
/// event wording: `sima run` prints these lines to stdout and the tui folds
/// them into its event log.
pub fn describe(event: &LifecycleEvent, committed: usize, tasks: usize) -> Option<String> {
    Some(match event {
        LifecycleEvent::RunStarted { tasks, .. } => format!("started: {tasks} tasks"),
        LifecycleEvent::Committed { task, .. } => {
            format!("committed {committed}/{tasks}  {}", short(task))
        }
        LifecycleEvent::Retried { task, next_attempt } => {
            format!("retrying {} (attempt {next_attempt})", short(task))
        }
        LifecycleEvent::Rejected { task, reason, .. } => {
            format!("rejected {}: {reason}", short(task))
        }
        LifecycleEvent::Failed {
            task,
            attempt,
            reason,
            ..
        } => format!("failed {} (attempt {attempt}): {reason}", short(task)),
        LifecycleEvent::Faulted { task, error, .. } => format!("fault {}: {error}", short(task)),
        LifecycleEvent::LeaseExpired {
            task, elapsed_ms, ..
        } => format!("lease expired {} ({elapsed_ms} ms)", short(task)),
        LifecycleEvent::CheckpointDegraded { task, error } => {
            format!("checkpoint degraded {}: {error}", short(task))
        }
        LifecycleEvent::RunFinalized { committed, .. } => {
            format!("finalized: {committed} tasks committed")
        }
        LifecycleEvent::RunFailed { task, reason, .. } => {
            format!("run failed on {}: {reason}", short(task))
        }
        LifecycleEvent::RunInterrupted { .. } => {
            "interrupted: store resumable, re-run to continue".to_string()
        }
        LifecycleEvent::Queued { .. } | LifecycleEvent::Leased { .. } => return None,
    })
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

    /// Prints the line `event` warrants, if any, keeping the running commit
    /// count for the `committed k/n` line. `Queued` and `Leased` yield no
    /// line and stay silent.
    pub fn event(&self, event: &LifecycleEvent) {
        if let LifecycleEvent::RunStarted { tasks, .. } = event {
            self.tasks.store(*tasks, Ordering::Relaxed);
        }
        // A commit advances the running count; every other line reads it
        // without moving it.
        let committed = match event {
            LifecycleEvent::Committed { .. } => self.committed.fetch_add(1, Ordering::Relaxed) + 1,
            _ => self.committed.load(Ordering::Relaxed),
        };
        if let Some(line) = describe(event, committed, self.tasks.load(Ordering::Relaxed)) {
            println!("{line}");
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
        "run                  {}\n\
         state                {state}\n\
         tasks                {}\n\
         committed            {}\n\
         retried              {}\n\
         rejected             {}\n\
         faulted              {}\n\
         lease expired        {}\n\
         checkpoint degraded  {}",
        status.run,
        status.tasks,
        status.committed,
        status.retried,
        status.rejected,
        status.faulted,
        status.lease_expired,
        status.checkpoint_degraded,
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
    fn a_degraded_checkpoint_renders_a_line() {
        let event = LifecycleEvent::CheckpointDegraded {
            task: "ab".repeat(32),
            error: "checkpoint dir is unwritable".to_string(),
        };
        let line = describe(&event, 0, 0).expect("a degraded checkpoint warrants a line");
        assert!(line.contains("checkpoint degraded"), "{line}");
        assert!(line.contains("unwritable"), "{line}");
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
            checkpoint_degraded: 0,
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
            "checkpoint degraded",
        ] {
            assert!(block.contains(field), "missing {field}: {block}");
        }
        assert!(block.contains("in progress"));
    }
}
