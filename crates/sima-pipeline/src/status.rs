//! [`RunStatus`]: a run's observable state, built from its lifecycle events.

use std::collections::BTreeMap;

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_scheduler::LifecycleEvent;
use sima_store::Store;

use crate::config::LoadedConfig;

/// A run's observable state, built from its lifecycle events. `sima status`
/// and the tui update the same `RunStatus` type through the same
/// [`apply`](RunStatus::apply) method, so both derive identical state from the
/// same events: `status` replays a stored journal, and the tui applies each
/// observer event as it arrives while a run proceeds.
#[derive(Debug, PartialEq, Eq)]
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
    /// Degraded checkpoint saves or loads across the whole journal.
    pub checkpoint_degraded: usize,
    /// The run's current state.
    pub state: RunState,
    /// Workers currently holding a lease: worker id → the task it leased and
    /// the attempt in flight. `Leased` sets an entry, the task's terminal or
    /// failed event clears it, and a segment's `RunStarted` empties the map.
    /// The tui reads this for its worker panel; `status` leaves it unprinted.
    pub occupancy: BTreeMap<u64, Occupancy>,
}

/// A worker's current lease, taken from the journal: the leased task's id
/// as journaled, and the attempt the worker is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Occupancy {
    /// The leased task's id — the journal's lowercase-hex string.
    pub task: String,
    /// The attempt the worker is running.
    pub attempt: u32,
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

impl RunStatus {
    /// A zeroed status for `run`: no events applied, `InProgress`, no leases
    /// held. Applying a journal or a live event stream through
    /// [`apply`](RunStatus::apply) drives it to the run's observable state.
    pub fn new(run: RunId) -> RunStatus {
        RunStatus {
            run,
            tasks: 0,
            committed: 0,
            retried: 0,
            rejected: 0,
            faulted: 0,
            lease_expired: 0,
            checkpoint_degraded: 0,
            state: RunState::InProgress,
            occupancy: BTreeMap::new(),
        }
    }

    /// Applies one lifecycle event to the status — the total function over
    /// the event vocabulary that `status`'s journal replay and the tui's
    /// live stream both use. Counters sum across resume segments; the
    /// run-level events overwrite the state so the last one decides; and
    /// worker occupancy tracks the in-flight leases.
    pub fn apply(&mut self, event: &LifecycleEvent) {
        match event {
            LifecycleEvent::RunStarted { tasks, .. } => {
                // A fresh segment: restate the count and drop any leases the
                // previous segment held.
                self.tasks = *tasks;
                self.state = RunState::InProgress;
                self.occupancy.clear();
            }
            LifecycleEvent::Leased {
                task,
                worker,
                attempt,
            } => {
                self.occupancy.insert(
                    *worker,
                    Occupancy {
                        task: task.clone(),
                        attempt: *attempt,
                    },
                );
            }
            LifecycleEvent::Committed { task, .. } => {
                self.committed += 1;
                self.free_task(task);
            }
            LifecycleEvent::Failed { task, .. } => {
                // The attempt ended and its worker is free; the retry (or a
                // definitive outcome) follows as its own event.
                self.free_task(task);
            }
            LifecycleEvent::Retried { .. } => self.retried += 1,
            LifecycleEvent::Rejected { task, .. } => {
                self.rejected += 1;
                self.free_task(task);
            }
            LifecycleEvent::Faulted { task, .. } => {
                self.faulted += 1;
                self.free_task(task);
            }
            LifecycleEvent::LeaseExpired { .. } => {
                // Detection only, no preemption: the lease stands, so
                // occupancy is untouched.
                self.lease_expired += 1;
            }
            LifecycleEvent::CheckpointDegraded { .. } => {
                // An optimization failed; the attempt's result is unaffected,
                // so only the counter moves.
                self.checkpoint_degraded += 1;
            }
            LifecycleEvent::RunFinalized { .. } => {
                self.state = RunState::Finalized;
                self.occupancy.clear();
            }
            LifecycleEvent::RunFailed { task, reason, .. } => {
                self.state = RunState::Failed {
                    task: task.clone(),
                    reason: reason.clone(),
                };
                self.occupancy.clear();
            }
            LifecycleEvent::RunInterrupted { .. } => {
                self.state = RunState::Interrupted;
                self.occupancy.clear();
            }
            LifecycleEvent::Queued { .. } => {}
        }
    }

    /// Frees whichever worker holds `task`. A task is leased by one worker at
    /// a time, so its terminal or failed event releases that single worker.
    fn free_task(&mut self, task: &str) {
        let held = self
            .occupancy
            .iter()
            .find(|(_, occ)| occ.task == task)
            .map(|(&worker, _)| worker);
        if let Some(worker) = held {
            self.occupancy.remove(&worker);
        }
    }
}

/// Computes the status of the run a loaded config describes, from its
/// journal alone — the read-only counterpart of
/// [`orchestrate`](crate::orchestrate). A store root that does not exist
/// is [`Error::Validation`] before anything touches the disk (opening a
/// store creates its skeleton, and a query must not); a run never started
/// in the store is [`Error::Validation`]; a journal line that fails to
/// parse is [`Error::Corruption`].
pub fn status(config: &LoadedConfig) -> Result<RunStatus> {
    if !config.store.is_dir() {
        return Err(Error::Validation(format!(
            "store {} does not exist: no run was ever driven there",
            config.store.display()
        )));
    }
    let store = Store::open(&config.store)?;
    from_journal(&store, &config.run.id())
}

/// Reads `run`'s journal in `store` and builds a [`RunStatus`] by replaying
/// every line through [`RunStatus::apply`] — the same method the tui runs
/// over the live event stream, so a resumed run and a first run derive their
/// state one way.
fn from_journal(store: &Store, run: &RunId) -> Result<RunStatus> {
    let lines = store.journal(run)?;
    if lines.is_empty() {
        return Err(Error::Validation(format!(
            "run {run} was never started in this store"
        )));
    }
    let mut report = RunStatus::new(*run);
    for line in &lines {
        let event = LifecycleEvent::from_line(line)
            .map_err(|e| Error::Corruption(format!("journal of run {run}: {e}")))?;
        report.apply(&event);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::hash_bytes;
    use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig};

    fn run_id() -> RunId {
        RunId::from_hash(hash_bytes(b"status test run"))
    }

    fn started(tasks: usize) -> LifecycleEvent {
        LifecycleEvent::RunStarted {
            run: "00".repeat(32),
            tasks,
        }
    }

    fn leased(task: &str, worker: u64, attempt: u32) -> LifecycleEvent {
        LifecycleEvent::Leased {
            task: task.to_string(),
            worker,
            attempt,
        }
    }

    fn committed(task: &str) -> LifecycleEvent {
        LifecycleEvent::Committed {
            task: task.to_string(),
            record: "11".repeat(32),
            stats_hex: String::new(),
        }
    }

    fn failed(task: &str, attempt: u32) -> LifecycleEvent {
        LifecycleEvent::Failed {
            task: task.to_string(),
            attempt,
            reason: "flaky".to_string(),
            stats_hex: String::new(),
        }
    }

    fn retried(task: &str, next_attempt: u32) -> LifecycleEvent {
        LifecycleEvent::Retried {
            task: task.to_string(),
            next_attempt,
        }
    }

    fn rejected(task: &str, attempt: u32) -> LifecycleEvent {
        LifecycleEvent::Rejected {
            task: task.to_string(),
            attempt,
            reason: "rejected".to_string(),
            stats_hex: String::new(),
        }
    }

    fn faulted(task: &str, attempt: u32) -> LifecycleEvent {
        LifecycleEvent::Faulted {
            task: task.to_string(),
            attempt,
            error: "io error".to_string(),
        }
    }

    fn occupancy(task: &str, attempt: u32) -> Occupancy {
        Occupancy {
            task: task.to_string(),
            attempt,
        }
    }

    #[test]
    fn leased_fills_occupancy_and_committed_clears_it_and_counts() {
        let mut status = RunStatus::new(run_id());
        status.apply(&started(2));
        status.apply(&leased("aa", 0, 0));
        assert_eq!(status.occupancy.get(&0), Some(&occupancy("aa", 0)));
        status.apply(&committed("aa"));
        assert_eq!(status.committed, 1);
        assert!(status.occupancy.is_empty(), "a commit frees the worker");
    }

    #[test]
    fn failed_frees_the_worker_and_retried_counts() {
        let mut status = RunStatus::new(run_id());
        status.apply(&started(1));
        status.apply(&leased("aa", 0, 0));
        status.apply(&failed("aa", 0));
        assert!(
            status.occupancy.is_empty(),
            "a failed attempt frees the worker"
        );
        status.apply(&retried("aa", 1));
        assert_eq!(status.retried, 1);
    }

    #[test]
    fn rejected_and_faulted_clear_occupancy_and_count() {
        let mut status = RunStatus::new(run_id());
        status.apply(&started(2));
        status.apply(&leased("aa", 0, 0));
        status.apply(&rejected("aa", 0));
        assert_eq!(status.rejected, 1);
        assert!(status.occupancy.is_empty(), "a rejection frees the worker");
        status.apply(&leased("bb", 1, 0));
        status.apply(&faulted("bb", 0));
        assert_eq!(status.faulted, 1);
        assert!(status.occupancy.is_empty(), "a fault frees the worker");
    }

    #[test]
    fn lease_expiry_counts_and_keeps_the_lease() {
        let mut status = RunStatus::new(run_id());
        status.apply(&started(1));
        status.apply(&leased("aa", 0, 0));
        status.apply(&LifecycleEvent::LeaseExpired {
            task: "aa".to_string(),
            worker: 0,
            elapsed_ms: 5,
        });
        assert_eq!(status.lease_expired, 1);
        assert!(
            status.occupancy.contains_key(&0),
            "expiry is detection only, so the lease stands"
        );
    }

    #[test]
    fn a_resume_segment_restates_tasks_and_resets_occupancy() {
        let mut status = RunStatus::new(run_id());
        status.apply(&started(2));
        status.apply(&leased("aa", 0, 0));
        status.apply(&started(5));
        assert_eq!(status.tasks, 5);
        assert_eq!(status.state, RunState::InProgress);
        assert!(status.occupancy.is_empty(), "a new segment holds no leases");
    }

    #[test]
    fn run_level_events_set_the_terminal_state() {
        let mut finalized = RunStatus::new(run_id());
        finalized.apply(&LifecycleEvent::RunFinalized {
            run: "00".repeat(32),
            committed: 3,
        });
        assert_eq!(finalized.state, RunState::Finalized);

        let mut failed = RunStatus::new(run_id());
        failed.apply(&LifecycleEvent::RunFailed {
            run: "00".repeat(32),
            task: "aa".to_string(),
            reason: "rejected".to_string(),
        });
        assert_eq!(
            failed.state,
            RunState::Failed {
                task: "aa".to_string(),
                reason: "rejected".to_string(),
            }
        );

        let mut interrupted = RunStatus::new(run_id());
        interrupted.apply(&LifecycleEvent::RunInterrupted {
            run: "00".repeat(32),
        });
        assert_eq!(interrupted.state, RunState::Interrupted);
    }

    /// A minimal run config whose id addresses the parity test's run.
    fn parity_test_config() -> Result<RunConfig> {
        Ok(RunConfig {
            root_seed: 1,
            segments: None,
            format: FormatId::new("stub.v1")?,
            generator: GeneratorConfig {
                id: GeneratorId::new("stub.v1")?,
                params: Vec::new(),
            },
            params: Params { bytes: Vec::new() },
        })
    }

    #[test]
    fn from_journal_equals_a_replay_through_apply() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let config = parity_test_config()?;
        let run = config.id();
        store.create_run(&config)?;

        // A journal exercising counters, occupancy churn, a retry, and the
        // finalize that decides the state.
        let events = vec![
            started(2),
            leased("aa", 0, 0),
            leased("bb", 1, 0),
            committed("aa"),
            failed("bb", 0),
            retried("bb", 1),
            leased("bb", 1, 1),
            committed("bb"),
            LifecycleEvent::RunFinalized {
                run: run.to_string(),
                committed: 2,
            },
        ];
        let mut writer = store.journal_writer(&run)?;
        for event in &events {
            writer.append(&event.to_line()?)?;
        }

        let mut replay = RunStatus::new(run);
        for event in &events {
            replay.apply(event);
        }
        assert_eq!(
            from_journal(&store, &run)?,
            replay,
            "status's journal read must equal a direct replay through apply"
        );
        assert_eq!(replay.state, RunState::Finalized);
        assert_eq!(replay.committed, 2);
        Ok(())
    }
}
