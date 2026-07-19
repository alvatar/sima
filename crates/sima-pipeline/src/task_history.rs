//! [`TaskHistory`]: one task's lifecycle, projected from a run's journal.
//!
//! Every per-task view folds the journal once through [`ledger`]: the attempt
//! timeline of a single task, the digest of the tasks that did not commit, and
//! the prefix resolution that turns a short key into the full one. The journal
//! is the only source; no store object is read.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::LoadedConfig;
use crate::journal;
use sima_core::{Error, Result, from_hex};
use sima_domains::{Domain, domain_for};
use sima_scheduler::{Event, Record};

/// One task's lifecycle, folded from the run journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskHistory {
    /// The task's key, as journaled — the lowercase-hex string.
    pub task: String,
    /// When the task first entered the ready queue, for the wait it sat there.
    pub queued_ms: Option<u64>,
    /// One entry per lease, in journal order.
    pub attempts: Vec<Attempt>,
    /// The terminal state the journal's last outcome for this task decides.
    pub outcome: TaskOutcome,
}

/// One leased attempt at a task, from the lease to whatever ended it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    /// The attempt number the lease carried.
    pub attempt: u32,
    /// The worker that held the lease.
    pub worker: u64,
    /// The device that worker reported, empty for a domain that uses none.
    pub device: String,
    /// The machine that worker's pool ran on, empty for a local one.
    pub host: String,
    /// The lease's stamp: when the attempt began, as the collector saw it.
    pub started_ms: u64,
    /// The terminating event's stamp; `None` while the attempt is open.
    pub ended_ms: Option<u64>,
    /// How the attempt ended.
    pub result: AttemptResult,
    /// Whether a lease expiry preempted the attempt before it ended.
    pub lease_expired: bool,
}

/// How one attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptResult {
    /// The attempt's result was committed.
    Committed,
    /// The attempt failed transiently; a retry may follow.
    Failed {
        /// Why it failed.
        reason: String,
    },
    /// The attempt failed definitively.
    Rejected {
        /// Why it was rejected.
        reason: String,
    },
    /// An infrastructure fault hit the attempt.
    Faulted {
        /// What went wrong.
        error: String,
    },
    /// The lease is open: the journal names no event that ended it, as a run
    /// still in flight or one whose orchestrator died leaves.
    InFlight,
}

/// The terminal state a task reached, as its journal outcome states it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    /// The task entered the ready queue and no worker leased it.
    Queued,
    /// A worker holds or held a lease and no outcome closed the task.
    InProgress,
    /// The task's result was committed.
    Committed {
        /// The committed record's id, as journaled.
        record: String,
        /// The task's stats rendered into one line by its domain.
        stats: String,
    },
    /// The task failed definitively.
    Rejected {
        /// The attempt that was rejected.
        attempt: u32,
        /// Why it was rejected.
        reason: String,
    },
    /// An infrastructure fault ended the task.
    Faulted {
        /// The attempt that faulted.
        attempt: u32,
        /// What went wrong.
        error: String,
    },
}

impl TaskHistory {
    /// An empty history for `task`: queued nowhere, no attempts, no outcome.
    fn new(task: &str) -> TaskHistory {
        TaskHistory {
            task: task.to_string(),
            queued_ms: None,
            attempts: Vec::new(),
            outcome: TaskOutcome::Queued,
        }
    }

    /// Whether the journal ended this task on a definitive failure — the
    /// tasks the failure digest names.
    fn failed(&self) -> bool {
        matches!(
            self.outcome,
            TaskOutcome::Rejected { .. } | TaskOutcome::Faulted { .. }
        )
    }

    /// Applies one of this task's records: opens an attempt on a lease, closes
    /// the open one on whatever ended it, and moves the outcome to what the
    /// event states. `workers` supplies the device and host the attempt's
    /// worker reported; `domain` renders a commit's stats.
    fn apply(
        &mut self,
        record: &Record,
        workers: &BTreeMap<u64, (String, String)>,
        domain: &Domain,
    ) -> Result<()> {
        let ts_ms = record.ts_ms;
        match &record.event {
            // The first queueing is when the task started waiting; a resume
            // segment re-queues it without restarting that wait.
            Event::Queued { .. } => {
                self.queued_ms.get_or_insert(ts_ms);
            }
            Event::Leased {
                worker, attempt, ..
            } => {
                let (device, host) = workers.get(worker).cloned().unwrap_or_default();
                self.attempts.push(Attempt {
                    attempt: *attempt,
                    worker: *worker,
                    device,
                    host,
                    started_ms: ts_ms,
                    ended_ms: None,
                    result: AttemptResult::InFlight,
                    lease_expired: false,
                });
                self.outcome = TaskOutcome::InProgress;
            }
            Event::Committed {
                record: object,
                stats_hex,
                ..
            } => {
                self.close(ts_ms, AttemptResult::Committed);
                self.outcome = TaskOutcome::Committed {
                    record: object.clone(),
                    stats: (domain.stats)(&from_hex(stats_hex)?)?,
                };
            }
            Event::Failed { reason, .. } => {
                // A transient failure ends the attempt and leaves the task
                // open: a retry, or the run's end, decides the outcome.
                self.close(
                    ts_ms,
                    AttemptResult::Failed {
                        reason: reason.clone(),
                    },
                );
            }
            Event::Rejected {
                attempt, reason, ..
            } => {
                self.close(
                    ts_ms,
                    AttemptResult::Rejected {
                        reason: reason.clone(),
                    },
                );
                self.outcome = TaskOutcome::Rejected {
                    attempt: *attempt,
                    reason: reason.clone(),
                };
            }
            Event::Faulted { attempt, error, .. } => {
                self.close(
                    ts_ms,
                    AttemptResult::Faulted {
                        error: error.clone(),
                    },
                );
                self.outcome = TaskOutcome::Faulted {
                    attempt: *attempt,
                    error: error.clone(),
                };
            }
            Event::LeaseExpired { .. } => {
                // The expiry preempts the running attempt and settles through
                // the failure that follows, which is what ends the attempt.
                if let Some(open) = self.open_attempt() {
                    open.lease_expired = true;
                }
            }
            // A retry is answered by the lease that follows it, and a degraded
            // checkpoint leaves the attempt's result untouched.
            _ => {}
        }
        Ok(())
    }

    /// Ends the open attempt at `ended_ms` with `result`. A journal may state
    /// an outcome with no lease before it — a resume segment restates prior
    /// commits — and then there is no attempt to close.
    fn close(&mut self, ended_ms: u64, result: AttemptResult) {
        if let Some(open) = self.open_attempt() {
            open.ended_ms = Some(ended_ms);
            open.result = result;
        }
    }

    /// The attempt still holding its lease. Leases for one task are
    /// sequential, so at most the last attempt is open.
    fn open_attempt(&mut self) -> Option<&mut Attempt> {
        self.attempts
            .last_mut()
            .filter(|attempt| attempt.ended_ms.is_none())
    }
}

/// The task a lifecycle event belongs to. A diagnostic's optional task field
/// is observational text rather than a lifecycle statement, and the run-level
/// events frame the run rather than a task, so neither names a history.
fn lifecycle_task(event: &Event) -> Option<&str> {
    match event {
        Event::Queued { task }
        | Event::Leased { task, .. }
        | Event::Committed { task, .. }
        | Event::Failed { task, .. }
        | Event::Retried { task, .. }
        | Event::Rejected { task, .. }
        | Event::Faulted { task, .. }
        | Event::LeaseExpired { task, .. }
        | Event::CheckpointDegraded { task, .. } => Some(task),
        Event::RunStarted { .. }
        | Event::RunFinalized { .. }
        | Event::RunFailed { .. }
        | Event::RunInterrupted { .. }
        | Event::WorkerBound { .. }
        | Event::ChainRebound { .. }
        | Event::Diagnostic { .. } => None,
    }
}

/// The device and host each worker reported at its last spawn. Read ahead of
/// the per-task fold so the join holds however the journal orders the two: a
/// resumed run leases before its workers restate their bindings.
fn worker_bindings(records: &[Record]) -> BTreeMap<u64, (String, String)> {
    let mut bound = BTreeMap::new();
    for record in records {
        if let Event::WorkerBound {
            worker,
            device,
            host,
            ..
        } = &record.event
        {
            bound.insert(*worker, (device.clone(), host.clone()));
        }
    }
    bound
}

/// Folds the journal into every task's history, keyed by task and ordered by
/// key. One pass reads the worker bindings, a second drives each task's
/// history through the events naming it, so the whole ledger costs one walk
/// per pass and one map.
fn ledger(records: &[Record], domain: &Domain) -> Result<BTreeMap<String, TaskHistory>> {
    let workers = worker_bindings(records);
    let mut ledger: BTreeMap<String, TaskHistory> = BTreeMap::new();
    for record in records {
        let Some(task) = lifecycle_task(&record.event) else {
            continue;
        };
        ledger
            .entry(task.to_string())
            .or_insert_with(|| TaskHistory::new(task))
            .apply(record, &workers, domain)?;
    }
    Ok(ledger)
}

/// Resolves a task-key prefix against the keys the journal names in a
/// lifecycle event — the tasks with a history to show. Any non-empty prefix
/// is accepted; ambiguity is the guard, not a minimum length. A prefix
/// matching no task, or more than one, is [`Error::Validation`].
pub(crate) fn resolve_task_key(records: &[Record], prefix: &str) -> Result<String> {
    let matched: BTreeSet<&str> = records
        .iter()
        .filter_map(|record| lifecycle_task(&record.event))
        .filter(|task| task.starts_with(prefix))
        .collect();
    let mut found = matched.into_iter();
    match (found.next(), found.next()) {
        (Some(task), None) => Ok(task.to_string()),
        (None, _) => Err(Error::Validation(format!(
            "no task in this run matches prefix {prefix}"
        ))),
        (Some(_), Some(_)) => Err(Error::Validation(format!(
            "prefix {prefix} is ambiguous: it matches {} tasks",
            found.count() + 2
        ))),
    }
}

/// One task's lifecycle in the run a loaded config describes, addressed by a
/// prefix of its key. The committed outcome carries the stats its domain
/// renders, the same rendering [`report`](crate::report) prints.
pub fn task_history(config: &LoadedConfig, prefix: &str) -> Result<TaskHistory> {
    let records = journal::records(config)?;
    let task = resolve_task_key(&records, prefix)?;
    let domain = domain_for(&config.run.format)?;
    ledger(&records, &domain)?
        .remove(&task)
        .ok_or_else(|| Error::Corruption(format!("task {task} resolved to no history")))
}

/// Every task the run ended on a definitive failure, ordered by key: the
/// tasks a finished run did not commit, and why.
pub fn failures(config: &LoadedConfig) -> Result<Vec<TaskHistory>> {
    let records = journal::records(config)?;
    let domain = domain_for(&config.run.format)?;
    Ok(ledger(&records, &domain)?
        .into_values()
        .filter(TaskHistory::failed)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_store::Store;

    use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig};

    /// The stub domain the synthetic journals render their stats through.
    fn stub_domain() -> Result<Domain> {
        domain_for(&FormatId::new("stub.v1")?)
    }

    /// A minimal stub run config; its id addresses the test's run.
    fn stub_config() -> Result<RunConfig> {
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

    /// A loaded config over `store` for the stub run.
    fn loaded(store: std::path::PathBuf) -> Result<LoadedConfig> {
        Ok(LoadedConfig {
            run: stub_config()?,
            devices: Vec::new(),
            remotes: Vec::new(),
            execution: sima_scheduler::ExecutionConfig::new(
                1,
                1,
                std::time::Duration::MAX,
                std::time::Duration::MAX,
                None,
            )?,
            store,
        })
    }

    /// Writes `records` to the run's journal in a fresh store, returning the
    /// temp dir (kept alive by the caller) and the loaded config over it.
    fn journal_with(records: &[Record]) -> Result<(tempfile::TempDir, LoadedConfig)> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let config = stub_config()?;
        store.create_run(&config)?;
        let mut writer = store.journal_writer(&config.id())?;
        for record in records {
            writer.append(&record.to_line()?)?;
        }
        let config = loaded(dir.path().to_path_buf())?;
        Ok((dir, config))
    }

    /// Wraps an event as a record stamped `ts_ms`.
    fn at(ts_ms: u64, event: Event) -> Record {
        Record { ts_ms, event }
    }

    fn queued(task: &str, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::Queued {
                task: task.to_string(),
            },
        )
    }

    fn leased(task: &str, worker: u64, attempt: u32, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::Leased {
                task: task.to_string(),
                worker,
                attempt,
            },
        )
    }

    fn committed(task: &str, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::Committed {
                task: task.to_string(),
                record: "11".repeat(32),
                stats_hex: "00000000".to_string(),
            },
        )
    }

    fn failed(task: &str, attempt: u32, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::Failed {
                task: task.to_string(),
                attempt,
                reason: "programmed flake".to_string(),
                stats_hex: String::new(),
            },
        )
    }

    fn retried(task: &str, next_attempt: u32, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::Retried {
                task: task.to_string(),
                next_attempt,
            },
        )
    }

    fn rejected(task: &str, attempt: u32, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::Rejected {
                task: task.to_string(),
                attempt,
                reason: "programmed rejection".to_string(),
                stats_hex: String::new(),
            },
        )
    }

    fn faulted(task: &str, attempt: u32, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::Faulted {
                task: task.to_string(),
                attempt,
                error: "executor died".to_string(),
            },
        )
    }

    fn worker_bound(worker: u64, device: &str, host: &str) -> Record {
        at(
            0,
            Event::WorkerBound {
                worker,
                device: device.to_string(),
                driver: String::new(),
                host: host.to_string(),
            },
        )
    }

    /// The ledger `records` fold to, through the stub domain.
    fn folded(records: &[Record]) -> Result<BTreeMap<String, TaskHistory>> {
        ledger(records, &stub_domain()?)
    }

    /// One task's history out of the ledger `records` fold to.
    fn history_of(records: &[Record], task: &str) -> Result<TaskHistory> {
        Ok(folded(records)?
            .remove(task)
            .expect("a history for the task"))
    }

    #[test]
    fn a_committed_task_folds_to_one_attempt_spanning_its_lease() -> Result<()> {
        let history = history_of(
            &[
                queued("aa", 10),
                leased("aa", 0, 0, 20),
                committed("aa", 50),
            ],
            "aa",
        )?;
        assert_eq!(history.queued_ms, Some(10));
        assert_eq!(history.attempts.len(), 1);
        assert_eq!(history.attempts[0].started_ms, 20);
        assert_eq!(history.attempts[0].ended_ms, Some(50));
        assert_eq!(history.attempts[0].result, AttemptResult::Committed);
        assert_eq!(
            history.outcome,
            TaskOutcome::Committed {
                record: "11".repeat(32),
                stats: "attempt 0".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn a_flaky_task_folds_to_a_failed_attempt_then_a_committed_one() -> Result<()> {
        let history = history_of(
            &[
                queued("aa", 0),
                leased("aa", 0, 0, 10),
                failed("aa", 0, 20),
                retried("aa", 1, 21),
                leased("aa", 1, 1, 30),
                committed("aa", 40),
            ],
            "aa",
        )?;
        assert_eq!(history.attempts.len(), 2);
        assert_eq!(
            history.attempts[0].result,
            AttemptResult::Failed {
                reason: "programmed flake".to_string(),
            }
        );
        assert_eq!(history.attempts[0].worker, 0);
        assert_eq!(history.attempts[1].attempt, 1);
        assert_eq!(history.attempts[1].result, AttemptResult::Committed);
        assert!(matches!(history.outcome, TaskOutcome::Committed { .. }));
        Ok(())
    }

    #[test]
    fn a_rejected_task_carries_its_reason_on_the_attempt_and_the_outcome() -> Result<()> {
        let history = history_of(
            &[
                queued("aa", 0),
                leased("aa", 0, 2, 10),
                rejected("aa", 2, 20),
            ],
            "aa",
        )?;
        assert_eq!(
            history.attempts[0].result,
            AttemptResult::Rejected {
                reason: "programmed rejection".to_string(),
            }
        );
        assert_eq!(
            history.outcome,
            TaskOutcome::Rejected {
                attempt: 2,
                reason: "programmed rejection".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn a_faulted_task_carries_its_error_on_the_attempt_and_the_outcome() -> Result<()> {
        let history = history_of(&[leased("aa", 0, 0, 10), faulted("aa", 0, 20)], "aa")?;
        assert_eq!(
            history.attempts[0].result,
            AttemptResult::Faulted {
                error: "executor died".to_string(),
            }
        );
        assert_eq!(
            history.outcome,
            TaskOutcome::Faulted {
                attempt: 0,
                error: "executor died".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn a_lease_with_no_outcome_stays_open_and_in_progress() -> Result<()> {
        let history = history_of(&[queued("aa", 0), leased("aa", 3, 0, 10)], "aa")?;
        assert_eq!(history.outcome, TaskOutcome::InProgress);
        assert_eq!(history.attempts[0].ended_ms, None);
        assert_eq!(history.attempts[0].result, AttemptResult::InFlight);
        Ok(())
    }

    #[test]
    fn a_queued_task_no_worker_leased_has_no_attempts() -> Result<()> {
        let history = history_of(&[queued("aa", 7)], "aa")?;
        assert_eq!(history.outcome, TaskOutcome::Queued);
        assert_eq!(history.queued_ms, Some(7));
        assert!(history.attempts.is_empty());
        Ok(())
    }

    #[test]
    fn each_attempt_joins_the_device_and_host_its_worker_reported() -> Result<()> {
        let ledger = folded(&[
            worker_bound(0, "Intel Arc 140T", ""),
            worker_bound(1, "NVIDIA RTX PRO 2000", "gpubox"),
            leased("aa", 0, 0, 10),
            committed("aa", 20),
            leased("bb", 1, 0, 10),
            committed("bb", 20),
        ])?;
        let local = &ledger["aa"].attempts[0];
        assert_eq!(local.device, "Intel Arc 140T");
        assert_eq!(local.host, "");
        let remote = &ledger["bb"].attempts[0];
        assert_eq!(remote.device, "NVIDIA RTX PRO 2000");
        assert_eq!(remote.host, "gpubox");
        Ok(())
    }

    #[test]
    fn a_worker_binding_after_the_lease_still_joins() -> Result<()> {
        // A resumed run leases before its workers restate their bindings; the
        // binding pass runs first, so the join holds either way.
        let history = history_of(
            &[
                leased("aa", 0, 0, 10),
                worker_bound(0, "Intel Arc 140T", ""),
                committed("aa", 20),
            ],
            "aa",
        )?;
        assert_eq!(history.attempts[0].device, "Intel Arc 140T");
        Ok(())
    }

    #[test]
    fn a_journal_naming_no_binding_leaves_the_attempt_deviceless() -> Result<()> {
        let history = history_of(&[leased("aa", 0, 0, 10), committed("aa", 20)], "aa")?;
        assert_eq!(history.attempts[0].device, "");
        assert_eq!(history.attempts[0].host, "");
        Ok(())
    }

    #[test]
    fn a_lease_expiry_marks_the_attempt_the_following_failure_ends() -> Result<()> {
        let history = history_of(
            &[
                leased("aa", 0, 0, 10),
                at(
                    15,
                    Event::LeaseExpired {
                        task: "aa".to_string(),
                        worker: 0,
                        elapsed_ms: 5,
                    },
                ),
                failed("aa", 0, 20),
            ],
            "aa",
        )?;
        assert!(history.attempts[0].lease_expired);
        assert_eq!(history.attempts[0].ended_ms, Some(20));
        Ok(())
    }

    #[test]
    fn a_diagnostic_names_no_history() -> Result<()> {
        let ledger = folded(&[
            leased("aa", 0, 0, 10),
            committed("aa", 20),
            at(
                30,
                Event::Diagnostic {
                    level: sima_scheduler::Level::Error,
                    source: "panic".to_string(),
                    message: "worker panicked".to_string(),
                    worker: Some(0),
                    host: None,
                    task: Some("bb".to_string()),
                },
            ),
        ])?;
        assert_eq!(ledger.keys().collect::<Vec<_>>(), vec!["aa"]);
        Ok(())
    }

    #[test]
    fn the_last_outcome_wins_when_a_resume_segment_restates_a_commit() -> Result<()> {
        // The task was leased and committed in an earlier segment, and the
        // resumed segment restates the commit with no lease before it.
        let history = history_of(
            &[
                leased("aa", 0, 0, 10),
                committed("aa", 20),
                committed("aa", 90),
            ],
            "aa",
        )?;
        assert_eq!(history.attempts.len(), 1);
        assert!(matches!(history.outcome, TaskOutcome::Committed { .. }));
        Ok(())
    }

    #[test]
    fn a_unique_prefix_resolves_to_the_full_key() -> Result<()> {
        let records = [leased("abcd", 0, 0, 10), leased("bcde", 1, 0, 10)];
        assert_eq!(resolve_task_key(&records, "ab")?, "abcd");
        assert_eq!(resolve_task_key(&records, "abcd")?, "abcd");
        Ok(())
    }

    #[test]
    fn a_prefix_matching_nothing_is_a_validation_error() {
        let records = [leased("abcd", 0, 0, 10)];
        let resolved = resolve_task_key(&records, "ff");
        assert!(
            matches!(resolved, Err(Error::Validation(_))),
            "{resolved:?}"
        );
        assert!(format!("{}", resolved.expect_err("an unmatched prefix")).contains("no task"));
    }

    #[test]
    fn an_ambiguous_prefix_is_a_validation_error_naming_the_match_count() {
        let records = [
            leased("abcd", 0, 0, 10),
            leased("abce", 1, 0, 10),
            leased("abcf", 2, 0, 10),
        ];
        let resolved = resolve_task_key(&records, "abc");
        assert!(
            matches!(resolved, Err(Error::Validation(_))),
            "{resolved:?}"
        );
        let message = format!("{}", resolved.expect_err("an ambiguous prefix"));
        assert!(message.contains("ambiguous"), "{message}");
        assert!(message.contains('3'), "{message}");
    }

    #[test]
    fn a_key_named_only_by_a_diagnostic_does_not_resolve() {
        let records = [at(
            0,
            Event::Diagnostic {
                level: sima_scheduler::Level::Warn,
                source: "transport".to_string(),
                message: "m".to_string(),
                worker: None,
                host: None,
                task: Some("abcd".to_string()),
            },
        )];
        assert!(matches!(
            resolve_task_key(&records, "ab"),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn failures_name_the_rejected_and_faulted_tasks_in_key_order() -> Result<()> {
        let (_dir, config) = journal_with(&[
            leased("cc", 0, 0, 10),
            faulted("cc", 0, 20),
            leased("aa", 1, 0, 10),
            committed("aa", 20),
            leased("bb", 2, 0, 10),
            rejected("bb", 0, 20),
        ])?;
        let failed: Vec<String> = failures(&config)?
            .into_iter()
            .map(|history| history.task)
            .collect();
        assert_eq!(failed, vec!["bb".to_string(), "cc".to_string()]);
        Ok(())
    }

    #[test]
    fn a_run_that_committed_everything_has_no_failures() -> Result<()> {
        let (_dir, config) = journal_with(&[leased("aa", 0, 0, 10), committed("aa", 20)])?;
        assert!(failures(&config)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_history_query_resolves_the_prefix_against_the_journal() -> Result<()> {
        let (_dir, config) = journal_with(&[
            queued("abcd", 0),
            leased("abcd", 0, 0, 10),
            committed("abcd", 20),
        ])?;
        let history = task_history(&config, "ab")?;
        assert_eq!(history.task, "abcd");
        assert!(matches!(history.outcome, TaskOutcome::Committed { .. }));
        Ok(())
    }

    #[test]
    fn a_query_over_a_missing_store_is_a_validation_error() -> Result<()> {
        let config = loaded(std::path::PathBuf::from("/no/such/store/here"))?;
        assert!(matches!(
            task_history(&config, "aa"),
            Err(Error::Validation(_))
        ));
        assert!(matches!(failures(&config), Err(Error::Validation(_))));
        Ok(())
    }

    #[test]
    fn a_query_over_a_run_never_started_is_a_validation_error() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        Store::open(dir.path())?;
        let config = loaded(dir.path().to_path_buf())?;
        assert!(matches!(
            task_history(&config, "aa"),
            Err(Error::Validation(_))
        ));
        assert!(matches!(failures(&config), Err(Error::Validation(_))));
        Ok(())
    }
}
