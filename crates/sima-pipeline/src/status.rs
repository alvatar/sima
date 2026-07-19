//! [`RunStatus`]: a run's observable state, built from its lifecycle events.

use std::collections::BTreeMap;

use sima_core::{Error, Result};
use sima_model::RunId;
use sima_scheduler::{Event, Record};
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
    /// Committed tasks per device, keyed by the composition label: the device
    /// name for a local pool, or `device @ host` when a remote pool ran it, so
    /// one device name on two machines counts separately. The run's
    /// composition: which hardware, on which machine, actually did the work,
    /// joined from each commit and the device its worker was bound to. Empty
    /// for a journal carrying no `WorkerBound` events — a run whose domain uses
    /// no device names none. An old journal without host fields renders the
    /// plain device name, exactly as before.
    pub devices: BTreeMap<String, usize>,
    /// Chains whose device class went absent and moved, across the whole
    /// journal.
    pub rebound_chains: usize,
    /// The run's current state.
    pub state: RunState,
    /// Workers currently holding a lease: worker id → the task it leased and
    /// the attempt in flight. `Leased` sets an entry, the task's terminal or
    /// failed event clears it, and a segment's `RunStarted` empties the map.
    /// The tui reads this for its worker panel; `status` leaves it unprinted.
    pub occupancy: BTreeMap<u64, Occupancy>,
    /// The device and host each worker reported at its last spawn: worker id →
    /// `(device, host)`. The key to reading a commit as work done on a device
    /// of a machine; the rendered composition is
    /// [`devices`](RunStatus::devices).
    worker_devices: BTreeMap<u64, (String, String)>,
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
            devices: BTreeMap::new(),
            rebound_chains: 0,
            state: RunState::InProgress,
            occupancy: BTreeMap::new(),
            worker_devices: BTreeMap::new(),
        }
    }

    /// Applies one journal record to the status — the total function over
    /// the event vocabulary that `status`'s journal replay and the tui's
    /// live stream both use. Counters sum across resume segments; the
    /// run-level events overwrite the state so the last one decides; and
    /// worker occupancy tracks the in-flight leases. The record's timestamp
    /// carries no state: only the event acts.
    pub fn apply(&mut self, record: &Record) {
        match &record.event {
            Event::RunStarted { tasks, .. } => {
                // A fresh segment: restate the count and drop any leases the
                // previous segment held.
                self.tasks = *tasks;
                self.state = RunState::InProgress;
                self.occupancy.clear();
            }
            Event::Leased {
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
            Event::Committed { task, .. } => {
                self.committed += 1;
                // The commit's device is the one its worker reported: the
                // journal says who leased the task, and the worker said where
                // it computes.
                if let Some(worker) = self.free_task(task)
                    && let Some((device, host)) = self.worker_devices.get(&worker)
                    && !device.is_empty()
                {
                    *self
                        .devices
                        .entry(composition_label(device, host))
                        .or_default() += 1;
                }
            }
            Event::Failed { task, .. } => {
                // The attempt ended and its worker is free; the retry (or a
                // definitive outcome) follows as its own event.
                self.free_task(task);
            }
            Event::Retried { .. } => self.retried += 1,
            Event::Rejected { task, .. } => {
                self.rejected += 1;
                self.free_task(task);
            }
            Event::Faulted { task, .. } => {
                self.faulted += 1;
                self.free_task(task);
            }
            Event::LeaseExpired { .. } => {
                // The preemption settles through its own Failed event, which
                // frees the task; here only the counter moves.
                self.lease_expired += 1;
            }
            Event::CheckpointDegraded { .. } => {
                // An optimization failed; the attempt's result is unaffected,
                // so only the counter moves.
                self.checkpoint_degraded += 1;
            }
            Event::RunFinalized { .. } => {
                self.state = RunState::Finalized;
                self.occupancy.clear();
            }
            Event::RunFailed { task, reason, .. } => {
                self.state = RunState::Failed {
                    task: task.clone(),
                    reason: reason.clone(),
                };
                self.occupancy.clear();
            }
            Event::RunInterrupted { .. } => {
                self.state = RunState::Interrupted;
                self.occupancy.clear();
            }
            Event::WorkerBound {
                worker,
                device,
                host,
                ..
            } => {
                // A respawned worker restates its device; the last one is what
                // its later commits ran on. The host travels with it, so a
                // commit is attributed to the machine that produced it.
                self.worker_devices
                    .insert(*worker, (device.clone(), host.clone()));
            }
            Event::ChainRebound { .. } => self.rebound_chains += 1,
            Event::Queued { .. } => {}
            // Diagnostics are observational text: no counter, no state
            // change.
            Event::Diagnostic { .. } => {}
        }
    }

    /// Frees whichever worker holds `task` and reports it. A task is leased by
    /// one worker at a time, so its terminal or failed event releases that
    /// single worker.
    fn free_task(&mut self, task: &str) -> Option<u64> {
        let held = self
            .occupancy
            .iter()
            .find(|(_, occ)| occ.task == task)
            .map(|(&worker, _)| worker)?;
        self.occupancy.remove(&held);
        Some(held)
    }
}

/// The composition key for a commit on `device` at `host`: the plain device
/// name for a local pool (empty host), or `device @ host` for a remote one, so
/// one device name on several machines counts separately. An old journal
/// without a host renders the plain form.
fn composition_label(device: &str, host: &str) -> String {
    if host.is_empty() {
        device.to_string()
    } else {
        format!("{device} @ {host}")
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
        let record = Record::from_line(line)
            .map_err(|e| Error::Corruption(format!("journal of run {run}: {e}")))?;
        report.apply(&record);
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

    /// Wraps an event as the unstamped record the tests apply.
    fn rec(event: Event) -> Record {
        Record { ts_ms: None, event }
    }

    fn started(tasks: usize) -> Record {
        rec(Event::RunStarted {
            run: "00".repeat(32),
            tasks,
            committed: 0,
        })
    }

    fn leased(task: &str, worker: u64, attempt: u32) -> Record {
        rec(Event::Leased {
            task: task.to_string(),
            worker,
            attempt,
        })
    }

    fn committed(task: &str) -> Record {
        rec(Event::Committed {
            task: task.to_string(),
            record: "11".repeat(32),
            stats_hex: String::new(),
        })
    }

    fn failed(task: &str, attempt: u32) -> Record {
        rec(Event::Failed {
            task: task.to_string(),
            attempt,
            reason: "flaky".to_string(),
            stats_hex: String::new(),
        })
    }

    fn worker_bound(worker: u64, device: &str) -> Record {
        worker_bound_on(worker, device, "")
    }

    fn worker_bound_on(worker: u64, device: &str, host: &str) -> Record {
        rec(Event::WorkerBound {
            worker,
            device: device.to_string(),
            driver: String::new(),
            host: host.to_string(),
        })
    }

    /// The status a fresh run reaches by applying `records` in order.
    fn folded(records: Vec<Record>) -> RunStatus {
        let mut status = RunStatus::new(run_id());
        for record in &records {
            status.apply(record);
        }
        status
    }

    fn retried(task: &str, next_attempt: u32) -> Record {
        rec(Event::Retried {
            task: task.to_string(),
            next_attempt,
        })
    }

    fn rejected(task: &str, attempt: u32) -> Record {
        rec(Event::Rejected {
            task: task.to_string(),
            attempt,
            reason: "rejected".to_string(),
            stats_hex: String::new(),
        })
    }

    fn faulted(task: &str, attempt: u32) -> Record {
        rec(Event::Faulted {
            task: task.to_string(),
            attempt,
            error: "io error".to_string(),
        })
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
    fn checkpoint_degradation_counts_and_keeps_the_lease() {
        let mut status = RunStatus::new(run_id());
        status.apply(&started(1));
        status.apply(&leased("aa", 0, 0));
        status.apply(&rec(Event::CheckpointDegraded {
            task: "aa".to_string(),
            error: "checkpoint dir is unwritable".to_string(),
        }));
        assert_eq!(status.checkpoint_degraded, 1);
        // The attempt continues: the worker still holds its lease.
        assert_eq!(status.occupancy.get(&0), Some(&occupancy("aa", 0)));
        assert!(matches!(status.state, RunState::InProgress));
    }

    #[test]
    fn lease_expiry_counts_and_keeps_the_lease() {
        let mut status = RunStatus::new(run_id());
        status.apply(&started(1));
        status.apply(&leased("aa", 0, 0));
        status.apply(&rec(Event::LeaseExpired {
            task: "aa".to_string(),
            worker: 0,
            elapsed_ms: 5,
        }));
        assert_eq!(status.lease_expired, 1);
        assert!(
            status.occupancy.contains_key(&0),
            "the preemption settles through its own failed event"
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
        finalized.apply(&rec(Event::RunFinalized {
            run: "00".repeat(32),
            committed: 3,
        }));
        assert_eq!(finalized.state, RunState::Finalized);

        let mut failed = RunStatus::new(run_id());
        failed.apply(&rec(Event::RunFailed {
            run: "00".repeat(32),
            task: "aa".to_string(),
            reason: "rejected".to_string(),
        }));
        assert_eq!(
            failed.state,
            RunState::Failed {
                task: "aa".to_string(),
                reason: "rejected".to_string(),
            }
        );

        let mut interrupted = RunStatus::new(run_id());
        interrupted.apply(&rec(Event::RunInterrupted {
            run: "00".repeat(32),
        }));
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
        let records = vec![
            started(2),
            leased("aa", 0, 0),
            leased("bb", 1, 0),
            committed("aa"),
            failed("bb", 0),
            retried("bb", 1),
            leased("bb", 1, 1),
            committed("bb"),
            rec(Event::RunFinalized {
                run: run.to_string(),
                committed: 2,
            }),
        ];
        let mut writer = store.journal_writer(&run)?;
        for record in &records {
            writer.append(&record.to_line()?)?;
        }

        let mut replay = RunStatus::new(run);
        for record in &records {
            replay.apply(record);
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

    #[test]
    fn each_commit_counts_against_the_device_its_worker_reported() {
        // The join the composition rests on: the journal says who leased the
        // task, and the worker said where it computes.
        let status = folded(vec![
            started(3),
            worker_bound(0, "NVIDIA RTX PRO 2000"),
            worker_bound(1, "Intel Arc 140T"),
            leased("aa", 0, 0),
            leased("bb", 1, 0),
            committed("aa"),
            committed("bb"),
            leased("cc", 0, 0),
            committed("cc"),
        ]);
        assert_eq!(
            status.devices,
            [
                ("NVIDIA RTX PRO 2000".to_string(), 2),
                ("Intel Arc 140T".to_string(), 1),
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(status.committed, 3);
    }

    #[test]
    fn a_respawned_workers_commits_count_against_its_current_device() {
        // A worker's child died and its replacement reported a device again:
        // the later commits ran on what the later child named.
        let status = folded(vec![
            started(2),
            worker_bound(0, "Intel Arc 140T"),
            leased("aa", 0, 0),
            committed("aa"),
            worker_bound(0, "NVIDIA RTX PRO 2000"),
            leased("bb", 0, 0),
            committed("bb"),
        ]);
        assert_eq!(
            status.devices,
            [
                ("Intel Arc 140T".to_string(), 1),
                ("NVIDIA RTX PRO 2000".to_string(), 1),
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn one_device_name_on_two_hosts_composes_separately() {
        // The same device name on a local pool and a remote one: each machine's
        // commits count under their own label, keyed by (device, host).
        let status = folded(vec![
            started(3),
            worker_bound(0, "NVIDIA RTX PRO 2000"),
            worker_bound_on(1, "NVIDIA RTX PRO 2000", "gpubox"),
            leased("aa", 0, 0),
            leased("bb", 1, 0),
            committed("aa"),
            committed("bb"),
            leased("cc", 0, 0),
            committed("cc"),
        ]);
        assert_eq!(
            status.devices,
            [
                ("NVIDIA RTX PRO 2000".to_string(), 2),
                ("NVIDIA RTX PRO 2000 @ gpubox".to_string(), 1),
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn a_deviceless_domain_contributes_no_composition() {
        // The stub reports an empty device: there is nothing to attribute, and
        // nothing is invented.
        let status = folded(vec![
            started(1),
            worker_bound(0, ""),
            leased("aa", 0, 0),
            committed("aa"),
        ]);
        assert!(status.devices.is_empty());
        assert_eq!(status.committed, 1);
    }

    #[test]
    fn a_journal_naming_no_device_yields_no_composition() {
        // A journal carrying no WorkerBound events: the commits count, and the
        // composition stays empty rather than guessing.
        let status = folded(vec![started(1), leased("aa", 0, 0), committed("aa")]);
        assert!(status.devices.is_empty());
        assert_eq!(status.committed, 1);
        assert_eq!(status.rebound_chains, 0);
    }

    #[test]
    fn a_diagnostic_changes_nothing() {
        let base = folded(vec![started(2), leased("aa", 0, 0)]);
        let with_diagnostic = folded(vec![
            started(2),
            leased("aa", 0, 0),
            rec(Event::Diagnostic {
                level: sima_scheduler::Level::Error,
                source: "panic".to_string(),
                message: "worker panicked".to_string(),
                worker: Some(0),
                host: None,
                task: Some("aa".to_string()),
            }),
        ]);
        assert_eq!(with_diagnostic, base);
    }

    /// Journal lines copied from a real stub run, in the format written
    /// before `ts_ms` existed: what every journal already on disk holds.
    /// One task retried once; all three committed; the run finalized.
    const OLD_FORMAT_LINES: &[&str] = &[
        r#"{"event":"run_started","run":"df27656c67e534f3d6de64173da73efae9e41809734a5c0b647fffa452da920b","tasks":3,"committed":0}"#,
        r#"{"event":"queued","task":"c543cde6cbedd1edb2d3b323fd31b269682e8c75a206eb0ff2557bcae7f31ea8"}"#,
        r#"{"event":"queued","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a"}"#,
        r#"{"event":"queued","task":"b10a30f53cf23913eb37f79c71851587719df963e803f5967765070f3981d625"}"#,
        r#"{"event":"worker_bound","worker":0,"device":"","driver":"","host":""}"#,
        r#"{"event":"leased","task":"c543cde6cbedd1edb2d3b323fd31b269682e8c75a206eb0ff2557bcae7f31ea8","worker":0,"attempt":0}"#,
        r#"{"event":"worker_bound","worker":1,"device":"","driver":"","host":""}"#,
        r#"{"event":"leased","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","worker":1,"attempt":0}"#,
        r#"{"event":"failed","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","attempt":0,"reason":"programmed failure: attempt 0 of 1","stats_hex":"00000000"}"#,
        r#"{"event":"retried","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","next_attempt":1}"#,
        r#"{"event":"leased","task":"b10a30f53cf23913eb37f79c71851587719df963e803f5967765070f3981d625","worker":1,"attempt":0}"#,
        r#"{"event":"committed","task":"c543cde6cbedd1edb2d3b323fd31b269682e8c75a206eb0ff2557bcae7f31ea8","record":"62e29c69cbeb106a03499e64158fa6a83115eb0aacec5d69eb5617a4468956a7","stats_hex":"00000000"}"#,
        r#"{"event":"leased","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","worker":0,"attempt":1}"#,
        r#"{"event":"committed","task":"b10a30f53cf23913eb37f79c71851587719df963e803f5967765070f3981d625","record":"15a083e519d05e2dab09bd9a4e347b664bd9d8f0e0396ed94c98a1cd32acb9ac","stats_hex":"00000000"}"#,
        r#"{"event":"committed","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","record":"5087167e14e7f401b5724edeb5a7368b98cf2c972eca980bcf884857f9a55471","stats_hex":"01000000"}"#,
        r#"{"event":"run_finalized","run":"df27656c67e534f3d6de64173da73efae9e41809734a5c0b647fffa452da920b","committed":3}"#,
    ];

    /// Writes raw journal lines for the parity-test run and returns the
    /// store, keeping the temp dir alive for the caller.
    fn store_with_raw_journal(lines: &[&str]) -> Result<(tempfile::TempDir, Store, RunId)> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path())?;
        let config = parity_test_config()?;
        let run = config.id();
        store.create_run(&config)?;
        let mut writer = store.journal_writer(&run)?;
        for line in lines {
            writer.append(line)?;
        }
        Ok((dir, store, run))
    }

    #[test]
    fn a_pre_existing_format_journal_replays_to_the_same_status() -> Result<()> {
        let (_dir, store, run) = store_with_raw_journal(OLD_FORMAT_LINES)?;
        let status = from_journal(&store, &run)?;
        // The status the run reached when it wrote this journal.
        assert_eq!(status.tasks, 3);
        assert_eq!(status.committed, 3);
        assert_eq!(status.retried, 1);
        assert_eq!(status.rejected, 0);
        assert_eq!(status.faulted, 0);
        assert_eq!(status.state, RunState::Finalized);
        assert!(status.occupancy.is_empty());
        // The stub names no device, so nothing is attributed.
        assert!(status.devices.is_empty());
        Ok(())
    }

    #[test]
    fn a_mixed_format_journal_replays_correctly() -> Result<()> {
        // The shape a resumed run produces: an old-format session that a
        // crash ended before finalize, then a new session whose lines carry
        // `ts_ms`, finding every commit already answered and finalizing.
        let old_session = &OLD_FORMAT_LINES[..OLD_FORMAT_LINES.len() - 1];
        let new_session = [
            r#"{"ts_ms":1700000000000,"event":"run_started","run":"df27656c67e534f3d6de64173da73efae9e41809734a5c0b647fffa452da920b","tasks":3,"committed":3}"#,
            r#"{"ts_ms":1700000000001,"event":"run_finalized","run":"df27656c67e534f3d6de64173da73efae9e41809734a5c0b647fffa452da920b","committed":3}"#,
        ];
        let lines: Vec<&str> = old_session
            .iter()
            .chain(new_session.iter())
            .copied()
            .collect();
        let (_dir, store, run) = store_with_raw_journal(&lines)?;
        let status = from_journal(&store, &run)?;
        assert_eq!(status.tasks, 3);
        assert_eq!(status.committed, 3, "commits sum across both sessions");
        assert_eq!(status.retried, 1);
        assert_eq!(status.state, RunState::Finalized);
        assert!(status.occupancy.is_empty());
        Ok(())
    }

    #[test]
    fn rebound_chains_count_across_the_journal() {
        let status = folded(vec![
            started(2),
            rec(Event::ChainRebound {
                chain: 0,
                from: "10de:2d39".to_string(),
                to: "8086:7d51".to_string(),
            }),
            rec(Event::ChainRebound {
                chain: 1,
                from: "10de:2d39".to_string(),
                to: "8086:7d51".to_string(),
            }),
        ]);
        assert_eq!(status.rebound_chains, 2);
    }
}
