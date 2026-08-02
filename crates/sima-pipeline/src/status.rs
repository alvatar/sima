//! [`RunStatus`]: a run's observable state, built from its lifecycle events.

use std::collections::BTreeMap;

use sima_core::Result;
use sima_model::RunId;
use sima_scheduler::{Event, Record};

use crate::config::LoadedConfig;
use crate::journal;

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
    /// name for a local pool, or `device @ host` when another machine ran it, so
    /// one device name on two machines counts separately. The run's
    /// composition: which hardware, on which machine, actually did the work,
    /// joined from each commit and the device its worker was bound to. Empty
    /// for a journal carrying no `WorkerBound` events — a run whose domain uses
    /// no device names none.
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
            // The program a session ran through and a device's driver change
            // are provenance of the build and the machines, not of the work:
            // they move no counter and free no worker.
            Event::ProgramBound { .. } | Event::DriverChanged { .. } => {}
            // Rental lifecycle is operational — rentals coming and going — and
            // states nothing about task progress or worker occupancy.
            Event::InstanceOnline { .. }
            | Event::InstanceLost { .. }
            | Event::InstanceReplaced { .. }
            | Event::BudgetSpendExhausted { .. }
            | Event::BudgetWallClockExhausted { .. } => {}
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
/// one device name on several machines counts separately.
fn composition_label(device: &str, host: &str) -> String {
    if host.is_empty() {
        device.to_string()
    } else {
        format!("{device} @ {host}")
    }
}

/// Computes the status of the run a loaded config describes, from its
/// journal alone — the read-only counterpart of
/// [`orchestrate`](crate::orchestrate). Every line is replayed through
/// [`RunStatus::apply`], the same method the tui runs over the live event
/// stream, so a resumed run and a first run derive their state one way. The
/// journal is read under the guards every read-only query applies: a missing
/// store root and a run never started there are
/// [`Error::Validation`](sima_core::Error::Validation), and a line that fails
/// to parse is [`Error::Corruption`](sima_core::Error::Corruption).
pub fn status(config: &LoadedConfig) -> Result<RunStatus> {
    Ok(status_records(config.run.id(), &journal::records(config)?))
}

/// The status of the run a loaded config describes, or the zeroed status when
/// there is no such run yet: no store at that root, or a run never driven in
/// it.
///
/// What a display seeded from prior progress wants: absence is the ordinary
/// case on a first run and reads as zeroes, while a corrupt journal or an I/O
/// fault still surfaces. A caller reading absence off an error variant would
/// bucket every other failure on this path with it and open on wrong counts.
pub fn seeded_status(config: &LoadedConfig) -> Result<RunStatus> {
    let run = config.run.id();
    Ok(match journal::journaled(config)? {
        Some(records) => status_records(run, &records),
        None => RunStatus::new(run),
    })
}

/// Folds `records` — a run's lifecycle events in append order — into the
/// status of `run`. The fold half of [`status`], over records from any
/// source: a journal read locally, or a stream from the host that drives the
/// run. It renders nothing through a domain, so it needs no format and cannot
/// fail.
pub fn status_records(run: RunId, records: &[Record]) -> RunStatus {
    let mut status = RunStatus::new(run);
    for record in records {
        status.apply(record);
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::hash_bytes;

    use sima_store::Store;

    use crate::fixtures::{journal_with, stub_config};

    fn run_id() -> RunId {
        RunId::from_hash(hash_bytes(b"status test run"))
    }

    /// Wraps an event as a record the tests apply. The stamp is irrelevant
    /// here, so every record carries the same one.
    fn rec(event: Event) -> Record {
        Record { ts_ms: 0, event }
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
            stats: Vec::new(),
            stats_blob_hex: String::new(),
        })
    }

    fn failed(task: &str, attempt: u32) -> Record {
        rec(Event::Failed {
            task: task.to_string(),
            attempt,
            reason: "flaky".to_string(),
            stats: Vec::new(),
            stats_blob_hex: String::new(),
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
            stats: Vec::new(),
            stats_blob_hex: String::new(),
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
    fn a_store_that_does_not_exist_seeds_a_zeroed_status() -> Result<()> {
        // The ordinary first run: nothing has been driven, so a display seeded
        // from prior progress opens on zeroes rather than failing. A query must
        // not create the store, so the absence is observed rather than probed
        // by opening.
        let dir = tempfile::tempdir().expect("temp dir");
        let config = crate::fixtures::loaded(dir.path().join("no-such-store"))?;
        let seeded = seeded_status(&config)?;
        assert_eq!(seeded.run, config.run.id());
        assert_eq!(seeded.committed, 0);
        Ok(())
    }

    #[test]
    fn a_run_never_driven_in_an_existing_store_seeds_a_zeroed_status() -> Result<()> {
        // The store is there because another run used it; this run has no
        // journal in it, which is the same absence.
        let dir = tempfile::tempdir().expect("temp dir");
        Store::open(dir.path())?;
        let config = crate::fixtures::loaded(dir.path().to_path_buf())?;
        assert_eq!(seeded_status(&config)?.committed, 0);
        Ok(())
    }

    #[test]
    fn a_corrupt_journal_surfaces_rather_than_seeding_zeroes() -> Result<()> {
        // The failure this exists to catch: a journal that cannot be read is a
        // real problem, and seeding zeroes over it would open a display on
        // counts that are simply wrong. Absence is answered as absence and
        // everything else is still an error, which is what reading the error
        // variant instead could not tell apart.
        let (_dir, config) = journal_with(&[])?;
        let store = Store::open(&config.store)?;
        let mut writer = store.journal_writer(&config.run.id())?;
        writer.append("this is not a journal line")?;
        drop(writer);
        assert!(matches!(
            seeded_status(&config),
            Err(sima_core::Error::Corruption(_))
        ));
        Ok(())
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

    #[test]
    fn the_journal_read_equals_a_replay_through_apply() -> Result<()> {
        let run = stub_config()?.id();

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
        let (_dir, config) = journal_with(&records)?;

        let mut replay = RunStatus::new(run);
        for record in &records {
            replay.apply(record);
        }
        assert_eq!(
            status(&config)?,
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

    #[test]
    fn the_record_fold_equals_the_status_read_from_the_journal() -> Result<()> {
        let records = vec![
            started(2),
            leased("aa", 0, 0),
            committed("aa"),
            failed("bb", 0),
        ];
        let (_dir, config) = journal_with(&records)?;
        assert_eq!(
            status_records(config.run.id(), &records),
            status(&config)?,
            "the fold over records is the status the journal path computes"
        );
        Ok(())
    }
}
