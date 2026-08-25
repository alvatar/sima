//! [`RunTimeline`]: a run's execution metrics and temporal shape, merged from
//! its journal.

use std::collections::{BTreeMap, BTreeSet};

use sima_model::RunId;
use sima_scheduler::{Event, Record};

use crate::task_history::worker_bindings;

/// A run's execution metrics and temporal shape, merged from its journal.
///
/// Every rate and per-worker figure covers the latest run session — a resumed
/// run's journal spans sessions separated by downtime, and a span across that
/// downtime would collapse every rate toward zero. [`committed`](Self::committed)
/// is the run-wide cumulative count, since a count carries across sessions
/// where a rate does not.
///
/// Every duration is elapsed wall-clock as the collector observed it: the
/// journal stamps each event at append, so a span covers the queueing and
/// transport around the work as well as the work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTimeline {
    /// The run the metrics describe.
    pub run: RunId,
    /// The session's task count, from its `RunStarted` — the denominator of
    /// the retry rates.
    pub tasks: usize,
    /// Committed tasks across the whole journal.
    pub committed: usize,
    /// Committed tasks within the session — the throughput numerator.
    pub session_committed: usize,
    /// When the session started: the last `RunStarted`'s stamp.
    pub session_start_ms: u64,
    /// The last stamp the journal carries, which bounds the session.
    pub session_end_ms: u64,
    /// The session's retry figures.
    pub retries: RetryStats,
    /// One entry per worker the session named, ordered by worker id.
    pub workers: Vec<WorkerMetrics>,
    /// The stamp of every commit within the session, in journal order — the
    /// temporal data a commits-over-time chart buckets.
    pub commit_times_ms: Vec<u64>,
}

/// A session's retry figures, each a numerator over its own denominator. They
/// answer three questions that disagree on the same run: how much retrying
/// happened, how many tasks it touched, and how much attempted work was
/// wasted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetryStats {
    /// `Retried` events: the volume of retrying, over the task count.
    pub total_retries: usize,
    /// Distinct tasks retried at least once: the prevalence, over the task
    /// count.
    pub retried_tasks: usize,
    /// Attempts that ended in a failure, rejection, or fault: the wasted
    /// share, over the attempts taken.
    pub failed_attempts: usize,
    /// Attempts taken, one per lease.
    pub total_attempts: usize,
}

/// One worker's session: when it came alive, how long it was occupied, and
/// what it produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerMetrics {
    /// The worker's id, stable across a respawn.
    pub worker: u64,
    /// The device the worker reported, empty for a domain that uses none.
    pub device: String,
    /// The machine the worker's pool ran on, empty for a local one.
    pub host: String,
    /// When the worker first became ready in the session. A worker the
    /// session never bound is taken to have lived from its start, since the
    /// journal states no other time.
    pub first_bind_ms: u64,
    /// How often the worker came back: its bindings in the session, past the
    /// first.
    pub respawns: usize,
    /// How long the worker held a lease, summed over its spans.
    pub busy_ms: u64,
    /// How long the worker existed: the session's end past its first binding.
    pub lifespan_ms: u64,
    /// Commits the worker produced in the session.
    pub commits: usize,
    /// Attempts the worker took in the session.
    pub attempts: usize,
    /// The worker's lease spans as `(start_ms, end_ms)`, in lease order — the
    /// temporal data an occupancy chart buckets.
    pub spans: Vec<(u64, u64)>,
}

/// What the merge accumulates per worker before the session's end closes its
/// open span and fixes its lifespan.
#[derive(Default)]
struct WorkerAccumulator {
    /// The earliest binding stamp seen, `None` until one is.
    first_bind_ms: Option<u64>,
    /// Bindings seen, of which everything past the first is a respawn.
    bindings: usize,
    /// Commits the worker produced.
    commits: usize,
    /// Attempts the worker took.
    attempts: usize,
    /// The worker's closed lease spans.
    spans: Vec<(u64, u64)>,
}

/// Merges `records` — a run's lifecycle events in append order — into the
/// metrics of `run`, over records from any source: a journal read locally,
/// or a stream from the host that drives the run. Every figure is an infrastructure fact the journal states, so no
/// domain is consulted and the merge cannot fail.
pub fn timeline_records(run: RunId, records: &[Record]) -> RunTimeline {
    let bindings = worker_bindings(records);
    let committed = records
        .iter()
        .filter(|record| matches!(record.event, Event::Committed { .. }))
        .count();
    let session = session(records);
    let session_start_ms = session.first().map_or(0, |record| record.ts_ms);
    let session_end_ms = session.last().map_or(0, |record| record.ts_ms);

    let mut tasks = 0;
    let mut retries = RetryStats::default();
    let mut retried: BTreeSet<&str> = BTreeSet::new();
    let mut workers: BTreeMap<u64, WorkerAccumulator> = BTreeMap::new();
    let mut commit_times_ms = Vec::new();
    // The lease each task currently holds: which worker took it, and when.
    let mut open: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    let mut session_committed = 0;

    for record in session {
        let ts_ms = record.ts_ms;
        match &record.event {
            Event::RunStarted { tasks: count, .. } => tasks = *count,
            Event::WorkerBound { worker, .. } => {
                let accumulator = workers.entry(*worker).or_default();
                // Journal order is chronological, but taking the minimum keeps
                // the first binding first however the stamps arrive.
                accumulator.first_bind_ms = Some(
                    accumulator
                        .first_bind_ms
                        .map_or(ts_ms, |first| first.min(ts_ms)),
                );
                accumulator.bindings += 1;
            }
            Event::Leased { task, worker, .. } => {
                retries.total_attempts += 1;
                workers.entry(*worker).or_default().attempts += 1;
                // A retry leases a task whose previous attempt already closed,
                // so the entry is replaced rather than merged.
                open.insert(task, (*worker, ts_ms));
            }
            Event::Committed { task, .. } => {
                session_committed += 1;
                commit_times_ms.push(ts_ms);
                if let Some(worker) = close(&mut open, &mut workers, task, ts_ms) {
                    workers.entry(worker).or_default().commits += 1;
                }
            }
            Event::Failed { task, .. }
            | Event::Rejected { task, .. }
            | Event::Faulted { task, .. } => {
                retries.failed_attempts += 1;
                close(&mut open, &mut workers, task, ts_ms);
            }
            Event::Retried { task, .. } => {
                retries.total_retries += 1;
                retried.insert(task);
            }
            // A lease expiry settles through the failure that follows it, and
            // the remaining events state nothing about occupancy or rates.
            Event::LeaseExpired { .. }
            | Event::CheckpointDegraded { .. }
            | Event::ChainRebound { .. }
            | Event::ProgramBound { .. }
            | Event::DriverChanged { .. }
            | Event::Queued { .. }
            | Event::Diagnostic { .. }
            | Event::RunFinalized { .. }
            | Event::RunFailed { .. }
            | Event::RunInterrupted { .. }
            // Rental lifecycle states nothing about worker timing or rates.
            | Event::InstanceOnline { .. }
            | Event::InstanceLost { .. }
            | Event::InstanceReplaced { .. }
            | Event::BudgetSpendExhausted { .. }
            | Event::BudgetWallClockExhausted { .. } => {}
        }
    }
    retries.retried_tasks = retried.len();

    // A lease the journal never closed held its worker up to the last thing
    // the journal saw: an attempt still in flight, or one a dead orchestrator
    // left open.
    for (worker, started_ms) in open.into_values() {
        push_span(&mut workers, worker, started_ms, session_end_ms);
    }

    RunTimeline {
        run,
        tasks,
        committed,
        session_committed,
        session_start_ms,
        session_end_ms,
        retries,
        workers: workers
            .into_iter()
            .map(|(worker, accumulator)| {
                metrics(
                    worker,
                    accumulator,
                    &bindings,
                    session_start_ms,
                    session_end_ms,
                )
            })
            .collect(),
        commit_times_ms,
    }
}

/// The records of the latest session: a resumed run restates `RunStarted` per
/// orchestration, and the last one opens the session the metrics cover. A
/// journal naming no session start is read whole, so a malformed one merges to
/// figures rather than to nothing.
fn session(records: &[Record]) -> &[Record] {
    records
        .iter()
        .rposition(|record| matches!(record.event, Event::RunStarted { .. }))
        .map_or(records, |start| &records[start..])
}

/// Closes `task`'s open lease at `ended_ms`, crediting the span to the worker
/// that took it, and reports that worker. A malformed or truncated journal may
/// state an outcome with no lease before it, and then there is no span to
/// close.
fn close<'a>(
    open: &mut BTreeMap<&'a str, (u64, u64)>,
    workers: &mut BTreeMap<u64, WorkerAccumulator>,
    task: &'a str,
    ended_ms: u64,
) -> Option<u64> {
    let (worker, started_ms) = open.remove(task)?;
    push_span(workers, worker, started_ms, ended_ms);
    Some(worker)
}

/// Credits `worker` with a lease span. An end before the start would be a
/// journal whose stamps run backwards, and the span is empty rather than
/// negative.
fn push_span(
    workers: &mut BTreeMap<u64, WorkerAccumulator>,
    worker: u64,
    started_ms: u64,
    ended_ms: u64,
) {
    workers
        .entry(worker)
        .or_default()
        .spans
        .push((started_ms, ended_ms.max(started_ms)));
}

/// One worker's metrics from what the session accumulated, joined to the
/// device and host it reported.
fn metrics(
    worker: u64,
    accumulator: WorkerAccumulator,
    bindings: &BTreeMap<u64, (String, String)>,
    session_start_ms: u64,
    session_end_ms: u64,
) -> WorkerMetrics {
    let (device, host) = bindings.get(&worker).cloned().unwrap_or_default();
    let first_bind_ms = accumulator.first_bind_ms.unwrap_or(session_start_ms);
    WorkerMetrics {
        worker,
        device,
        host,
        first_bind_ms,
        respawns: accumulator.bindings.saturating_sub(1),
        busy_ms: accumulator
            .spans
            .iter()
            .map(|(start, end)| end.saturating_sub(*start))
            .sum(),
        lifespan_ms: session_end_ms.saturating_sub(first_bind_ms),
        commits: accumulator.commits,
        attempts: accumulator.attempts,
        spans: accumulator.spans,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::{Result, hash_bytes};

    use crate::fixtures::{journal_with, stub_config};

    fn run_id() -> RunId {
        RunId::from_hash(hash_bytes(b"timeline test run"))
    }

    /// Wraps an event as a record stamped `ts_ms`.
    fn at(ts_ms: u64, event: Event) -> Record {
        Record { ts_ms, event }
    }

    fn started(tasks: usize, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::RunStarted {
                run: "00".repeat(32),
                tasks,
                committed: 0,
            },
        )
    }

    fn worker_bound(worker: u64, device: &str, host: &str, ts_ms: u64) -> Record {
        at(
            ts_ms,
            Event::WorkerBound {
                worker,
                device: device.to_string(),
                driver: String::new(),
                host: host.to_string(),
                program: None,
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
                stats: Vec::new(),
                stats_blob_hex: "00000000".to_string(),
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
                stats: Vec::new(),
                stats_blob_hex: String::new(),
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
                stats: Vec::new(),
                stats_blob_hex: String::new(),
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

    /// The metrics `records` merge to.
    fn merged(records: &[Record]) -> RunTimeline {
        timeline_records(run_id(), records)
    }

    /// The metrics of the worker `id`, which the merge must have named.
    fn worker(timeline: &RunTimeline, id: u64) -> WorkerMetrics {
        timeline
            .workers
            .iter()
            .find(|metrics| metrics.worker == id)
            .cloned()
            .expect("metrics for the worker")
    }

    #[test]
    fn throughput_is_the_sessions_commits_over_its_span() {
        let timeline = merged(&[
            started(2, 1_000),
            leased("aa", 0, 0, 1_100),
            committed("aa", 2_100),
            leased("bb", 0, 0, 2_100),
            committed("bb", 5_000),
        ]);
        assert_eq!(timeline.tasks, 2);
        assert_eq!(timeline.session_committed, 2);
        assert_eq!(timeline.session_start_ms, 1_000);
        assert_eq!(timeline.session_end_ms, 5_000);
        // Two commits over a four-second session: half a task per second.
        let span_s = (timeline.session_end_ms - timeline.session_start_ms) as f64 / 1_000.0;
        assert_eq!(timeline.session_committed as f64 / span_s, 0.5);
        assert_eq!(timeline.commit_times_ms, vec![2_100, 5_000]);
    }

    #[test]
    fn busy_time_is_the_sum_of_a_workers_lease_spans() {
        let timeline = merged(&[
            started(2, 0),
            worker_bound(0, "", "", 0),
            leased("aa", 0, 0, 100),
            committed("aa", 400),
            leased("bb", 0, 0, 1_000),
            committed("bb", 1_500),
        ]);
        let worker = worker(&timeline, 0);
        assert_eq!(worker.spans, vec![(100, 400), (1_000, 1_500)]);
        assert_eq!(worker.busy_ms, 800);
        // The worker existed from its binding to the last thing the journal saw.
        assert_eq!(worker.lifespan_ms, 1_500);
        assert_eq!(worker.commits, 2);
        assert_eq!(worker.attempts, 2);
    }

    #[test]
    fn spawn_latency_is_the_first_binding_past_the_session_start() {
        // A local worker binds as the pool launches; a remote one binds once
        // its ssh connection and container are up.
        let timeline = merged(&[
            started(2, 1_000),
            worker_bound(0, "Intel Arc 140T", "", 1_000),
            worker_bound(1, "NVIDIA RTX PRO 2000", "gpubox", 42_200),
            committed("aa", 50_000),
        ]);
        assert_eq!(
            worker(&timeline, 0).first_bind_ms - timeline.session_start_ms,
            0
        );
        assert_eq!(
            worker(&timeline, 1).first_bind_ms - timeline.session_start_ms,
            41_200
        );
        // The late worker's lifespan starts at its binding, not at the session.
        assert_eq!(worker(&timeline, 1).lifespan_ms, 7_800);
    }

    #[test]
    fn a_respawned_worker_counts_its_bindings_past_the_first() {
        let timeline = merged(&[
            started(1, 0),
            worker_bound(0, "Intel Arc 140T", "", 100),
            worker_bound(0, "Intel Arc 140T", "", 500),
            worker_bound(0, "Intel Arc 140T", "", 900),
            committed("aa", 1_000),
        ]);
        let worker = worker(&timeline, 0);
        assert_eq!(worker.respawns, 2);
        assert_eq!(
            worker.first_bind_ms, 100,
            "the first binding is the earliest"
        );
    }

    #[test]
    fn the_three_retry_figures_measure_volume_prevalence_and_waste() {
        // A run where a few very flaky tasks carry all the retrying: ten tasks
        // fail ten times each before committing, and the rest commit first try.
        // Prevalence reads 1% where volume reads 10%.
        let mut records = vec![started(1_000, 0)];
        let mut ts = 1;
        for task in 0..990 {
            let key = format!("{task:04x}");
            records.push(leased(&key, 0, 0, ts));
            records.push(committed(&key, ts + 1));
            ts += 2;
        }
        for task in 990..1_000 {
            let key = format!("{task:04x}");
            for attempt in 0..10 {
                records.push(leased(&key, 1, attempt, ts));
                records.push(failed(&key, attempt, ts + 1));
                records.push(retried(&key, attempt + 1, ts + 2));
                ts += 3;
            }
            records.push(leased(&key, 1, 10, ts));
            records.push(committed(&key, ts + 1));
            ts += 2;
        }
        let timeline = merged(&records);
        assert_eq!(timeline.tasks, 1_000);
        assert_eq!(timeline.session_committed, 1_000);
        assert_eq!(
            timeline.retries,
            RetryStats {
                total_retries: 100,
                retried_tasks: 10,
                failed_attempts: 100,
                total_attempts: 1_100,
            }
        );
    }

    #[test]
    fn a_rejection_and_a_fault_count_as_failed_attempts() {
        let timeline = merged(&[
            started(2, 0),
            leased("aa", 0, 0, 10),
            rejected("aa", 0, 20),
            leased("bb", 1, 0, 10),
            faulted("bb", 0, 30),
        ]);
        assert_eq!(timeline.retries.failed_attempts, 2);
        assert_eq!(timeline.retries.total_attempts, 2);
        assert_eq!(timeline.retries.retried_tasks, 0);
        assert_eq!(timeline.session_committed, 0);
        // Both attempts ended, so each worker's span closes on its own outcome.
        assert_eq!(worker(&timeline, 0).busy_ms, 10);
        assert_eq!(worker(&timeline, 1).busy_ms, 20);
    }

    #[test]
    fn a_lease_the_journal_never_closed_ends_at_the_session_end() {
        // An attempt still in flight, or one a dead orchestrator left open: the
        // worker was occupied up to the last thing the journal saw.
        let timeline = merged(&[
            started(2, 0),
            worker_bound(0, "", "", 0),
            leased("aa", 0, 0, 100),
            committed("aa", 200),
            leased("bb", 0, 1, 300),
            at(
                900,
                Event::Diagnostic {
                    level: sima_scheduler::Level::Info,
                    source: "worker stderr".to_string(),
                    message: "still working".to_string(),
                    worker: Some(0),
                    host: None,
                    task: None,
                },
            ),
        ]);
        let worker = worker(&timeline, 0);
        assert_eq!(worker.spans, vec![(100, 200), (300, 900)]);
        assert_eq!(worker.busy_ms, 700);
        assert_eq!(worker.commits, 1, "an open lease produced no commit");
        assert_eq!(worker.attempts, 2);
    }

    #[test]
    fn the_rates_cover_the_latest_session_and_the_commit_count_the_run() {
        // A run resumed after an hour of downtime: merging the gap into the
        // rates would collapse them, so they cover the latest session alone.
        let timeline = merged(&[
            started(2, 0),
            worker_bound(0, "", "", 0),
            leased("aa", 0, 0, 100),
            committed("aa", 200),
            started(2, 3_600_000),
            worker_bound(0, "", "", 3_600_010),
            leased("bb", 0, 0, 3_600_100),
            committed("bb", 3_600_400),
        ]);
        assert_eq!(timeline.session_start_ms, 3_600_000);
        assert_eq!(timeline.session_end_ms, 3_600_400);
        assert_eq!(timeline.session_committed, 1);
        assert_eq!(timeline.committed, 2, "the commit count is run-wide");
        assert_eq!(timeline.commit_times_ms, vec![3_600_400]);
        let worker = worker(&timeline, 0);
        assert_eq!(worker.spans, vec![(3_600_100, 3_600_400)]);
        assert_eq!(
            worker.respawns, 0,
            "the earlier session's binding is not one"
        );
        assert_eq!(worker.first_bind_ms, 3_600_010);
    }

    #[test]
    fn each_worker_joins_the_device_and_host_it_reported() {
        let timeline = merged(&[
            started(2, 0),
            worker_bound(0, "Intel Arc 140T", "", 0),
            worker_bound(1, "NVIDIA RTX PRO 2000", "gpubox", 0),
            leased("aa", 0, 0, 10),
            committed("aa", 20),
            leased("bb", 1, 0, 10),
            committed("bb", 20),
        ]);
        assert_eq!(worker(&timeline, 0).device, "Intel Arc 140T");
        assert_eq!(
            worker(&timeline, 0).host,
            "",
            "a local worker names no host"
        );
        assert_eq!(worker(&timeline, 1).device, "NVIDIA RTX PRO 2000");
        assert_eq!(worker(&timeline, 1).host, "gpubox");
    }

    #[test]
    fn a_session_that_committed_nothing_reports_its_idle_workers() {
        let timeline = merged(&[
            started(4, 1_000),
            worker_bound(0, "", "", 1_000),
            worker_bound(1, "", "", 1_000),
        ]);
        assert_eq!(timeline.session_committed, 0);
        assert_eq!(timeline.committed, 0);
        assert!(timeline.commit_times_ms.is_empty());
        assert_eq!(timeline.workers.len(), 2);
        for id in [0, 1] {
            let worker = worker(&timeline, id);
            assert_eq!(worker.busy_ms, 0);
            assert_eq!(worker.attempts, 0);
            // The session is one instant long: the renderer's guard, not the
            // merge's, is what keeps a rate off a zero denominator.
            assert_eq!(worker.lifespan_ms, 0);
        }
    }

    #[test]
    fn a_journal_naming_no_session_start_merges_over_every_record() {
        // A malformed journal still merges to figures rather than to nothing.
        let timeline = merged(&[leased("aa", 0, 0, 100), committed("aa", 400)]);
        assert_eq!(timeline.session_start_ms, 100);
        assert_eq!(timeline.session_end_ms, 400);
        assert_eq!(timeline.session_committed, 1);
        assert_eq!(timeline.tasks, 0);
        // The journal binds the worker nowhere, so it is taken to have lived
        // from the session's start.
        assert_eq!(worker(&timeline, 0).first_bind_ms, 100);
    }

    #[test]
    fn the_record_merge_equals_the_timeline_read_from_the_journal() -> Result<()> {
        let records = vec![
            started(2, 0),
            worker_bound(0, "", "", 0),
            leased("aa", 0, 0, 10),
            committed("aa", 20),
            leased("bb", 0, 0, 30),
            rejected("bb", 0, 40),
        ];
        let (_dir, config) = journal_with(&records)?;
        assert_eq!(
            timeline_records(stub_config()?.id(), &records),
            timeline_records(config.run.id(), &crate::journal::records(&config)?)
        );
        Ok(())
    }
}
