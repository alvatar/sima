//! Terminal rendering: one plain line per meaningful lifecycle event, and
//! the status block. Ids render short — the first twelve hex characters —
//! since a run's journal names them consistently.

use std::sync::atomic::{AtomicUsize, Ordering};

use sima_pipeline::{
    Attempt, AttemptResult, Event, MachineReport, Record, RetryStats, RunId, RunState, RunStatus,
    RunTimeline, SpendReport, TaskHistory, TaskOutcome, WorkerMetrics,
};

/// How many hex characters of an id a progress line shows.
const SHORT: usize = 12;

/// The leading `SHORT` characters of a journaled id.
pub fn short(id: &str) -> &str {
    &id[..id.len().min(SHORT)]
}

/// Renders `event` to the one line it warrants, or `None` for the `Queued`,
/// `Leased`, `WorkerBound`, and `ProgramBound` bookkeeping events.
/// `committed`/`tasks` supply
/// the running `committed k/n` count a commit line shows; on `RunStarted`, a
/// nonzero `committed` is the run's prior progress and the line names it, so a
/// resumed session does not read as a restart. The single source of the
/// event wording: `sima run` prints these lines to stdout and the tui folds
/// them into its event log.
pub fn describe(event: &Event, committed: usize, tasks: usize) -> Option<String> {
    Some(match event {
        Event::RunStarted { tasks, .. } if committed > 0 => {
            format!("started: {tasks} tasks, {committed} already committed")
        }
        Event::RunStarted { tasks, .. } => format!("started: {tasks} tasks"),
        Event::Committed { task, .. } => {
            format!("committed {committed}/{tasks}  {}", short(task))
        }
        Event::Retried { task, next_attempt } => {
            format!("retrying {} (attempt {next_attempt})", short(task))
        }
        Event::Rejected { task, reason, .. } => {
            format!("rejected {}: {reason}", short(task))
        }
        Event::Failed {
            task,
            attempt,
            reason,
            ..
        } => format!("failed {} (attempt {attempt}): {reason}", short(task)),
        Event::Faulted { task, error, .. } => format!("fault {}: {error}", short(task)),
        Event::LeaseExpired {
            task, elapsed_ms, ..
        } => format!("lease expired {} ({elapsed_ms} ms)", short(task)),
        Event::CheckpointDegraded { task, error } => {
            format!("checkpoint degraded {}: {error}", short(task))
        }
        Event::RunFinalized { committed, .. } => {
            format!("finalized: {committed} tasks committed")
        }
        Event::RunFailed { task, reason, .. } => {
            format!("run failed on {}: {reason}", short(task))
        }
        Event::RunInterrupted { .. } => {
            "interrupted: store resumable, re-run to continue".to_string()
        }
        // A rebind means the hardware changed under the search: the chain's
        // device is gone and its work moved. Loud by design.
        Event::ChainRebound { chain, from, to } => {
            format!("chain {chain} rebound: {from} is absent, continuing on {to}")
        }
        // A warn or error diagnostic is worth a console line; info-level
        // diagnostics (worker stderr) are journaled, not echoed, so the
        // run's console output stays clean.
        Event::Diagnostic {
            level,
            source,
            message,
            worker,
            ..
        } => {
            let level = match level {
                sima_pipeline::Level::Warn => "warn",
                sima_pipeline::Level::Error => "error",
                sima_pipeline::Level::Info => return None,
            };
            match worker {
                Some(worker) => format!("{level} {source} worker {worker}: {message}"),
                None => format!("{level} {source}: {message}"),
            }
        }
        // A rented machine came online: reported at supervisor start and for
        // each replacement, naming where the work will run.
        Event::InstanceOnline {
            instance,
            gpu_model,
            gpu_count,
            rate_microusd_hour,
            host,
            ..
        } => {
            let hardware = if *gpu_count == 0 || gpu_model.is_empty() {
                "no GPU".to_string()
            } else {
                format!("{gpu_count}× {gpu_model}")
            };
            format!(
                "instance online {} on {host}: {hardware} at {}/hr",
                short(instance),
                dollars(*rate_microusd_hour)
            )
        }
        Event::InstanceLost { instance, .. } => {
            format!("instance lost {}", short(instance))
        }
        Event::InstanceReplaced { from, to, .. } => {
            format!("instance replaced {} with {}", short(from), short(to))
        }
        Event::BudgetSpendExhausted {
            accrued_microusd,
            cap_microusd,
        } => format!(
            "budget exhausted: spent {} of {}, winding down",
            dollars(*accrued_microusd),
            dollars(*cap_microusd)
        ),
        Event::BudgetWallClockExhausted { deadline_ms } => format!(
            "budget exhausted: rental deadline (epoch ms {deadline_ms}) passed, winding down"
        ),
        // The one warning-class provenance line: results already stored and
        // those about to be computed come from different driver builds.
        Event::DriverChanged {
            host,
            device,
            from,
            to,
        } => {
            let place = if host.is_empty() {
                device.clone()
            } else {
                format!("{device} on {host}")
            };
            format!("warning: the driver for {place} changed: {from} is now {to}")
        }
        Event::Queued { .. }
        | Event::Leased { .. }
        | Event::WorkerBound { .. }
        | Event::ProgramBound { .. } => return None,
    })
}

/// A micro-USD amount as dollars, to three decimals so sub-cent hourly rates
/// stay legible: `$0.412`, `$5.000`.
pub fn dollars(micro_usd: u64) -> String {
    format!("${:.3}", micro_usd as f64 / 1_000_000.0)
}

/// Progress rendering over a run's event stream: prints one line per
/// meaningful event. Called from the collector thread, one record at a
/// time, in journal order; the counters give the `committed k/n` running
/// count.
pub struct Progress {
    /// The run's task count, from `RunStarted`.
    tasks: AtomicUsize,
    /// Commits accounted for: the run's prior commits plus those seen live.
    committed: AtomicUsize,
}

impl Progress {
    /// A progress renderer over a session's events. Both counters come from
    /// the run's own `RunStarted`, so nothing needs to be known before the
    /// run starts.
    pub fn new() -> Progress {
        Progress {
            tasks: AtomicUsize::new(0),
            committed: AtomicUsize::new(0),
        }
    }

    /// Prints the line the record's event warrants, if any, keeping the
    /// running commit count for the `committed k/n` line. `Queued`, `Leased`,
    /// and `WorkerBound` yield no line and stay silent.
    pub fn event(&self, record: &Record) {
        let event = &record.event;
        if let Event::RunStarted {
            tasks, committed, ..
        } = event
        {
            self.tasks.store(*tasks, Ordering::Relaxed);
            // The run's prior commits, counted from the store's records by
            // the source that derived the frontier. A resumed session counts
            // on from there.
            self.committed.store(*committed, Ordering::Relaxed);
        }
        // A commit advances the running count; every other line reads it
        // without moving it.
        let committed = match event {
            Event::Committed { .. } => self.committed.fetch_add(1, Ordering::Relaxed) + 1,
            _ => self.committed.load(Ordering::Relaxed),
        };
        if let Some(line) = describe(event, committed, self.tasks.load(Ordering::Relaxed)) {
            println!("{line}");
        }
    }

    /// The commits accounted for so far.
    #[cfg(test)]
    fn committed(&self) -> usize {
        self.committed.load(Ordering::Relaxed)
    }
}

/// The attempt table's column headers, in render order.
const ATTEMPT_COLUMNS: [&str; 6] = ["attempt", "worker", "host", "device", "outcome", "elapsed"];

/// Renders one task's timeline: its key and terminal state, one row per
/// attempt, and the committed result. Every duration is the span the
/// collector observed between the lease and the event that ended it, so it
/// carries the queue and transport latency around the work as well.
pub fn task_block(history: &TaskHistory) -> String {
    let mut block = format!(
        "task     {}\nstate    {}",
        short(&history.task),
        task_state(&history.outcome)
    );
    if !history.attempts.is_empty() {
        block.push_str("\n\n");
        block.push_str(&attempt_table(&history.attempts));
    }
    if let TaskOutcome::Committed { record, stats } = &history.outcome {
        block.push_str(&format!("\n\nresult   {}  {stats}", short(record)));
    }
    block
}

/// The failure digest's column headers, in render order.
const FAILURE_COLUMNS: [&str; 5] = ["task", "final", "attempts", "worker", "reason"];

/// Renders the failure digest: a header counting the tasks the run did not
/// commit, then one row each naming how the task ended and why. An empty
/// digest is the header alone.
pub fn failures_block(run: &RunId, failures: &[TaskHistory]) -> String {
    let tasks = if failures.len() == 1 { "task" } else { "tasks" };
    let header = format!(
        "run {}   {} {tasks} did not commit",
        short(&run.to_string()),
        failures.len()
    );
    if failures.is_empty() {
        return header;
    }
    let rows: Vec<Vec<String>> = failures.iter().map(failure_cells).collect();
    format!("{header}\n\n{}", table(&FAILURE_COLUMNS, &rows, false))
}

/// One failed task's cells, in [`FAILURE_COLUMNS`] order: the worker is the
/// one that ran the deciding attempt, which is the last the task took.
fn failure_cells(history: &TaskHistory) -> Vec<String> {
    let (outcome, reason) = match &history.outcome {
        TaskOutcome::Rejected { reason, .. } => ("rejected", reason.clone()),
        TaskOutcome::Faulted { error, .. } => ("faulted", error.clone()),
        TaskOutcome::Queued | TaskOutcome::InProgress | TaskOutcome::Committed { .. } => {
            ("open", String::new())
        }
    };
    let worker = history
        .attempts
        .last()
        .map_or_else(|| "—".to_string(), |attempt| format!("w{}", attempt.worker));
    vec![
        short(&history.task).to_string(),
        outcome.to_string(),
        history.attempts.len().to_string(),
        worker,
        reason,
    ]
}

/// The state line for a task's outcome: the terminal ones carry the attempt
/// that decided them and why.
fn task_state(outcome: &TaskOutcome) -> String {
    match outcome {
        TaskOutcome::Queued => "queued".to_string(),
        TaskOutcome::InProgress => "in progress".to_string(),
        TaskOutcome::Committed { .. } => "committed".to_string(),
        TaskOutcome::Rejected { attempt, reason } => {
            format!("rejected on attempt {attempt}: {reason}")
        }
        TaskOutcome::Faulted { attempt, error } => {
            format!("faulted on attempt {attempt}: {error}")
        }
    }
}

/// Renders the attempt rows under their headers. The failure text of an
/// attempt that has one trails its row, in a column the headers leave unnamed
/// since it reads as the row's note rather than a field.
fn attempt_table(attempts: &[Attempt]) -> String {
    let mut headers: Vec<&str> = ATTEMPT_COLUMNS.to_vec();
    headers.push("");
    let rows: Vec<Vec<String>> = attempts
        .iter()
        .map(|attempt| {
            let mut cells = attempt_cells(attempt).to_vec();
            cells.push(attempt_note(attempt));
            cells
        })
        .collect();
    table(&headers, &rows, true)
}

/// Renders `rows` under `headers`, indented two spaces, with every column
/// widened to its longest cell so the table aligns whatever it holds.
/// `number_first` right-aligns the leading column, for a table whose first
/// field is a count rather than a name.
fn table(headers: &[&str], rows: &[Vec<String>], number_first: bool) -> String {
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            rows.iter()
                .filter_map(|row| row.get(column))
                .map(|cell| cell.chars().count())
                .chain([header.chars().count()])
                .max()
                .unwrap_or_default()
        })
        .collect();
    let header_cells: Vec<String> = headers.iter().map(|header| header.to_string()).collect();
    let mut table = padded_row(&header_cells, &widths, number_first);
    for row in rows {
        table.push('\n');
        table.push_str(&padded_row(row, &widths, number_first));
    }
    table
}

/// One table line, indented and column-padded. Trailing padding is dropped,
/// so no line carries whitespace past its last word.
fn padded_row(cells: &[String], widths: &[usize], number_first: bool) -> String {
    let mut line = String::from(" ");
    for (column, cell) in cells.iter().enumerate() {
        let width = widths[column];
        line.push(' ');
        if column == 0 && number_first {
            line.push_str(&format!("{cell:>width$}"));
        } else {
            line.push_str(&format!("{cell:<width$}"));
        }
        line.push(' ');
    }
    line.trim_end().to_string()
}

/// One attempt's cells, in [`ATTEMPT_COLUMNS`] order. A local worker's host
/// and a deviceless domain's device render as placeholders, since the journal
/// states neither.
fn attempt_cells(attempt: &Attempt) -> [String; 6] {
    [
        attempt.attempt.to_string(),
        format!("w{}", attempt.worker),
        placeholder(&attempt.host, "—"),
        placeholder(&attempt.device, "(none)"),
        attempt_outcome(&attempt.result).to_string(),
        elapsed(attempt),
    ]
}

/// `value`, or `absent` where the journal states nothing.
fn placeholder(value: &str, absent: &str) -> String {
    if value.is_empty() {
        absent.to_string()
    } else {
        value.to_string()
    }
}

/// The one word naming how an attempt ended.
fn attempt_outcome(result: &AttemptResult) -> &'static str {
    match result {
        AttemptResult::Committed => "committed",
        AttemptResult::Failed { .. } => "failed",
        AttemptResult::Rejected { .. } => "rejected",
        AttemptResult::Faulted { .. } => "faulted",
        AttemptResult::InFlight => "in flight",
    }
}

/// The span the collector observed over an attempt, in seconds to one
/// decimal, or a placeholder while its lease is still open.
fn elapsed(attempt: &Attempt) -> String {
    match attempt.ended_ms {
        Some(ended_ms) => format!(
            "{:.1}s",
            ended_ms.saturating_sub(attempt.started_ms) as f64 / 1_000.0
        ),
        None => "—".to_string(),
    }
}

/// What trails an attempt's row: why it failed, and whether a lease expiry
/// preempted it.
fn attempt_note(attempt: &Attempt) -> String {
    let mut note = match &attempt.result {
        AttemptResult::Failed { reason } | AttemptResult::Rejected { reason } => reason.clone(),
        AttemptResult::Faulted { error } => error.clone(),
        AttemptResult::Committed | AttemptResult::InFlight => String::new(),
    };
    if attempt.lease_expired {
        if !note.is_empty() {
            note.push(' ');
        }
        note.push_str("(lease expired)");
    }
    note
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
    let block = format!(
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
    );
    match devices_line(status) {
        Some(devices) => format!("{block}\ndevices              {devices}"),
        None => block,
    }
}

/// The run's device composition: committed tasks per device, busiest first,
/// and the chains that moved when their device went absent.
///
/// `None` when the journal names no device, as a run whose domain uses none
/// does. Nothing is inferred: what is not in the journal is not printed.
fn devices_line(status: &RunStatus) -> Option<String> {
    if status.devices.is_empty() {
        return None;
    }
    let mut composition: Vec<(&String, &usize)> = status.devices.iter().collect();
    // Busiest first; the name breaks ties, so one journal renders one way.
    composition.sort_by(|(a_name, a_count), (b_name, b_count)| {
        b_count.cmp(a_count).then(a_name.cmp(b_name))
    });
    let mut line = composition
        .iter()
        .map(|(name, count)| format!("{name} ×{count}"))
        .collect::<Vec<String>>()
        .join(", ");
    if status.rebound_chains > 0 {
        line.push_str(&format!(", rebound chains: {}", status.rebound_chains));
    }
    Some(line)
}

/// Renders the rental-spend ledger: the closed rentals with their duration,
/// rate, and cost; the rentals still open with what they have accrued; and the
/// total the two together have cost. Every amount is in dollars.
pub fn spend_block(report: &SpendReport) -> String {
    let mut block = format!("closed rentals   {}", report.entries.len());
    for entry in &report.entries {
        block.push_str(&format!(
            "\n  {}  {}  {}/hr  {}",
            entry.tag,
            duration(entry.ended_ms.saturating_sub(entry.started_ms)),
            dollars(entry.price_micro_usd_hour),
            dollars(entry.cost_micro_usd),
        ));
    }
    block.push_str(&format!("\nopen rentals     {}", report.open.len()));
    for open in &report.open {
        block.push_str(&format!(
            "\n  {}  {}/hr  {} so far",
            open.tag,
            dollars(open.rate.0),
            dollars(open.accrued.0),
        ));
    }
    block.push_str(&format!("\ntotal            {}", dollars(report.total.0)));
    block
}

/// Renders the machine-reputation ledger: one line per machine with a recorded
/// incident, its counts by kind, and whether it is blacklisted; an explicit
/// line when the store holds none. Machines are already sorted by provider then
/// machine, so one store renders one way.
pub fn machines_block(report: &MachineReport) -> String {
    if report.machines.is_empty() {
        return "no machine incidents recorded".to_string();
    }
    let blacklisted = report.machines.iter().filter(|m| m.blacklisted).count();
    let mut block = format!(
        "machines with incidents   {}   blacklisted   {}",
        report.machines.len(),
        blacklisted,
    );
    for machine in &report.machines {
        let noun = if machine.incidents == 1 {
            "incident"
        } else {
            "incidents"
        };
        block.push_str(&format!(
            "\n  {}-{}  {} {noun} (lost {}, never-ready {}, probe-failed {}, \
             install-failed {}){}",
            machine.provider,
            machine.machine,
            machine.incidents,
            machine.lost,
            machine.never_ready,
            machine.probe_failed,
            machine.install_failed,
            if machine.blacklisted {
                "  blacklisted"
            } else {
                ""
            },
        ));
    }
    block
}

/// How many columns wide the temporal chart draws, whatever the terminal is.
/// A fixed axis makes one journal render one way, on any screen.
const CHART_WIDTH: usize = 48;

/// The glyphs a commit bucket's count maps to, lightest first.
const COMMIT_GLYPHS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

/// The worker table's column headers, in render order.
const WORKER_COLUMNS: [&str; 8] = [
    "worker", "device", "host", "spawn", "respawns", "util", "commits", "attempts",
];

/// What a figure reads when the counts it is taken over leave it undefined.
const UNDEFINED: &str = "—";

/// Renders a run's metrics: the summary scalars, the three retry ratios, the
/// per-worker table, and the temporal chart beneath them.
///
/// Every duration is elapsed wall-clock as the run's journal stamped it, so a
/// worker's occupancy covers the queueing and transport around its work as
/// well as the work.
pub fn timeline_block(timeline: &RunTimeline) -> String {
    let span_ms = timeline
        .session_end_ms
        .saturating_sub(timeline.session_start_ms);
    let mut block = format!(
        "run          {}\n\
         wall-clock   {}\n\
         committed    {}\n\
         throughput   {}",
        timeline.run,
        duration(span_ms),
        timeline.committed,
        throughput(timeline.session_committed, span_ms),
    );
    block.push_str("\n\nretry rates\n");
    block.push_str(&retry_block(&timeline.retries, timeline.tasks));
    let rows: Vec<Vec<String>> = timeline
        .workers
        .iter()
        .map(|worker| worker_cells(worker, timeline.session_start_ms).to_vec())
        .collect();
    block.push_str("\n\n");
    block.push_str(&table(&WORKER_COLUMNS, &rows, false));
    if let Some(chart) = chart(timeline, span_ms) {
        block.push_str("\n\n");
        block.push_str(&chart);
    }
    block
}

/// Committed tasks per second over a session of `span_ms`, or a placeholder
/// for a session of no span — a run that journaled its start and nothing
/// since has no rate, rather than an infinite one.
fn throughput(committed: usize, span_ms: u64) -> String {
    if span_ms == 0 {
        return UNDEFINED.to_string();
    }
    format!(
        "{:.1} task/s",
        committed as f64 / (span_ms as f64 / 1_000.0)
    )
}

/// An elapsed span: seconds to two decimals under a minute, minutes and whole
/// seconds above. One pinned precision, so one journal renders one way.
fn duration(ms: u64) -> String {
    if ms < 60_000 {
        return format!("{:.2}s", ms as f64 / 1_000.0);
    }
    format!("{}m{:02}s", ms / 60_000, (ms % 60_000) / 1_000)
}

/// The three retry figures, each on its own line naming the numerator and
/// denominator it is taken over. They answer different questions and can
/// disagree by an order of magnitude on one run, so each states its own ratio
/// rather than leaving the reader to guess which a bare number is.
fn retry_block(retries: &RetryStats, tasks: usize) -> String {
    let rows = [
        [
            "retries / tasks".to_string(),
            counts(retries.total_retries, tasks),
            per_task(retries.total_retries, tasks),
        ],
        [
            "tasks retried / tasks".to_string(),
            counts(retries.retried_tasks, tasks),
            percent(retries.retried_tasks, tasks),
        ],
        [
            "failed attempts / attempts".to_string(),
            counts(retries.failed_attempts, retries.total_attempts),
            percent(retries.failed_attempts, retries.total_attempts),
        ],
    ];
    let label_width = rows
        .iter()
        .map(|row| row[0].len())
        .max()
        .unwrap_or_default();
    let counts_width = rows
        .iter()
        .map(|row| row[1].len())
        .max()
        .unwrap_or_default();
    rows.iter()
        .map(|[label, counts, result]| {
            format!("  {label:<label_width$}  {counts:>counts_width$}  =  {result}")
        })
        .collect::<Vec<String>>()
        .join("\n")
}

/// The `n / of` pair a ratio is taken over, stated whether or not the ratio
/// itself is defined.
fn counts(n: usize, of: usize) -> String {
    format!("{n} / {of}")
}

/// `n` per task over `of` tasks, to two decimals, or a placeholder where there
/// are no tasks to divide by.
fn per_task(n: usize, of: usize) -> String {
    if of == 0 {
        return UNDEFINED.to_string();
    }
    format!("{:.2} per task", n as f64 / of as f64)
}

/// `n` as a percentage of `of`, to one decimal, or a placeholder where there
/// is nothing to divide by.
fn percent(n: usize, of: usize) -> String {
    if of == 0 {
        return UNDEFINED.to_string();
    }
    format!("{:.1}%", 100.0 * n as f64 / of as f64)
}

/// One worker's cells, in [`WORKER_COLUMNS`] order. Spawn is the provisioning
/// cost the worker's first binding past the session's start states, which is
/// the ssh and container startup for a pool on another machine and near zero for a local
/// one; utilization is over the worker's own lifespan, so provisioning time
/// is not charged as idleness. A local worker's host and a deviceless domain's
/// device render as the same placeholders the attempt table uses.
fn worker_cells(worker: &WorkerMetrics, session_start_ms: u64) -> [String; 8] {
    [
        format!("w{}", worker.worker),
        placeholder(&worker.device, "(none)"),
        placeholder(&worker.host, UNDEFINED),
        duration(worker.first_bind_ms.saturating_sub(session_start_ms)),
        worker.respawns.to_string(),
        utilization(worker),
        worker.commits.to_string(),
        worker.attempts.to_string(),
    ]
}

/// The share of its lifespan a worker held a lease, as a whole percentage, or
/// a placeholder for a worker that bound as the journal ended.
fn utilization(worker: &WorkerMetrics) -> String {
    if worker.lifespan_ms == 0 {
        return UNDEFINED.to_string();
    }
    format!(
        "{:.0}%",
        100.0 * worker.busy_ms as f64 / worker.lifespan_ms as f64
    )
}

/// The temporal chart over the session: commits per column, then one
/// occupancy bar per worker, all on the one axis so they align. `None` for a
/// session of no span, which has no axis to bucket.
fn chart(timeline: &RunTimeline, span_ms: u64) -> Option<String> {
    if span_ms == 0 {
        return None;
    }
    let buckets: Vec<(u64, u64)> = (0..CHART_WIDTH)
        .map(|column| {
            (
                timeline.session_start_ms + span_ms * column as u64 / CHART_WIDTH as u64,
                timeline.session_start_ms + span_ms * (column as u64 + 1) / CHART_WIDTH as u64,
            )
        })
        .collect();
    let mut lines = vec![(
        "commits".to_string(),
        sparkline(&timeline.commit_times_ms, &buckets),
    )];
    lines.extend(timeline.workers.iter().map(|worker| {
        (
            format!("w{}", worker.worker),
            occupancy_bar(worker, &buckets),
        )
    }));
    let label_width = lines.iter().map(|(label, _)| label.len()).max()?;
    let chart = lines
        .iter()
        // Indented to the left edge the tables above it share.
        .map(|(label, glyphs)| format!("  {label:<label_width$}  {glyphs}"))
        .collect::<Vec<String>>()
        .join("\n");
    Some(format!(
        "each column spans {}\n\n{chart}",
        duration(span_ms / CHART_WIDTH as u64)
    ))
}

/// Commits per column, each count a glyph scaled to the busiest column, and a
/// blank where nothing committed.
fn sparkline(commit_times_ms: &[u64], buckets: &[(u64, u64)]) -> String {
    let mut counts = vec![0usize; buckets.len()];
    for &ts_ms in commit_times_ms {
        counts[column_of(ts_ms, buckets)] += 1;
    }
    let busiest = counts.iter().copied().max().unwrap_or_default();
    counts
        .iter()
        .map(|&count| {
            if count == 0 || busiest == 0 {
                return ' ';
            }
            // Scale to the busiest column and round up, so any commit at all
            // draws a mark rather than vanishing into the lightest glyph.
            let step = (count * COMMIT_GLYPHS.len()).div_ceil(busiest);
            COMMIT_GLYPHS[step.saturating_sub(1).min(COMMIT_GLYPHS.len() - 1)]
        })
        .collect()
}

/// The column a stamp falls in. The last column carries the session's end, so
/// the final commit lands on the axis rather than past it.
fn column_of(ts_ms: u64, buckets: &[(u64, u64)]) -> usize {
    buckets
        .iter()
        .rposition(|(start, _)| ts_ms >= *start)
        .unwrap_or_default()
}

/// One worker's occupancy across the columns: blank before it came alive, so
/// its provisioning gap is drawn to scale; filled for a column it held a lease
/// through at least half of; light otherwise.
fn occupancy_bar(worker: &WorkerMetrics, buckets: &[(u64, u64)]) -> String {
    buckets
        .iter()
        .map(|&(start, end)| {
            if end <= worker.first_bind_ms {
                return ' ';
            }
            let busy_ms: u64 = worker
                .spans
                .iter()
                .map(|&(from, to)| to.min(end).saturating_sub(from.max(start)))
                .sum();
            if end > start && busy_ms * 2 >= end - start {
                '█'
            } else {
                '░'
            }
        })
        .collect()
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
    fn a_run_started_line_reports_prior_commits_when_resuming() {
        // The started line names the prior commits so a resumed session does
        // not read as a restart. A fresh run keeps the bare form.
        let event = Event::RunStarted {
            run: "ab".repeat(32),
            tasks: 200,
            committed: 26,
        };
        assert_eq!(
            describe(&event, 26, 200).expect("a started line"),
            "started: 200 tasks, 26 already committed"
        );
        assert_eq!(
            describe(&event, 0, 200).expect("a started line"),
            "started: 200 tasks"
        );
    }

    /// Wraps an event as a record the tests feed the renderer. The stamp is
    /// irrelevant here, so every record carries the same one.
    fn rec(event: Event) -> Record {
        Record { ts_ms: 0, event }
    }

    #[test]
    fn a_session_counts_commits_on_from_the_started_event() {
        let progress = Progress::new();
        progress.event(&rec(Event::RunStarted {
            run: "ab".repeat(32),
            tasks: 3,
            committed: 2,
        }));
        // The count is the event's, which the run derived from its store —
        // the journal replay this once read from can lag its own records.
        assert_eq!(progress.committed(), 2);

        progress.event(&rec(Event::Committed {
            task: "cd".repeat(32),
            record: "ef".repeat(32),
            stats: Vec::new(),
            stats_blob_hex: String::new(),
        }));
        assert_eq!(progress.committed(), 3);
    }

    #[test]
    fn a_degraded_checkpoint_renders_a_line() {
        let event = Event::CheckpointDegraded {
            task: "ab".repeat(32),
            error: "checkpoint dir is unwritable".to_string(),
        };
        let line = describe(&event, 0, 0).expect("a degraded checkpoint warrants a line");
        assert!(line.contains("checkpoint degraded"), "{line}");
        assert!(line.contains("unwritable"), "{line}");
    }

    /// A diagnostic event over `level`, attributed to worker 3.
    fn diagnostic(level: sima_pipeline::Level, source: &str, message: &str) -> Event {
        Event::Diagnostic {
            level,
            source: source.to_string(),
            message: message.to_string(),
            worker: Some(3),
            host: None,
            task: None,
        }
    }

    #[test]
    fn a_warn_or_error_diagnostic_renders_level_source_and_worker() {
        let warn = diagnostic(
            sima_pipeline::Level::Warn,
            "transport",
            "undecodable event frame",
        );
        let line = describe(&warn, 0, 0).expect("a warn diagnostic warrants a line");
        for part in ["warn", "transport", "worker 3", "undecodable event frame"] {
            assert!(line.contains(part), "missing {part}: {line}");
        }
        let error = diagnostic(sima_pipeline::Level::Error, "panic", "thread panicked");
        let line = describe(&error, 0, 0).expect("an error diagnostic warrants a line");
        for part in ["error", "panic", "worker 3"] {
            assert!(line.contains(part), "missing {part}: {line}");
        }
    }

    #[test]
    fn the_fleet_events_render_their_lines() {
        let online = Event::InstanceOnline {
            tag: "sima-abc-1".to_string(),
            instance: "aabbccddeeff0011".to_string(),
            gpu_model: "RTX 4090".to_string(),
            gpu_count: 2,
            rate_microusd_hour: 412_000,
            host: "203.0.113.7".to_string(),
        };
        let line = describe(&online, 0, 0).expect("an online line");
        for part in [
            "instance online",
            "aabbccddeeff",
            "203.0.113.7",
            "2× RTX 4090",
            "$0.412",
        ] {
            assert!(line.contains(part), "missing {part}: {line}");
        }

        let deviceless = Event::InstanceOnline {
            tag: "sima-abc-1".to_string(),
            instance: "i0".to_string(),
            gpu_model: String::new(),
            gpu_count: 0,
            rate_microusd_hour: 0,
            host: "local".to_string(),
        };
        assert!(
            describe(&deviceless, 0, 0)
                .expect("a line")
                .contains("no GPU"),
            "a deviceless instance names no GPU"
        );

        let lost = Event::InstanceLost {
            tag: "t".to_string(),
            instance: "aabbccddeeff0011".to_string(),
        };
        assert!(
            describe(&lost, 0, 0)
                .expect("a lost line")
                .contains("instance lost aabbccddeeff")
        );

        let replaced = Event::InstanceReplaced {
            tag: "t".to_string(),
            from: "aaaaaaaaaaaa1111".to_string(),
            to: "bbbbbbbbbbbb2222".to_string(),
        };
        let line = describe(&replaced, 0, 0).expect("a replaced line");
        assert!(
            line.contains("aaaaaaaaaaaa") && line.contains("bbbbbbbbbbbb"),
            "{line}"
        );

        let spend = Event::BudgetSpendExhausted {
            accrued_microusd: 5_100_000,
            cap_microusd: 5_000_000,
        };
        let line = describe(&spend, 0, 0).expect("a spend line");
        assert!(
            line.contains("budget exhausted") && line.contains("$5.100") && line.contains("$5.000"),
            "{line}"
        );

        let wall = Event::BudgetWallClockExhausted {
            deadline_ms: 1_700_000_000_000,
        };
        assert!(
            describe(&wall, 0, 0)
                .expect("a wall-clock line")
                .contains("rental deadline")
        );
    }

    #[test]
    fn an_info_diagnostic_renders_nothing() {
        // Worker stderr is journaled, not echoed: the run's console output
        // stays clean.
        let info = diagnostic(sima_pipeline::Level::Info, "worker stderr", "starting up");
        assert!(describe(&info, 0, 0).is_none());
    }

    /// A zeroed status for a throwaway run; tests set the fields they assert.
    fn a_status() -> RunStatus {
        RunStatus::new(a_run())
    }

    #[test]
    fn the_status_block_names_every_field() {
        let mut status = a_status();
        status.tasks = 3;
        status.committed = 2;
        status.retried = 1;
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

    #[test]
    fn the_status_block_reports_the_run_s_device_composition() {
        let mut status = a_status();
        status.committed = 1000;
        status.devices = [
            ("Intel Arc 140T".to_string(), 388),
            ("NVIDIA RTX PRO 2000".to_string(), 612),
        ]
        .into_iter()
        .collect();
        status.rebound_chains = 2;
        let block = status_block(&status);
        // Busiest device first, whatever order the map holds.
        assert!(
            block.contains("devices              NVIDIA RTX PRO 2000 ×612, Intel Arc 140T ×388, rebound chains: 2"),
            "{block}"
        );
    }

    #[test]
    fn the_composition_shows_the_host_for_remote_pools() {
        // One device name on a local pool and a remote one: the remote entry
        // reads `device @ host`, the local one the plain name, busiest first.
        let mut status = a_status();
        status.committed = 1000;
        status.devices = [
            ("NVIDIA RTX PRO 2000".to_string(), 612),
            ("NVIDIA RTX PRO 2000 @ gpubox".to_string(), 388),
        ]
        .into_iter()
        .collect();
        let block = status_block(&status);
        assert!(
            block.contains(
                "devices              NVIDIA RTX PRO 2000 ×612, NVIDIA RTX PRO 2000 @ gpubox ×388"
            ),
            "{block}"
        );
    }

    #[test]
    fn a_run_that_moved_no_chain_reports_no_rebinds() {
        let mut status = a_status();
        status.devices = [("Intel Arc 140T".to_string(), 4)].into_iter().collect();
        let block = status_block(&status);
        assert!(
            block.contains("devices              Intel Arc 140T ×4"),
            "{block}"
        );
        assert!(!block.contains("rebound"), "{block}");
    }

    /// `text` with every run of spaces collapsed to one, so an assertion
    /// names the cells of a line rather than the padding between them.
    fn squeezed(text: &str) -> String {
        text.split_whitespace().collect::<Vec<&str>>().join(" ")
    }

    /// One attempt over the fields a row varies in; the rest are fixed.
    fn attempt(number: u32, worker: u64, result: AttemptResult, ended_ms: Option<u64>) -> Attempt {
        Attempt {
            attempt: number,
            worker,
            device: String::new(),
            host: String::new(),
            started_ms: 1_000,
            ended_ms,
            result,
            lease_expired: false,
        }
    }

    #[test]
    fn a_task_block_names_the_task_its_state_and_every_attempt() {
        let history = TaskHistory {
            task: "4e".repeat(32),
            queued_ms: Some(900),
            attempts: vec![
                attempt(
                    0,
                    0,
                    AttemptResult::Failed {
                        reason: "programmed flake".to_string(),
                    },
                    Some(1_400),
                ),
                attempt(1, 1, AttemptResult::Committed, Some(1_300)),
            ],
            outcome: TaskOutcome::Committed {
                record: "3f".repeat(32),
                stats: "attempt 1".to_string(),
            },
        };
        let block = task_block(&history);
        let squeezed = squeezed(&block);
        assert!(squeezed.contains("task 4e4e4e4e4e4e"), "{block}");
        assert!(squeezed.contains("state committed"), "{block}");
        // The column header, then one row per attempt: a deviceless local
        // worker renders its host and device as the placeholders.
        assert!(
            squeezed.contains("attempt worker host device outcome elapsed"),
            "{block}"
        );
        assert!(
            squeezed.contains("0 w0 — (none) failed 0.4s programmed flake"),
            "{block}"
        );
        assert!(squeezed.contains("1 w1 — (none) committed 0.3s"), "{block}");
        assert!(
            squeezed.contains("result 3f3f3f3f3f3f attempt 1"),
            "{block}"
        );
    }

    #[test]
    fn an_open_attempt_renders_no_elapsed_and_an_in_progress_state() {
        let history = TaskHistory {
            task: "aa".repeat(32),
            queued_ms: Some(0),
            attempts: vec![attempt(0, 2, AttemptResult::InFlight, None)],
            outcome: TaskOutcome::InProgress,
        };
        let block = task_block(&history);
        assert!(squeezed(&block).contains("state in progress"), "{block}");
        assert!(block.contains("in flight"), "{block}");
        assert!(!block.contains("result "), "no result yet: {block}");
    }

    #[test]
    fn a_rejected_task_block_states_the_attempt_and_reason() {
        let history = TaskHistory {
            task: "aa".repeat(32),
            queued_ms: Some(0),
            attempts: vec![attempt(
                2,
                0,
                AttemptResult::Rejected {
                    reason: "programmed rejection".to_string(),
                },
                Some(1_500),
            )],
            outcome: TaskOutcome::Rejected {
                attempt: 2,
                reason: "programmed rejection".to_string(),
            },
        };
        let block = task_block(&history);
        assert!(
            squeezed(&block).contains("state rejected on attempt 2: programmed rejection"),
            "{block}"
        );
    }

    #[test]
    fn a_preempted_attempt_notes_its_lease_expiry() {
        let mut expired = attempt(
            0,
            0,
            AttemptResult::Failed {
                reason: "lease expired".to_string(),
            },
            Some(1_500),
        );
        expired.lease_expired = true;
        let history = TaskHistory {
            task: "aa".repeat(32),
            queued_ms: None,
            attempts: vec![expired],
            outcome: TaskOutcome::InProgress,
        };
        assert!(
            task_block(&history).contains("(lease expired)"),
            "{}",
            task_block(&history)
        );
    }

    #[test]
    fn a_queued_task_block_names_no_attempt() {
        let history = TaskHistory {
            task: "aa".repeat(32),
            queued_ms: Some(5),
            attempts: Vec::new(),
            outcome: TaskOutcome::Queued,
        };
        let block = task_block(&history);
        assert!(squeezed(&block).contains("state queued"), "{block}");
        assert!(
            !squeezed(&block).contains("attempt worker"),
            "no table: {block}"
        );
    }

    #[test]
    fn an_attempt_on_a_remote_device_names_both() {
        let mut bound = attempt(0, 0, AttemptResult::Committed, Some(2_000));
        bound.device = "NVIDIA RTX PRO 2000".to_string();
        bound.host = "gpubox".to_string();
        let history = TaskHistory {
            task: "aa".repeat(32),
            queued_ms: None,
            attempts: vec![bound],
            outcome: TaskOutcome::Committed {
                record: "3f".repeat(32),
                stats: "attempt 0".to_string(),
            },
        };
        let block = task_block(&history);
        assert!(block.contains("gpubox"), "{block}");
        assert!(block.contains("NVIDIA RTX PRO 2000"), "{block}");
    }

    /// The run the digest tests render against.
    fn a_run() -> RunId {
        RunId::from_hash(sima_core::hash_bytes(b"a run to render"))
    }

    /// One task the run ended on a rejection.
    fn a_rejected_history() -> TaskHistory {
        TaskHistory {
            task: "7f".repeat(32),
            queued_ms: Some(0),
            attempts: vec![attempt(
                0,
                1,
                AttemptResult::Rejected {
                    reason: "programmed rejection".to_string(),
                },
                Some(1_500),
            )],
            outcome: TaskOutcome::Rejected {
                attempt: 0,
                reason: "programmed rejection".to_string(),
            },
        }
    }

    #[test]
    fn the_failure_digest_names_each_task_its_outcome_and_its_reason() {
        let block = failures_block(&a_run(), &[a_rejected_history()]);
        let squeezed = squeezed(&block);
        assert!(squeezed.contains("1 task did not commit"), "{block}");
        assert!(
            squeezed.contains("task final attempts worker reason"),
            "{block}"
        );
        assert!(
            squeezed.contains("7f7f7f7f7f7f rejected 1 w1 programmed rejection"),
            "{block}"
        );
    }

    #[test]
    fn a_faulted_task_is_named_by_its_error() {
        let history = TaskHistory {
            task: "aa".repeat(32),
            queued_ms: None,
            attempts: vec![attempt(
                3,
                0,
                AttemptResult::Faulted {
                    error: "executor died".to_string(),
                },
                Some(2_000),
            )],
            outcome: TaskOutcome::Faulted {
                attempt: 3,
                error: "executor died".to_string(),
            },
        };
        let block = squeezed(&failures_block(&a_run(), &[history]));
        assert!(block.contains("faulted"), "{block}");
        assert!(block.contains("executor died"), "{block}");
    }

    #[test]
    fn a_digest_of_no_failures_is_the_header_alone() {
        let block = failures_block(&a_run(), &[]);
        assert!(block.contains("0 tasks did not commit"), "{block}");
        assert!(!block.contains("reason"), "no table: {block}");
        assert_eq!(block.lines().count(), 1, "{block}");
    }

    #[test]
    fn the_digest_header_pluralizes_its_count() {
        let two = [a_rejected_history(), a_rejected_history()];
        assert!(
            failures_block(&a_run(), &two).contains("2 tasks did not commit"),
            "two failures read as tasks"
        );
    }

    #[test]
    fn a_run_that_names_no_device_renders_no_device_line() {
        // A journal carrying no WorkerBound events, as a run whose domain uses
        // no device writes: there is nothing truthful to print.
        let block = status_block(&a_status());
        assert!(!block.contains("devices"), "{block}");
    }

    /// One worker's metrics over the fields the tests vary; the rest are fixed.
    fn worker_metrics(
        worker: u64,
        host: &str,
        first_bind_ms: u64,
        busy_ms: u64,
        spans: Vec<(u64, u64)>,
    ) -> WorkerMetrics {
        WorkerMetrics {
            worker,
            device: String::new(),
            host: host.to_string(),
            first_bind_ms,
            respawns: 0,
            busy_ms,
            lifespan_ms: 134_000 - first_bind_ms,
            commits: 340,
            attempts: 351,
            spans,
        }
    }

    /// A run of a thousand tasks over two minutes and change: one worker alive
    /// from the start, one that took forty seconds to provision.
    fn a_timeline() -> RunTimeline {
        RunTimeline {
            run: a_run(),
            tasks: 1_000,
            committed: 1_000,
            session_committed: 1_000,
            session_start_ms: 0,
            session_end_ms: 134_000,
            retries: RetryStats {
                total_retries: 120,
                retried_tasks: 100,
                failed_attempts: 120,
                total_attempts: 1_120,
            },
            workers: vec![
                worker_metrics(0, "", 0, 100_000, vec![(0, 100_000)]),
                worker_metrics(2, "gpubox", 41_200, 60_000, vec![(41_200, 101_200)]),
            ],
            commit_times_ms: vec![1_000, 2_000, 2_500, 60_000, 133_000],
        }
    }

    #[test]
    fn the_timeline_block_names_the_run_its_wall_clock_and_its_throughput() {
        let squeezed = squeezed(&timeline_block(&a_timeline()));
        assert!(squeezed.contains(&format!("run {}", a_run())), "{squeezed}");
        assert!(squeezed.contains("wall-clock 2m14s"), "{squeezed}");
        assert!(squeezed.contains("committed 1000"), "{squeezed}");
        // A thousand commits over 134 seconds.
        assert!(squeezed.contains("throughput 7.5 task/s"), "{squeezed}");
    }

    #[test]
    fn each_retry_ratio_is_rendered_with_the_counts_it_is_taken_over() {
        // The three figures answer different questions and disagree on the same
        // run, so each states its own numerator and denominator in words.
        let squeezed = squeezed(&timeline_block(&a_timeline()));
        assert!(
            squeezed.contains("retries / tasks 120 / 1000 = 0.12 per task"),
            "{squeezed}"
        );
        assert!(
            squeezed.contains("tasks retried / tasks 100 / 1000 = 10.0%"),
            "{squeezed}"
        );
        assert!(
            squeezed.contains("failed attempts / attempts 120 / 1120 = 10.7%"),
            "{squeezed}"
        );
    }

    #[test]
    fn each_worker_row_states_its_spawn_respawns_utilization_and_counts() {
        let block = timeline_block(&a_timeline());
        let squeezed = squeezed(&block);
        assert!(
            squeezed.contains("worker device host spawn respawns util commits attempts"),
            "{block}"
        );
        // A local worker bound as the pool launched, and its device is one the
        // journal never named: both render as placeholders.
        assert!(
            squeezed.contains("w0 (none) — 0.00s 0 75% 340 351"),
            "{block}"
        );
        // The remote worker was provisioned forty seconds in, so it was alive
        // for less of the run and its utilization is over that lifespan.
        assert!(
            squeezed.contains("w2 (none) gpubox 41.20s 0 65% 340 351"),
            "{block}"
        );
    }

    /// The chart lines of a rendered block: the sparkline and the occupancy
    /// bars, as `(label, glyphs)` pairs. A chart line ends in exactly
    /// `CHART_WIDTH` glyphs, which is what tells it from a table row — the
    /// bar's own leading blanks make a separator unreliable.
    fn chart_lines(block: &str) -> Vec<(String, String)> {
        block
            .lines()
            .filter_map(|line| {
                let chars: Vec<char> = line.chars().collect();
                let label_width = chars.len().checked_sub(CHART_WIDTH)?;
                let (label, glyphs) = chars.split_at(label_width);
                glyphs
                    .iter()
                    .all(|glyph| " ▁▂▃▄▅▆▇█░".contains(*glyph))
                    .then(|| {
                        (
                            label.iter().collect::<String>().trim().to_string(),
                            glyphs.iter().collect::<String>(),
                        )
                    })
            })
            .collect()
    }

    #[test]
    fn the_chart_draws_one_fixed_width_line_per_worker_under_the_commits() {
        let block = timeline_block(&a_timeline());
        let lines = chart_lines(&block);
        let labels: Vec<&str> = lines.iter().map(|(label, _)| label.as_str()).collect();
        assert_eq!(labels, vec!["commits", "w0", "w2"], "{block}");
        for (label, glyphs) in &lines {
            assert_eq!(
                glyphs.chars().count(),
                CHART_WIDTH,
                "{label} spans the fixed axis: {block}"
            );
        }
    }

    #[test]
    fn a_late_bound_workers_bar_begins_with_the_blanks_its_spawn_gap_spans() {
        // Forty-one seconds of a 134-second run: the provisioning gap is drawn
        // to scale as columns where the worker did not yet exist.
        let block = timeline_block(&a_timeline());
        let lines = chart_lines(&block);
        let (_, remote) = lines.iter().find(|(label, _)| label == "w2").expect("w2");
        let blanks = remote.chars().take_while(|glyph| *glyph == ' ').count();
        assert_eq!(blanks, 14, "{block}");
        let (_, local) = lines.iter().find(|(label, _)| label == "w0").expect("w0");
        assert!(
            !local.starts_with(' '),
            "a worker alive from the start has no gap: {block}"
        );
    }

    #[test]
    fn the_spend_block_reports_closed_entries_open_rentals_and_the_total() {
        let report = SpendReport {
            entries: vec![sima_pipeline::SpendEntry {
                tag: "sima-run-1".to_string(),
                provider: "vast".to_string(),
                owner: "ab".repeat(32),
                price_micro_usd_hour: 412_000,
                started_ms: 1_000,
                ended_ms: 3_601_000,
                cost_micro_usd: 412_000,
            }],
            open: vec![sima_pipeline::OpenSpend {
                tag: "sima-run-2".to_string(),
                rate: sima_pipeline::Price(500_000),
                started_ms: 2_000,
                accrued: sima_pipeline::Cost(250_000),
            }],
            total: sima_pipeline::Cost(662_000),
        };
        let block = spend_block(&report);
        // A closed entry names its tag, duration, rate, and cost.
        assert!(block.contains("closed rentals   1"), "{block}");
        assert!(block.contains("sima-run-1"), "{block}");
        assert!(block.contains("$0.412/hr"), "{block}");
        // An open rental names its accrual so far.
        assert!(block.contains("open rentals     1"), "{block}");
        assert!(block.contains("sima-run-2"), "{block}");
        assert!(block.contains("$0.250 so far"), "{block}");
        // And the total in dollars.
        assert!(block.contains("total            $0.662"), "{block}");
    }

    #[test]
    fn the_machines_block_names_each_machine_its_counts_and_its_status() {
        let report = MachineReport {
            machines: vec![
                sima_pipeline::MachineSummary {
                    provider: "vastai".to_string(),
                    machine: "81234".to_string(),
                    incidents: 3,
                    lost: 2,
                    never_ready: 1,
                    probe_failed: 0,
                    install_failed: 0,
                    first_occurred_ms: 10,
                    last_occurred_ms: 30,
                    blacklisted: true,
                },
                sima_pipeline::MachineSummary {
                    provider: "vastai".to_string(),
                    machine: "90000".to_string(),
                    incidents: 1,
                    lost: 0,
                    never_ready: 0,
                    probe_failed: 1,
                    install_failed: 0,
                    first_occurred_ms: 5,
                    last_occurred_ms: 5,
                    blacklisted: false,
                },
            ],
        };
        let block = machines_block(&report);
        assert!(
            block.contains("machines with incidents   2   blacklisted   1"),
            "{block}"
        );
        // The blacklisted machine names its counts by kind and its status.
        assert!(
            block.contains(
                "vastai-81234  3 incidents (lost 2, never-ready 1, probe-failed 0, \
                 install-failed 0)  blacklisted"
            ),
            "{block}"
        );
        // A machine below the threshold names no status, and its single
        // incident reads in the singular.
        assert!(
            block.contains(
                "vastai-90000  1 incident (lost 0, never-ready 0, probe-failed 1, \
                 install-failed 0)"
            ),
            "{block}"
        );
        assert!(
            !block.contains(
                "vastai-90000  1 incident (lost 0, never-ready 0, probe-failed 1)  blacklisted"
            ),
            "an untainted machine is not blacklisted: {block}"
        );
    }

    #[test]
    fn the_machines_block_over_a_clean_store_states_no_incidents() {
        let report = MachineReport {
            machines: Vec::new(),
        };
        assert_eq!(machines_block(&report), "no machine incidents recorded");
    }

    #[test]
    fn a_session_of_no_span_renders_its_scalars_without_a_chart() {
        // A run that journaled its start and nothing since: there is no axis to
        // bucket, and no rate to take over a zero denominator.
        let timeline = RunTimeline {
            run: a_run(),
            tasks: 4,
            committed: 0,
            session_committed: 0,
            session_start_ms: 1_000,
            session_end_ms: 1_000,
            retries: RetryStats::default(),
            workers: vec![worker_metrics(0, "", 1_000, 0, Vec::new())],
            commit_times_ms: Vec::new(),
        };
        let block = timeline_block(&timeline);
        let squeezed = squeezed(&block);
        assert!(squeezed.contains("throughput —"), "{block}");
        assert!(
            squeezed.contains("failed attempts / attempts 0 / 0 = —"),
            "{block}"
        );
        assert!(chart_lines(&block).is_empty(), "no axis to draw: {block}");
    }
}
