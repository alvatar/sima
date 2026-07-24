//! `sima` command-line binary. `run` drives a config to its outcome with
//! live progress and graceful Ctrl-C; the query commands read the run's
//! journal along two axes — what the command reports, and how much of the
//! run it covers:
//!
//! - `status` reports execution: the run's state and counters, one task's
//!   attempt timeline under `--task <key>`, or the tasks that did not commit
//!   under `--failed`.
//! - `report` reports results: the committed stats, grouped by default, one
//!   line per task under `--all`, or one task's under `--task <key>`.
//! - `timeline` reports efficiency: the run's throughput, retry rates, and
//!   per-worker utilization, over a chart of commits and worker occupancy.
//!
//! A `<key>` is any prefix of a task key that names one task. All
//! orchestration lives in `sima-pipeline` — this binary parses arguments,
//! renders output, registers the interrupt flag, and maps outcomes to exit
//! codes:
//!
//! - 0 — the run finalized (or `status` answered);
//! - 2 — a definitive candidate failure;
//! - 130 — interrupted by Ctrl-C, store resumable;
//! - 1 — everything else: infrastructure fault, config error, usage error.

mod follow;
mod reconcile;
mod render;
mod tui;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use sima_core::{Error, Result};
use sima_pipeline::{
    FeedInfo, LoadedConfig, LocalFeed, Record, RemoteFeed, RemovalReport, ReportRow, RunControl,
    RunFeed, RunId, RunOutcome, RunStatus, RunTimeline, TaskHistory, failures_records,
    follow_serve, load, local_snapshot, orchestrate, remote_snapshot, report_records,
    report_task_records, status, status_records, task_history_records, timeline_records,
};

/// Exit code for a definitive candidate failure.
pub(crate) const EXIT_FAILED: u8 = 2;
/// Exit code for a run wound down by an interrupt, matching the shell
/// convention for death by SIGINT.
pub(crate) const EXIT_INTERRUPTED: u8 = 130;
/// Exit code for everything else that is not success: infrastructure
/// fault, config error, usage error.
pub(crate) const EXIT_ERROR: u8 = 1;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (args, host) = split_target(&args);
    match args[..] {
        // The write commands never observe: `run` drives a run, which happens
        // where the hardware is, and `rm` and `reconcile` mutate a store. A
        // host on any of them falls through to the usage error.
        ["run", config] if host.is_none() => run_command(&resolve_config(config)),
        ["rm", config] if host.is_none() => rm_command(&resolve_config(config)),
        ["reconcile", config] if host.is_none() => {
            reconcile::reconcile_command(&resolve_config(config))
        }
        // The far half of the follow transport, invoked over ssh by another
        // machine's read command. It is not a user-facing verb.
        ["follow-serve", config] if host.is_none() => serve_command(config, false),
        ["follow-serve", config, "--once"] if host.is_none() => serve_command(config, true),
        ["status", config] => status_command(&Target::new(config, host)),
        ["status", config, "--failed"] => status_failed_command(&Target::new(config, host)),
        ["status", config, "--task", key] => status_task_command(&Target::new(config, host), key),
        ["report", config] => report_command(&Target::new(config, host), Report::Summary),
        ["report", config, "--all"] => report_command(&Target::new(config, host), Report::All),
        ["report", config, "--task", key] => report_task_command(&Target::new(config, host), key),
        ["timeline", config] => timeline_command(&Target::new(config, host)),
        ["tui", config] => tui::tui_command(&Target::new(config, host)),
        ["follow", config] => follow::follow_command(&Target::new(config, host)),
        _ => {
            eprint!(
                "usage: sima run <config>                  drive the configured run\n\
                 \x20      sima status <config>               report the run's state\n\
                 \x20      sima status <config> --task <key>  print one task's attempt timeline\n\
                 \x20      sima status <config> --failed      digest the tasks that did not commit\n\
                 \x20      sima report <config>               count committed tasks per distinct stats value\n\
                 \x20      sima report <config> --all         print each committed task's stats\n\
                 \x20      sima report <config> --task <key>  print one committed task's stats\n\
                 \x20      sima rm <config>                   delete the run and what only it references\n\
                 \x20      sima reconcile <config>            destroy the machines a crashed run left running\n\
                 \x20      sima tui <config>                  drive the run in a full-screen terminal UI\n\
                 \x20      sima follow <config>               stream the run's events until it ends\n\
                 \x20      sima timeline <config>             report the run's metrics and its timeline\n\
                 \x20      <config> is a sima.toml path; the .toml extension may be omitted\n\
                 \x20      <key> is any prefix of a task key that names one task\n\
                 \x20      --on <host> observes a run on an ssh destination: status, report,\n\
                 \x20      timeline, tui, and follow accept it, and <config> is then a path\n\
                 \x20      on that host\n"
            );
            ExitCode::from(EXIT_ERROR)
        }
    }
}

/// Splits `--on <host>` out of the arguments, wherever in them it appears,
/// returning the rest and the host it named. The commands match on the rest,
/// so every command form keeps its exact shape whether or not a host is set.
///
/// A trailing `--on` with nothing after it names no host and stays in the
/// remaining arguments, where it matches no command form and falls to the
/// usage error. A repeated `--on` takes the last host given.
fn split_target(args: &[String]) -> (Vec<&str>, Option<&str>) {
    let mut rest = Vec::new();
    let mut host = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--on" if index + 1 < args.len() => {
                host = Some(args[index + 1].as_str());
                index += 2;
            }
            arg => {
                rest.push(arg);
                index += 1;
            }
        }
    }
    (rest, host)
}

/// The run a read command addresses: one on this machine, or one on the host
/// its orchestrator runs on.
///
/// A run's identity is the hash of its config, and its store path resolves
/// relative to the config file's directory, so a remote target carries the
/// config argument unresolved: it names a path on the far side, and the far
/// side is what interprets it.
enum Target {
    /// A config file on this machine, resolved as written.
    Local(PathBuf),
    /// A config file on `host`, as typed.
    Remote {
        /// The ssh destination the local ssh client resolves.
        host: String,
        /// The config argument, passed through to that host verbatim.
        config: String,
    },
}

impl Target {
    /// The target a command's config argument and optional host name.
    fn new(config: &str, host: Option<&str>) -> Target {
        match host {
            None => Target::Local(resolve_config(config)),
            Some(host) => Target::Remote {
                host: host.to_string(),
                config: config.to_string(),
            },
        }
    }
}

/// Opens a live feed over the target's run: the journal on this machine, or
/// one follow stream from the host the orchestrator runs on. The views that
/// tail a run consume the feed and never learn which it is.
fn feed(target: &Target) -> Result<Box<dyn RunFeed>> {
    match target {
        Target::Local(path) => Ok(Box::new(LocalFeed::open(&load(path)?)?)),
        Target::Remote { host, config } => Ok(Box::new(RemoteFeed::open(host, config)?)),
    }
}

/// Reads everything the target's run journaled, with the metadata the views
/// render through: locally through the store, remotely over one follow
/// stream. The fold that renders it is the same either way.
fn snapshot(target: &Target) -> Result<(FeedInfo, Vec<Record>)> {
    match target {
        Target::Local(path) => local_snapshot(&load(path)?),
        Target::Remote { host, config } => remote_snapshot(host, config),
    }
}

/// Resolves the config argument to a path: the argument as given when it
/// names a file, otherwise the argument with `.toml` appended when that
/// names one — so `sima run demo` finds `demo.toml`. When neither exists,
/// the argument passes through unchanged and loading reports the error
/// against what the user typed.
fn resolve_config(arg: &str) -> PathBuf {
    let path = PathBuf::from(arg);
    if !path.is_file() {
        let with_toml = PathBuf::from(format!("{arg}.toml"));
        if with_toml.is_file() {
            return with_toml;
        }
    }
    path
}

/// `sima run <config.toml>`: loads, prints the run id, orchestrates with
/// progress rendering and the SIGINT flag installed, and maps the outcome
/// to the exit code.
fn run_command(config: &Path) -> ExitCode {
    match drive(config) {
        Ok(outcome) => ExitCode::from(outcome_exit_code(&outcome)),
        Err(e) => report(e),
    }
}

/// Loads the config and drives its run. The interrupt flag is registered
/// before any output, so Ctrl-C is graceful from the first line on; a
/// second Ctrl-C falls through to default death — which is safe, since
/// that is exactly the crash the recovery guarantees cover.
fn drive(config: &Path) -> Result<RunOutcome> {
    let loaded = load(config)?;
    let interrupt = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register_conditional_default(signal_hook::consts::SIGINT, interrupt.clone())
        .map_err(register_error)?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, interrupt.clone())
        .map_err(register_error)?;

    println!("run {}", loaded.run.id());
    // The run's own `RunStarted` carries the prior commits, counted from the
    // store, so a resumed run counts on from where it stopped.
    let progress = render::Progress::new();
    let control = RunControl {
        observer: &|record| progress.event(record),
        interrupt: &interrupt,
    };
    orchestrate(&loaded, &control)
}

/// `sima status <config.toml>`: the config's execution section names the
/// store, its identity section derives the run id.
fn status_command(target: &Target) -> ExitCode {
    match read_status(target) {
        Ok(report) => {
            println!("{}", render::status_block(&report));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Computes the target run's status from the records it journaled.
fn read_status(target: &Target) -> Result<RunStatus> {
    let (info, records) = snapshot(target)?;
    Ok(status_records(info.run, &records))
}

/// `sima timeline <config.toml>`: the run's execution metrics and the
/// temporal shape of the session behind them. The query answers whatever the
/// run's own outcome was, so a report over a failed run still exits 0.
fn timeline_command(target: &Target) -> ExitCode {
    match read_timeline(target) {
        Ok(timeline) => {
            println!("{}", render::timeline_block(&timeline));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Computes the target run's metrics from the records it journaled.
fn read_timeline(target: &Target) -> Result<RunTimeline> {
    let (info, records) = snapshot(target)?;
    Ok(timeline_records(info.run, &records))
}

/// `sima status <config.toml> --task <key>`: one task's attempt timeline,
/// addressed by a prefix of its key. The store and run id come from the
/// config the same way the aggregate status derives them.
fn status_task_command(target: &Target, prefix: &str) -> ExitCode {
    match read_task_history(target, prefix) {
        Ok(history) => {
            println!("{}", render::task_block(&history));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Projects one task's lifecycle from the records the target run journaled.
fn read_task_history(target: &Target, prefix: &str) -> Result<TaskHistory> {
    let (_info, records) = snapshot(target)?;
    task_history_records(&records, prefix)
}

/// `sima status <config.toml> --failed`: the tasks the run did not commit,
/// one line each. The query answers whatever the run's own outcome was, so a
/// digest over a failed run still exits 0.
fn status_failed_command(target: &Target) -> ExitCode {
    match read_failures(target) {
        Ok((run, failures)) => {
            println!("{}", render::failures_block(&run, &failures));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Projects the tasks the target run did not commit, with the run the digest
/// names.
fn read_failures(target: &Target) -> Result<(RunId, Vec<TaskHistory>)> {
    let (info, records) = snapshot(target)?;
    Ok((info.run, failures_records(&records)))
}

/// Seeds the tui's display from any existing journal for `config`'s run,
/// replaying it through the same `apply` method `sima status` uses so a
/// resumed run opens on its prior progress. This is the observational view:
/// it reports what the journal says, which is the whole of what `sima status`
/// answers too.
///
/// A store that does not exist yet, or a run never driven, seeds a zeroed
/// status; a corrupt journal or an I/O fault is a real problem `sima status`
/// reports, so it surfaces here rather than hiding behind wrong counts.
pub(crate) fn seed_status(config: &LoadedConfig) -> Result<RunStatus> {
    match status(config) {
        Ok(mut seeded) => {
            // The counters and last state are worth seeding, but a journal
            // ending mid-run leaves leases no live worker holds; a fresh
            // session starts with every worker idle and repopulates occupancy
            // from live `Leased` events.
            seeded.occupancy.clear();
            Ok(seeded)
        }
        Err(Error::Validation(_)) => Ok(RunStatus::new(config.run.id())),
        Err(other) => Err(other),
    }
}

/// How much of a run's committed stats `sima report` prints.
enum Report {
    /// A total header, then one line per distinct rendered stats value with
    /// its count.
    Summary,
    /// One `<short task key>  <rendered stats>` line per committed task.
    All,
}

/// `sima report [--all] <config.toml>`: renders the run's committed stats,
/// compactly by default. The store and run id come from the config the same
/// way `status` derives them.
fn report_command(target: &Target, scope: Report) -> ExitCode {
    match read_report(target) {
        Ok(rows) => write_rows(&rows, scope),
        Err(e) => report(e),
    }
}

/// `sima report <config.toml> --task <key>`: one committed task's stats,
/// addressed by a prefix of its key.
fn report_task_command(target: &Target, prefix: &str) -> ExitCode {
    match read_report_task(target, prefix) {
        Ok(row) => write_rows(&[row], Report::All),
        Err(e) => report(e),
    }
}

/// Writes `rows` to stdout, taken locked once, in the form `scope` names.
fn write_rows(rows: &[ReportRow], scope: Report) -> ExitCode {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let written = match scope {
        Report::Summary => write_summary(&mut out, rows),
        Report::All => write_report(&mut out, rows),
    };
    match written {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => report(e),
    }
}

/// Maps one report line's write outcome: `Ok(true)` when written, `Ok(false)`
/// when the reader closed the pipe, `Err` otherwise. Piping into a reader
/// that closes early (`sima report ... | head`) is ordinary use, so the
/// resulting `BrokenPipe` is that reader's normal exit — the caller stops
/// writing and reports success. Any other write failure is an infrastructure
/// fault against stdout.
fn line_written(result: std::io::Result<()>) -> Result<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(false),
        Err(e) => Err(Error::Io {
            path: PathBuf::from("stdout"),
            source: e,
        }),
    }
}

/// Writes one line per reported task — `<short task key>  <rendered stats>` —
/// to `out`, taken locked once by the caller.
fn write_report(out: &mut impl std::io::Write, rows: &[ReportRow]) -> Result<()> {
    for row in rows {
        if !line_written(writeln!(out, "{}  {}", render::short(&row.task), row.stats))? {
            return Ok(());
        }
    }
    Ok(())
}

/// Writes the compact summary to `out`, taken locked once by the caller: a
/// `<total> committed tasks` header, then one `<count>  <stats>` line per
/// distinct rendered stats value.
fn write_summary(out: &mut impl std::io::Write, rows: &[ReportRow]) -> Result<()> {
    if !line_written(writeln!(out, "{} committed tasks", rows.len()))? {
        return Ok(());
    }
    for (count, stats) in group_stats(rows) {
        if !line_written(writeln!(out, "{count}  {stats}"))? {
            return Ok(());
        }
    }
    Ok(())
}

/// Groups report rows by their rendered stats value: one entry per distinct
/// value with its task count, ordered by count descending, ties by the stats
/// string ascending — so the summary is deterministic.
fn group_stats(rows: &[ReportRow]) -> Vec<(usize, &str)> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for row in rows {
        *counts.entry(&row.stats).or_default() += 1;
    }
    let mut groups: Vec<(usize, &str)> = counts
        .into_iter()
        .map(|(stats, count)| (count, stats))
        .collect();
    // The map iterates by stats ascending; the stable sort by count descending
    // keeps that as the order among equal counts.
    groups.sort_by_key(|&(count, _)| std::cmp::Reverse(count));
    groups
}

/// Renders each committed task's stats from the records the target run
/// journaled.
fn read_report(target: &Target) -> Result<Vec<ReportRow>> {
    let (_info, records) = snapshot(target)?;
    report_records(&records)
}

/// Renders one committed task's stats from the records the target run
/// journaled.
fn read_report_task(target: &Target, prefix: &str) -> Result<ReportRow> {
    let (_info, records) = snapshot(target)?;
    report_task_records(&records, prefix)
}

/// `sima follow-serve <config> [--once]`: writes the run's follow stream to
/// stdout, which carries frames and nothing else — every diagnostic goes to
/// stderr, which ssh keeps on its own channel. The near half of the transport
/// spawns this over ssh; it is not a user-facing verb and stays out of the
/// usage text.
fn serve_command(config: &str, once: bool) -> ExitCode {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    match follow_serve(&resolve_config(config), once, &mut out) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => report(e),
    }
}

/// `sima rm <config.toml>`: deletes the run — and everything no surviving run
/// references — under its run lock, and prints what was removed. The run id
/// comes from the config's identity section, as `status` derives it.
fn rm_command(config: &Path) -> ExitCode {
    match remove_run(config) {
        Ok(report) => {
            println!(
                "removed run: {} objects, {} index entries",
                report.objects_removed, report.index_entries_removed
            );
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Loads the config and removes its run.
fn remove_run(config: &Path) -> Result<RemovalReport> {
    let loaded = load(config)?;
    sima_pipeline::remove(&loaded)
}

/// Prints `error` to stderr and yields the generic error exit code.
pub(crate) fn report(error: Error) -> ExitCode {
    eprintln!("sima: {error}");
    ExitCode::from(EXIT_ERROR)
}

/// Wraps a signal-registration failure: an OS-level refusal to install
/// the handler, surfaced before the run starts.
fn register_error(e: std::io::Error) -> Error {
    Error::Validation(format!("cannot register the SIGINT handler: {e}"))
}

/// The exit code a finished run maps to — the mapping `run` and `tui` share:
/// success when finalized, the failure code for a definitive candidate
/// failure, and the interrupt code for a wound-down run.
pub(crate) fn outcome_exit_code(outcome: &RunOutcome) -> u8 {
    match outcome {
        RunOutcome::Finalized { .. } => 0,
        RunOutcome::Failed { .. } => EXIT_FAILED,
        RunOutcome::Interrupted { .. } => EXIT_INTERRUPTED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::hash_bytes;
    use sima_model::{RunId, TaskKey};

    /// A writer that fails every write with a fixed error kind, to drive
    /// `write_report`'s error handling without a real pipe.
    struct FailingWriter(std::io::ErrorKind);

    impl std::io::Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(self.0))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// One report row over the given task key and rendered stats.
    fn row(task: &str, stats: &str) -> ReportRow {
        ReportRow {
            task: task.to_string(),
            stats: stats.to_string(),
        }
    }

    #[test]
    fn a_broken_pipe_while_writing_the_report_is_success() {
        // A reader closing the pipe (`sima report ... | head`) surfaces as
        // BrokenPipe; that is its normal exit, so the write reports success.
        let mut sink = FailingWriter(std::io::ErrorKind::BrokenPipe);
        assert!(write_report(&mut sink, &[row("aa", "attempt 0")]).is_ok());
        assert!(write_summary(&mut sink, &[row("aa", "attempt 0")]).is_ok());
    }

    #[test]
    fn any_other_stdout_write_failure_is_reported() {
        let mut sink = FailingWriter(std::io::ErrorKind::PermissionDenied);
        assert!(matches!(
            write_report(&mut sink, &[row("aa", "attempt 0")]),
            Err(Error::Io { .. })
        ));
        assert!(matches!(
            write_summary(&mut sink, &[row("aa", "attempt 0")]),
            Err(Error::Io { .. })
        ));
    }

    #[test]
    fn grouping_orders_by_count_descending_then_stats_ascending() {
        // "a" and "c" tie at two rows each: the tie breaks on the stats string,
        // ascending, so equal counts render in a deterministic order.
        let rows = [
            row("1", "c"),
            row("2", "a"),
            row("3", "c"),
            row("4", "b"),
            row("5", "a"),
        ];
        assert_eq!(group_stats(&rows), vec![(2, "a"), (2, "c"), (1, "b")]);
    }

    #[test]
    fn the_summary_prints_the_header_then_grouped_lines() {
        let rows = [
            row("aa", "attempt 1"),
            row("bb", "attempt 0"),
            row("cc", "attempt 0"),
        ];
        let mut out = Vec::new();
        write_summary(&mut out, &rows).expect("write");
        assert_eq!(
            String::from_utf8(out).expect("utf-8"),
            "3 committed tasks\n2  attempt 0\n1  attempt 1\n"
        );
    }

    /// Splits an argument list given as string slices.
    fn split(args: &[&str]) -> (Vec<String>, Option<String>) {
        let args: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        let (rest, host) = split_target(&args);
        (
            rest.into_iter().map(str::to_string).collect(),
            host.map(str::to_string),
        )
    }

    #[test]
    fn a_host_leaves_every_command_form_intact() {
        // The commands match on the rest, so extracting the pair — from any
        // position — must leave exactly the argument list they already match.
        let (rest, host) = split(&["status", "exp.toml", "--task", "ab", "--on", "gpubox"]);
        assert_eq!(rest, ["status", "exp.toml", "--task", "ab"]);
        assert_eq!(host.as_deref(), Some("gpubox"));

        let (rest, host) = split(&["status", "--on", "gpubox", "exp.toml", "--failed"]);
        assert_eq!(rest, ["status", "exp.toml", "--failed"]);
        assert_eq!(host.as_deref(), Some("gpubox"));
    }

    #[test]
    fn arguments_without_a_host_pass_through_unchanged() {
        let (rest, host) = split(&["report", "exp.toml", "--all"]);
        assert_eq!(rest, ["report", "exp.toml", "--all"]);
        assert_eq!(host, None);
    }

    #[test]
    fn a_trailing_host_flag_names_no_host_and_stays_in_the_arguments() {
        // Left in place, it matches no command form and falls to the usage
        // error, rather than silently reading as a local command.
        let (rest, host) = split(&["status", "exp.toml", "--on"]);
        assert_eq!(rest, ["status", "exp.toml", "--on"]);
        assert_eq!(host, None);
    }

    #[test]
    fn a_repeated_host_flag_takes_the_last_host() {
        let (rest, host) = split(&["status", "exp.toml", "--on", "a", "--on", "b"]);
        assert_eq!(rest, ["status", "exp.toml"]);
        assert_eq!(host.as_deref(), Some("b"));
    }

    #[test]
    fn each_outcome_maps_to_its_exit_code() {
        let run = RunId::from_hash(hash_bytes(b"exit code run"));
        assert_eq!(outcome_exit_code(&RunOutcome::Finalized { run }), 0);
        assert_eq!(
            outcome_exit_code(&RunOutcome::Failed {
                task: TaskKey::from_hash(hash_bytes(b"a task")),
                reason: "rejected".to_string(),
            }),
            EXIT_FAILED
        );
        assert_eq!(
            outcome_exit_code(&RunOutcome::Interrupted { run }),
            EXIT_INTERRUPTED
        );
    }
}
