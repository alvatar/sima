//! `sima` command-line binary: `run` drives a config to its outcome with
//! live progress and graceful Ctrl-C; `status` reports a run's journal
//! state. All orchestration lives in `sima-pipeline` — this binary parses
//! arguments, renders output, registers the interrupt flag, and maps
//! outcomes to exit codes:
//!
//! - 0 — the run finalized (or `status` answered);
//! - 2 — a definitive candidate failure;
//! - 130 — interrupted by Ctrl-C, store resumable;
//! - 1 — everything else: infrastructure fault, config error, usage error.

mod render;
mod tui;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use sima_core::{Error, Result};
use sima_pipeline::{
    LoadedConfig, RemovalReport, ReportRow, RunControl, RunOutcome, RunStatus, load, orchestrate,
    status,
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
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["run", config] => run_command(&resolve_config(config)),
        ["status", config] => status_command(&resolve_config(config)),
        ["report", config] => report_command(&resolve_config(config), false),
        ["report", "--full", config] => report_command(&resolve_config(config), true),
        ["rm", config] => rm_command(&resolve_config(config)),
        ["tui", config] => tui::tui_command(&resolve_config(config)),
        _ => {
            eprint!(
                "usage: sima run <config>     drive the configured run\n\
                 \x20      sima status <config>  report the run's state\n\
                 \x20      sima report <config>  count committed tasks per distinct stats value\n\
                 \x20      sima report --full <config>  print each committed task's stats\n\
                 \x20      sima rm <config>      delete the run and what only it references\n\
                 \x20      sima tui <config>     drive the run in a full-screen terminal UI\n\
                 \x20      <config> is a sima.toml path; the .toml extension may be omitted\n"
            );
            ExitCode::from(EXIT_ERROR)
        }
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
        observer: &|event| progress.event(event),
        interrupt: &interrupt,
    };
    orchestrate(&loaded, &control)
}

/// `sima status <config.toml>`: the config's execution section names the
/// store, its identity section derives the run id.
fn status_command(config: &Path) -> ExitCode {
    match read_status(config) {
        Ok(report) => {
            println!("{}", render::status_block(&report));
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

/// Loads the config and computes the run's status from its journal.
fn read_status(config: &Path) -> Result<RunStatus> {
    let loaded = load(config)?;
    status(&loaded)
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

/// `sima report [--full] <config.toml>`: renders the run's committed stats.
/// The default is the compact summary — a total header, then one line per
/// distinct rendered stats value with its count; `--full` prints one
/// `<short task key>  <rendered stats>` line per task. The store and run id
/// come from the config the same way `status` derives them.
fn report_command(config: &Path, full: bool) -> ExitCode {
    match read_report(config) {
        Ok(rows) => {
            let stdout = std::io::stdout();
            let mut out = stdout.lock();
            let written = if full {
                write_report(&mut out, &rows)
            } else {
                write_summary(&mut out, &rows)
            };
            match written {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => report(e),
            }
        }
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

/// Loads the config and renders each committed task's stats from its journal.
fn read_report(config: &Path) -> Result<Vec<ReportRow>> {
    let loaded = load(config)?;
    sima_pipeline::report(&loaded)
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
