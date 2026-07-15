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
        ["report", config] => report_command(&resolve_config(config)),
        ["rm", config] => rm_command(&resolve_config(config)),
        ["tui", config] => tui::tui_command(&resolve_config(config)),
        _ => {
            eprint!(
                "usage: sima run <config>     drive the configured run\n\
                 \x20      sima status <config>  report the run's state\n\
                 \x20      sima report <config>  print each committed task's stats\n\
                 \x20      sima rm <config>      delete the run's exclusive closure\n\
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

    // Seed the progress counter from the store's prior commits, so a resumed
    // run counts on from where it stopped instead of appearing to restart.
    let seed = seed_status(&loaded)?;
    println!("run {}", loaded.run.id());
    let progress = render::Progress::new(seed.committed);
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

/// Seeds a session's display from any existing journal for `config`'s run,
/// replaying it through the same `apply` method `sima status` uses so a
/// resumed run shows its prior progress. Shared by `run`'s progress renderer
/// and the tui.
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

/// `sima report <config.toml>`: prints one line per committed task —
/// `<short task key>  <rendered stats>`. The store and run id come from the
/// config the same way `status` derives them.
fn report_command(config: &Path) -> ExitCode {
    match read_report(config) {
        Ok(rows) => {
            let stdout = std::io::stdout();
            let mut out = stdout.lock();
            match write_report(&mut out, &rows) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => report(e),
            }
        }
        Err(e) => report(e),
    }
}

/// Writes one line per reported task — `<short task key>  <rendered stats>` —
/// to `out`, taken locked once by the caller. `report` emits a line per
/// committed task, so piping into a reader that closes early (`sima report
/// ... | head`) is ordinary use: the resulting `BrokenPipe` is that reader's
/// normal exit and maps to success. Any other write failure is an
/// infrastructure fault against stdout.
fn write_report(out: &mut impl std::io::Write, rows: &[ReportRow]) -> Result<()> {
    for row in rows {
        match writeln!(out, "{}  {}", render::short(&row.task), row.stats) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            Err(e) => {
                return Err(Error::Io {
                    path: PathBuf::from("stdout"),
                    source: e,
                });
            }
        }
    }
    Ok(())
}

/// Loads the config and renders each committed task's stats from its journal.
fn read_report(config: &Path) -> Result<Vec<ReportRow>> {
    let loaded = load(config)?;
    sima_pipeline::report(&loaded)
}

/// `sima rm <config.toml>`: deletes the run's exclusive closure under its run
/// lock and prints what was removed. The run id comes from the config's
/// identity section, as `status` derives it.
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

    /// One report row to write.
    fn a_row() -> ReportRow {
        ReportRow {
            task: "aa".to_string(),
            stats: "attempt 0".to_string(),
        }
    }

    #[test]
    fn a_broken_pipe_while_writing_the_report_is_success() {
        // A reader closing the pipe (`sima report ... | head`) surfaces as
        // BrokenPipe; that is its normal exit, so the write reports success.
        let mut sink = FailingWriter(std::io::ErrorKind::BrokenPipe);
        assert!(write_report(&mut sink, &[a_row()]).is_ok());
    }

    #[test]
    fn any_other_stdout_write_failure_is_reported() {
        let mut sink = FailingWriter(std::io::ErrorKind::PermissionDenied);
        assert!(matches!(
            write_report(&mut sink, &[a_row()]),
            Err(Error::Io { .. })
        ));
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
