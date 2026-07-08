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
use sima_pipeline::{RunControl, RunOutcome, RunStatus, load, orchestrate, status};

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
        ["tui", config] => tui::tui_command(&resolve_config(config)),
        _ => {
            eprint!(
                "usage: sima run <config>     drive the configured run\n\
                 \x20      sima status <config>  report the run's state\n\
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

/// Prints `error` to stderr and yields the generic error exit code.
fn report(error: Error) -> ExitCode {
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
