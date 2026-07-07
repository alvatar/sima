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

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use sima_core::{Error, Result};
use sima_pipeline::{RunControl, RunOutcome, RunStatus, load, orchestrate, status};
use sima_store::Store;

/// Exit code for a definitive candidate failure.
const EXIT_FAILED: u8 = 2;
/// Exit code for a run wound down by Ctrl-C, matching the shell convention
/// for death by SIGINT.
const EXIT_INTERRUPTED: u8 = 130;
/// Exit code for everything else that is not success: infrastructure
/// fault, config error, usage error.
const EXIT_ERROR: u8 = 1;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["run", config] => run_command(Path::new(config)),
        ["status", config] => status_command(Path::new(config)),
        _ => {
            eprint!(
                "usage: sima run <config.toml>     drive the configured run\n\
                 \x20      sima status <config.toml>  report the run's state\n"
            );
            ExitCode::from(EXIT_ERROR)
        }
    }
}

/// `sima run <config.toml>`: loads, prints the run id, orchestrates with
/// progress rendering and the SIGINT flag installed, and maps the outcome
/// to the exit code.
fn run_command(config: &Path) -> ExitCode {
    match drive(config) {
        Ok(RunOutcome::Finalized { .. }) => ExitCode::SUCCESS,
        Ok(RunOutcome::Failed { .. }) => ExitCode::from(EXIT_FAILED),
        Ok(RunOutcome::Interrupted { .. }) => ExitCode::from(EXIT_INTERRUPTED),
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
    let store = Store::open(&loaded.store)?;
    status(&store, &loaded.run.id())
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
