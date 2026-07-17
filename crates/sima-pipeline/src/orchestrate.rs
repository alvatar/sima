//! [`orchestrate`]: one loaded config driven to its outcome.

use std::path::{Path, PathBuf};

use sima_core::{Error, Result};
use sima_domains::devices::enumerate_devices;
use sima_domains::{domain_for, generator_for};
use sima_scheduler::{ExecutionConfig, RunControl, RunOutcome};
use sima_store::Store;
use sima_transport::SubprocessTransport;

use crate::config::LoadedConfig;
use crate::devices;

/// Drives the run a loaded config describes: opens the store (creating it
/// where missing), takes the run's orchestrator lock, dispatches the domain
/// and the generator, locates the worker binary, and runs the scheduler over
/// subprocess workers. Resume and re-evaluation are this same call — the
/// frontier re-derives from store state, so an interrupted or failed run
/// continues and a finalized one re-finalizes without touching an executor.
/// The lock is held for the whole call and releases on return.
pub fn orchestrate(config: &LoadedConfig, control: &RunControl) -> Result<RunOutcome> {
    // Dispatch and discovery precede every store mutation: a config naming an
    // unknown format or generator, or a build without the worker binary, must
    // not leave a store, a run directory, or a lock file behind for a run
    // that can never execute.
    let domain = domain_for(&config.run.format)?;
    let generator = generator_for(&config.run.generator.id)?;
    let transport = SubprocessTransport::new(
        worker_binary()?,
        // A local worker runs the bare binary: no arguments.
        Vec::new(),
        config.run.format.clone(),
        config.execution.checkpoint_interval,
        config.execution.checkpoint_interval_steps,
    );
    // A device selector names hardware, so it resolves here — where the run
    // starts and the hardware is at hand — and not at load, which must work on
    // a machine with no device.
    let execution = resolve_devices(config)?;
    let store = Store::open(&config.store)?;
    let run = config.run.id();
    let _lock = store.acquire_run_lock(&run)?;
    sima_scheduler::run(
        &store,
        &config.run,
        &domain.environment,
        generator.as_ref(),
        &transport,
        &execution,
        control,
    )
}

/// The run's execution settings with its device selectors resolved against the
/// machine's devices. A config naming no device passes through untouched, so a
/// run that never asked about devices never enumerates them.
fn resolve_devices(config: &LoadedConfig) -> Result<ExecutionConfig> {
    if config.devices.is_empty() {
        return Ok(config.execution.clone());
    }
    let entries = devices::resolve(&config.devices, &enumerate_devices()?)?;
    ExecutionConfig::with_devices(
        entries,
        config.execution.max_attempts,
        config.execution.attempt_timeout,
        config.execution.checkpoint_interval,
        config.execution.checkpoint_interval_steps,
    )
}

/// Locates the `sima-worker` binary, in order:
///
/// - the `SIMA_WORKER` environment variable (an absolute path), for tests
///   and later remote layouts;
/// - `sima-worker` beside the current executable;
/// - `sima-worker` in the parent directory of the current executable's
///   directory, which covers test executables under `target/debug/deps`
///   finding the binary in `target/debug`.
///
/// A missing binary is a validation error naming the searched locations.
fn worker_binary() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("SIMA_WORKER") {
        return Ok(PathBuf::from(path));
    }
    let exe = std::env::current_exe().map_err(|e| {
        Error::Validation(format!(
            "cannot locate sima-worker: the current executable's path is unknown: {e}"
        ))
    })?;
    let mut searched = Vec::new();
    for dir in [exe.parent(), exe.parent().and_then(Path::parent)] {
        let Some(dir) = dir else { continue };
        let candidate = dir.join("sima-worker");
        if candidate.is_file() {
            return Ok(candidate);
        }
        searched.push(candidate);
    }
    Err(Error::Validation(format!(
        "sima-worker binary not found; set SIMA_WORKER or place it at one of: {}",
        searched
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}
