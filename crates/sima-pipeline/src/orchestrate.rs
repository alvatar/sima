//! [`orchestrate`]: one loaded config driven to its outcome.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::devices::{DeviceInfo, enumerate_devices};
use sima_domains::{domain_for, generator_for};
use sima_model::RunId;
use sima_scheduler::{ExecutionConfig, RunControl, RunOutcome, WorkerPool, worker_slots};
use sima_store::Store;
use sima_transport::remote::{image_inspect_argv, probe_argv};
use sima_transport::{RemoteTransport, SubprocessTransport};

use crate::config::{LoadedConfig, RemoteConfig};
use crate::devices;
use crate::fleet::{acquire_fleet, provider_for, release_all, transport_mode};

/// Drives the run a loaded config describes: opens the store (creating it
/// where missing), takes the run's orchestrator lock, dispatches the domain
/// and the generator, locates the worker binary, and runs the scheduler over
/// subprocess workers. Resume and re-evaluation are this same call — the
/// frontier re-derives from store state, so an interrupted or failed run
/// continues and a finalized one re-finalizes without touching an executor.
/// The lock is held for the whole call and releases on return.
pub fn orchestrate(config: &LoadedConfig, control: &RunControl) -> Result<RunOutcome> {
    // Dispatch and discovery precede every store mutation: a config naming an
    // unknown format or generator, a build without the worker binary, or a
    // fleet whose provider cannot be reached, must not leave a store, a run
    // directory, or a lock file behind for a run that can never execute.
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
    let run = config.run.id();
    // The fleet backend and the mode its instances are reached through are
    // built before the store: a vast fleet without its key fails here, before
    // any store mutation. A run with no fleet builds no provider.
    let fleet = match &config.fleet {
        Some(fleet) => Some((provider_for(fleet)?, transport_mode(fleet)?, fleet)),
        None => None,
    };
    // Remote pools resolve at run start too, over each remote's own hardware:
    // the image is verified present, then the enumeration probe drives the
    // remote's device-table resolution. Both precede the store so a
    // misconfigured remote leaves nothing behind.
    let remotes = build_remote_pools(config, &run, &execution)?;
    let store = Store::open(&config.store)?;
    let lock = store.acquire_run_lock(&run)?;
    // The fleet is acquired under the held lock — each instance behind a
    // teardown guard held for the run's life. A strict-fill shortfall tears
    // down whatever was acquired and fails here, before any task runs.
    let fleet_instances = match &fleet {
        Some((provider, mode, fleet_config)) => acquire_fleet(
            fleet_config,
            provider.as_ref(),
            &store,
            &lock,
            mode,
            &config.run.format,
            &execution,
        )?,
        None => Vec::new(),
    };
    // The pools, local first: the subprocess pool when a local pool is
    // configured, then one container pool per manual remote, then one pool per
    // fleet instance. Worker ids run global and sequential across them. The
    // pools borrow the transports and guards, so they live in an inner scope
    // that ends before teardown.
    let outcome = {
        let mut pools: Vec<WorkerPool<'_>> = Vec::new();
        let local_slots = worker_slots(&execution);
        if !local_slots.is_empty() {
            pools.push(WorkerPool {
                transport: &transport,
                host: String::new(),
                slots: local_slots,
            });
        }
        for remote in &remotes {
            pools.push(WorkerPool {
                transport: &remote.transport,
                host: remote.host.clone(),
                slots: remote.slots.clone(),
            });
        }
        for instance in &fleet_instances {
            pools.push(WorkerPool {
                transport: &instance.transport,
                host: instance.host.clone(),
                slots: instance.slots.clone(),
            });
        }
        sima_scheduler::run(
            &store,
            &config.run,
            &domain.environment,
            generator.as_ref(),
            &pools,
            &execution,
            control,
        )
    };
    // Guards release explicitly on the success path, surfacing a teardown
    // failure — a machine still running is worth an operator's attention. A run
    // that already faulted keeps its fault; teardown is best-effort, and the
    // ledger record a failed teardown leaves is what reconcile acts on next.
    let released = release_all(fleet_instances);
    match outcome {
        Ok(run_outcome) => released.map(|()| run_outcome),
        Err(error) => Err(error),
    }
}

/// A resolved remote pool: its transport, the host it runs on, and its slots.
struct RemoteBuilt {
    transport: RemoteTransport,
    host: String,
    slots: Vec<Option<DeviceBinding>>,
}

/// Resolves every `[[execution.remote]]` pool at run start: verifies its image
/// is present, then builds its slots — plain workers, or the remote's device
/// tables resolved against what the enumeration probe reports over its own
/// hardware.
fn build_remote_pools(
    config: &LoadedConfig,
    run: &RunId,
    execution: &ExecutionConfig,
) -> Result<Vec<RemoteBuilt>> {
    let mut built = Vec::with_capacity(config.remotes.len());
    for (index, remote) in config.remotes.iter().enumerate() {
        bootstrap_image(remote)?;
        let slots = match remote.workers {
            Some(workers) => vec![None; workers],
            None => {
                let enumerated = probe_remote_devices(remote)?;
                let entries = devices::resolve(&remote.devices, &enumerated)?;
                let exec = ExecutionConfig::with_devices(
                    entries,
                    execution.max_attempts,
                    execution.attempt_timeout,
                    execution.checkpoint_interval,
                    execution.checkpoint_interval_steps,
                )?;
                worker_slots(&exec)
            }
        };
        // A deterministic per-run, per-pool container-name stem; the transport
        // adds a spawn suffix. The run id prefix keeps names distinct across
        // concurrent runs on one machine.
        let stem = run.to_string();
        let prefix = format!("sima-w-{}-{index}", &stem[..stem.len().min(12)]);
        let transport = RemoteTransport::new(
            remote.host.clone(),
            remote.runtime.clone(),
            remote.image.clone(),
            remote.run_args.clone(),
            prefix,
            config.run.format.clone(),
            execution.checkpoint_interval,
            execution.checkpoint_interval_steps,
        );
        built.push(RemoteBuilt {
            transport,
            // A container on this machine (no host) binds as local in the
            // journal — it is the local machine.
            host: remote.host.clone().unwrap_or_default(),
            slots,
        });
    }
    Ok(built)
}

/// Verifies a pool's worker image is present, failing with the command that
/// puts it there. A missing image must be a clean error, not a hanging
/// handshake. The fix differs by where the container runs: build it locally, or
/// ship the local build to the remote.
fn bootstrap_image(remote: &RemoteConfig) -> Result<()> {
    let argv = image_inspect_argv(remote.host.as_deref(), &remote.runtime, &remote.image);
    if command_succeeds(&argv)? {
        return Ok(());
    }
    let (place, fix) = match &remote.host {
        Some(host) => (
            format!("on {host:?}"),
            format!(
                "podman save {} | ssh {host} {} load",
                remote.image, remote.runtime
            ),
        ),
        None => (
            "locally".to_string(),
            format!(
                "podman build -t {} -f containers/worker/Containerfile .",
                remote.image
            ),
        ),
    };
    Err(Error::Validation(format!(
        "worker image {:?} is not present {place}; put it there with: {fix}",
        remote.image
    )))
}

/// Runs the enumeration probe in the remote's container and parses the devices
/// it reports.
fn probe_remote_devices(remote: &RemoteConfig) -> Result<Vec<DeviceInfo>> {
    let argv = probe_argv(
        remote.host.as_deref(),
        &remote.runtime,
        &remote.image,
        &remote.run_args,
    );
    let stdout = command_stdout(&argv)?;
    devices::parse_enumeration(&stdout)
}

/// Runs `argv`, discarding its streams, and reports whether it exited zero.
fn command_succeeds(argv: &[String]) -> Result<bool> {
    let (program, args) = argv.split_first().expect("a non-empty command vector");
    let status = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| Error::Validation(format!("running {program:?} failed: {e}")))?;
    Ok(status.success())
}

/// Runs `argv` and returns its stdout, or an error if it fails or its output
/// is not UTF-8. Stderr is inherited for diagnostics.
pub(crate) fn command_stdout(argv: &[String]) -> Result<String> {
    let (program, args) = argv.split_first().expect("a non-empty command vector");
    let output = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| Error::Validation(format!("running {program:?} failed: {e}")))?;
    if !output.status.success() {
        return Err(Error::Validation(format!(
            "{program:?} exited with {}",
            output.status
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|e| Error::Validation(format!("{program:?} output is not UTF-8: {e}")))
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
pub(crate) fn worker_binary() -> Result<PathBuf> {
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
