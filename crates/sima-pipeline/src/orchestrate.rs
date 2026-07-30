//! [`orchestrate`]: one loaded config driven to its outcome.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::{FormatId, RunId};
use sima_scheduler::{ExecutionConfig, RunControl, RunOutcome, WorkerPool, worker_slots};
use sima_store::Store;
use sima_transport::container::{image_inspect_argv, probe_argv};
use sima_transport::{ContainerTransport, SpawnPolicy, SpawnSettings, SubprocessTransport};

use crate::config::{Container, LoadedConfig, Pool};
use crate::devices;
use crate::domain_registry::DomainSource;
use crate::fleet::{Engagement, Members, OwnedMachine, members};
use crate::rental::{
    RentalGroup, StopSignal, Supervisor, acquire_hosts, provider_for, release_all, transport_mode,
};

/// Drives the run a loaded config describes: opens the store (creating it
/// where missing), takes the run's orchestrator lock, dispatches the domain
/// and the generator, locates the worker binary, and runs the scheduler over
/// subprocess workers. Resume and re-evaluation are this same call — the
/// frontier re-derives from store state, so an interrupted or failed run
/// continues and a finalized one re-finalizes without touching an executor.
/// The lock is held for the whole call and releases on return.
///
/// `engagement` is the invocation's answer to which machines the run uses. Under
/// [`Engagement::Orchestrator`] the fleet is never resolved, so no provider is
/// constructed and no credential is read whatever the config declares.
pub fn orchestrate(
    config: &LoadedConfig,
    control: &RunControl,
    engagement: Engagement,
) -> Result<RunOutcome> {
    // Dispatch and discovery precede every store mutation: a config naming an
    // unknown format or generator, a build without the worker binary, or a
    // rental whose provider cannot be reached, must not leave a store, a run
    // directory, or a lock file behind for a run that can never execute.
    let source = config.domains.source(&config.run.format);
    let environment = source.environment(&config.run.format)?;
    let generator = source.generator(&config.run.generator.id, &config.run.format)?;
    let members = match engagement {
        Engagement::Orchestrator => Members::default(),
        Engagement::Fleet => members(config),
    };
    // A run with nowhere to execute is a config error, not a run that starts
    // and stalls. Without the flag the orchestrator is the whole run, so the
    // error names the flag that would engage the rest.
    if config.orchestrator.pool.is_none() && members.is_empty() {
        return Err(match engagement {
            Engagement::Orchestrator => Error::Validation(
                "the orchestrator declares no workers and no devices, so this run has nothing \
                 to execute on; give [orchestrator] a worker layout, or pass --fleet to engage \
                 the machines [fleet] names"
                    .to_string(),
            ),
            Engagement::Fleet => Error::Validation(
                "the orchestrator declares no workers and no devices, and [fleet] names no \
                 machine, so this run has nothing to execute on"
                    .to_string(),
            ),
        });
    }
    let run = config.run.id();
    // A device selector names hardware, so it resolves here — where the run
    // starts and the hardware is at hand — and not at load, which must work on
    // a machine with no device.
    let execution = resolve_devices(config, source)?;
    let local = local_pool(config, &run, &execution, source)?;
    // The fleet's control planes and the modes their machines are reached
    // through are built before the store: a vast rental without its key fails
    // here, before any store mutation.
    let providers = members
        .rentals
        .iter()
        .map(provider_for)
        .collect::<Result<Vec<_>>>()?;
    let modes = providers
        .iter()
        .map(|provider| transport_mode(provider.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    // Machines of yours resolve at run start too, over each machine's own
    // hardware: the image is verified present, then the enumeration probe
    // drives its device-table resolution. Both precede the store so a
    // misconfigured machine leaves nothing behind.
    let owned = owned_pools(&members.owned, &run, &execution, &config.run.format)?;
    let store = Store::open(&config.store)?;
    let lock = store.acquire_run_lock(&run)?;
    // Rentals are acquired under the held lock — each machine behind a teardown
    // guard held for the run's life. A strict-fill shortfall tears down whatever
    // was acquired and fails here, before any task runs.
    let mut groups: Vec<RentalGroup<'_>> = Vec::with_capacity(members.rentals.len());
    for ((rental, provider), mode) in members.rentals.iter().zip(&providers).zip(&modes) {
        let hosts = acquire_hosts(
            rental,
            &config.budget,
            provider.as_ref(),
            &store,
            &lock,
            mode,
            &config.run.format,
            &execution,
        )?;
        groups.push(RentalGroup {
            provider: provider.as_ref(),
            spec: rental.spec,
            fill: rental.fill,
            hosts,
        });
    }
    // The pools, the orchestrator's first: its own workers, then one container
    // pool per machine of yours, then one pool per rented machine. Worker ids
    // run global and sequential across them. The pools borrow the transports
    // and guards, so they live in an inner scope that ends before teardown.
    let mut pools: Vec<WorkerPool<'_>> = Vec::new();
    if let Some(local) = &local {
        pools.push(WorkerPool {
            transport: local.transport.as_ref(),
            host: String::new(),
            slots: local.slots.clone(),
        });
    }
    for machine in &owned {
        pools.push(WorkerPool {
            transport: &machine.transport,
            host: machine.host.clone(),
            slots: machine.slots.clone(),
        });
    }
    for host in groups.iter().flat_map(|group| &group.hosts) {
        pools.push(WorkerPool {
            transport: &host.transport,
            host: host.host.clone(),
            slots: host.slots.clone(),
        });
    }
    // A run with rentals drives a supervisor thread alongside the scheduler: it
    // keeps them within the run's budget and replaces lost machines while the
    // run proceeds. Both live in one scope so the supervisor borrows the store,
    // lock, and groups; the scheduler runs on this thread, and the stop signal
    // winds the supervisor down when it returns.
    let outcome = if groups.is_empty() {
        sima_scheduler::run(
            &store,
            &config.run,
            &environment,
            generator.as_ref(),
            &pools,
            &execution,
            control,
        )
    } else {
        let stop = StopSignal::new();
        // The run's emitter reaches the supervisor through the start hook,
        // filled once the collector spawns; the supervisor emits rental events
        // through it, so they cross the same journal boundary as the rest. No
        // scheduler edge to the provider appears — the hook is an opaque
        // closure.
        let emitter: std::sync::Mutex<Option<sima_trace::Emitter>> = std::sync::Mutex::new(None);
        let on_start = |e: sima_trace::Emitter| {
            *emitter.lock().expect("the emitter lock is never poisoned") = Some(e);
        };
        // Aborts a replacement acquisition in flight, so teardown never waits
        // out an offer walk. Set on the terminal event and again after the
        // driver returns; distinct from the caller's interrupt, which the run
        // owns.
        let cancel = std::sync::atomic::AtomicBool::new(false);
        // Wrap the caller's observer to stop the supervisor the moment the run
        // reaches a terminal event: the supervisor then drops its emitter
        // clone, so the run's collector — which joins only once every emitter
        // is dropped — can shut down. A fault emits no run-level event, so
        // `Faulted` is a stop trigger too. The same event cancels a replacement
        // still acquiring.
        let caller_observer = control.observer;
        let stopper = |record: &sima_trace::Record| {
            (caller_observer)(record);
            if matches!(
                record.event,
                sima_trace::Event::RunFinalized { .. }
                    | sima_trace::Event::RunFailed { .. }
                    | sima_trace::Event::RunInterrupted { .. }
                    | sima_trace::Event::Faulted { .. }
            ) {
                cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                stop.raise();
            }
        };
        let control = RunControl {
            observer: &stopper,
            interrupt: control.interrupt,
            on_start: Some(&on_start),
        };
        let supervisor = Supervisor::new(
            &store,
            &lock,
            &config.budget,
            &groups,
            control.interrupt,
            &emitter,
        )
        .on_cancel(&cancel);
        std::thread::scope(|scope| {
            let handle = scope.spawn(|| supervisor.run(&stop));
            let outcome = sima_scheduler::run(
                &store,
                &config.run,
                &environment,
                generator.as_ref(),
                &pools,
                &execution,
                &control,
            );
            // Cancel any replacement the supervisor is still acquiring before
            // joining it: teardown must not wait out an offer walk for a
            // machine the finished run no longer needs.
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            stop.raise();
            handle.join().expect("the supervisor thread joins");
            outcome
        })
    };
    // The pools' borrow of the rented transports and guards ends here, before
    // teardown.
    drop(pools);
    // Guards release explicitly on the success path, surfacing a teardown
    // failure — a machine still running is worth an operator's attention. A run
    // that already faulted keeps its fault; teardown is best-effort, and the
    // ledger record a failed teardown leaves is what reconcile acts on next.
    let released = release_all(groups);
    match outcome {
        Ok(run_outcome) => released.map(|()| run_outcome),
        Err(error) => Err(error),
    }
}

/// The orchestrator's own worker pool: plain subprocesses, or a container pool
/// on this machine when `[orchestrator]` names an image.
struct LocalPool {
    transport: Box<dyn sima_transport::WorkerTransport>,
    slots: Vec<Option<DeviceBinding>>,
}

/// A resolved container pool on one machine: its transport, the machine it runs
/// on, and its slots.
struct ContainerPool {
    transport: ContainerTransport,
    host: String,
    slots: Vec<Option<DeviceBinding>>,
}

/// Builds the orchestrator's own pool, or `None` when it declares no worker
/// layout and the fleet carries the run.
///
/// Without an image the workers are plain subprocesses of the binary `source`
/// names, and their device selectors resolve against this machine's own
/// hardware. With one they run in a container here, so the image is verified
/// and the selectors resolve against what the enumeration probe reports from
/// inside it — the same path a machine of yours follows, minus the ssh hop.
fn local_pool(
    config: &LoadedConfig,
    run: &RunId,
    execution: &ExecutionConfig,
    source: &dyn DomainSource,
) -> Result<Option<LocalPool>> {
    let Some(pool) = &config.orchestrator.pool else {
        return Ok(None);
    };
    match &config.orchestrator.container {
        None => Ok(Some(LocalPool {
            transport: Box::new(SubprocessTransport::new(
                // The binary the format's tasks execute in: sima's own worker,
                // or the program the config routed this format to.
                source.worker_binary()?,
                // A local worker runs the bare binary: no arguments.
                Vec::new(),
                // Inherited for sima's own worker, scrubbed for a program a
                // config routed this format to.
                spawn_settings(source.spawn_policy(), execution, &config.run.format),
            )),
            slots: worker_slots(execution),
        })),
        Some(container) => {
            let built =
                container_pool(None, container, pool, 0, run, execution, &config.run.format)?;
            Ok(Some(LocalPool {
                transport: Box::new(built.transport),
                slots: built.slots,
            }))
        }
    }
}

/// Resolves every machine of yours the fleet drew in: verifies its image is
/// present, then builds its slots — plain workers, or its device tables resolved
/// against what the enumeration probe reports over its own hardware.
fn owned_pools(
    machines: &[OwnedMachine<'_>],
    run: &RunId,
    execution: &ExecutionConfig,
    format: &FormatId,
) -> Result<Vec<ContainerPool>> {
    machines
        .iter()
        .enumerate()
        .map(|(index, machine)| {
            container_pool(
                Some(machine.ssh),
                machine.container,
                machine.pool,
                // The orchestrator's own container pool takes index 0, so the
                // fleet's machines start after it and no two pools on one
                // machine can collide on a container name.
                index + 1,
                run,
                execution,
                format,
            )
        })
        .collect()
}

/// Builds one container pool: verifies the image is present where the container
/// will run, derives the pool's slots, and constructs the transport under a
/// container-name stem unique to this run and pool.
#[allow(clippy::too_many_arguments)]
fn container_pool(
    host: Option<&str>,
    container: &Container,
    pool: &Pool,
    index: usize,
    run: &RunId,
    execution: &ExecutionConfig,
    format: &FormatId,
) -> Result<ContainerPool> {
    // A pool fails on either answer; only the migration's first contact waits
    // for a machine that is still coming up.
    if let ImageCheck::Unreachable(error) = bootstrap_image(host, container)? {
        return Err(error);
    }
    let slots = match pool {
        Pool::Workers(workers) => vec![None; *workers],
        Pool::Devices(selectors) => {
            let enumerated = probe_container_devices(host, container, format)?;
            let entries = devices::resolve(selectors, &enumerated)?;
            let exec = ExecutionConfig::with_devices(
                entries,
                execution.max_attempts,
                execution.attempt_timeout,
                execution.answer_timeout,
                execution.checkpoint_interval,
                execution.checkpoint_interval_steps,
            )?;
            worker_slots(&exec)
        }
    };
    // A deterministic per-run, per-pool container-name stem; the transport adds
    // a spawn suffix. The run id prefix keeps names distinct across concurrent
    // runs on one machine.
    let stem = run.to_string();
    let prefix = format!("sima-w-{}-{index}", &stem[..stem.len().min(12)]);
    Ok(ContainerPool {
        transport: ContainerTransport::new(
            host.map(str::to_string),
            container.runtime.clone(),
            container.image.clone(),
            container.run_args.clone(),
            prefix,
            // The runtime client is sima's own process: it reads its
            // configuration from the ambient environment, and the worker it
            // nests is the sima image's.
            spawn_settings(SpawnPolicy::Inherit, execution, format),
        ),
        // A container on this machine binds as local in the journal — it is the
        // local machine.
        host: host.unwrap_or_default().to_string(),
        slots,
    })
}

/// The code ssh exits with when it could not make the connection at all, as
/// distinct from a command on the far side exiting non-zero.
const SSH_UNREACHABLE: i32 = 255;

/// What asking a machine for a pool's worker image found.
pub(crate) enum ImageCheck {
    /// The runtime answered and holds the image.
    Present,
    /// The machine could not be reached to ask, which is what a fresh or
    /// rebooting one answers until it is up. The error names the destination
    /// and is what to report if it never comes up.
    Unreachable(Error),
}

/// Verifies a pool's worker image is present, failing with the command that
/// puts it there. A missing image must be a clean error, not a hanging
/// handshake. The fix differs by where the container runs: build it locally, or
/// ship the local build to the machine.
///
/// A machine that could not be reached at all is [`ImageCheck::Unreachable`]
/// rather than an error, because the two failures are worth different
/// responses: an image the runtime does not hold will not appear by being asked
/// again, while a machine that is still coming up will answer shortly. The
/// caller decides — a pool fails on either, a migration's first contact waits
/// out the second.
pub(crate) fn bootstrap_image(host: Option<&str>, container: &Container) -> Result<ImageCheck> {
    let argv = image_inspect_argv(host, &container.runtime, &container.image);
    let status = command_status(&argv)?;
    if status.success() {
        return Ok(ImageCheck::Present);
    }
    // 255 is ssh's own code for a connection it could not make, and it can only
    // mean that when there is an ssh in the command at all: a local runtime
    // exiting 255 is the runtime speaking, about the image.
    if let Some(host) = host
        && status.code() == Some(SSH_UNREACHABLE)
    {
        return Ok(ImageCheck::Unreachable(Error::Validation(format!(
            "cannot reach {host:?}: ssh exited with {status}"
        ))));
    }
    let (place, fix) = match host {
        Some(host) => (
            format!("on {host:?}"),
            format!(
                "podman save {} | ssh {host} {} load",
                container.image, container.runtime
            ),
        ),
        None => (
            "locally".to_string(),
            format!(
                "podman build -t {} -f containers/sima/Containerfile .",
                container.image
            ),
        ),
    };
    Err(Error::Validation(format!(
        "worker image {:?} is not present {place}; put it there with: {fix}",
        container.image
    )))
}

/// Runs the enumeration probe in a throwaway container where the pool's own
/// containers run, and parses the devices it reports for `format`.
fn probe_container_devices(
    host: Option<&str>,
    container: &Container,
    format: &FormatId,
) -> Result<Vec<DeviceInfo>> {
    let argv = probe_argv(
        host,
        &container.runtime,
        &container.image,
        &container.run_args,
        format,
    );
    let stdout = command_stdout(&argv)?;
    devices::parse_enumeration(&stdout)
}

/// Runs `argv`, discarding its streams, and reports whether it exited zero.
fn command_status(argv: &[String]) -> Result<ExitStatus> {
    let (program, args) = argv.split_first().expect("a non-empty command vector");
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| Error::Validation(format!("running {program:?} failed: {e}")))
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

/// The run's execution settings with the orchestrator's device selectors
/// resolved against this machine's devices. An orchestrator naming no device
/// passes through untouched, so a run that never asked about devices never
/// enumerates them, and a containerized pool resolves inside its container
/// instead.
fn resolve_devices(config: &LoadedConfig, source: &dyn DomainSource) -> Result<ExecutionConfig> {
    let selectors = match (&config.orchestrator.pool, &config.orchestrator.container) {
        (Some(pool), None) => pool.devices(),
        _ => &[],
    };
    if selectors.is_empty() {
        return Ok(config.execution.clone());
    }
    let entries = devices::resolve(selectors, &source.enumerate_devices(&config.run.format)?)?;
    ExecutionConfig::with_devices(
        entries,
        config.execution.max_attempts,
        config.execution.attempt_timeout,
        config.execution.answer_timeout,
        config.execution.checkpoint_interval,
        config.execution.checkpoint_interval_steps,
    )
}

/// The settings a pool's workers are spawned and greeted under: the policy the
/// pool's binary calls for, plus the run's answer deadline and checkpoint
/// cadence.
fn spawn_settings(
    policy: SpawnPolicy,
    execution: &ExecutionConfig,
    format: &FormatId,
) -> SpawnSettings {
    SpawnSettings::new(
        policy,
        execution.answer_timeout,
        format.clone(),
        execution.checkpoint_interval,
        execution.checkpoint_interval_steps,
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
///
/// This is one of the two environment channels the pipeline reads. The other is
/// `SIMA_STUB_SSH`, which points the stub backend at a machine that is really
/// there so the ssh path runs against a throwaway server; it is read in
/// `rental::provider_for` and nowhere else.
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
