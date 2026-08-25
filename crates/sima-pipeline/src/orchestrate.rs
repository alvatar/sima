//! [`orchestrate`]: one loaded config driven to its outcome.

use std::time::Duration;

use sima_contracts::DeviceBinding;
use sima_core::{Error, Hash, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::{FormatId, RunId};
use sima_scheduler::{ExecutionConfig, RunControl, RunOutcome, WorkerPool, worker_slots};
use sima_store::Store;
use sima_transport::DeviceProbe;
use sima_transport::container::{ContainerRun, once_argv, probe_argv};
use sima_transport::domain_service::DomainService;
use sima_transport::serve::serve_domain_args;
use sima_transport::{ContainerTransport, SpawnPolicy, SpawnSettings, SubprocessTransport};

use crate::config::{Container, LoadedConfig, Pool};
use crate::devices;
use crate::domain_registry::DomainSource;
use crate::fleet::{Engagement, Members, OwnedMachine, members};
use crate::process::{ImageCheck, bootstrap_image, command_stdout};
use crate::program_binding::{BinaryChange, bind};
use crate::program_delivery::{ProgramDelivery, deliver_to_owned, ingest_program, sendable};
use crate::rental::{
    RentalGroup, StopSignal, Supervisor, acquire_hosts, provider_for_rental, release_all,
    transport_mode,
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
///
/// `accept` is the invocation's answer to a config-routed program whose build
/// changed since this run last ran. A run whose format this build carries has
/// no program, so nothing is compared and the answer is inert.
pub fn orchestrate(
    config: &LoadedConfig,
    control: &RunControl,
    engagement: Engagement,
    accept: BinaryChange,
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
    // Whether this run has to put its program on its machines before they can
    // serve it: the fleet drew in machines, and the format is a program rather
    // than one this build carries. An entry that declares nothing to send is
    // refused here, before any machine is contacted — no machine's answer could
    // change it.
    let delivers = !members.is_empty() && config.domains.routed(&config.run.format).is_some();
    if delivers {
        sendable(config)?;
    }
    let run = config.run.id();
    // A device selector names hardware, so it resolves here — where the run
    // starts and the hardware is at hand — and not at load, which must work on
    // a machine with no device.
    let execution = resolve_devices(config, source)?;
    let program = WorkerProgram::of(config);
    let local = local_pool(config, &run, &execution, source, &program)?;
    // The fleet's control planes and the modes their machines are reached
    // through are built before the store: a vast rental without its key fails
    // here, before any store mutation.
    let providers = members
        .rentals
        .iter()
        .map(provider_for_rental)
        .collect::<Result<Vec<_>>>()?;
    let modes = providers
        .iter()
        .map(|provider| transport_mode(provider.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    // Machines of yours are verified at run start too, over each machine's own
    // hardware: the image is confirmed present here, before the store, so a
    // machine that is unreachable or missing its image leaves no store, no run
    // directory, and no lock behind.
    for machine in &members.owned {
        if let ImageCheck::Unreachable(error) =
            bootstrap_image(Some(machine.ssh), machine.container)?
        {
            return Err(error);
        }
    }
    // A run whose format this build carries puts nothing on its machines that
    // the image does not already hold, so its pools — the enumeration probe
    // that drives their device tables, and their transports — are built here,
    // still before the store. A run that delivers a program builds them below
    // instead, because a delivery reads the store the program is ingested into.
    let owned = match delivers {
        false => Some(owned_pools(
            &members.owned,
            &run,
            &execution,
            &program,
            None,
        )?),
        true => None,
    };
    let store = Store::open(&config.store)?;
    let lock = store.acquire_run_lock(&run)?;
    // The build serving a config-routed format is compared against the one the
    // run was last driven by, and recorded, under the held lock: the journal
    // read and the append race no other orchestrator. A format this build
    // carries has no program, so nothing is compared and nothing is recorded.
    if let Some(routed) = config.domains.routed(&config.run.format) {
        bind(&store, &config.run, &routed, accept)?;
    }
    let owned = match owned {
        Some(pools) => pools,
        // The program reaches every machine before any pool of one exists, so
        // a pool is only ever built where a worker can actually be served. The
        // one thing this ordering softens: a machine that fails its install
        // leaves an ingested payload in the local store — local,
        // content-addressed, and what the next attempt reuses.
        None => {
            let delivery =
                ingest_program(config, &store)?.expect("a routed format's program is ingested");
            deliver_to_owned(&members.owned, &store, &delivery)?;
            owned_pools(&members.owned, &run, &execution, &program, Some(&delivery))?
        }
    };
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
            *emitter
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(e);
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
        let stopper = |record: &sima_scheduler::Record| {
            (caller_observer)(record);
            if matches!(
                record.event,
                sima_scheduler::Event::RunFinalized { .. }
                    | sima_scheduler::Event::RunFailed { .. }
                    | sima_scheduler::Event::RunInterrupted { .. }
                    | sima_scheduler::Event::Faulted { .. }
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

/// What every pool's workers are spawned to answer for: the run's format, and
/// the program the run sent for it. The two travel together because one
/// handshake states both, and every pool of one run states the same pair.
#[derive(Clone)]
struct WorkerProgram {
    format: FormatId,
    /// `Some` exactly when the config entry resolved a `payload_digest` — the
    /// program this run installed where it resolved. Every worker answers it
    /// back; one that answers another digest, or none, fails its spawn, which
    /// is what a machine holding some other program looks like from here.
    digest: Option<String>,
}

impl WorkerProgram {
    /// What `config`'s run spawns for on this machine: its format, and the
    /// digest of the program routed to that format where the entry stated one.
    fn of(config: &LoadedConfig) -> WorkerProgram {
        WorkerProgram {
            format: config.run.format.clone(),
            digest: config
                .domains
                .routed(&config.run.format)
                .and_then(|routed| routed.payload_digest.map(Hash::to_string)),
        }
    }

    /// The same run on a machine `delivery` reached: what that machine
    /// installed is what its workers answer, whatever this one holds.
    fn delivered(&self, delivery: &ProgramDelivery) -> WorkerProgram {
        WorkerProgram {
            format: self.format.clone(),
            digest: Some(delivery.payload().to_string()),
        }
    }
}

/// What a machine's containers run for this run.
///
/// A format this build carries is answered by the image's own worker: nothing
/// was delivered there, so nothing is mounted and no digest is expected back. A
/// format that is a program is answered by what the delivery installed under
/// the machine's own `root`, which is what its workers are spawned as and what
/// they answer at the handshake.
enum MachineProgram<'a> {
    /// The image answers for the run's format itself.
    Image,
    /// The program a delivery installed under `root` on that machine.
    Delivered {
        delivery: &'a ProgramDelivery,
        root: &'a str,
    },
}

impl MachineProgram<'_> {
    /// What one worker's container runs there.
    fn worker_run(&self) -> ContainerRun {
        match self {
            MachineProgram::Image => ContainerRun::worker(Vec::new()),
            MachineProgram::Delivered { delivery, root } => delivery.container_run(root, &[]),
        }
    }

    /// The devices this run's work can be placed on there, enumerated in a
    /// throwaway container where the pool's own containers run — so the answer
    /// covers the same hardware the workers will reach.
    ///
    /// The image's worker is asked about the format when the image carries it.
    /// It cannot resolve a program's format at all, so the delivered program is
    /// asked instead, over the domain service it already answers on this
    /// machine: the classes are the program's backend's own.
    fn devices(
        &self,
        host: Option<&str>,
        container: &Container,
        format: &FormatId,
        answer_timeout: Duration,
    ) -> Result<Vec<DeviceInfo>> {
        match self {
            MachineProgram::Image => {
                let argv = probe_argv(
                    host,
                    &container.runtime,
                    &container.image,
                    &container.run_args,
                    DeviceProbe::Format(format),
                );
                devices::parse_enumeration(&command_stdout(&argv)?)
            }
            MachineProgram::Delivered { delivery, root } => {
                let role = serve_domain_args(format);
                let argv = once_argv(
                    host,
                    &container.runtime,
                    &container.image,
                    &container.run_args,
                    &delivery.container_run(root, &[&role[0], &role[1]]),
                );
                // The session ends with this scope: its drop says goodbye,
                // closes the pipe, and reaps the container's client.
                DomainService::spawn_argv(&argv, answer_timeout)?.enumerate_devices(format)
            }
        }
    }
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
    program: &WorkerProgram,
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
                // Inherited for sima's own worker, explicit for a program a
                // config routed this format to.
                spawn_settings(source.spawn_policy(), execution, program),
            )),
            slots: worker_slots(execution),
        })),
        Some(container) => {
            // A pool fails on either answer; only the migration's first contact
            // waits for a machine that is still coming up.
            if let ImageCheck::Unreachable(error) = bootstrap_image(None, container)? {
                return Err(error);
            }
            let built = container_pool(
                None,
                container,
                pool,
                0,
                run,
                execution,
                program,
                &MachineProgram::Image,
            )?;
            Ok(Some(LocalPool {
                transport: Box::new(built.transport),
                slots: built.slots,
            }))
        }
    }
}

/// Builds a pool on every machine of yours the fleet drew in: its slots — plain
/// workers, or its device tables resolved against what that machine's own
/// enumeration reports — and the transport its workers are spawned through.
///
/// `delivery` is what was put on those machines, and `None` for a run whose
/// format the image answers for itself. It decides both what a worker there
/// runs and what it is expected to answer.
fn owned_pools(
    machines: &[OwnedMachine<'_>],
    run: &RunId,
    execution: &ExecutionConfig,
    program: &WorkerProgram,
    delivery: Option<&ProgramDelivery>,
) -> Result<Vec<ContainerPool>> {
    machines
        .iter()
        .enumerate()
        .map(|(index, machine)| {
            let (machine_program, program) = match delivery {
                None => (MachineProgram::Image, program.clone()),
                Some(delivery) => (
                    MachineProgram::Delivered {
                        delivery,
                        root: machine.root,
                    },
                    program.delivered(delivery),
                ),
            };
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
                &program,
                &machine_program,
            )
        })
        .collect()
}

/// Builds one container pool: derives the pool's slots, and constructs the
/// transport under a container-name stem unique to this run and pool.
///
/// The image is confirmed present by the caller, which is where the ordering
/// against the store is decided.
#[allow(clippy::too_many_arguments)]
fn container_pool(
    host: Option<&str>,
    container: &Container,
    pool: &Pool,
    index: usize,
    run: &RunId,
    execution: &ExecutionConfig,
    program: &WorkerProgram,
    machine: &MachineProgram<'_>,
) -> Result<ContainerPool> {
    let slots = match pool {
        Pool::Workers(workers) => vec![None; *workers],
        Pool::Devices(selectors) => {
            let enumerated =
                machine.devices(host, container, &program.format, execution.answer_timeout)?;
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
            machine.worker_run(),
            // The runtime client is sima's own process: it reads its
            // configuration from the ambient environment. What the worker it
            // nests sees is stated inside the container instead.
            spawn_settings(SpawnPolicy::Inherit, execution, program),
        ),
        // A container on this machine binds as local in the journal — it is the
        // local machine.
        host: host.unwrap_or_default().to_string(),
        slots,
    })
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
    program: &WorkerProgram,
) -> SpawnSettings {
    SpawnSettings::new(
        policy,
        execution.answer_timeout,
        program.format.clone(),
        execution.checkpoint_interval,
        execution.checkpoint_interval_steps,
    )
    .expecting_program(program.digest.clone())
}
