//! [`orchestrate`]: one loaded config driven to its outcome.

use std::time::Duration;

use sima_contracts::DeviceBinding;
use sima_core::{Error, Hash, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::{FormatId, SearchId};
use sima_scheduler::{ExecutionConfig, SearchControl, SearchOutcome, WorkerPool, worker_slots};
use sima_store::Store;
use sima_transport::DeviceProbe;
use sima_transport::container::{ContainerRun, once_argv, probe_argv};
use sima_transport::domain_service::DomainService;
use sima_transport::serve::serve_domain_args;
use sima_transport::{ContainerTransport, SpawnPolicy, SpawnSettings, SubprocessTransport};

use crate::ceiling::{report_ceiling, under_ceiling};
use crate::config::{Container, LoadedConfig, Pool};
use crate::devices;
use crate::domain_registry::DomainSource;
use crate::fleet::{Engagement, Members, OwnedMachine, members};
use crate::journal::under_collector;
use crate::process::{ImageCheck, bootstrap_image, command_stdout};
use crate::program_binding::{BinaryChange, bind};
use crate::program_delivery::{ProgramDelivery, deliver_to_owned, ingest_program, sendable};
use crate::rental::{
    RentalGroup, StopOnSpawnFailure, StopSignal, Supervisor, acquire_hosts, provider_for_rental,
    release_all, transport_mode,
};

/// Drives the search a loaded config describes: opens the store (creating it
/// where missing), takes the search's orchestrator lock, dispatches the domain
/// and the generator, locates the worker binary, and searches the scheduler over
/// subprocess workers. Resume and re-evaluation are this same call — the
/// frontier re-derives from store state, so an interrupted or failed search
/// continues and a finalized one re-finalizes without touching an executor.
/// The lock is held for the whole call and releases on return.
///
/// `engagement` is the invocation's answer to which machines the search uses. Under
/// [`Engagement::Orchestrator`] the fleet is never resolved, so no provider is
/// constructed and no credential is read whatever the config declares.
///
/// `accept` is the invocation's answer to a config-routed program whose build
/// changed since this search last ran. A search whose format this build carries has
/// no program, so nothing is compared and the answer is inert.
pub fn orchestrate(
    config: &LoadedConfig,
    control: &SearchControl,
    engagement: Engagement,
    accept: BinaryChange,
) -> Result<SearchOutcome> {
    // Dispatch and discovery precede every store mutation: a config naming an
    // unknown format or generator, a build without the worker binary, or a
    // rental whose provider cannot be reached, must not leave a store, a search
    // directory, or a lock file behind for a search that can never execute.
    let source = config.domains.source(&config.search.format);
    let environment = source.environment(&config.search.format)?;
    let generator = source.generator(&config.search.generator.id, &config.search.format)?;
    let members = match engagement {
        Engagement::Orchestrator => Members::default(),
        Engagement::Fleet => members(config),
    };
    // A search with nowhere to execute is a config error, not a search that starts
    // and stalls. Without the flag the orchestrator is the whole search, so the
    // error names the flag that would engage the rest.
    if config.orchestrator.pool.is_none() && members.is_empty() && !derives_workers(config) {
        return Err(match engagement {
            Engagement::Orchestrator => Error::Validation(
                "the orchestrator declares no workers and no devices, so this search has nothing \
                 to execute on; give [orchestrator] a worker layout, or pass --fleet to engage \
                 the machines [fleet] names"
                    .to_string(),
            ),
            Engagement::Fleet => Error::Validation(
                "the orchestrator declares no workers and no devices, and [fleet] names no \
                 machine, so this search has nothing to execute on"
                    .to_string(),
            ),
        });
    }
    // Whether this search has to put its program on its machines before they can
    // serve it: the fleet drew in machines, and the format is a program rather
    // than one this build carries. An entry that declares nothing to send is
    // refused here, before any machine is contacted — no machine's answer could
    // change it.
    let delivers = !members.is_empty() && config.domains.routed(&config.search.format).is_some();
    if delivers {
        sendable(config)?;
    }
    let search = config.search.id();
    // A device selector names hardware, so it resolves here — where the search
    // starts and the hardware is at hand — and not at load, which must work on
    // a machine with no device.
    let execution = resolve_devices(config, source)?;
    let program = WorkerProgram::of(config);
    let local = local_pool(config, &search, &execution, source, &program)?;
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
    // Machines of yours are verified at search start too, over each machine's own
    // hardware: the image is confirmed present here, before the store, so a
    // machine that is unreachable or missing its image leaves no store, no search
    // directory, and no lock behind.
    for machine in &members.owned {
        if let ImageCheck::Unreachable(error) =
            bootstrap_image(Some(machine.ssh), machine.container)?
        {
            return Err(error);
        }
    }
    // A search whose format this build carries puts nothing on its machines that
    // the image does not already hold, so its pools — the enumeration probe
    // that drives their device tables, and their transports — are built here,
    // still before the store. A search that delivers a program builds them below
    // instead, because a delivery reads the store the program is ingested into.
    let owned = if delivers {
        None
    } else {
        Some(owned_pools(
            &members.owned,
            &search,
            &execution,
            &program,
            None,
        )?)
    };
    let store = Store::open(&config.store)?;
    let lock = store.acquire_search_lock(&search)?;
    // Registering the search gives it a journal before any machine is asked for,
    // so what putting the search on its machines takes is journaled where the
    // work will be. The driver performs the same registration, idempotently,
    // when it takes over.
    store.create_search(&config.search)?;
    // The build serving a config-routed format is compared against the one the
    // search was last driven by, and recorded, under the held lock: the journal
    // read and the append race no other orchestrator. A format this build
    // carries has no program, so nothing is compared and nothing is recorded.
    if let Some(routed) = config.domains.routed(&config.search.format) {
        bind(&store, &config.search, &routed, accept)?;
    }
    // What this search puts on the machines it uses, ingested under the held lock:
    // what a delivery sends is in the store that sends it. `None` for a search
    // whose format every machine's image answers for itself.
    //
    // The one thing this ordering softens: a machine that fails its install
    // leaves an ingested payload in the local store — local, content-addressed,
    // and what the next attempt reuses.
    let delivery = if delivers {
        ingest_program(config, &store)?
    } else {
        None
    };
    // Rentals are acquired under the held lock — each machine behind a teardown
    // guard held for the search's life. A strict-fill shortfall tears down whatever
    // was acquired and fails here, before any task searches.
    //
    // Putting the search on its machines happens under the search's own journal
    // boundary: it is minutes of delivery and spending with no worker yet
    // bound, and what it is doing crosses the same boundary the search's records
    // cross once it drives.
    let built = under_collector(&store, &search, control.observer, |events| {
        // The program reaches every machine of yours before any pool of
        // one exists, so a pool is only ever built where a worker can
        // actually be served.
        deliver_to_owned(&members.owned, &store, delivery.as_ref(), events)?;
        let owned = match owned {
            Some(pools) => pools,
            None => owned_pools(
                &members.owned,
                &search,
                &execution,
                &program,
                delivery.as_ref(),
            )?,
        };
        let mut groups = Vec::with_capacity(members.rentals.len());
        for ((rental, provider), mode) in members.rentals.iter().zip(&providers).zip(&modes) {
            let hosts = acquire_hosts(
                rental,
                &config.budget,
                provider.as_ref(),
                &store,
                &lock,
                mode,
                &config.search.format,
                &execution,
                delivery.as_ref(),
                control.interrupt,
                events,
            )?;
            groups.push(RentalGroup {
                provider: provider.as_ref(),
                spec: rental.spec,
                fill: rental.fill,
                hosts,
            });
        }
        Ok((owned, groups))
    });
    // An interrupt reaching the search while its machines were still being
    // acquired ends it here. The acquisition released every machine it held as
    // it unwound and nothing has executed, so the store stands exactly as it
    // did and the search is resumable — which is what the search's own interrupted
    // outcome states, and is why one Ctrl-C during a placement is answered like
    // one during the work.
    let (owned, groups) = match built {
        Ok(built) => built,
        Err(_) if control.interrupt.load(std::sync::atomic::Ordering::Relaxed) => {
            return Ok(SearchOutcome::Interrupted { search });
        }
        Err(error) => return Err(error),
    };
    // The pools, the orchestrator's first: its own workers, then one container
    // pool per machine of yours, then one pool per rented machine. Worker ids
    // search global and sequential across them. The pools borrow the transports
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
    // Every rented pool spawns through the stop signal: a worker that cannot
    // spawn faults the search with nothing journaled to observe, and the
    // supervisor beside it has to wind down all the same.
    let stop = StopSignal::new();
    // Aborts a replacement acquisition in flight, so teardown never waits out
    // an offer walk. Set on a terminal event, on a spawn failure, and again
    // after the driver returns; distinct from the caller's interrupt, which the
    // search owns.
    let cancel = std::sync::atomic::AtomicBool::new(false);
    let stopping: Vec<StopOnSpawnFailure<'_>> = groups
        .iter()
        .flat_map(|group| &group.hosts)
        .map(|host| StopOnSpawnFailure {
            inner: &host.transport,
            stop: &stop,
            cancel: &cancel,
        })
        .collect();
    for (host, transport) in groups.iter().flat_map(|group| &group.hosts).zip(&stopping) {
        pools.push(WorkerPool {
            transport,
            host: host.host.clone(),
            slots: host.slots.clone(),
        });
    }
    // A search with rentals drives a supervisor thread alongside the scheduler: it
    // keeps them within the search's budget and replaces lost machines while the
    // search proceeds. Both live in one scope so the supervisor borrows the store,
    // lock, and groups; the scheduler searches on this thread, and the stop signal
    // winds the supervisor down when it returns.
    //
    // The whole of it searches under the search's own wall-clock ceiling, so a search
    // nobody is watching still ends: the flag the ceiling raises is the one
    // `SIGINT` raises, and every pool winds down on it.
    let (outcome, ceiling_fired) =
        under_ceiling(config.budget.max_wall_clock, control.interrupt, || {
            if groups.is_empty() {
                sima_scheduler::run(
                    &store,
                    &config.search,
                    &environment,
                    generator.as_ref(),
                    &pools,
                    &execution,
                    control,
                )
            } else {
                // The search's emitter reaches the supervisor through the start hook,
                // filled once the collector spawns; the supervisor emits rental events
                // through it, so they cross the same journal boundary as the rest. No
                // scheduler edge to the provider appears — the hook is an opaque
                // closure.
                let emitter: std::sync::Mutex<Option<sima_trace::Emitter>> =
                    std::sync::Mutex::new(None);
                let on_start = |e: sima_trace::Emitter| {
                    *emitter
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(e);
                };
                // Wrap the caller's observer to stop the supervisor the moment the search
                // reaches a terminal event: the supervisor then drops its emitter
                // clone, so the search's collector — which joins only once every emitter
                // is dropped — can shut down. A fault emits no search-level event, so
                // `Faulted` is a stop trigger too. The same event cancels a replacement
                // still acquiring.
                let caller_observer = control.observer;
                let stopper = |record: &sima_scheduler::Record| {
                    (caller_observer)(record);
                    if matches!(
                        record.event,
                        sima_scheduler::Event::SearchFinalized { .. }
                            | sima_scheduler::Event::SearchFailed { .. }
                            | sima_scheduler::Event::SearchInterrupted { .. }
                            | sima_scheduler::Event::Faulted { .. }
                    ) {
                        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                        stop.raise();
                    }
                };
                let control = SearchControl {
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
                    let handle = scope.spawn(|| supervisor.search(&stop));
                    let outcome = sima_scheduler::run(
                        &store,
                        &config.search,
                        &environment,
                        generator.as_ref(),
                        &pools,
                        &execution,
                        &control,
                    );
                    // Cancel any replacement the supervisor is still acquiring before
                    // joining it: teardown must not wait out an offer walk for a
                    // machine the finished search no longer needs.
                    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                    stop.raise();
                    handle.join().expect("the supervisor thread joins");
                    outcome
                })
            }
        });
    // The pools' borrow of the rented transports and guards ends here, before
    // teardown.
    drop(pools);
    // A ceiling that fired says so in the search's journal, so the operator reads
    // why the search interrupted rather than inferring it from the outcome.
    if let (true, Some(limit)) = (ceiling_fired, config.budget.max_wall_clock) {
        report_ceiling(&store, &search, control.observer, limit)?;
    }
    // Guards release explicitly on the success path, surfacing a teardown
    // failure — a machine still running is worth an operator's attention. A search
    // that already faulted keeps its fault; teardown is best-effort, and the
    // ledger record a failed teardown leaves is what reconcile acts on next.
    let released = release_all(groups);
    match outcome {
        Ok(search_outcome) => released.map(|()| search_outcome),
        Err(error) => Err(error),
    }
}

/// The orchestrator's own worker pool: plain subprocesses, or a container pool
/// on this machine when `[orchestrator]` names an image.
struct LocalPool {
    transport: Box<dyn sima_transport::WorkerTransport>,
    slots: Vec<Option<DeviceBinding>>,
}

/// A resolved container pool on one machine: its transport, the machine it searches
/// on, and its slots.
struct ContainerPool {
    transport: ContainerTransport,
    host: String,
    slots: Vec<Option<DeviceBinding>>,
}

/// What every pool's workers are spawned to answer for: the search's format, and
/// the program the search sent for it. The two travel together because one
/// handshake states both, and every pool of one search states the same pair.
#[derive(Clone)]
struct WorkerProgram {
    format: FormatId,
    /// `Some` exactly when the config entry resolved a `payload_digest` — the
    /// program this search installed where it resolved. Every worker answers it
    /// back; one that answers another digest, or none, fails its spawn, which
    /// is what a machine holding some other program looks like from here.
    digest: Option<String>,
}

impl WorkerProgram {
    /// What `config`'s search spawns for on this machine: its format, and the
    /// digest of the program routed to that format where the entry stated one.
    fn of(config: &LoadedConfig) -> WorkerProgram {
        WorkerProgram {
            format: config.search.format.clone(),
            digest: config
                .domains
                .routed(&config.search.format)
                .and_then(|routed| routed.payload_digest.map(Hash::to_string)),
        }
    }

    /// The same search on a machine `delivery` reached: what that machine
    /// installed is what its workers answer, whatever this one holds.
    fn delivered(&self, delivery: &ProgramDelivery) -> WorkerProgram {
        WorkerProgram {
            format: self.format.clone(),
            digest: Some(delivery.payload().to_string()),
        }
    }
}

/// What a machine's containers search for this search.
///
/// A format this build carries is answered by the image's own worker: nothing
/// was delivered there, so nothing is mounted and no digest is expected back. A
/// format that is a program is answered by what the delivery installed under
/// the machine's own `root`, which is what its workers are spawned as and what
/// they answer at the handshake.
enum MachineProgram<'a> {
    /// The image answers for the search's format itself.
    Image,
    /// The program a delivery installed under `root` on that machine.
    Delivered {
        delivery: &'a ProgramDelivery,
        root: &'a str,
    },
}

impl MachineProgram<'_> {
    /// What one worker's container searches there.
    fn worker_run(&self) -> ContainerRun {
        match self {
            MachineProgram::Image => ContainerRun::worker(Vec::new()),
            MachineProgram::Delivered { delivery, root } => delivery.container_run(root, &[]),
        }
    }

    /// The devices this search's work can be placed on there, enumerated in a
    /// throwaway container where the pool's own containers search — so the answer
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

/// Whether this config leaves its worker layout to the program its format is
/// routed to.
///
/// A config states no `[orchestrator]` layout and routes its format through an
/// entry carrying `payload_digest`. That is what a migration onto a rented
/// machine writes: nothing on that machine could say where the search's work goes
/// until the program was installed there, so the answer is deferred to the far
/// search, which takes it from the program's own enumeration at start.
///
/// The digest is what scopes it. It is a key only a migration writes, so a
/// hand-written config naming a program on this machine still states its own
/// layout, as every config does, and nothing about it changes meaning.
fn derives_workers(config: &LoadedConfig) -> bool {
    config.orchestrator.pool.is_none()
        && config
            .domains
            .routed(&config.search.format)
            .is_some_and(|routed| routed.payload_digest.is_some())
}

/// Builds the orchestrator's own pool, or `None` when it declares no worker
/// layout and the fleet carries the search.
///
/// Without an image the workers are plain subprocesses of the binary `source`
/// names, and their device selectors resolve against this machine's own
/// hardware. With one they search in a container here, so the image is verified
/// and the selectors resolve against what the enumeration probe reports from
/// inside it — the same path a machine of yours follows, minus the ssh hop.
fn local_pool(
    config: &LoadedConfig,
    search: &SearchId,
    execution: &ExecutionConfig,
    source: &dyn DomainSource,
    program: &WorkerProgram,
) -> Result<Option<LocalPool>> {
    let Some(pool) = &config.orchestrator.pool else {
        if !derives_workers(config) {
            return Ok(None);
        }
        // The layout the program itself decides: one worker per usable device
        // of its own enumeration. It is answered here, on the machine the
        // program is installed on, because that is the only place the answer
        // exists — the load that just ran is what put the program there.
        return Ok(Some(LocalPool {
            transport: Box::new(SubprocessTransport::new(
                source.worker_binary()?,
                Vec::new(),
                spawn_settings(source.spawn_policy(), execution, program),
            )),
            slots: devices::derived_slots(&source.enumerate_devices(&config.search.format)?),
        }));
    };
    match &config.orchestrator.container {
        None => Ok(Some(LocalPool {
            transport: Box::new(SubprocessTransport::new(
                // The binary the format's tasks execute in: sima's own worker,
                // or the program the config routed this format to.
                source.worker_binary()?,
                // A local worker searches the bare binary: no arguments.
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
                search,
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
/// `delivery` is what was put on those machines, and `None` for a search whose
/// format the image answers for itself. It decides both what a worker there
/// searches and what it is expected to answer.
fn owned_pools(
    machines: &[OwnedMachine<'_>],
    search: &SearchId,
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
                search,
                execution,
                &program,
                &machine_program,
            )
        })
        .collect()
}

/// Builds one container pool: derives the pool's slots, and constructs the
/// transport under a container-name stem unique to this search and pool.
///
/// The image is confirmed present by the caller, which is where the ordering
/// against the store is decided.
#[allow(clippy::too_many_arguments)]
fn container_pool(
    host: Option<&str>,
    container: &Container,
    pool: &Pool,
    index: usize,
    search: &SearchId,
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
    // A deterministic per-search, per-pool container-name stem; the transport adds
    // a spawn suffix. The search id prefix keeps names distinct across concurrent
    // searches on one machine.
    let stem = search.to_string();
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

/// The search's execution settings with the orchestrator's device selectors
/// resolved against this machine's devices. An orchestrator naming no device
/// passes through untouched, so a search that never asked about devices never
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
    let entries = devices::resolve(selectors, &source.enumerate_devices(&config.search.format)?)?;
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
/// pool's binary calls for, plus the search's answer deadline and checkpoint
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
