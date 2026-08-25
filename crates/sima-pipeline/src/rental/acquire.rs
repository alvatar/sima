//! Acquiring the machines a rented entry declares, and letting them go.
//!
//! A rental asks its control plane for `count` machines, holds each behind a
//! teardown guard until the whole group is admitted, and probes every one for
//! the devices its workers are placed on. What a partial acquisition means is
//! the entry's own declaration: strict fails the run and tears down what came
//! up, best-effort runs on whatever did.

use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::FormatId;
use sima_provider::{
    AcquireLimits, Budget, Exhaustion, IncidentKind, InstanceGuard, Objective, Provider,
    Reachability, SshEndpoint, acquire, now_ms, record_incident,
};
use sima_scheduler::Event;
use sima_scheduler::ExecutionConfig;
use sima_store::{Rental as RentalRole, RunLock, Store};
use sima_transport::{SpawnMode, SshDestination, SshTransport};

use crate::config::{FillPolicy, Rented};
use crate::devices::{derived_slots, parse_enumeration};
use crate::fleet::Rental;
use crate::process::{command_stdout, worker_binary};
use crate::program_delivery::ProgramDelivery;
use crate::providers::{ProviderSettings, provider_for};
use crate::rental::rented_program::RentedProgram;

/// The control-plane backend a rental acquires its machines through.
///
/// The rental's declared image, disk, and count are the settings the backend is
/// built with; which backend that is comes from the one provider registry, so a
/// run and a reconciliation resolve the same id the same way.
pub(crate) fn provider_for_rental(rental: &Rental<'_>) -> Result<Box<dyn Provider + Sync>> {
    provider_for(
        rental.spec.provider.as_str(),
        &ProviderSettings {
            image: &rental.spec.image,
            disk_gb: rental.spec.disk_gb,
            count: rental.count,
        },
    )
}

/// The transport mode a control plane's machines are reached through, from what
/// the control plane says about them: ssh to a machine that is really there, or
/// a local spawn for a backend whose machines are this machine.
///
/// The answer is the provider's, not the config's. A backend knows whether the
/// endpoint it reports names anything, and the worker binary a local spawn
/// needs is this layer's to supply — which is why [`Reachability`] and not
/// [`SpawnMode`] is what crosses the boundary.
pub(crate) fn transport_mode(provider: &(dyn Provider + Sync)) -> Result<SpawnMode> {
    match provider.reachability() {
        Reachability::Ssh => Ok(SpawnMode::Ssh),
        Reachability::Local => Ok(SpawnMode::Local(worker_binary()?)),
    }
}

/// Maps a provider's ssh endpoint into the transport's target, the boundary that
/// keeps the transport free of any dependency on the provider crate.
pub(crate) fn endpoint_target(endpoint: SshEndpoint) -> SshDestination {
    SshDestination::rented(endpoint.host, endpoint.port, endpoint.user)
}

/// How many machines one instance's acquisition may burn through before it
/// gives up. Each attempt is a paid rental torn down again, so the bound
/// stays small; a machine that fails twice across runs is blacklisted by
/// its incidents and stops being offered at all.
///
/// Each attempt runs under one `ready_timeout` covering everything it waits
/// for, so this is what the worst case multiplies: `PROBE_ACQUIRE_ATTEMPTS`
/// machines at one `ready_timeout` each, however many offers a walk tries
/// inside one of them.
const PROBE_ACQUIRE_ATTEMPTS: usize = 4;

/// One acquired machine: the guard that owns and tears it down, the transport
/// its pool spawns workers through, and the worker slots its probe derived (one
/// per enumerated GPU, or one deviceless slot when it reports none).
pub(crate) struct RentedHost<'a> {
    /// Ownership of the rented machine; its teardown runs on release or drop.
    /// Behind a lock and an `Option` so the supervisor can swap in a
    /// replacement without disturbing the pool's shared borrow of the
    /// transport; `None` once the guard has been released or the machine has
    /// retired with no replacement.
    pub(crate) guard: Mutex<Option<InstanceGuard<'a, dyn Provider + Sync + 'a>>>,
    /// The transport spawning this machine's workers, its target swappable
    /// under the running pool.
    pub(crate) transport: SshTransport,
    /// The machine's host label, for the journal.
    pub(crate) host: String,
    /// One slot per enumerated GPU, or a single deviceless slot. Fixed for the
    /// run: a replacement must carry at least this many GPUs.
    pub(crate) slots: Vec<Option<DeviceBinding>>,
}

/// One rented entry's machines, under the control plane and specification they
/// were acquired through. A run may draw on several, each with its own provider
/// and its own shortfall policy, all under the run's single budget.
pub(crate) struct RentalGroup<'a> {
    /// The control plane its machines came from.
    pub(crate) provider: &'a (dyn Provider + Sync),
    /// What each machine was rented as.
    pub(crate) spec: &'a Rented,
    /// What a shortfall does, read again when a lost machine cannot be
    /// replaced.
    pub(crate) fill: FillPolicy,
    /// The machines that came up.
    pub(crate) hosts: Vec<RentedHost<'a>>,
}

/// Acquires a rental's machines, each behind a teardown guard, and builds a
/// transport and worker slots for each.
///
/// Every acquisition is budget-admitted and intent-recorded by
/// [`acquire`](sima_provider::acquire), and a machine that fails to acquire or
/// probe is torn down individually. On a shortfall the fill policy decides:
/// strict tears down everything acquired so far and fails the run; best-effort
/// proceeds with what came up, so long as one machine did.
#[allow(clippy::too_many_arguments)]
pub(crate) fn acquire_hosts<'a>(
    rental: &Rental<'_>,
    budget: &Budget,
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &RunLock,
    mode: &SpawnMode,
    format: &FormatId,
    exec: &ExecutionConfig,
    delivery: Option<&ProgramDelivery>,
) -> Result<Vec<RentedHost<'a>>> {
    let program = match delivery {
        None => RentedProgram::Image,
        Some(delivery) => RentedProgram::Delivered {
            delivery,
            binary: rental.binary,
            root: rental.root,
        },
    };
    let mut hosts: Vec<RentedHost<'a>> = Vec::with_capacity(rental.count);
    for _ in 0..rental.count {
        // A machine that fails to acquire, probe, or receive the program is
        // torn down inside `acquire_one` before its error returns here.
        match acquire_one(
            provider,
            store,
            lock,
            rental.spec,
            budget,
            mode,
            format,
            exec,
            &program,
        ) {
            Ok(host) => hosts.push(host),
            Err(error) => match rental.fill {
                // Strict: the declared count or nothing. Dropping `hosts` here
                // tears down every machine already acquired.
                FillPolicy::Strict => return Err(error),
                // Best-effort: run with what came up. Stop asking on the first
                // shortfall — the market is not filling the count.
                FillPolicy::BestEffort => break,
            },
        }
    }
    if hosts.is_empty() {
        return Err(Error::Provider(format!(
            "the rental {:?} acquired no machine",
            rental.name
        )));
    }
    Ok(hosts)
}

/// Acquires one machine, probes it, and builds its transport and slots. On a
/// probe failure the guard drops here, tearing the machine down, so no
/// half-acquired rental leaks, and the acquisition moves to another machine: a
/// marketplace serves hosts that come up but never accept a session, and one of
/// them must cost a machine rather than the run.
#[allow(clippy::too_many_arguments)]
fn acquire_one<'a>(
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &RunLock,
    spec: &Rented,
    budget: &Budget,
    mode: &SpawnMode,
    format: &FormatId,
    exec: &ExecutionConfig,
    program: &RentedProgram<'_>,
) -> Result<RentedHost<'a>> {
    // A machine that fails its probe is excluded from the attempts that
    // follow, so the retry reaches a different machine instead of renting
    // the same broken one again. The exclusion is local to this
    // acquisition; the durable incident it also records is what carries
    // the machine's reputation across runs.
    let mut constraints = spec.constraints.clone();
    let mut refused: Option<Error> = None;
    for _ in 0..PROBE_ACQUIRE_ATTEMPTS {
        // The clock on this machine starts where it is first asked for, and
        // both stages that wait for it — reporting ready, then answering a
        // probe — run under the one deadline. Each attempt reaches a different
        // machine, so each gets a whole budget and none gets two.
        let usable_by = Instant::now() + spec.ready_timeout;
        let limits = AcquireLimits {
            usable_by,
            ready_poll: spec.ready_poll,
        };
        // Pin the trait object to `Sync`, which the supervisor thread's shared
        // borrow of the provider needs; without it inference drops the bound.
        let guard = acquire::<dyn Provider + Sync>(
            provider,
            store,
            lock,
            RentalRole::Worker,
            &constraints,
            Objective::CheapestPerHour,
            &limits,
            budget,
            // Run-start acquisition has nothing to cancel: the run is not yet
            // driving, so no wind-down is in flight.
            never_cancelled(),
        )?;
        let target = endpoint_target(guard.endpoint().clone());
        let host = target.host().to_string();
        // Three stages, each of which can cost the machine rather than the
        // run: it answers, it receives what the run needs, and it says where
        // that work can go. A failure in any of them records an incident, drops
        // the guard — tearing the machine down — and moves to another machine.
        let outcome = probe_ready(mode, &target, usable_by, spec.ready_poll, program, format)
            .map(|answered| (answered, IncidentKind::ProbeFailed))
            .and_then(|(answered, _)| {
                program
                    .install(store, mode, &target)
                    .map(|()| answered)
                    .map_err(|error| (error, IncidentKind::InstallFailed))
            })
            .and_then(|answered| {
                program
                    .devices(answered, mode, &target, format, exec.answer_timeout)
                    .map_err(|error| (error, IncidentKind::ProbeFailed))
            });
        let slots = match outcome {
            Ok(devices) => derived_slots(&devices),
            Err((error, kind)) => {
                // The machine reported ready but cannot serve this run: an
                // incident against it, recorded before the guard drops and
                // tears it down. A store failure recording the incident
                // supersedes the original error.
                record_incident(
                    store,
                    provider.id(),
                    guard.machine(),
                    guard.tag(),
                    kind,
                    now_ms(),
                )?;
                if !guard.machine().is_empty() {
                    constraints
                        .excluded_machines
                        .push(guard.machine().to_string());
                }
                refused = Some(error);
                continue;
            }
        };
        // What the machine's workers run there, and what they answer for. The
        // ssh client is sima's own process, so it keeps the ambient
        // environment: it reads its agent socket and client configuration from
        // it, and what the far side sees is stated on the far side.
        let (mode, command, settings) = program.spawn(mode, format, exec)?;
        let transport = SshTransport::new(
            mode,
            target,
            command,
            settings,
            // The transport waits out a respawn against a dead host on the
            // same readiness bounds the machine was acquired under, bridging
            // the window until the supervisor swaps a replacement in.
            spec.ready_timeout,
            spec.ready_poll,
        );
        return Ok(RentedHost {
            guard: Mutex::new(Some(guard)),
            transport,
            host,
            slots,
        });
    }
    Err(refused.unwrap_or_else(|| Error::Provider("the acquisition never ran".to_string())))
}

/// Waits for a machine to answer its readiness probe, and returns what it
/// answered, retrying under the machine's own readiness bounds.
///
/// A provider reports an instance ready when its container is running, which is
/// before the route to it carries an ssh, so the first probes against a fresh
/// machine are refused. `usable_by` is the deadline the machine was asked for
/// under, which its readiness wait has already been spending: the entry states
/// one budget for how long from asking for a machine until it is usable, and
/// this is the second stage of that one wait. A machine that answers at once
/// costs nothing. Giving up destroys this rental and takes the next offer, so
/// the bound is what separates a machine that is slow from one that is broken.
fn probe_ready(
    mode: &SpawnMode,
    target: &SshDestination,
    usable_by: Instant,
    poll: Duration,
    program: &RentedProgram<'_>,
    format: &FormatId,
) -> std::result::Result<Vec<DeviceInfo>, (Error, IncidentKind)> {
    let argv = sima_transport::ssh::probe_argv(mode, target, program.readiness(format));
    let deadline = usable_by;
    loop {
        match command_stdout(&argv).and_then(|stdout| parse_enumeration(&stdout)) {
            Ok(devices) => return Ok(devices),
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err((error, IncidentKind::ProbeFailed));
                }
            }
        }
        thread::sleep(poll);
    }
}

/// Releases every rented machine's guard on the way out, returning the first
/// teardown failure. Every guard is released whatever the others do, so one
/// failure never strands the rest; a guard whose release is not reached is torn
/// down by its drop, and the ledger record a failed teardown leaves is what the
/// next reconciliation pass acts on.
pub(crate) fn release_all(groups: Vec<RentalGroup<'_>>) -> Result<()> {
    let mut first: Option<Error> = None;
    for group in groups {
        for host in group.hosts {
            // The transport drops with the machine; only the guard's teardown
            // can fail and is worth reporting. A `None` guard was already
            // released by a replacement, or retired with none, so there is
            // nothing to tear down.
            let guard = host
                .guard
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(guard) = guard
                && let Err(error) = guard.release()
                && first.is_none()
            {
                first = Some(error);
            }
        }
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The event a spent ceiling raises. A fleet's supervisor and a migration both
/// report exhaustion, and one journal reads the two the same way.
pub(crate) fn budget_exhausted(exhaustion: Exhaustion) -> Event {
    match exhaustion {
        Exhaustion::Spend { accrued, cap } => Event::BudgetSpendExhausted {
            accrued_microusd: accrued.0,
            cap_microusd: cap.0,
        },
        Exhaustion::WallClock { deadline_ms } => Event::BudgetWallClockExhausted { deadline_ms },
    }
}

/// A cancellation flag that is never set, for an acquisition with no wind-down
/// to observe — the run-start acquisition, before the run drives.
pub(super) fn never_cancelled() -> &'static AtomicBool {
    static NEVER: AtomicBool = AtomicBool::new(false);
    &NEVER
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use sima_contracts::DeviceClass;
    use sima_domains::devices::DeviceType;
    use sima_provider::stub::StubProvider;
    use sima_provider::{InstanceStatus, Provision};

    use super::*;
    use crate::config::FillPolicy;
    use crate::rental::fixtures::{
        acquisition_env, deviceless_probe, exec, offer, one_group, rental, spec,
    };

    /// One enumerated device of the given category.
    fn device(class: &str, name: &str, device_type: DeviceType) -> DeviceInfo {
        DeviceInfo {
            class: DeviceClass::new(class).expect("class id"),
            name: name.to_string(),
            device_type,
            member: 0,
        }
    }

    #[test]
    fn a_machine_with_no_device_gets_one_deviceless_slot() {
        assert_eq!(derived_slots(&[]), vec![None]);
    }

    #[test]
    fn every_gpu_gets_a_slot_bound_to_it() {
        let devices = [
            device("10de:2684", "NVIDIA GeForce RTX 4090", DeviceType::Discrete),
            device("8086:7d51", "Intel(R) Graphics", DeviceType::Integrated),
        ];
        let slots = derived_slots(&devices);
        assert_eq!(slots.len(), 2);
        assert_eq!(
            slots[0],
            Some(DeviceBinding {
                class: DeviceClass::new("10de:2684").expect("class id"),
                member: 0
            })
        );
        assert_eq!(
            slots[1],
            Some(DeviceBinding {
                class: DeviceClass::new("8086:7d51").expect("class id"),
                member: 0
            })
        );
    }

    #[test]
    fn a_software_rasterizer_beside_a_gpu_gets_no_slot() {
        // What a rented host reports: its card, and the CPU rasterizer the
        // graphics stack falls back to. The machine was rented for the card.
        let devices = [
            device("10005:0000", "llvmpipe (LLVM 19)", DeviceType::Cpu),
            device("10de:2684", "NVIDIA GeForce RTX 4090", DeviceType::Discrete),
        ];
        let slots = derived_slots(&devices);
        assert_eq!(slots.len(), 1, "one slot, on the GPU");
        assert_eq!(
            slots[0],
            Some(DeviceBinding {
                class: DeviceClass::new("10de:2684").expect("class id"),
                member: 0
            })
        );
    }

    #[test]
    fn a_machine_with_only_a_rasterizer_still_gets_a_slot() {
        // With no GPU to prefer, the rasterizer is the only device this
        // program can open and takes the slot. This is what a rented machine
        // reports to a WGSL run when its Vulkan loader cannot initialize the
        // NVIDIA driver: the card is there, and CUDA would enumerate it, but a
        // slot bound to it would hand a worker a device Vulkan cannot open.
        let devices = [device("10005:0000", "llvmpipe (LLVM 19)", DeviceType::Cpu)];
        let slots = derived_slots(&devices);
        assert_eq!(slots.len(), 1);
        assert_eq!(
            slots[0],
            Some(DeviceBinding {
                class: DeviceClass::new("10005:0000").expect("class id"),
                member: 0
            })
        );
    }

    #[test]
    fn a_strict_shortfall_tears_down_what_was_acquired_and_fails() -> Result<()> {
        // One offer for two requested machines under strict fill: the run
        // fails, and the one machine that came up is torn down.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let result = acquire_hosts(
            &rental(&spec, 2, FillPolicy::Strict),
            &Budget::default(),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
            None,
        );
        assert!(matches!(result, Err(Error::Provider(_))));
        assert_eq!(
            provider.destroyed().len(),
            1,
            "the acquired machine is torn down"
        );
        assert!(provider.live().is_empty(), "no machine is left running");
        Ok(())
    }

    #[test]
    fn a_best_effort_shortfall_proceeds_with_what_came_up() -> Result<()> {
        // One offer for two requested machines under best-effort: the run
        // proceeds with the one machine, torn down on release.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let hosts = acquire_hosts(
            &rental(&spec, 2, FillPolicy::BestEffort),
            &Budget::default(),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
            None,
        )?;
        assert_eq!(hosts.len(), 1, "best-effort runs on what came up");
        assert!(
            provider.destroyed().is_empty(),
            "still running before release"
        );
        release_all(one_group(&provider, &spec, FillPolicy::BestEffort, hosts))?;
        assert_eq!(
            provider.destroyed().len(),
            1,
            "release tears the machine down"
        );
        assert!(provider.live().is_empty());
        Ok(())
    }

    #[test]
    fn a_rental_acquires_and_probes_every_machine() -> Result<()> {
        // Two offers for two machines: both acquire, each probed into a single
        // deviceless slot, all torn down on release.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let hosts = acquire_hosts(
            &rental(&spec, 2, FillPolicy::Strict),
            &Budget::default(),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
            None,
        )?;
        assert_eq!(hosts.len(), 2);
        for host in &hosts {
            assert_eq!(
                host.slots,
                vec![None],
                "a probe reporting no GPU is one slot"
            );
        }
        release_all(one_group(&provider, &spec, FillPolicy::Strict, hosts))?;
        assert_eq!(provider.destroyed().len(), 2);
        assert!(provider.live().is_empty());
        Ok(())
    }

    #[test]
    fn a_probe_failure_tears_the_machine_down() -> Result<()> {
        // The machine acquires but its probe never runs: it is torn down rather
        // than left running with no slots.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let result = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &Budget::default(),
            &provider,
            &store,
            &lock,
            &SpawnMode::Local(PathBuf::from("/nonexistent/sima-worker")),
            &format,
            &exec(),
            None,
        );
        assert!(result.is_err(), "a probe failure fails the acquisition");
        assert_eq!(provider.destroyed().len(), 1, "the machine is torn down");
        // The market held one machine, so the retry has nowhere to go.
        assert!(provider.live().is_empty());
        // A machine that reported ready but failed the probe cannot run work:
        // one ProbeFailed incident against it.
        let incidents = store.machine_incidents()?;
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].kind, IncidentKind::ProbeFailed);
        assert_eq!(incidents[0].machine, "machine-a");
        Ok(())
    }

    #[test]
    fn a_machine_that_refuses_its_probe_costs_a_machine_not_the_acquisition() -> Result<()> {
        // A marketplace serves hosts that come up but never accept a session.
        // The acquisition moves to the next machine instead of failing the
        // run, and does not rent the refusing machine again: both offers are
        // tried, each torn down, each carrying its own incident.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let result = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &Budget::default(),
            &provider,
            &store,
            &lock,
            &SpawnMode::Local(PathBuf::from("/nonexistent/sima-worker")),
            &format,
            &exec(),
            None,
        );
        assert!(result.is_err(), "no machine in the market could be probed");
        assert_eq!(provider.destroyed().len(), 2, "each attempt is torn down");
        assert!(provider.live().is_empty());
        let mut machines: Vec<String> = store
            .machine_incidents()?
            .into_iter()
            .map(|incident| incident.machine)
            .collect();
        machines.sort();
        assert_eq!(machines, vec!["machine-a", "machine-b"]);
        Ok(())
    }

    #[test]
    fn each_reachability_routes_onto_its_spawn_mode() -> Result<()> {
        // A stub pointed at a machine that is really there is reached over ssh,
        // exactly as a rented one is; one pointed at nothing spawns here.
        let reached = StubProvider::new(Vec::new()).endpoint("127.0.0.1", 41022, "tester");
        assert!(matches!(transport_mode(&reached)?, SpawnMode::Ssh));
        let in_process = StubProvider::new(Vec::new());
        assert!(matches!(transport_mode(&in_process)?, SpawnMode::Local(_)));
        Ok(())
    }

    #[test]
    fn the_stub_provider_lists_an_offer_per_requested_machine() -> Result<()> {
        let spec = spec();
        let provider = provider_for_rental(&rental(&spec, 3, FillPolicy::Strict))?;
        assert_eq!(provider.id(), "stub");
        assert_eq!(provider.offers()?.len(), 3);
        Ok(())
    }

    #[test]
    fn the_stub_provider_acquires_a_machine_that_reaches_ready() -> Result<()> {
        // The stub acquires: provisioning an offer yields an instance that its
        // own status call reports Ready with an ssh endpoint, which maps to a
        // transport target.
        let spec = spec();
        let provider = provider_for_rental(&rental(&spec, 1, FillPolicy::Strict))?;
        let offer = provider.offers()?.into_iter().next().expect("an offer");
        let Provision::Provisioned(instance) = provider.provision(&offer.id, "tag-0")? else {
            panic!("the stub provisions an always-available offer");
        };
        let InstanceStatus::Ready(endpoint) = provider.instance(&instance.id)? else {
            panic!("the stub instance is ready at once");
        };
        let target = endpoint_target(endpoint.clone());
        assert_eq!(target.host(), endpoint.host);
        // The endpoint's port and user reach the invocation, which is where
        // they are observable: a rented destination states both explicitly.
        assert_eq!(
            target.prefix(),
            SshDestination::rented(&endpoint.host, endpoint.port, &endpoint.user).prefix()
        );
        Ok(())
    }
}
