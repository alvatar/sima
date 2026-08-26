//! Acquiring the machines a rented entry declares, and letting them go.
//!
//! A rental asks its control plane for `count` machines, holds each behind a
//! teardown guard until the whole group is admitted, and probes every one for
//! the devices its workers are placed on. What a partial acquisition means is
//! the entry's own declaration: strict fails the run and tears down what came
//! up, best-effort runs on whatever did.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::FormatId;
use sima_provider::{
    AcquireLimits, Budget, Exhaustion, IncidentKind, InstanceGuard, Objective, Offer, Provider,
    Reachability, SshEndpoint, acquire, now_ms, record_incident,
};
use sima_scheduler::ExecutionConfig;
use sima_scheduler::{Event, Level};
use sima_store::{Rental as RentalRole, RunLock, Store};
use sima_trace::Emitter;
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
///
/// `interrupt` is the run's own wind-down flag, read inside every wait an
/// acquisition spends: the machines are minutes of paid-for waiting before the
/// run drives, and an operator who lets go there must not have to wait them out
/// or kill the process over them.
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
    interrupt: &AtomicBool,
    events: &Emitter,
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
    for index in 0..rental.count {
        let member = member(rental.name, index);
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
            interrupt,
            events,
            &member,
        ) {
            Ok(host) => hosts.push(host),
            Err(error) => {
                // An operator who let go stops the acquisition where it is,
                // whatever the fill policy would make of a member that could
                // not be brought up: nothing fell short, the run is ending.
                // Dropping `hosts` on the way out tears down every machine
                // this rental had, which is the whole of what is said here —
                // a fleet released before it ran leaves nothing else to read
                // it from.
                if interrupt.load(Ordering::Relaxed) {
                    events.emit(Event::AcquisitionAbandoned {
                        released: hosts.len(),
                    });
                    return Err(error);
                }
                // What the shortfall costs is the entry's own declaration, and
                // the operator is told which member fell short and what
                // follows from it — one machine short of a fleet is otherwise
                // invisible until the run's rate looks wrong.
                events.emit(shortfall(&member, rental, &error, hosts.len()));
                match rental.fill {
                    // Strict: the declared count or nothing. Dropping `hosts`
                    // here tears down every machine already acquired.
                    FillPolicy::Strict => return Err(error),
                    // Best-effort: run with what came up. Stop asking on the
                    // first shortfall — the market is not filling the count.
                    FillPolicy::BestEffort => break,
                }
            }
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
    interrupt: &AtomicBool,
    events: &Emitter,
    member: &str,
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
            // The wait for a machine to come up is the longest thing an
            // acquisition does, so the run's interrupt reaches inside it: an
            // operator letting go here is answered without waiting the machine
            // out.
            interrupt,
            &|offer| taken(events, member, spec.ready_timeout, offer),
        )?;
        let target = endpoint_target(guard.endpoint().clone());
        let host = target.host().to_string();
        // Three stages, each of which can cost the machine rather than the
        // run: it answers, it receives what the run needs, and it says where
        // that work can go. A failure in any of them records an incident, drops
        // the guard — tearing the machine down — and moves to another machine.
        let outcome = probe_ready(
            mode,
            &target,
            usable_by,
            spec.ready_poll,
            program,
            format,
            interrupt,
        )
        .and_then(|answered| {
            if matches!(program, RentedProgram::Delivered { .. }) {
                events.emit(Event::InstallingProgram {
                    member: member.to_string(),
                });
            }
            program
                .install(store, mode, &target)
                .map(|()| answered)
                .map_err(|error| Unusable::Machine(error, IncidentKind::InstallFailed))
        })
        .and_then(|answered| {
            program
                .devices(answered, mode, &target, format, exec.answer_timeout)
                .map_err(|error| Unusable::Machine(error, IncidentKind::ProbeFailed))
        });
        let slots = match outcome {
            Ok(devices) => derived_slots(&devices),
            // The operator let go while this machine was being brought up. It
            // was never given its time to answer, so nothing is held against
            // it and no other machine is rented in its place; the guard drops
            // on the way out and tears this one down.
            Err(Unusable::Interrupted) => return Err(interrupted()),
            Err(Unusable::Machine(error, kind)) => {
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

/// Why a machine did not come to serve this run.
enum Unusable {
    /// The machine's own doing: an incident is recorded against it, it is
    /// excluded from the attempts that follow, and another machine is tried.
    Machine(Error, IncidentKind),
    /// The run was interrupted while the machine was being brought up. It
    /// answered for nothing, so nothing is recorded against it and no other
    /// machine is asked for.
    Interrupted,
}

/// The error an acquisition the run's interrupt reached returns. What an
/// operator reads is the abandoned line and the run's own interrupted
/// outcome; this states the fact for a caller holding the error alone.
fn interrupted() -> Error {
    Error::Provider("the acquisition was interrupted".to_string())
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
///
/// `interrupt` is read once per attempt, before the wait for the next one, so
/// an operator letting go is answered inside the wait rather than after it.
fn probe_ready(
    mode: &SpawnMode,
    target: &SshDestination,
    usable_by: Instant,
    poll: Duration,
    program: &RentedProgram<'_>,
    format: &FormatId,
    interrupt: &AtomicBool,
) -> std::result::Result<Vec<DeviceInfo>, Unusable> {
    let argv = sima_transport::ssh::probe_argv(mode, target, program.readiness(format));
    let deadline = usable_by;
    loop {
        match command_stdout(&argv).and_then(|stdout| parse_enumeration(&stdout)) {
            Ok(devices) => return Ok(devices),
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(Unusable::Machine(error, IncidentKind::ProbeFailed));
                }
            }
        }
        // Read after the attempt that just refused and before the sleep, so a
        // refusal the operator interrupted is not read as the machine's
        // answer: the deadline is what decides that, and it has not passed.
        if interrupt.load(Ordering::Relaxed) {
            return Err(Unusable::Interrupted);
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

/// What an acquisition says as it takes an offer: what is now being paid for,
/// and the wait for that machine to become usable.
///
/// The wait is stated once, whatever it costs in polls. It spans the provider
/// reporting the instance ready and the route to it carrying an ssh — a boot
/// and an image pull — and the operator is owed the reason for the silence
/// rather than one line per attempt. `ready_timeout` is what the entry
/// describing the machine states about how long that may take.
pub(crate) fn taken(events: &Emitter, member: &str, ready_timeout: Duration, offer: &Offer) {
    events.emit(renting(member, offer));
    events.emit(Event::AwaitingMachine {
        timeout_ms: ready_timeout.as_millis() as u64,
    });
}

/// The `Renting` event for an offer a machine has just been provisioned
/// against: what is now being paid for, stated before the wait for it to come
/// up. `member` names the fleet member it was taken for, and is empty for a
/// migration, which rents the one machine its destination names.
fn renting(member: &str, offer: &Offer) -> Event {
    Event::Renting {
        member: member.to_string(),
        machine: offer.machine.clone(),
        gpu_model: offer.gpu_model.clone(),
        gpu_count: offer.gpu_count,
        rate_microusd_hour: offer.price.0,
    }
}

/// How a fleet member is named in what the run says about it: the entry that
/// declared it and its index within that entry's count.
fn member(name: &str, index: usize) -> String {
    format!("{name}[{index}]")
}

/// What a member that could not be brought up is reported as: the member, why,
/// and what the entry's fill policy makes of it. `acquired` is how many
/// machines of this entry did come up.
fn shortfall(member: &str, rental: &Rental<'_>, error: &Error, acquired: usize) -> Event {
    let consequence = match rental.fill {
        FillPolicy::Strict => format!(
            "{:?} states strict fill, so the run stops and what it acquired is torn down",
            rental.name
        ),
        FillPolicy::BestEffort => format!(
            "{:?} states best-effort fill, so the run goes on with the {acquired} machine(s) \
             that came up",
            rental.name
        ),
    };
    Event::Diagnostic {
        level: Level::Warn,
        source: "rental".to_string(),
        message: format!("{member} could not be brought up: {error}; {consequence}"),
        worker: None,
        host: None,
        task: None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc::Receiver;

    use sima_contracts::DeviceClass;
    use sima_domains::devices::DeviceType;
    use sima_provider::stub::StubProvider;
    use sima_provider::{InstanceStatus, OfferId, Provision, never_cancelled};

    use super::*;
    use crate::config::FillPolicy;
    use crate::rental::fixtures::{
        acquisition_env, deviceless_probe, exec, heard, offer, one_group, rental, spec, unheard,
        waiting_spec,
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
            never_cancelled(),
            &unheard(),
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
            never_cancelled(),
            &unheard(),
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

    /// The warn diagnostics `events` carried, which is where a shortfall is
    /// reported.
    fn warnings(heard: Receiver<Event>) -> Vec<String> {
        heard
            .into_iter()
            .filter_map(|event| match event {
                Event::Diagnostic {
                    level: Level::Warn,
                    message,
                    ..
                } => Some(message),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_strict_shortfall_names_the_member_and_says_the_run_stops() -> Result<()> {
        // One offer for two machines: the member that could not be brought up
        // is named, with what the entry's fill policy makes of it. A fleet one
        // machine short is otherwise invisible until the run's rate looks
        // wrong.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let (events, said) = heard();
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
            never_cancelled(),
            &events,
        );
        assert!(matches!(result, Err(Error::Provider(_))));
        drop(events);
        let warnings = warnings(said);
        let [warning] = warnings.as_slice() else {
            panic!("one shortfall, one warning: {warnings:?}");
        };
        assert!(warning.contains("rented[1]"), "names the member: {warning}");
        assert!(
            warning.contains("strict fill") && warning.contains("the run stops"),
            "states what follows from it: {warning}"
        );
        Ok(())
    }

    #[test]
    fn a_best_effort_shortfall_names_the_member_and_says_the_run_goes_on() -> Result<()> {
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let (events, said) = heard();
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
            never_cancelled(),
            &events,
        )?;
        drop(events);
        let warnings = warnings(said);
        let [warning] = warnings.as_slice() else {
            panic!("one shortfall, one warning: {warnings:?}");
        };
        assert!(warning.contains("rented[1]"), "names the member: {warning}");
        // Pinned whole, because this is the sentence an operator reads when a
        // fleet comes up short and it has to read as one.
        assert!(
            warning.ends_with(
                "; \"rented\" states best-effort fill, so the run goes on with the 1 machine(s) \
                 that came up"
            ),
            "states what the run does instead: {warning}"
        );
        release_all(one_group(&provider, &spec, FillPolicy::BestEffort, hosts))?;
        Ok(())
    }

    #[test]
    fn every_machine_a_fleet_takes_says_it_is_waiting_for_it() -> Result<()> {
        // Between taking an offer and the machine answering lie a boot and an
        // image pull, and the run is paying through all of it. What is being
        // waited for is stated once per machine, right after what it costs.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let (events, said) = heard();
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
            never_cancelled(),
            &events,
        )?;
        drop(events);
        let waits: Vec<Event> = said
            .into_iter()
            .filter(|event| matches!(event, Event::Renting { .. } | Event::AwaitingMachine { .. }))
            .collect();
        let [
            Event::Renting { member: first, .. },
            Event::AwaitingMachine {
                timeout_ms: first_wait,
            },
            Event::Renting { member: second, .. },
            Event::AwaitingMachine {
                timeout_ms: second_wait,
            },
        ] = waits.as_slice()
        else {
            panic!("each member says what it took and what it waits for: {waits:?}");
        };
        assert_eq!(
            (first.as_str(), second.as_str()),
            ("rented[0]", "rented[1]")
        );
        let stated = spec.ready_timeout.as_millis() as u64;
        assert_eq!((*first_wait, *second_wait), (stated, stated));
        release_all(one_group(&provider, &spec, FillPolicy::Strict, hosts))?;
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
            never_cancelled(),
            &unheard(),
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
            never_cancelled(),
            &unheard(),
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
            never_cancelled(),
            &unheard(),
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

    /// Runs `acquisition` over `events` with `watching` beside it on another
    /// thread, and answers what each of them returned.
    ///
    /// An interrupt has to land inside a wait the acquisition is already in,
    /// and that window is one no test hits by sleeping. So the watcher reads
    /// the evidence the wait itself leaves — a journal line, a file the probe
    /// touches — and sets the flag off that.
    ///
    /// The emitter is owned here and dropped the moment the acquisition
    /// returns, which is what lets a watcher reading the journal drain and
    /// join rather than waiting on an emitter the test still holds.
    fn while_watched<T: Send, W: Send>(
        events: Emitter,
        watching: impl FnOnce() -> W + Send,
        acquisition: impl FnOnce(&Emitter) -> T + Send,
    ) -> (T, W) {
        thread::scope(|scope| {
            let watcher = scope.spawn(watching);
            let done = acquisition(&events);
            drop(events);
            (done, watcher.join().expect("the watching thread joins"))
        })
    }

    /// Sets `interrupt` when the acquisition emits an event matching `at`, and
    /// answers everything it emitted.
    ///
    /// The receiver drains when the acquisition drops its emitter, so a run
    /// that never emits a match ends this rather than stranding it.
    fn interrupt_on<'a>(
        interrupt: &'a AtomicBool,
        heard: Receiver<Event>,
        at: fn(&Event) -> bool,
    ) -> impl FnOnce() -> Vec<Event> + Send + 'a {
        move || {
            let mut said = Vec::new();
            for event in heard {
                if at(&event) {
                    interrupt.store(true, Ordering::Relaxed);
                }
                said.push(event);
            }
            said
        }
    }

    /// Sets `interrupt` once `marker` exists, which is how a probe run as a
    /// local command says it is being retried — the one wait in an acquisition
    /// nothing is emitted from.
    fn interrupt_on_touch(interrupt: &AtomicBool, marker: PathBuf) -> impl FnOnce() + Send {
        move || {
            while !marker.exists() {
                thread::sleep(Duration::from_millis(1));
            }
            interrupt.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn an_interrupt_in_the_boot_wait_ends_the_acquisition_rather_than_waiting_it_out() -> Result<()>
    {
        // Waiting for a rented machine to come up is the longest thing an
        // acquisition does, and the run is paying through all of it. An
        // operator who lets go there is answered while the wait is still
        // running: the machine is torn down and nothing is held against it,
        // since it was never given its time to answer.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider =
            StubProvider::new(vec![offer("a", 100_000)]).never_ready(OfferId("a".to_string()));
        let format = FormatId::new("stub.v1")?;
        let spec = waiting_spec();
        let interrupt = AtomicBool::new(false);
        let (events, said) = heard();
        let started = Instant::now();
        let (result, _) = while_watched(
            events,
            interrupt_on(&interrupt, said, |event| {
                matches!(event, Event::AwaitingMachine { .. })
            }),
            |events| {
                acquire_hosts(
                    &rental(&spec, 1, FillPolicy::Strict),
                    &Budget::default(),
                    &provider,
                    &store,
                    &lock,
                    &deviceless_probe(),
                    &format,
                    &exec(),
                    None,
                    &interrupt,
                    events,
                )
            },
        );
        assert!(result.is_err(), "the acquisition ends with no machine");
        assert!(
            started.elapsed() < spec.ready_timeout,
            "the wait ended on the flag, not on its deadline: {:?}",
            started.elapsed()
        );
        assert!(
            provider.live().is_empty(),
            "the machine that was coming up is torn down"
        );
        assert!(
            store.machine_incidents()?.is_empty(),
            "a machine an operator interrupted answered for nothing"
        );
        Ok(())
    }

    #[test]
    fn an_interrupt_releases_the_machines_the_acquisition_had_and_says_so() -> Result<()> {
        // Two members: the first is up and paid for when the operator lets go
        // during the second. Machines already rented are the whole cost of
        // stopping here, so they are released, and the line that says so is
        // the run's last word — no shortfall is reported, because nothing fell
        // short. Both fill policies answer alike: best-effort runs on what came
        // up when the market fell short, and this is not the market.
        for fill in [FillPolicy::Strict, FillPolicy::BestEffort] {
            abandons_under(fill)?;
        }
        Ok(())
    }

    /// Interrupts a two-member acquisition during its second member, under
    /// `fill`, and holds it to abandoning: one machine released and counted,
    /// no shortfall reported, nothing left running, and the interrupt reaching
    /// the caller.
    fn abandons_under(fill: FillPolicy) -> Result<()> {
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)])
            .never_ready(OfferId("b".to_string()));
        let format = FormatId::new("stub.v1")?;
        let spec = waiting_spec();
        let interrupt = AtomicBool::new(false);
        let (events, heard) = heard();
        let (result, said) = while_watched(
            events,
            // The second member's offer is taken and its machine is coming up:
            // one member is held, and this one is in the wait the flag lands
            // in.
            interrupt_on(
                &interrupt,
                heard,
                |event| matches!(event, Event::Renting { member, .. } if member == "rented[1]"),
            ),
            |events| {
                acquire_hosts(
                    &rental(&spec, 2, fill),
                    &Budget::default(),
                    &provider,
                    &store,
                    &lock,
                    &deviceless_probe(),
                    &format,
                    &exec(),
                    None,
                    &interrupt,
                    events,
                )
            },
        );
        assert!(
            result.is_err(),
            "under {fill:?} the acquisition surfaces the interrupt"
        );
        assert!(
            said.contains(&Event::AcquisitionAbandoned { released: 1 }),
            "the member that was up is released, and counted: {said:?}"
        );
        assert!(
            !said.iter().any(|event| matches!(
                event,
                Event::Diagnostic {
                    level: Level::Warn,
                    ..
                }
            )),
            "nothing fell short, so no shortfall is reported: {said:?}"
        );
        assert!(
            provider.live().is_empty(),
            "neither the machine that was up nor the one coming up is left running"
        );
        assert_eq!(provider.destroyed().len(), 2);
        Ok(())
    }

    /// A probe that refuses every attempt and leaves `marker` behind on each
    /// one, written where the acquisition's own temp directory is.
    ///
    /// A machine still being retried and a machine that has failed look the
    /// same from outside the probe, so the probe is what the tests take the
    /// difference from.
    fn refusing_probe(dir: &std::path::Path, marker: &std::path::Path) -> Result<SpawnMode> {
        use std::os::unix::fs::PermissionsExt;

        let script = dir.join("probe");
        std::fs::write(&script, format!("#!/bin/sh\ntouch {marker:?}\nexit 1\n")).map_err(
            |source| Error::Io {
                path: script.clone(),
                source,
            },
        )?;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).map_err(
            |source| Error::Io {
                path: script.clone(),
                source,
            },
        )?;
        Ok(SpawnMode::Local(script))
    }

    #[test]
    fn an_interrupt_in_the_probe_holds_nothing_against_the_machine() -> Result<()> {
        // A machine that is up but has not answered its probe yet is still
        // inside the time the entry gave it. An operator letting go there says
        // nothing about the machine, so it is torn down carrying no incident
        // — it would otherwise be excluded from the retries and, at two across
        // runs, blacklisted — and no second machine is rented in its place.
        let (dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = waiting_spec();
        let marker = dir.path().join("probed");
        let probe = refusing_probe(dir.path(), &marker)?;
        let interrupt = AtomicBool::new(false);
        let started = Instant::now();
        let (result, ()) = while_watched(
            unheard(),
            interrupt_on_touch(&interrupt, marker),
            |events| {
                acquire_hosts(
                    &rental(&spec, 1, FillPolicy::Strict),
                    &Budget::default(),
                    &provider,
                    &store,
                    &lock,
                    &probe,
                    &format,
                    &exec(),
                    None,
                    &interrupt,
                    events,
                )
            },
        );
        assert!(result.is_err(), "the acquisition ends with no machine");
        assert!(
            started.elapsed() < spec.ready_timeout,
            "the probe ended on the flag, not on its deadline: {:?}",
            started.elapsed()
        );
        assert!(
            store.machine_incidents()?.is_empty(),
            "a machine an operator interrupted answered for nothing"
        );
        assert_eq!(
            provider.destroyed().len(),
            1,
            "one machine taken and torn down, and no other rented after it"
        );
        assert!(provider.live().is_empty());
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
