//! Acquiring the machines a rented entry declares, and letting them go.
//!
//! A rental asks its control plane for `count` machines, holds each behind a
//! teardown guard until the whole group is admitted, and probes every one for
//! the devices its workers are placed on. The members are asked for at once —
//! a boot is minutes, and the same minutes for each of them — with only the
//! offer take serialized. What a partial acquisition means is the entry's own
//! declaration: strict fails the search and tears down what came up, best-effort
//! searches on whatever did.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::FormatId;
use sima_provider::{
    AcquireLimits, Admission, Budget, Exhaustion, IncidentKind, InstanceGuard, Objective, Offer,
    Provider, Reachability, SshEndpoint, acquire, now_ms, record_incident,
};
use sima_scheduler::ExecutionConfig;
use sima_scheduler::{Event, Level};
use sima_store::{Rental as RentalRole, SearchLock, Store};
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
/// search and a reconciliation resolve the same id the same way.
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
/// stays small; a machine that fails twice across searches is blacklisted by
/// its incidents and stops being offered at all.
///
/// Each attempt searches under one `ready_timeout` covering everything it waits
/// for, so this is what the worst case multiplies: `PROBE_ACQUIRE_ATTEMPTS`
/// machines at one `ready_timeout` each, however many offers a walk tries
/// inside one of them.
const PROBE_ACQUIRE_ATTEMPTS: usize = 4;

/// One acquired machine: the guard that owns and tears it down, the transport
/// its pool spawns workers through, and the worker slots its probe derived (one
/// per enumerated GPU, or one deviceless slot when it reports none).
pub(crate) struct RentedHost<'a> {
    /// Ownership of the rented machine; its teardown searches on release or drop.
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
    /// search: a replacement must carry at least this many GPUs.
    pub(crate) slots: Vec<Option<DeviceBinding>>,
}

/// One rented entry's machines, under the control plane and specification they
/// were acquired through. A search may draw on several, each with its own provider
/// and its own shortfall policy, all under the search's single budget.
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
/// The members are acquired at once, one thread each, sharing the admission
/// gate that keeps their offer takes off one another. Every acquisition is
/// budget-admitted and intent-recorded by [`acquire`](sima_provider::acquire),
/// and a machine that fails to acquire or probe is torn down individually. The
/// fill policy decides once, over every member's answer: strict tears down
/// every machine that came up and fails the search; best-effort proceeds with
/// them, so long as one machine did.
///
/// `interrupt` is the search's own wind-down flag, read inside every wait an
/// acquisition spends: the machines are minutes of paid-for waiting before the
/// search drives, and an operator who lets go there must not have to wait them out
/// or kill the process over them.
#[allow(clippy::too_many_arguments)]
pub(crate) fn acquire_hosts<'a>(
    rental: &Rental<'_>,
    budget: &Budget,
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &SearchLock,
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
    // The gate the entry's members take their offers through: one machine is
    // admitted, recorded, and provisioned at a time, whatever else this rental
    // has in flight.
    let admission = Admission::new();
    let members: Vec<String> = (0..rental.count)
        .map(|index| member(rental.name, index))
        .collect();
    // One thread per member. What a member spends is a boot and an image pull
    // — minutes, and the same minutes for every one of them — so asking for
    // them at once costs one member's wait rather than the sum. Only the take
    // is serialized, by the gate they share. A machine that fails to acquire,
    // probe, or receive the program is torn down inside `acquire_one` before
    // its error comes back here.
    let attempts: Vec<Result<RentedHost<'a>>> = thread::scope(|scope| {
        let running: Vec<_> = members
            .iter()
            .map(|member| {
                let program = &program;
                let admission = &admission;
                scope.spawn(move || {
                    acquire_one(
                        provider,
                        store,
                        lock,
                        rental.spec,
                        budget,
                        mode,
                        format,
                        exec,
                        program,
                        admission,
                        interrupt,
                        events,
                        member,
                    )
                })
            })
            .collect();
        // Joined in the order they were spawned, so what comes back is in
        // member order however the machines came up, and each answer stays
        // beside the member that asked for it.
        running
            .into_iter()
            .map(|handle| handle.join().expect("a member's acquisition joins"))
            .collect()
    });
    let mut hosts: Vec<RentedHost<'a>> = Vec::with_capacity(rental.count);
    let mut short: Vec<(&str, Error)> = Vec::new();
    for (member, attempt) in members.iter().zip(attempts) {
        match attempt {
            Ok(host) => hosts.push(host),
            Err(error) => short.push((member.as_str(), error)),
        }
    }
    // An operator who let go stops the whole rental, whatever the fill policy
    // would make of a member that could not be brought up: nothing fell short,
    // the search is ending. Dropping `hosts` on the way out tears down every
    // machine this rental had, which is the whole of what is said here — a
    // fleet released before it ran leaves nothing else to read it from.
    if interrupt.load(Ordering::Relaxed) {
        events.emit(Event::AcquisitionAbandoned {
            released: hosts.len(),
        });
        return Err(first_error(short));
    }
    // What the shortfall costs is the entry's own declaration, and the operator
    // is told which member fell short and what follows from it — one machine
    // short of a fleet is otherwise invisible until the search's rate looks wrong.
    // Every member that fell short says so: they were asked for at once, so any
    // number of them may have.
    for (member, error) in &short {
        events.emit(shortfall(member, rental, error, hosts.len()));
    }
    match rental.fill {
        // Strict: the declared count or nothing. Dropping `hosts` here tears
        // down every machine that did come up.
        FillPolicy::Strict if !short.is_empty() => Err(first_error(short)),
        // Best-effort: search with what came up, so long as one machine did. The
        // verdict is taken here, at the join, and it is what keeps the entry
        // from paying a market that is not filling it: with every member asked
        // for at once there is no first shortfall left to stop asking after.
        _ if hosts.is_empty() => Err(Error::Provider(format!(
            "the rental {:?} acquired no machine",
            rental.name
        ))),
        _ => Ok(hosts),
    }
}

/// What a rental several of whose members fell short fails with: the first by
/// member index, since one has to be named and the members are alike.
///
/// A rental failing with no member short of anything was interrupted — every
/// member came up and the operator let go — so that is what it says.
fn first_error(short: Vec<(&str, Error)>) -> Error {
    short
        .into_iter()
        .next()
        .map_or_else(interrupted, |(_, error)| error)
}

/// Acquires one machine, probes it, and builds its transport and slots. On a
/// probe failure the guard drops here, tearing the machine down, so no
/// half-acquired rental leaks, and the acquisition moves to another machine: a
/// marketplace serves hosts that come up but never accept a session, and one of
/// them must cost a machine rather than the search.
#[allow(clippy::too_many_arguments)]
fn acquire_one<'a>(
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &SearchLock,
    spec: &Rented,
    budget: &Budget,
    mode: &SpawnMode,
    format: &FormatId,
    exec: &ExecutionConfig,
    program: &RentedProgram<'_>,
    admission: &Admission,
    interrupt: &AtomicBool,
    events: &Emitter,
    member: &str,
) -> Result<RentedHost<'a>> {
    // A machine that fails its probe is excluded from the attempts that
    // follow, so the retry reaches a different machine instead of renting
    // the same broken one again. The exclusion is local to this
    // acquisition; the durable incident it also records is what carries
    // the machine's reputation across searches.
    let mut constraints = spec.constraints.clone();
    let mut refused: Option<Error> = None;
    for _ in 0..PROBE_ACQUIRE_ATTEMPTS {
        // The clock on this machine starts where it is first asked for, and
        // both stages that wait for it — reporting ready, then answering a
        // probe — search under the one deadline. Each attempt reaches a different
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
            admission,
            // The wait for a machine to come up is the longest thing an
            // acquisition does, so the search's interrupt reaches inside it: an
            // operator letting go here is answered without waiting the machine
            // out.
            interrupt,
            &|offer| taken(events, member, spec.ready_timeout, offer),
        )?;
        let target = endpoint_target(guard.endpoint().clone());
        let host = target.host().to_string();
        // Three stages, each of which can cost the machine rather than the
        // search: it answers, it receives what the search needs, and it says where
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
                // The machine reported ready but cannot serve this search: an
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
        // What the machine's workers search there, and what they answer for. The
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

/// Why a machine did not come to serve this search.
enum Unusable {
    /// The machine's own doing: an incident is recorded against it, it is
    /// excluded from the attempts that follow, and another machine is tried.
    Machine(Error, IncidentKind),
    /// The search was interrupted while the machine was being brought up. It
    /// answered for nothing, so nothing is recorded against it and no other
    /// machine is asked for.
    Interrupted,
}

/// The error an acquisition the search's interrupt reached returns. What an
/// operator reads is the abandoned line and the search's own interrupted
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
        member: member.to_string(),
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

/// How a fleet member is named in what the search says about it: the entry that
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
            "{:?} states strict fill, so the search stops and what it acquired is torn down",
            rental.name
        ),
        FillPolicy::BestEffort => format!(
            "{:?} states best-effort fill, so the search goes on with the {acquired} machine(s) \
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
    use std::sync::mpsc::{Receiver, RecvTimeoutError};

    use sima_contracts::DeviceClass;
    use sima_domains::devices::DeviceType;
    use sima_provider::stub::StubProvider;
    use sima_provider::{Cost, InstanceStatus, OfferId, Provision, never_cancelled};
    use sima_store::SpendEntry;

    use super::*;
    use crate::config::FillPolicy;
    use crate::rental::fixtures::{
        BOOT_POLL, BOOT_POLLS, acquisition_env, booting_spec, deviceless_probe, exec, heard, offer,
        one_group, rental, spec, unheard, waiting_spec,
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
        // reports to a WGSL search when its Vulkan loader cannot initialize the
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
        // One offer for two requested machines under strict fill: the search
        // fails, and the one machine that came up is torn down.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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
        // One offer for two requested machines under best-effort: the search
        // proceeds with the one machine, torn down on release.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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
        assert_eq!(hosts.len(), 1, "best-effort searches on what came up");
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
        // machine short is otherwise invisible until the search's rate looks
        // wrong.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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
        // The members race for the one offer, so which of them lost it is not
        // fixed; that one of them is named, and named as this entry's, is.
        assert!(
            warning.starts_with("rented[0] ") || warning.starts_with("rented[1] "),
            "names the member: {warning}"
        );
        assert!(
            warning.contains("strict fill") && warning.contains("the search stops"),
            "states what follows from it: {warning}"
        );
        Ok(())
    }

    #[test]
    fn a_best_effort_shortfall_names_the_member_and_says_the_run_goes_on() -> Result<()> {
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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
        assert!(
            warning.starts_with("rented[0] ") || warning.starts_with("rented[1] "),
            "names the member: {warning}"
        );
        // Pinned whole, because this is the sentence an operator reads when a
        // fleet comes up short and it has to read as one.
        assert!(
            warning.ends_with(
                "; \"rented\" states best-effort fill, so the search goes on with the 1 machine(s) \
                 that came up"
            ),
            "states what the search does instead: {warning}"
        );
        release_all(one_group(&provider, &spec, FillPolicy::BestEffort, hosts))?;
        Ok(())
    }

    #[test]
    fn every_machine_a_fleet_takes_says_it_is_waiting_for_it() -> Result<()> {
        // Between taking an offer and the machine answering lie a boot and an
        // image pull, and the search is paying through all of it. What is being
        // waited for is stated once per machine, right after what it costs.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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
        // Each line is read as (what it says, whose it is). Which member gets
        // there first is not pinned — they are asked for at once — but every
        // member says both, and says the wait after the rental it is waiting
        // on.
        let stated = spec.ready_timeout.as_millis() as u64;
        let waits: Vec<(&str, String)> = said
            .into_iter()
            .filter_map(|event| match event {
                Event::Renting { member, .. } => Some(("renting", member)),
                Event::AwaitingMachine { member, timeout_ms } => {
                    assert_eq!(timeout_ms, stated, "the wait the entry states");
                    Some(("waiting", member))
                }
                _ => None,
            })
            .collect();
        for member in ["rented[0]", "rented[1]"] {
            let at = |said: &str| {
                waits
                    .iter()
                    .position(|(line, whose)| *line == said && whose == member)
            };
            let (rented, waiting) = (at("renting"), at("waiting"));
            assert!(
                matches!((rented, waiting), (Some(rented), Some(waiting)) if rented < waiting),
                "{member} says what it took, then what it waits for: {waits:?}"
            );
        }
        assert_eq!(waits.len(), 4, "and neither says either twice: {waits:?}");
        release_all(one_group(&provider, &spec, FillPolicy::Strict, hosts))?;
        Ok(())
    }

    #[test]
    fn a_rental_acquires_and_probes_every_machine() -> Result<()> {
        // Two offers for two machines: both acquire, each probed into a single
        // deviceless slot, all torn down on release.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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
    fn the_members_of_one_rental_come_up_at_once() -> Result<()> {
        // A boot and an image pull are minutes, and every member of a rental
        // spends them on its own machine. Asked for one after another they add
        // up; asked for at once they are the same minutes. Four members, each
        // held in a readiness wait of the same length: the whole acquisition
        // costs about one of them, and nothing like four.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
        let provider = StubProvider::new(vec![
            offer("a", 100_000),
            offer("b", 200_000),
            offer("c", 300_000),
            offer("d", 400_000),
        ])
        .ready_after(BOOT_POLLS);
        let format = FormatId::new("stub.v1")?;
        let spec = booting_spec();
        let started = Instant::now();
        let hosts = acquire_hosts(
            &rental(&spec, 4, FillPolicy::Strict),
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
        let took = started.elapsed();
        assert_eq!(hosts.len(), 4, "every member came up");
        // Two boots is the bound: one is what the members overlap into, four
        // is what asking for them in turn would cost.
        let boot = BOOT_POLL * BOOT_POLLS;
        assert!(
            took < boot * 2,
            "four boots of {boot:?} overlapped into {took:?}"
        );
        release_all(one_group(&provider, &spec, FillPolicy::Strict, hosts))?;
        Ok(())
    }

    #[test]
    fn an_exhausted_budget_refuses_every_member() -> Result<()> {
        // Admission is serialized, so what the budget refuses it refuses to
        // every member alike. What it can refuse is a budget already spent: a
        // rental is charged from its ledger stamp to now, so one admitted a
        // moment ago has accrued nothing and can never be what stops the next.
        // A cap is kept by the wind-down that follows it, not by prevention
        // here, and that is as true of members asked for at once as of members
        // asked for in turn.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
        // A rental of this search, closed out, that already cost more than the cap.
        store.put_spend(&SpendEntry {
            tag: "sima-deadbeefdeadbeef-1-aabbccdd-0".to_string(),
            provider: "stub".to_string(),
            owner: search.to_string(),
            price_micro_usd_hour: 2_000_000,
            started_ms: 1,
            ended_ms: 3_600_001,
            cost_micro_usd: 2_000_000,
        })?;
        let budget = Budget {
            max_spend: Some(Cost(1_000_000)),
            max_wall_clock: None,
        };
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let result = acquire_hosts(
            &rental(&spec, 2, FillPolicy::Strict),
            &budget,
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
        assert!(
            provider.live().is_empty() && provider.destroyed().is_empty(),
            "no member was ever provisioned"
        );
        assert!(
            store
                .instance_records()?
                .iter()
                .all(|record| record.owner != search.to_string()),
            "and none of them wrote an intent record"
        );
        Ok(())
    }

    #[test]
    fn a_probe_failure_tears_the_machine_down() -> Result<()> {
        // The machine acquires but its probe never searches: it is torn down rather
        // than left running with no slots.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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
        // A machine that reported ready but failed the probe cannot search work:
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
        // search, and does not rent the refusing machine again: both offers are
        // tried, each torn down, each carrying its own incident.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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

    #[test]
    fn a_member_whose_machine_spends_the_ready_wait_rents_nothing_further() -> Result<()> {
        // The ready timeout is what the operator gave this member to come up
        // in, and one machine spending it ends the member's acquisition: the
        // second offer is never rented, and the probe retry does not hand the
        // member another window on top of the one it was given.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)])
            .never_ready(OfferId("a".to_string()));
        let format = FormatId::new("stub.v1")?;
        // A window reached by elapsed time over several polls, short enough to
        // keep the suite quick.
        let spec = Rented {
            ready_timeout: Duration::from_millis(50),
            ready_poll: Duration::from_millis(1),
            ..spec()
        };
        let result = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
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
        assert!(matches!(
            result,
            Err(Error::Provider(message))
                if message == "the ready wait ran out before a machine came up"
        ));
        // One machine was rented and torn down. The second offer was never
        // taken, by this attempt or by another one.
        assert_eq!(provider.destroyed().len(), 1);
        assert!(provider.live().is_empty());
        assert!(store.instance_records()?.is_empty());
        let incidents = store.machine_incidents()?;
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].kind, IncidentKind::NeverReady);
        assert_eq!(incidents[0].machine, "machine-a");
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
    /// The receiver drains when the acquisition drops its emitter, so a search
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

    /// Sets `interrupt` once `members` machines have been rented and `marker`
    /// exists, and answers everything the acquisition said.
    ///
    /// Both conditions are read, because either alone leaves the acquisition
    /// holding something the test cannot name. A `Renting` line is emitted once
    /// a machine is provisioned, so one per member is what says every member's
    /// machine is taken and paid for; the marker, which a probe search as a local
    /// command touches, is what says one of them got through its boot wait. A
    /// flag set off the marker alone lands while a slower member is still at
    /// the admission gate, and that member then rents nothing at all.
    fn interrupt_once_rented_and_probed(
        interrupt: &AtomicBool,
        marker: PathBuf,
        heard: Receiver<Event>,
        members: usize,
    ) -> impl FnOnce() -> Vec<Event> + Send {
        move || {
            let mut said = Vec::new();
            let mut rented: Vec<String> = Vec::new();
            loop {
                match heard.recv_timeout(Duration::from_millis(1)) {
                    Ok(event) => {
                        if let Event::Renting { member, .. } = &event
                            && !rented.contains(member)
                        {
                            rented.push(member.clone());
                        }
                        said.push(event);
                    }
                    // The stretch this waits through emits nothing: a member
                    // that is through is inside its probe and one that is not
                    // is inside its boot wait. So the marker is read between
                    // reads of the journal rather than off an event.
                    Err(RecvTimeoutError::Timeout) => {}
                    // The acquisition ended without ever reaching the state the
                    // interrupt is aimed at. The flag stays down and the test
                    // fails on what it asserts, rather than spinning here.
                    Err(RecvTimeoutError::Disconnected) => return said,
                }
                if rented.len() >= members && marker.exists() {
                    interrupt.store(true, Ordering::Relaxed);
                    break;
                }
            }
            said.extend(heard);
            said
        }
    }

    #[test]
    fn an_interrupt_in_the_boot_wait_ends_the_acquisition_rather_than_waiting_it_out() -> Result<()>
    {
        // Waiting for a rented machine to come up is the longest thing an
        // acquisition does, and the search is paying through all of it. An
        // operator who lets go there is answered while the wait is still
        // running: the machine is torn down and nothing is held against it,
        // since it was never given its time to answer.
        let (_dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
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
        // the search's last word — no shortfall is reported, because nothing fell
        // short. Both fill policies answer alike: best-effort searches on what came
        // up when the market fell short, and this is not the market.
        for fill in [FillPolicy::Strict, FillPolicy::BestEffort] {
            abandons_under(fill)?;
        }
        Ok(())
    }

    /// Interrupts a two-member acquisition once one member is up and the other
    /// is still coming up, under `fill`, and holds it to abandoning: one
    /// machine released and counted, no shortfall reported, nothing left
    /// running, and the interrupt reaching the caller.
    ///
    /// The cheaper offer is ready at once and the dearer one never comes up, so
    /// of the two members racing for them one is through and one is in its boot
    /// wait. The flag lands once both have rented and one has probed, which is
    /// exactly the state the assertions name: two machines paid for, one of
    /// them a host the acquisition is holding.
    fn abandons_under(fill: FillPolicy) -> Result<()> {
        let (dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)])
            .never_ready(OfferId("b".to_string()));
        let format = FormatId::new("stub.v1")?;
        let spec = waiting_spec();
        let marker = dir.path().join("probed");
        let probe = marking_probe(dir.path(), &marker, Answer::Enumerates)?;
        let interrupt = AtomicBool::new(false);
        let (events, heard) = heard();
        let (result, said) = while_watched(
            events,
            interrupt_once_rented_and_probed(&interrupt, marker, heard, 2),
            |events| {
                acquire_hosts(
                    &rental(&spec, 2, fill),
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

    /// What a machine's readiness probe answers.
    enum Answer {
        /// It enumerates, printing nothing, so the machine derives one
        /// deviceless slot.
        Enumerates,
        /// It refuses, as a machine whose route is not carrying an ssh yet
        /// does.
        Refuses,
    }

    /// A probe that answers `answer` and leaves `marker` behind on every
    /// attempt, written into the acquisition's own temp directory.
    ///
    /// The marker is how a test sees a member get as far as probing: a machine
    /// still being retried and one that has failed look the same from outside,
    /// and nothing is emitted between a machine coming up and its slots being
    /// derived.
    fn marking_probe(
        dir: &std::path::Path,
        marker: &std::path::Path,
        answer: Answer,
    ) -> Result<SpawnMode> {
        use std::os::unix::fs::PermissionsExt;

        let code = match answer {
            Answer::Enumerates => 0,
            Answer::Refuses => 1,
        };
        let script = dir.join("probe");
        let write = |result: std::io::Result<()>| {
            result.map_err(|source| Error::Io {
                path: script.clone(),
                source,
            })
        };
        write(std::fs::write(
            &script,
            format!("#!/bin/sh\ntouch {marker:?}\nexit {code}\n"),
        ))?;
        write(std::fs::set_permissions(
            &script,
            std::fs::Permissions::from_mode(0o755),
        ))?;
        Ok(SpawnMode::Local(script))
    }

    #[test]
    fn an_interrupt_in_the_probe_holds_nothing_against_the_machine() -> Result<()> {
        // A machine that is up but has not answered its probe yet is still
        // inside the time the entry gave it. An operator letting go there says
        // nothing about the machine, so it is torn down carrying no incident
        // — it would otherwise be excluded from the retries and, at two across
        // searches, blacklisted — and no second machine is rented in its place.
        let (dir, store, search) = acquisition_env();
        let lock = store.acquire_search_lock(&search)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = waiting_spec();
        let marker = dir.path().join("probed");
        let probe = marking_probe(dir.path(), &marker, Answer::Refuses)?;
        let interrupt = AtomicBool::new(false);
        let started = Instant::now();
        let (events, heard) = heard();
        let (result, _) = while_watched(
            events,
            interrupt_once_rented_and_probed(&interrupt, marker, heard, 1),
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
