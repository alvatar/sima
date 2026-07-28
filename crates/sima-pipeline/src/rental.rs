//! Renting: a rented entry's `provider` id resolved to a control-plane backend,
//! the machines acquired behind teardown guards, and the supervisor that keeps
//! them within budget and replaces the ones that vanish.
//!
//! The pipeline is where provider choice becomes concrete, so this is the one
//! edge from configuration to a boxed [`Provider`]. A run whose fleet is not
//! engaged never reaches here, so it constructs no provider and reads no
//! `VAST_API_KEY`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Duration;

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::FormatId;
use sima_provider::stub::StubProvider;
use sima_provider::{
    AcquireLimits, Budget, Exhaustion, IncidentKind, InstanceGuard, InstanceStatus, Objective,
    Offer, OfferId, Price, Provider, Reachability, SshEndpoint, Verdict, acquire, assess, now_ms,
    record_incident,
};
use sima_provider_vast::{VastConfig, VastProvider};
use sima_scheduler::ExecutionConfig;
use sima_store::{Rental as RentalRole, RunLock, Store};
use sima_trace::{Emitter, Event};
use sima_transport::{SpawnMode, SshDestination, SshTransport};

use crate::config::{FillPolicy, ProviderId, Rented};
use crate::devices::{parse_enumeration, usable};
use crate::fleet::Rental;
use crate::orchestrate::{command_stdout, worker_binary};

/// The environment channel that points the stub backend at a machine that is
/// really there, as `user@host:port`.
///
/// It exists so a test can exercise the ssh path against a throwaway server of
/// its own, without a key in the configuration schema that would be valid for
/// one provider and rejected for every other. Unset, the stub fabricates an
/// endpoint naming no machine and is reached in process.
const STUB_SSH: &str = "SIMA_STUB_SSH";

/// Builds the control-plane backend a rental acquires its machines through.
///
/// The `vast` backend reads its key from `VAST_API_KEY`; an absent key is an
/// [`Error::Provider`](sima_core::Error::Provider) naming the variable, raised
/// here before any store mutation. The `stub` backend is in-process, listing a
/// generous always-available marketplace so a stub rental fills its declared
/// count. An unknown id never reaches here — the config load rejects it.
///
/// [`STUB_SSH`] is read here and nowhere else, so a config naming any other
/// provider never looks at it.
pub(crate) fn provider_for(rental: &Rental<'_>) -> Result<Box<dyn Provider + Sync>> {
    match rental.spec.provider {
        ProviderId::Vast => {
            let config = VastConfig::from_env(&rental.spec.image, rental.spec.disk_gb)?;
            Ok(Box::new(VastProvider::new(config)))
        }
        ProviderId::Stub => {
            let stub = StubProvider::new(stub_offers(rental.count));
            Ok(Box::new(match std::env::var_os(STUB_SSH) {
                Some(value) => {
                    let endpoint = stub_endpoint(&value.to_string_lossy())?;
                    stub.endpoint(&endpoint.host, endpoint.port, &endpoint.user)
                }
                None => stub,
            }))
        }
    }
}

/// The endpoint a `user@host:port` value names.
///
/// A value that does not parse is an error naming the variable rather than a
/// fall back to the in-process path. A caller that set it meant to cross a hop,
/// and one that quietly did not would report a success that tested nothing.
fn stub_endpoint(value: &str) -> Result<SshEndpoint> {
    let malformed = || {
        Error::Validation(format!(
            "{STUB_SSH} is {value:?}, which is not a user@host:port endpoint"
        ))
    };
    let (user, rest) = value.split_once('@').ok_or_else(malformed)?;
    // From the right: an IPv6 literal in brackets holds colons of its own.
    let (host, port) = rest.rsplit_once(':').ok_or_else(malformed)?;
    let port: u16 = port.parse().map_err(|_| malformed())?;
    if user.is_empty() || host.is_empty() || port == 0 {
        return Err(malformed());
    }
    Ok(SshEndpoint {
        host: host.to_string(),
        port,
        user: user.to_string(),
    })
}

/// The transport mode a control plane's machines are reached through, from what
/// the control plane says about them: ssh to a machine that is really there, or
/// a local spawn for a backend whose machines are this machine.
///
/// The answer is the provider's, not the config's. A backend knows whether the
/// endpoint it reports names anything, and the worker binary a local spawn
/// needs is this layer's to supply — which is why [`Reachability`] and not
/// [`SpawnMode`] is what crosses the seam.
pub(crate) fn transport_mode(provider: &(dyn Provider + Sync)) -> Result<SpawnMode> {
    match provider.reachability() {
        Reachability::Ssh => Ok(SpawnMode::Ssh),
        Reachability::Local => Ok(SpawnMode::Local(worker_binary()?)),
    }
}

/// Maps a provider's ssh endpoint into the transport's target, the seam that
/// keeps the transport free of any dependency on the provider crate.
pub(crate) fn endpoint_target(endpoint: SshEndpoint) -> SshDestination {
    SshDestination::rented(endpoint.host, endpoint.port, endpoint.user)
}

/// How many times a first contact with a machine is retried before it is given
/// up on: sshd can lag the provider's `Ready`, so the first connection to a
/// fresh host may be refused. Both paths that reach a machine for the first
/// time take this bound — the enumeration probe an acquisition runs, and the
/// migration's own first contact with its destination.
pub(crate) const PROBE_ATTEMPTS: u32 = 6;

/// The longest wait between those attempts, whatever readiness poll the
/// destination states. A poll is stated for a machine coming up and can be far
/// longer than the lag this covers, so waiting a full one between connection
/// attempts would spend the whole bound on a machine that was ready early.
pub(crate) const PROBE_INTERVAL_CAP: Duration = Duration::from_secs(5);

/// How many machines one instance's acquisition may burn through before it
/// gives up. Each attempt is a paid rental torn down again, so the bound
/// stays small; a machine that fails twice across runs is blacklisted by
/// its incidents and stops being offered at all.
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
) -> Result<Vec<RentedHost<'a>>> {
    let limits = AcquireLimits {
        ready_timeout: rental.spec.ready_timeout,
        ready_poll: rental.spec.ready_poll,
    };
    let mut hosts: Vec<RentedHost<'a>> = Vec::with_capacity(rental.count);
    for _ in 0..rental.count {
        // A machine that fails to acquire or probe is torn down inside
        // `acquire_one` before its error returns here.
        match acquire_one(
            provider,
            store,
            lock,
            rental.spec,
            budget,
            &limits,
            mode,
            format,
            exec,
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
    limits: &AcquireLimits,
    mode: &SpawnMode,
    format: &FormatId,
    exec: &ExecutionConfig,
) -> Result<RentedHost<'a>> {
    // A machine that fails its probe is excluded from the attempts that
    // follow, so the retry reaches a different machine instead of renting
    // the same broken one again. The exclusion is local to this
    // acquisition; the durable incident it also records is what carries
    // the machine's reputation across runs.
    let mut constraints = spec.constraints.clone();
    let mut refused: Option<Error> = None;
    for _ in 0..PROBE_ACQUIRE_ATTEMPTS {
        // Pin the trait object to `Sync`, which the supervisor thread's shared
        // borrow of the provider needs; without it inference drops the bound.
        let guard = acquire::<dyn Provider + Sync>(
            provider,
            store,
            lock,
            RentalRole::Worker,
            &constraints,
            Objective::CheapestPerHour,
            limits,
            budget,
            // Run-start acquisition has nothing to cancel: the run is not yet
            // driving, so no wind-down is in flight.
            never_cancelled(),
        )?;
        let target = endpoint_target(guard.endpoint().clone());
        let host = target.host().to_string();
        // The probe drives the machine's device enumeration; a failure drops
        // the guard, tearing the machine down.
        let slots = match probe_slots(mode, &target, spec.ready_poll, format) {
            Ok(slots) => slots,
            Err(error) => {
                // The machine reported ready but cannot run work: an incident
                // against it, recorded before the guard drops and tears it
                // down. A store failure recording the incident supersedes the
                // probe error.
                record_incident(
                    store,
                    provider.id(),
                    guard.machine(),
                    guard.tag(),
                    IncidentKind::ProbeFailed,
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
        let transport = SshTransport::new(
            mode.clone(),
            target,
            format.clone(),
            exec.checkpoint_interval,
            exec.checkpoint_interval_steps,
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

/// Probes a machine for the devices `format`'s program can run on and derives
/// its worker slots, retrying briefly because sshd can lag the provider's
/// `Ready`.
fn probe_slots(
    mode: &SpawnMode,
    target: &SshDestination,
    poll: Duration,
    format: &FormatId,
) -> Result<Vec<Option<DeviceBinding>>> {
    let argv = sima_transport::ssh::probe_argv(mode, target, format);
    let mut last: Option<Error> = None;
    for attempt in 0..PROBE_ATTEMPTS {
        match command_stdout(&argv).and_then(|stdout| parse_enumeration(&stdout)) {
            Ok(devices) => return Ok(rented_slots(&devices)),
            Err(error) => {
                last = Some(error);
                // No sleep after the final attempt.
                if attempt + 1 < PROBE_ATTEMPTS {
                    thread::sleep(poll.min(PROBE_INTERVAL_CAP));
                }
            }
        }
    }
    Err(last.unwrap_or_else(|| Error::Provider("the machine probe never ran".to_string())))
}

/// One worker slot per usable device, each bound to it; a probe reporting no
/// device at all yields a single deviceless worker — the stub testing path, and
/// any device-free machine.
///
/// Which devices are usable is [`devices::usable`]'s rule, shared with the
/// far-side config a migration synthesizes: both are deriving a worker layout
/// from one enumeration, and they must agree on what the machine offers.
fn rented_slots(devices: &[DeviceInfo]) -> Vec<Option<DeviceBinding>> {
    if devices.is_empty() {
        return vec![None];
    }
    usable(devices)
        .map(|device| {
            Some(DeviceBinding {
                vendor_id: device.vendor_id,
                device_id: device.device_id,
                member: device.member,
            })
        })
        .collect()
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
                .expect("the rental guard lock is never poisoned");
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

/// The supervisor's heartbeat period. Rental health is a low-frequency concern
/// and the poll is cheap, so a fixed period suffices; no config knob.
const HEARTBEAT: Duration = Duration::from_secs(10);

/// A cancellation flag that is never set, for an acquisition with no wind-down
/// to observe — the run-start acquisition, before the run drives.
fn never_cancelled() -> &'static AtomicBool {
    static NEVER: AtomicBool = AtomicBool::new(false);
    &NEVER
}

/// The stop signal the orchestrator raises when the scheduler returns, waking
/// the supervisor from its heartbeat wait at once rather than at the next tick.
pub(crate) struct StopSignal {
    stopped: Mutex<bool>,
    changed: Condvar,
}

impl StopSignal {
    pub(crate) fn new() -> StopSignal {
        StopSignal {
            stopped: Mutex::new(false),
            changed: Condvar::new(),
        }
    }

    /// Raises the signal, waking a supervisor parked in [`StopSignal::wait`].
    pub(crate) fn raise(&self) {
        *self
            .stopped
            .lock()
            .expect("the stop lock is never poisoned") = true;
        self.changed.notify_all();
    }

    /// Waits up to `timeout` for the signal, returning whether it is raised.
    fn wait(&self, timeout: Duration) -> bool {
        let stopped = self
            .stopped
            .lock()
            .expect("the stop lock is never poisoned");
        if *stopped {
            return true;
        }
        let (stopped, _) = self
            .changed
            .wait_timeout(stopped, timeout)
            .expect("the stop lock is never poisoned");
        *stopped
    }
}

/// The rental supervisor: one thread that, each heartbeat, assesses the run's
/// budget once and polls the health of every rented machine.
///
/// Budget exhaustion sets the interrupt flag SIGINT sets, so the run winds down
/// gracefully and the guards tear the rentals down. A machine polled `Gone` is
/// replaced: its record is closed out, a replacement is acquired under its own
/// group's constraints (raised to the slots in use), and the transport's target
/// swaps, so the worker loop's existing respawn lands on the new machine. A
/// replacement that cannot be made retires the transport — fatally under strict
/// fill, a clean degradation under best-effort.
///
/// The budget is the run's, not a group's, so it is assessed once per heartbeat
/// however many groups the run draws on.
pub(crate) struct Supervisor<'a, 'b> {
    store: &'a Store,
    lock: &'a RunLock,
    /// The run-global spend and wall-clock ceilings.
    budget: &'a Budget,
    /// The slice borrow is a lifetime of its own, shorter than the groups' own
    /// `'a`, so the caller can move the `Vec` for teardown once the supervisor
    /// is done.
    groups: &'b [RentalGroup<'a>],
    /// The wind-down flag, shared with the driver and SIGINT. It carries the
    /// slice's short lifetime, so a caller holding a longer-lived flag passes
    /// it freely. The supervisor reads it to stop starting replacements, and
    /// sets it on budget exhaustion; it never writes it on a clean exit.
    interrupt: &'b AtomicBool,
    /// Aborts a replacement acquisition already in flight, so run teardown
    /// never waits out an offer walk. The run's teardown sets it; the
    /// supervisor only reads it. `None` for a caller that never cancels — the
    /// unit tests, which drive ticks to completion.
    cancel: Option<&'b AtomicBool>,
    /// The run's emitter, delivered by the start hook once the collector spawns
    /// and `None` until then. Rental events cross the same single-writer
    /// journal seam every other event does.
    emitter: &'b Mutex<Option<Emitter>>,
    /// Whether the initial composition has been announced, so the online events
    /// fire once.
    announced: AtomicBool,
    /// Whether a budget exhaustion has been announced, so its event fires once.
    budget_announced: AtomicBool,
}

impl<'a, 'b> Supervisor<'a, 'b> {
    pub(crate) fn new(
        store: &'a Store,
        lock: &'a RunLock,
        budget: &'a Budget,
        groups: &'b [RentalGroup<'a>],
        interrupt: &'b AtomicBool,
        emitter: &'b Mutex<Option<Emitter>>,
    ) -> Supervisor<'a, 'b> {
        Supervisor {
            store,
            lock,
            budget,
            groups,
            interrupt,
            cancel: None,
            emitter,
            announced: AtomicBool::new(false),
            budget_announced: AtomicBool::new(false),
        }
    }

    /// Sets the flag that aborts a replacement acquisition in flight, so run
    /// teardown never waits out an offer walk. The orchestrator supplies it;
    /// the unit tests leave it unset.
    pub(crate) fn on_cancel(mut self, cancel: &'b AtomicBool) -> Supervisor<'a, 'b> {
        self.cancel = Some(cancel);
        self
    }

    /// Runs the heartbeat loop until `stop` is raised. One drop guard owns
    /// every supervisor exit duty: it clears the run's emitter clone on every
    /// path — a clone held past the run would block the collector's shutdown —
    /// and, on an unwinding stack alone, retires the transports as fatal so a
    /// worker blocked in `Replacing` never waits on a panicked supervisor. A
    /// clean return disarms the retirement; the emitter is cleared regardless.
    pub(crate) fn run(&self, stop: &StopSignal) {
        let guard = SupervisorExit::<'a, 'b> {
            groups: self.groups,
            emitter: self.emitter,
            armed: true,
        };
        while !stop.wait(HEARTBEAT) {
            // A tick error is a provider or store failure; the run's own
            // machinery surfaces failures, so the supervisor logs nothing and
            // simply tries again next heartbeat.
            let _ = self.tick(now_ms());
        }
        guard.disarm();
    }

    /// Emits `event` through the run's journal, if the emitter has arrived.
    fn emit(&self, event: Event) {
        if let Some(emitter) = &*self
            .emitter
            .lock()
            .expect("the emitter lock is never poisoned")
        {
            emitter.emit(event);
        }
    }

    /// Announces the initial composition once, as soon as the emitter is
    /// available: one `InstanceOnline` per machine still holding a guard.
    fn announce_composition(&self) {
        // Latch only once the emitter has arrived. A tick that beats the start
        // hook would otherwise spend the latch with no emitter to receive the
        // events, dropping the whole initial composition from the journal; a
        // later tick, once the emitter is present, announces it instead.
        if self
            .emitter
            .lock()
            .expect("the emitter lock is never poisoned")
            .is_none()
        {
            return;
        }
        if self.announced.swap(true, Ordering::Relaxed) {
            return;
        }
        for host in self.groups.iter().flat_map(|group| &group.hosts) {
            if let Some(guard) = &*host
                .guard
                .lock()
                .expect("the rental guard lock is never poisoned")
            {
                self.emit(instance_online(guard, &host.host));
            }
        }
    }

    /// Winds the run down for budget exhaustion: sets the interrupt flag SIGINT
    /// sets, so the run winds down gracefully and the guards tear the rentals
    /// down, and announces the exhaustion once. Shared by the heartbeat's own
    /// budget check and a replacement refused for want of budget.
    fn wind_down_for_budget(&self, exhaustion: Exhaustion) {
        self.interrupt.store(true, Ordering::Relaxed);
        if !self.budget_announced.swap(true, Ordering::Relaxed) {
            self.emit(budget_exhausted(exhaustion));
        }
    }

    /// One heartbeat: observe the wind-down flag, announce the composition
    /// once, assess the run's budget, then every machine's health. Budget
    /// exhaustion short-circuits the health poll — the whole run is winding
    /// down. `now_ms` is a parameter so a test can drive exhaustion without
    /// waiting on the clock.
    fn tick(&self, now_ms: u64) -> Result<()> {
        // Once wind-down is requested — Ctrl-C or budget — the marketplace is
        // off limits: rent no replacement while the run is ending. The flag is
        // read first, before any provider call.
        if self.interrupt.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.announce_composition();
        // One assessment per heartbeat: the ceiling is the run's, so polling it
        // per group would read the same ledger several times for one answer.
        if let Verdict::Exhausted(exhaustion) =
            assess(self.store, self.lock.run(), self.budget, now_ms)?
        {
            self.wind_down_for_budget(exhaustion);
            return Ok(());
        }
        for group in self.groups {
            for host in &group.hosts {
                self.check_host(group, host, now_ms)?;
            }
        }
        Ok(())
    }

    /// Polls one machine's health, replacing it if the provider reports it
    /// gone.
    fn check_host(
        &self,
        group: &RentalGroup<'a>,
        host: &RentedHost<'a>,
        now_ms: u64,
    ) -> Result<()> {
        let id = match &*host
            .guard
            .lock()
            .expect("the rental guard lock is never poisoned")
        {
            Some(guard) => guard.id().clone(),
            // Already retired with no replacement: nothing to poll.
            None => return Ok(()),
        };
        match group.provider.instance(&id)? {
            InstanceStatus::Ready(_) | InstanceStatus::Provisioning => Ok(()),
            InstanceStatus::Gone => self.replace(group, host, now_ms),
        }
    }

    /// Replaces a lost machine: blocks the transport's spawns, closes the dead
    /// machine out, acquires a replacement under its group's specification, and
    /// swaps the target — or retires the transport when no replacement can be
    /// made. `now_ms` dates the budget re-assessment a failed acquisition
    /// triggers.
    fn replace(&self, group: &RentalGroup<'a>, host: &RentedHost<'a>, now_ms: u64) -> Result<()> {
        // Block the pool's spawns while the target is in flux; the worker
        // threads wait this out rather than spawning against the dead host.
        host.transport.mark_replacing();
        // Close the dead machine out. It is already gone, so a teardown error
        // just leaves a record for the next reconciliation pass.
        let lost = host
            .guard
            .lock()
            .expect("the rental guard lock is never poisoned")
            .take();
        let lost_id = if let Some(old) = lost {
            let tag = old.tag().to_string();
            let id = old.id().0.clone();
            let machine = old.machine().to_string();
            self.emit(Event::InstanceLost {
                tag: tag.clone(),
                instance: id.clone(),
            });
            // A live machine the supervisor found gone mid-run is an incident
            // against it; a machine with no identity records nothing.
            record_incident(
                self.store,
                group.provider.id(),
                &machine,
                &tag,
                IncidentKind::Lost,
                now_ms,
            )?;
            let _ = old.release();
            id
        } else {
            String::new()
        };
        // A replacement must carry at least the GPUs the pool's slots bind.
        let gpu_slots = host.slots.iter().filter(|slot| slot.is_some()).count() as u32;
        let mut constraints = group.spec.constraints.clone();
        constraints.min_gpu_count = Some(constraints.min_gpu_count.unwrap_or(0).max(gpu_slots));
        let limits = AcquireLimits {
            ready_timeout: group.spec.ready_timeout,
            ready_poll: group.spec.ready_poll,
        };
        match acquire::<dyn Provider + Sync>(
            group.provider,
            self.store,
            self.lock,
            RentalRole::Worker,
            &constraints,
            Objective::CheapestPerHour,
            &limits,
            self.budget,
            // Run teardown sets this to abort a replacement mid-flight, so a
            // slow offer walk never delays the run's exit; a caller with no
            // cancellation (the unit tests) walks to completion.
            self.cancel.unwrap_or(never_cancelled()),
        ) {
            Ok(new_guard) => {
                self.emit(Event::InstanceReplaced {
                    tag: new_guard.tag().to_string(),
                    from: lost_id,
                    to: new_guard.id().0.clone(),
                });
                // The replacement answers on its own endpoint's host, not the
                // dead machine's host fixed at construction.
                self.emit(instance_online(&new_guard, &new_guard.endpoint().host));
                host.transport
                    .swap_to_live(endpoint_target(new_guard.endpoint().clone()));
                *host
                    .guard
                    .lock()
                    .expect("the rental guard lock is never poisoned") = Some(new_guard);
                Ok(())
            }
            Err(_) => {
                // A replacement refused because the budget is exhausted is a
                // graceful wind-down, not a strict-fill fault: exhaustion is
                // resumable, a fault is not. Re-assess, and on exhaustion take
                // the interrupt path — the transport retires non-fatally as the
                // run is ending. Any other failure retires per the fill policy:
                // strict faults the run, best-effort runs on with one fewer
                // pool.
                match assess(self.store, self.lock.run(), self.budget, now_ms)? {
                    Verdict::Exhausted(exhaustion) => {
                        self.wind_down_for_budget(exhaustion);
                        host.transport.retire(false);
                    }
                    Verdict::Within { .. } => {
                        let fatal = matches!(group.fill, FillPolicy::Strict);
                        host.transport.retire(fatal);
                    }
                }
                Ok(())
            }
        }
    }
}

/// The `InstanceOnline` event for a guarded machine: the rental's tag, the
/// provider's id, the offer's hardware, its rate, and the host it answers on.
fn instance_online<P: Provider + ?Sized>(guard: &InstanceGuard<'_, P>, host: &str) -> Event {
    Event::InstanceOnline {
        tag: guard.tag().to_string(),
        instance: guard.id().0.clone(),
        gpu_model: guard.gpu_model().to_string(),
        gpu_count: guard.gpu_count(),
        rate_microusd_hour: guard.rate().0,
        host: host.to_string(),
    }
}

/// The supervisor's exit guard, run on every path out of [`Supervisor::run`]:
/// it clears the run's emitter clone so the collector can join, and retires the
/// transports as fatal on an unwind so a worker blocked in `Replacing` never
/// waits forever on a target the panicked supervisor will never swap. A clean
/// return disarms the retirement; the emitter clearing is unconditional.
struct SupervisorExit<'a, 'b> {
    groups: &'b [RentalGroup<'a>],
    emitter: &'b Mutex<Option<Emitter>>,
    armed: bool,
}

impl SupervisorExit<'_, '_> {
    /// Disarms the transport retirement: a clean return leaves the transports
    /// alive for the run's own wind-down. The emitter is still cleared on drop.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for SupervisorExit<'_, '_> {
    fn drop(&mut self) {
        // The run's collector joins only once every emitter clone drops, so the
        // supervisor's clone must be released on every exit path — including
        // the panic path the explicit clearing in `run` would skip.
        *self
            .emitter
            .lock()
            .expect("the emitter lock is never poisoned") = None;
        if self.armed {
            for host in self.groups.iter().flat_map(|group| &group.hosts) {
                host.transport.retire(true);
            }
        }
    }
}

/// The stub marketplace: `count` always-available offers, each generous enough
/// to pass typical constraints, priced distinctly so selection's ranking is
/// deterministic.
fn stub_offers(count: usize) -> Vec<Offer> {
    (0..count.max(1))
        .map(|n| Offer {
            id: OfferId(format!("stub-offer-{n}")),
            machine: format!("stub-machine-{n}"),
            gpu_model: "stub-gpu".to_string(),
            gpu_count: 1,
            vram_mb: 24_000,
            // Distinct rates keep the cheapest-per-hour ranking a total order;
            // $0.10/hr and up, low enough to sit under an ordinary price cap.
            price: Price(100_000 + n as u64),
            reliability: 1.0,
            verified: true,
            disk_gb: 1_000,
            bandwidth_mbps: 10_000,
            location: String::new(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use sima_domains::devices::DeviceType;
    use sima_model::{GeneratorConfig, GeneratorId, Params, RunConfig, RunId};
    use sima_provider::stub::StubProvider;
    use sima_provider::{Constraints, Cost, InstanceStatus, Provision};
    use sima_trace::{Collector, DurableSink, Record};
    use tempfile::TempDir;

    use super::*;

    /// A journal sink that discards every line: tests capture events through the
    /// collector's observer, not its durable output.
    struct NullSink;

    impl DurableSink for NullSink {
        fn append_line(&mut self, _line: &str) -> Result<()> {
            Ok(())
        }
    }

    /// Runs `body` with a live collector, capturing every event the supervisor
    /// emits into the returned vector. The emitter clone is released before the
    /// collector joins, so the scoped thread always returns.
    fn capture_events(
        body: impl FnOnce(&Mutex<Option<Emitter>>) -> Result<()>,
    ) -> Result<Vec<Event>> {
        let captured: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
        let captured_obs = Arc::clone(&captured);
        let observer = move |record: &Record| {
            captured_obs
                .lock()
                .expect("capture lock")
                .push(record.event.clone());
        };
        thread::scope(|scope| -> Result<()> {
            let collector = Collector::spawn(scope, NullSink, &observer);
            let emitter = Mutex::new(Some(collector.emitter()));
            body(&emitter)?;
            // Release the emitter clone so the collector thread can join.
            *emitter.lock().expect("emitter lock") = None;
            collector.shutdown()
        })?;
        let events = std::mem::take(&mut *captured.lock().expect("capture lock"));
        Ok(events)
    }

    /// A rented specification reaching the stub control plane, polling without
    /// waiting so a probe retry never sleeps in tests.
    fn spec() -> Rented {
        Rented {
            provider: ProviderId::Stub,
            image: "ghcr.io/alvatar/sima:latest".to_string(),
            disk_gb: 32,
            ready_timeout: Duration::from_millis(500),
            ready_poll: Duration::ZERO,
            constraints: Constraints::default(),
        }
    }

    /// A rental of `count` machines under `fill`, over `spec`.
    fn rental(spec: &Rented, count: usize, fill: FillPolicy) -> Rental<'_> {
        Rental {
            name: "rented",
            spec,
            count,
            fill,
        }
    }

    /// The one group a single-rental test supervises.
    fn one_group<'a>(
        provider: &'a (dyn Provider + Sync),
        spec: &'a Rented,
        fill: FillPolicy,
        hosts: Vec<RentedHost<'a>>,
    ) -> Vec<RentalGroup<'a>> {
        vec![RentalGroup {
            provider,
            spec,
            fill,
            hosts,
        }]
    }

    /// A generous stub offer at `price` micro-USD/hour, distinct by `id`.
    fn offer(id: &str, price: u64) -> Offer {
        Offer {
            id: OfferId(id.to_string()),
            machine: format!("machine-{id}"),
            gpu_model: "stub-gpu".to_string(),
            gpu_count: 1,
            vram_mb: 24_000,
            price: Price(price),
            reliability: 1.0,
            verified: true,
            disk_gb: 1_000,
            bandwidth_mbps: 10_000,
            location: String::new(),
        }
    }

    /// A store over a fresh temp directory and a run id to own acquisitions.
    fn acquisition_env() -> (TempDir, Store, RunId) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        let run = RunConfig {
            root_seed: 1,
            segments: None,
            format: FormatId::new("stub.v1").expect("format id"),
            generator: GeneratorConfig {
                id: GeneratorId::new("stub.v1").expect("generator id"),
                params: Vec::new(),
            },
            params: Params { bytes: vec![1] },
        }
        .id();
        (dir, store, run)
    }

    /// The execution settings the transport carries; no checkpoint cadence.
    fn exec() -> ExecutionConfig {
        ExecutionConfig::new(1, 3, Duration::MAX, Duration::MAX, None).expect("execution config")
    }

    /// A local probe that enumerates no device, so every acquired machine
    /// derives a single deviceless slot without a real worker binary or GPU.
    fn deviceless_probe() -> SpawnMode {
        SpawnMode::Local(PathBuf::from("/bin/true"))
    }

    /// One enumerated device of the given category.
    fn device(vendor_id: u32, device_id: u32, name: &str, device_type: DeviceType) -> DeviceInfo {
        DeviceInfo {
            vendor_id,
            device_id,
            name: name.to_string(),
            device_type,
            member: 0,
        }
    }

    #[test]
    fn a_machine_with_no_device_gets_one_deviceless_slot() {
        assert_eq!(rented_slots(&[]), vec![None]);
    }

    #[test]
    fn every_gpu_gets_a_slot_bound_to_it() {
        let devices = [
            device(
                0x10de,
                0x2684,
                "NVIDIA GeForce RTX 4090",
                DeviceType::Discrete,
            ),
            device(0x8086, 0x7d51, "Intel(R) Graphics", DeviceType::Integrated),
        ];
        let slots = rented_slots(&devices);
        assert_eq!(slots.len(), 2);
        assert_eq!(
            slots[0],
            Some(DeviceBinding {
                vendor_id: 0x10de,
                device_id: 0x2684,
                member: 0
            })
        );
        assert_eq!(
            slots[1],
            Some(DeviceBinding {
                vendor_id: 0x8086,
                device_id: 0x7d51,
                member: 0
            })
        );
    }

    #[test]
    fn a_software_rasterizer_beside_a_gpu_gets_no_slot() {
        // What a rented host reports: its card, and the CPU rasterizer the
        // graphics stack falls back to. The machine was rented for the card.
        let devices = [
            device(0x10005, 0x0000, "llvmpipe (LLVM 19)", DeviceType::Cpu),
            device(
                0x10de,
                0x2684,
                "NVIDIA GeForce RTX 4090",
                DeviceType::Discrete,
            ),
        ];
        let slots = rented_slots(&devices);
        assert_eq!(slots.len(), 1, "one slot, on the GPU");
        assert_eq!(
            slots[0],
            Some(DeviceBinding {
                vendor_id: 0x10de,
                device_id: 0x2684,
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
        let devices = [device(
            0x10005,
            0x0000,
            "llvmpipe (LLVM 19)",
            DeviceType::Cpu,
        )];
        let slots = rented_slots(&devices);
        assert_eq!(slots.len(), 1);
        assert_eq!(
            slots[0],
            Some(DeviceBinding {
                vendor_id: 0x10005,
                device_id: 0x0000,
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
    fn a_stub_endpoint_reads_as_a_user_a_host_and_a_port() -> Result<()> {
        assert_eq!(
            stub_endpoint("tester@127.0.0.1:41022")?,
            SshEndpoint {
                host: "127.0.0.1".to_string(),
                port: 41022,
                user: "tester".to_string(),
            }
        );
        // Taken from the right, so a bracketed IPv6 literal keeps its own
        // colons.
        assert_eq!(stub_endpoint("root@[::1]:22")?.host, "[::1]");
        Ok(())
    }

    #[test]
    fn a_malformed_stub_endpoint_names_the_variable_and_falls_back_to_nothing() {
        // Falling back to the in-process path would report a success that
        // tested nothing, which is the failure this whole seam exists to avoid.
        for value in [
            "127.0.0.1:41022",
            "tester@127.0.0.1",
            "tester@127.0.0.1:0",
            "tester@127.0.0.1:not-a-port",
            "@127.0.0.1:22",
            "tester@:22",
            "",
        ] {
            match stub_endpoint(value) {
                Err(Error::Validation(message)) => assert!(
                    message.contains(STUB_SSH) && message.contains(value),
                    "names the variable and the value: {message}"
                ),
                other => panic!("expected {value:?} to be refused, got {other:?}"),
            }
        }
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
        let provider = provider_for(&rental(&spec, 3, FillPolicy::Strict))?;
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
        let provider = provider_for(&rental(&spec, 1, FillPolicy::Strict))?;
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

    #[test]
    fn budget_spend_exhaustion_sets_the_interrupt() -> Result<()> {
        // One rental admitted under a small spend cap; a heartbeat at a far
        // future time sees its accrual cross the cap and winds the run down.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget {
            max_spend: Some(Cost(50_000)),
            max_wall_clock: None,
        };
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        let interrupt = AtomicBool::new(false);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, &emitter);
        // A far-future heartbeat: the open rental's accrual is well past the cap.
        supervisor.tick(u64::MAX)?;
        assert!(
            interrupt.load(Ordering::Relaxed),
            "spend exhaustion interrupts"
        );
        release_all(groups)?;
        Ok(())
    }

    #[test]
    fn wall_clock_exhaustion_sets_the_interrupt() -> Result<()> {
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget {
            max_spend: None,
            max_wall_clock: Some(Duration::from_millis(1)),
        };
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        let interrupt = AtomicBool::new(false);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, &emitter);
        supervisor.tick(u64::MAX)?;
        assert!(
            interrupt.load(Ordering::Relaxed),
            "wall-clock exhaustion interrupts"
        );
        release_all(groups)?;
        Ok(())
    }

    #[test]
    fn a_healthy_rental_makes_no_teardown_or_replacement() -> Result<()> {
        // A heartbeat over healthy machines polls status and assesses budget,
        // and does nothing else: nothing destroyed, nothing replaced.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget::default();
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        let live_before = provider.live();
        let interrupt = AtomicBool::new(false);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, &emitter);
        supervisor.tick(now_ms())?;
        assert!(
            !interrupt.load(Ordering::Relaxed),
            "a healthy rental is not interrupted"
        );
        assert!(provider.destroyed().is_empty(), "nothing is torn down");
        assert_eq!(provider.live(), live_before, "nothing is replaced");
        release_all(groups)?;
        Ok(())
    }

    #[test]
    fn a_lost_machine_is_replaced_and_the_target_swaps() -> Result<()> {
        // Two offers, one machine: the acquired machine is killed on the
        // provider side, and a heartbeat replaces it — the dead one torn down, a
        // new one acquired, and the transport target swapped to the new host.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget::default();
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        let host = &groups[0].hosts[0];
        let original_host = host.transport.live_host().expect("a live host");
        // Kill the machine on the provider side: its next status is Gone.
        let dead_id = host
            .guard
            .lock()
            .expect("the rental guard lock is never poisoned")
            .as_ref()
            .expect("a live guard")
            .id()
            .clone();
        provider.destroy(&dead_id)?;

        let interrupt = AtomicBool::new(false);
        let events = capture_events(|emitter| {
            let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, emitter);
            supervisor.tick(now_ms())
        })?;

        // The transport now points at a different, live host, and a fresh
        // machine is running.
        let new_host = host
            .transport
            .live_host()
            .expect("a live host after replacement");
        assert_ne!(
            new_host, original_host,
            "the target swapped to the replacement"
        );
        assert_eq!(
            provider.live().len(),
            1,
            "exactly one machine runs after replacement"
        );
        // The replacement's InstanceOnline reports the host it actually answers
        // on — the new endpoint's host — not the dead machine's original host.
        let online_hosts: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                Event::InstanceOnline { host, .. } => Some(host.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            online_hosts.last().copied(),
            Some(new_host.as_str()),
            "the replacement announces its own host, not the dead machine's"
        );
        // The lost machine left one Lost incident naming it, so a machine that
        // keeps vanishing is eventually disqualified.
        let incidents = store.machine_incidents()?;
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].kind, IncidentKind::Lost);
        assert_eq!(incidents[0].machine, "machine-a");
        release_all(groups)?;
        assert!(
            provider.live().is_empty(),
            "release tears the replacement down"
        );
        Ok(())
    }

    #[test]
    fn a_replacement_that_cannot_be_made_retires_the_transport() -> Result<()> {
        // One offer, one machine, killed on the provider side: no offer remains
        // for a replacement, so the transport retires and points at no host.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget::default();
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        let dead_id = groups[0].hosts[0]
            .guard
            .lock()
            .expect("the rental guard lock is never poisoned")
            .as_ref()
            .expect("a live guard")
            .id()
            .clone();
        provider.destroy(&dead_id)?;

        let interrupt = AtomicBool::new(false);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, &emitter);
        supervisor.tick(now_ms())?;

        assert!(
            groups[0].hosts[0].transport.live_host().is_none(),
            "a transport with no replacement retires"
        );
        release_all(groups)?;
        Ok(())
    }

    /// A closed rental of `owner` that cost `cost` micro-USD, anchoring a run's
    /// spend at a fixed past window.
    fn closed_spend(owner: &RunId, cost: u64) -> sima_store::SpendEntry {
        sima_store::SpendEntry {
            tag: "sima-prior-0".to_string(),
            provider: "stub".to_string(),
            owner: owner.to_string(),
            price_micro_usd_hour: 100_000,
            started_ms: 1_700_000_000_000,
            ended_ms: 1_700_000_000_000 + 3_600_000,
            cost_micro_usd: cost,
        }
    }

    #[test]
    fn a_wind_down_tick_rents_no_replacement() -> Result<()> {
        // Two offers, one machine: a Gone machine is normally replaced from the
        // second offer. With the interrupt already set, the tick reads it first
        // and does nothing — no paid replacement while the run winds down.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget::default();
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        let original_host = groups[0].hosts[0]
            .transport
            .live_host()
            .expect("a live host");
        let dead_id = groups[0].hosts[0]
            .guard
            .lock()
            .expect("the rental guard lock is never poisoned")
            .as_ref()
            .expect("a live guard")
            .id()
            .clone();
        provider.destroy(&dead_id)?;

        let interrupt = AtomicBool::new(true);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, &emitter);
        supervisor.tick(now_ms())?;

        // Nothing new was provisioned, and the transport is untouched — still
        // naming the original host, not swapped or retired.
        assert!(provider.live().is_empty(), "a wind-down tick rents nothing");
        assert_eq!(
            groups[0].hosts[0].transport.live_host().as_deref(),
            Some(original_host.as_str()),
            "the transport is left as the wind-down found it"
        );
        release_all(groups)?;
        Ok(())
    }

    #[test]
    fn a_replacement_refused_for_budget_winds_down_rather_than_faulting() -> Result<()> {
        // A strict-fill rental whose replacement cannot be paid for: exhaustion
        // must surface as the resumable interrupt, not a strict-fill fault.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget {
            max_spend: Some(Cost(50_000)),
            max_wall_clock: None,
        };
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        // A prior closed rental already past the cap, so any replacement
        // acquisition is refused for want of budget whatever the clock reads.
        store.put_spend(&closed_spend(&run, 120_000))?;
        let dead_id = groups[0].hosts[0]
            .guard
            .lock()
            .expect("the rental guard lock is never poisoned")
            .as_ref()
            .expect("a live guard")
            .id()
            .clone();
        provider.destroy(&dead_id)?;

        let interrupt = AtomicBool::new(false);
        let events = capture_events(|emitter| {
            let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, emitter);
            // Drive the replacement directly: acquisition is refused for
            // budget, and the failure must classify as exhaustion.
            supervisor.replace(&groups[0], &groups[0].hosts[0], u64::MAX)
        })?;

        assert!(
            interrupt.load(Ordering::Relaxed),
            "a budget-refused replacement winds the run down"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Event::BudgetSpendExhausted { .. })),
            "the exhaustion is announced to the journal"
        );
        // The transport retired, and no run-level fault event was emitted: the
        // run will resume as Interrupted, not fault under strict fill.
        assert!(groups[0].hosts[0].transport.live_host().is_none());
        release_all(groups)?;
        Ok(())
    }

    #[test]
    fn every_group_is_supervised_under_the_runs_one_budget() -> Result<()> {
        // Two rented entries, each its own control plane and specification. One
        // heartbeat polls both, and the ceiling that stops them is the run's.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let first = StubProvider::new(vec![offer("a", 100_000)]);
        let second = StubProvider::new(vec![offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget {
            max_spend: Some(Cost(50_000)),
            max_wall_clock: None,
        };
        let mut groups = Vec::new();
        for provider in [&first as &(dyn Provider + Sync), &second] {
            let hosts = acquire_hosts(
                &rental(&spec, 1, FillPolicy::Strict),
                &budget,
                provider,
                &store,
                &lock,
                &deviceless_probe(),
                &format,
                &exec(),
            )?;
            groups.push(RentalGroup {
                provider,
                spec: &spec,
                fill: FillPolicy::Strict,
                hosts,
            });
        }
        let interrupt = AtomicBool::new(false);
        let events = capture_events(|emitter| {
            let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, emitter);
            supervisor.tick(u64::MAX)
        })?;
        assert!(
            interrupt.load(Ordering::Relaxed),
            "the run's ceiling stops every group at once"
        );
        // One exhaustion announcement for the run, not one per group.
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::BudgetSpendExhausted { .. }))
                .count(),
            1
        );
        release_all(groups)?;
        assert!(first.live().is_empty());
        assert!(second.live().is_empty());
        Ok(())
    }

    #[test]
    fn the_composition_is_announced_once_the_emitter_arrives_not_before() -> Result<()> {
        // A tick that beats the start hook must not spend the announcement
        // latch: the initial composition would vanish from the journal. The
        // latch holds until the emitter arrives, then a later tick announces.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget::default();
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        let interrupt = AtomicBool::new(false);
        let captured: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
        let captured_obs = Arc::clone(&captured);
        let observer = move |record: &Record| {
            captured_obs
                .lock()
                .expect("capture lock")
                .push(record.event.clone());
        };
        thread::scope(|scope| -> Result<()> {
            let collector = Collector::spawn(scope, NullSink, &observer);
            let emitter = Mutex::new(None);
            let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, &emitter);
            // The emitter has not arrived: this tick announces nothing and
            // leaves the latch unspent.
            supervisor.tick(now_ms())?;
            *emitter.lock().expect("emitter lock") = Some(collector.emitter());
            // The emitter is present now: this tick announces the composition.
            supervisor.tick(now_ms())?;
            // The latch holds: a third tick announces nothing more.
            supervisor.tick(now_ms())?;
            *emitter.lock().expect("emitter lock") = None;
            collector.shutdown()
        })?;
        let online = captured
            .lock()
            .expect("capture lock")
            .iter()
            .filter(|event| matches!(event, Event::InstanceOnline { .. }))
            .count();
        assert_eq!(
            online, 1,
            "the composition is announced exactly once, after the emitter arrives"
        );
        release_all(groups)?;
        Ok(())
    }

    #[test]
    fn the_composition_is_announced_once_when_the_emitter_is_present_from_the_start() -> Result<()>
    {
        // The regression: with the emitter present from the first tick, the
        // announcement fires once, and a second tick does not repeat it.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let budget = Budget::default();
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &budget,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        let interrupt = AtomicBool::new(false);
        let events = capture_events(|emitter| {
            let supervisor = Supervisor::new(&store, &lock, &budget, &groups, &interrupt, emitter);
            supervisor.tick(now_ms())?;
            supervisor.tick(now_ms())
        })?;
        let online = events
            .iter()
            .filter(|event| matches!(event, Event::InstanceOnline { .. }))
            .count();
        assert_eq!(online, 1, "announced exactly once from the start");
        release_all(groups)?;
        Ok(())
    }

    #[test]
    fn a_clean_supervisor_exit_clears_the_emitter_and_leaves_transports_alive() -> Result<()> {
        // The run's own wind-down tears the rentals down, so a clean supervisor
        // exit must leave the transports alive — but it must still release the
        // emitter clone, or the collector never joins.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &Budget::default(),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        assert!(groups[0].hosts[0].transport.live_host().is_some());
        let observer = |_: &Record| {};
        thread::scope(|scope| -> Result<()> {
            let collector = Collector::spawn(scope, NullSink, &observer);
            let emitter = Mutex::new(Some(collector.emitter()));
            let guard = SupervisorExit {
                groups: &groups,
                emitter: &emitter,
                armed: true,
            };
            guard.disarm();
            assert!(
                emitter.lock().expect("emitter lock").is_none(),
                "a clean exit still clears the emitter"
            );
            collector.shutdown()
        })?;
        assert!(
            groups[0].hosts[0].transport.live_host().is_some(),
            "a clean exit leaves the transports alive for the run's wind-down"
        );
        release_all(groups)?;
        Ok(())
    }

    #[test]
    fn a_supervisor_panic_clears_the_emitter_and_retires_the_transports() -> Result<()> {
        // A tick panic unwinds past the emitter-clearing line the old `run`
        // held; the collector would then wait forever on the leaked clone and
        // the rentals would never tear down. The exit guard clears the emitter
        // and retires the transports on the unwind alike.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let spec = spec();
        let hosts = acquire_hosts(
            &rental(&spec, 1, FillPolicy::Strict),
            &Budget::default(),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let groups = one_group(&provider, &spec, FillPolicy::Strict, hosts);
        assert!(groups[0].hosts[0].transport.live_host().is_some());
        let observer = |_: &Record| {};
        thread::scope(|scope| -> Result<()> {
            let collector = Collector::spawn(scope, NullSink, &observer);
            let emitter = Mutex::new(Some(collector.emitter()));
            let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = SupervisorExit {
                    groups: &groups,
                    emitter: &emitter,
                    armed: true,
                };
                panic!("a supervisor tick panicked");
            }));
            assert!(unwound.is_err(), "the panic propagated");
            assert!(
                emitter.lock().expect("emitter lock").is_none(),
                "the panic path still clears the emitter"
            );
            collector.shutdown()
        })?;
        assert!(
            groups[0].hosts[0].transport.live_host().is_none(),
            "the panic retires the transports so no worker waits on a dead supervisor"
        );
        release_all(groups)?;
        Ok(())
    }
}
