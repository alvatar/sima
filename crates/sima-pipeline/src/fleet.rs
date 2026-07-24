//! Fleet dispatch: the config's `provider` id resolved to a control-plane
//! backend and the transport mode its instances are reached through.
//!
//! The pipeline is where provider choice becomes concrete, so this is the one
//! edge from configuration to a boxed [`Provider`]. A run that names no
//! `[fleet]` never reaches here, so it constructs no provider and reads no
//! `VAST_API_KEY`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::FormatId;
use sima_provider::stub::StubProvider;
use sima_provider::{
    AcquireLimits, Exhaustion, InstanceGuard, InstanceStatus, Objective, Offer, OfferId, Price,
    Provider, SshEndpoint, Verdict, acquire, assess,
};
use sima_provider_vast::{VastConfig, VastProvider};
use sima_scheduler::ExecutionConfig;
use sima_store::{RunLock, Store};
use sima_trace::{Emitter, Event};
use sima_transport::{FleetMode, FleetTransport, SshTarget};

use crate::config::{FillPolicy, FleetConfig, FleetProvider};
use crate::devices::parse_enumeration;
use crate::orchestrate::{command_stdout, worker_binary};

/// Builds the control-plane backend the fleet acquires instances through.
///
/// The `vast` backend reads its key from `VAST_API_KEY`; an absent key is an
/// [`Error::Provider`](sima_core::Error::Provider) naming the variable, raised
/// here before any store mutation. The `stub` backend is in-process, listing a
/// generous always-available marketplace so a stub fleet fills its declared
/// count. An unknown id never reaches here — the config load rejects it.
pub(crate) fn provider_for(fleet: &FleetConfig) -> Result<Box<dyn Provider + Sync>> {
    match fleet.provider {
        FleetProvider::Vast => {
            let config = VastConfig::from_env(&fleet.image, fleet.disk_gb)?;
            Ok(Box::new(VastProvider::new(config)))
        }
        FleetProvider::Stub => Ok(Box::new(StubProvider::new(stub_offers(fleet.count)))),
    }
}

/// The transport mode the fleet's instances are reached through: ssh to a real
/// rented instance, or a local `sima-worker` spawn for the stub, so the stub
/// exercises every layer above the transport with no network.
pub(crate) fn transport_mode(fleet: &FleetConfig) -> Result<FleetMode> {
    match fleet.provider {
        FleetProvider::Vast => Ok(FleetMode::Ssh),
        FleetProvider::Stub => Ok(FleetMode::Local(worker_binary()?)),
    }
}

/// Maps a provider's ssh endpoint into the transport's target, the seam that
/// keeps the transport free of any dependency on the provider crate.
pub(crate) fn endpoint_target(endpoint: SshEndpoint) -> SshTarget {
    SshTarget {
        host: endpoint.host,
        port: endpoint.port,
        user: endpoint.user,
    }
}

/// How many times an instance's enumeration probe is retried before its
/// acquisition is abandoned: sshd can lag the provider's `Ready`, so the first
/// probe against a fresh host may be refused.
const PROBE_ATTEMPTS: u32 = 6;

/// One acquired fleet instance: the guard that owns and tears it down, the
/// transport its pool spawns workers through, and the worker slots its probe
/// derived (one per enumerated GPU, or one deviceless slot when it reports
/// none).
pub(crate) struct FleetInstance<'a> {
    /// Ownership of the rented instance; its teardown runs on release or drop.
    /// Behind a lock and an `Option` so the supervisor can swap in a
    /// replacement without disturbing the pool's shared borrow of the
    /// transport; `None` once the guard has been released or the instance has
    /// retired with no replacement.
    pub(crate) guard: Mutex<Option<InstanceGuard<'a, dyn Provider + Sync + 'a>>>,
    /// The transport spawning this instance's workers, its target swappable
    /// under the running pool.
    pub(crate) transport: FleetTransport,
    /// The instance's host label, for the journal.
    pub(crate) host: String,
    /// One slot per enumerated GPU, or a single deviceless slot. Fixed for the
    /// run: a replacement must carry at least this many GPUs.
    pub(crate) slots: Vec<Option<DeviceBinding>>,
}

/// Acquires the fleet's instances, each behind a teardown guard, and builds a
/// transport and worker slots for each.
///
/// Every acquisition is budget-admitted and intent-recorded by
/// [`acquire`](sima_provider::acquire), and an instance that fails to acquire
/// or probe is torn down individually. On a shortfall the fill policy decides:
/// strict tears down everything acquired so far and fails the run; best-effort
/// proceeds with what came up, so long as one instance did.
pub(crate) fn acquire_fleet<'a>(
    fleet: &FleetConfig,
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &RunLock,
    mode: &FleetMode,
    format: &FormatId,
    exec: &ExecutionConfig,
) -> Result<Vec<FleetInstance<'a>>> {
    let limits = AcquireLimits {
        ready_timeout: fleet.ready_timeout,
        ready_poll: fleet.ready_poll,
    };
    let mut instances: Vec<FleetInstance<'a>> = Vec::with_capacity(fleet.count);
    for _ in 0..fleet.count {
        // An instance that fails to acquire or probe is torn down inside
        // `acquire_one` before its error returns here.
        match acquire_one(provider, store, lock, fleet, &limits, mode, format, exec) {
            Ok(instance) => instances.push(instance),
            Err(error) => match fleet.fill {
                // Strict: the declared count or nothing. Dropping `instances`
                // here tears down every instance already acquired.
                FillPolicy::Strict => return Err(error),
                // Best-effort: run with what came up. Stop asking on the first
                // shortfall — the market is not filling the count.
                FillPolicy::BestEffort => break,
            },
        }
    }
    if instances.is_empty() {
        return Err(Error::Provider(
            "the fleet acquired no instances".to_string(),
        ));
    }
    Ok(instances)
}

/// Acquires one instance, probes it, and builds its transport and slots. On a
/// probe failure the guard drops here, tearing the instance down, so no
/// half-acquired instance leaks.
#[allow(clippy::too_many_arguments)]
fn acquire_one<'a>(
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &RunLock,
    fleet: &FleetConfig,
    limits: &AcquireLimits,
    mode: &FleetMode,
    format: &FormatId,
    exec: &ExecutionConfig,
) -> Result<FleetInstance<'a>> {
    // Pin the trait object to `Sync`, which the supervisor thread's shared
    // borrow of the provider needs; without it inference drops the bound.
    let guard = acquire::<dyn Provider + Sync>(
        provider,
        store,
        lock,
        &fleet.constraints,
        Objective::CheapestPerHour,
        limits,
        &fleet.budget,
        // Run-start acquisition has nothing to cancel: the run is not yet
        // driving, so no wind-down is in flight.
        never_cancelled(),
    )?;
    let target = endpoint_target(guard.endpoint().clone());
    let host = target.host.clone();
    // The probe drives the instance's device enumeration; a failure drops the
    // guard, tearing the instance down.
    let slots = probe_slots(mode, &target, fleet.ready_poll)?;
    let transport = FleetTransport::new(
        mode.clone(),
        target,
        format.clone(),
        exec.checkpoint_interval,
        exec.checkpoint_interval_steps,
        // The transport waits out a respawn against a dead host on the same
        // readiness bounds the fleet acquires under, bridging the window until
        // the supervisor swaps a replacement in.
        fleet.ready_timeout,
        fleet.ready_poll,
    );
    Ok(FleetInstance {
        guard: Mutex::new(Some(guard)),
        transport,
        host,
        slots,
    })
}

/// Probes an instance for its devices and derives its worker slots, retrying
/// briefly because sshd can lag the provider's `Ready`.
fn probe_slots(
    mode: &FleetMode,
    target: &SshTarget,
    poll: Duration,
) -> Result<Vec<Option<DeviceBinding>>> {
    let argv = sima_transport::fleet::probe_argv(mode, target);
    let mut last: Option<Error> = None;
    for attempt in 0..PROBE_ATTEMPTS {
        match command_stdout(&argv).and_then(|stdout| parse_enumeration(&stdout)) {
            Ok(devices) => return Ok(fleet_slots(&devices)),
            Err(error) => {
                last = Some(error);
                // No sleep after the final attempt.
                if attempt + 1 < PROBE_ATTEMPTS {
                    thread::sleep(poll.min(Duration::from_secs(5)));
                }
            }
        }
    }
    Err(last.unwrap_or_else(|| Error::Provider("the instance probe never ran".to_string())))
}

/// One worker slot per enumerated GPU, each bound to its own device; a probe
/// reporting no GPU yields a single deviceless worker — the stub testing path,
/// and any device-free instance.
fn fleet_slots(devices: &[DeviceInfo]) -> Vec<Option<DeviceBinding>> {
    if devices.is_empty() {
        return vec![None];
    }
    devices
        .iter()
        .map(|device| {
            Some(DeviceBinding {
                vendor_id: device.vendor_id,
                device_id: device.device_id,
                member: device.member,
            })
        })
        .collect()
}

/// Releases every fleet instance's guard on the way out, returning the first
/// teardown failure. Every guard is released whatever the others do, so one
/// failure never strands the rest; a guard whose release is not reached is torn
/// down by its drop, and the ledger record a failed teardown leaves is what the
/// next reconciliation pass acts on.
pub(crate) fn release_all(instances: Vec<FleetInstance<'_>>) -> Result<()> {
    let mut first: Option<Error> = None;
    for instance in instances {
        // The transport drops with the instance; only the guard's teardown can
        // fail and is worth reporting. A `None` guard was already released by a
        // replacement, or retired with none, so there is nothing to tear down.
        let guard = instance
            .guard
            .into_inner()
            .expect("the fleet guard lock is never poisoned");
        if let Some(guard) = guard
            && let Err(error) = guard.release()
            && first.is_none()
        {
            first = Some(error);
        }
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The supervisor's heartbeat period. The fleet's health is a low-frequency
/// concern and the poll is cheap, so a fixed period suffices; no config knob.
const HEARTBEAT: Duration = Duration::from_secs(10);

/// A cancellation flag that is never set, for an acquisition with no wind-down
/// to observe — the run-start fleet acquisition, before the run drives.
fn never_cancelled() -> &'static AtomicBool {
    static NEVER: AtomicBool = AtomicBool::new(false);
    &NEVER
}

/// Milliseconds since the epoch, the clock the budget ledger is stamped in.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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

/// The fleet supervisor: one thread that, each heartbeat, assesses the run's
/// budget and polls each instance's health.
///
/// Budget exhaustion sets the interrupt flag SIGINT sets, so the run winds down
/// gracefully and the guards tear the fleet down. An instance polled `Gone` is
/// replaced: its record is closed out, a replacement is acquired under the same
/// constraints (raised to the slots in use), and the transport's target swaps,
/// so the worker loop's existing respawn lands on the new machine. A
/// replacement that cannot be made retires the transport — fatally under strict
/// fill, a clean degradation under best-effort.
pub(crate) struct Supervisor<'a, 'b> {
    provider: &'a (dyn Provider + Sync),
    store: &'a Store,
    lock: &'a RunLock,
    fleet: &'a FleetConfig,
    /// The slice borrow is a lifetime of its own, shorter than the instances'
    /// own `'a`, so the caller can move the `Vec` for teardown once the
    /// supervisor is done.
    instances: &'b [FleetInstance<'a>],
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
    /// and `None` until then. Fleet events cross the same single-writer journal
    /// seam every other event does.
    emitter: &'b Mutex<Option<Emitter>>,
    /// Whether the initial fleet composition has been announced, so the online
    /// events fire once.
    announced: AtomicBool,
    /// Whether a budget exhaustion has been announced, so its event fires once.
    budget_announced: AtomicBool,
}

impl<'a, 'b> Supervisor<'a, 'b> {
    pub(crate) fn new(
        provider: &'a (dyn Provider + Sync),
        store: &'a Store,
        lock: &'a RunLock,
        fleet: &'a FleetConfig,
        instances: &'b [FleetInstance<'a>],
        interrupt: &'b AtomicBool,
        emitter: &'b Mutex<Option<Emitter>>,
    ) -> Supervisor<'a, 'b> {
        Supervisor {
            provider,
            store,
            lock,
            fleet,
            instances,
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
            instances: self.instances,
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

    /// Announces the initial fleet composition once, as soon as the emitter is
    /// available: one `InstanceOnline` per instance still holding a guard.
    fn announce_composition(&self) {
        if self.announced.swap(true, Ordering::Relaxed) {
            return;
        }
        for instance in self.instances {
            if let Some(guard) = &*instance
                .guard
                .lock()
                .expect("the fleet guard lock is never poisoned")
            {
                self.emit(instance_online(guard, &instance.host));
            }
        }
    }

    /// Winds the run down for budget exhaustion: sets the interrupt flag SIGINT
    /// sets, so the run winds down gracefully and the guards tear the fleet
    /// down, and announces the exhaustion once. Shared by the heartbeat's own
    /// budget check and a replacement refused for want of budget.
    fn wind_down_for_budget(&self, exhaustion: Exhaustion) {
        self.interrupt.store(true, Ordering::Relaxed);
        if !self.budget_announced.swap(true, Ordering::Relaxed) {
            self.emit(match exhaustion {
                Exhaustion::Spend { accrued, cap } => Event::BudgetSpendExhausted {
                    accrued_microusd: accrued.0,
                    cap_microusd: cap.0,
                },
                Exhaustion::WallClock { deadline_ms } => {
                    Event::BudgetWallClockExhausted { deadline_ms }
                }
            });
        }
    }

    /// One heartbeat: observe the wind-down flag, announce the composition
    /// once, assess the budget, then each instance's health. Budget exhaustion
    /// short-circuits the health poll — the whole run is winding down. `now_ms`
    /// is a parameter so a test can drive exhaustion without waiting on the
    /// clock.
    fn tick(&self, now_ms: u64) -> Result<()> {
        // Once wind-down is requested — Ctrl-C or budget — the marketplace is
        // off limits: rent no replacement while the run is ending. The flag is
        // read first, before any provider call.
        if self.interrupt.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.announce_composition();
        if let Verdict::Exhausted(exhaustion) =
            assess(self.store, self.lock.run(), &self.fleet.budget, now_ms)?
        {
            self.wind_down_for_budget(exhaustion);
            return Ok(());
        }
        for instance in self.instances {
            self.check_instance(instance, now_ms)?;
        }
        Ok(())
    }

    /// Polls one instance's health, replacing it if the provider reports it
    /// gone.
    fn check_instance(&self, instance: &FleetInstance<'a>, now_ms: u64) -> Result<()> {
        let id = match &*instance
            .guard
            .lock()
            .expect("the fleet guard lock is never poisoned")
        {
            Some(guard) => guard.id().clone(),
            // Already retired with no replacement: nothing to poll.
            None => return Ok(()),
        };
        match self.provider.instance(&id)? {
            InstanceStatus::Ready(_) | InstanceStatus::Provisioning => Ok(()),
            InstanceStatus::Gone => self.replace(instance, now_ms),
        }
    }

    /// Replaces a lost instance: blocks the transport's spawns, closes the dead
    /// instance out, acquires a replacement, and swaps the target — or retires
    /// the transport when no replacement can be made. `now_ms` dates the budget
    /// re-assessment a failed acquisition triggers.
    fn replace(&self, instance: &FleetInstance<'a>, now_ms: u64) -> Result<()> {
        // Block the pool's spawns while the target is in flux; the worker
        // threads wait this out rather than spawning against the dead host.
        instance.transport.mark_replacing();
        // Close the dead instance out. It is already gone, so a teardown error
        // just leaves a record for the next reconciliation pass.
        let lost = instance
            .guard
            .lock()
            .expect("the fleet guard lock is never poisoned")
            .take();
        let lost_id = if let Some(old) = lost {
            let tag = old.tag().to_string();
            let id = old.id().0.clone();
            self.emit(Event::InstanceLost {
                tag,
                instance: id.clone(),
            });
            let _ = old.release();
            id
        } else {
            String::new()
        };
        // A replacement must carry at least the GPUs the pool's slots bind.
        let gpu_slots = instance.slots.iter().filter(|slot| slot.is_some()).count() as u32;
        let mut constraints = self.fleet.constraints.clone();
        constraints.min_gpu_count = Some(constraints.min_gpu_count.unwrap_or(0).max(gpu_slots));
        let limits = AcquireLimits {
            ready_timeout: self.fleet.ready_timeout,
            ready_poll: self.fleet.ready_poll,
        };
        match acquire::<dyn Provider + Sync>(
            self.provider,
            self.store,
            self.lock,
            &constraints,
            Objective::CheapestPerHour,
            &limits,
            &self.fleet.budget,
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
                // dead instance's host fixed at construction.
                self.emit(instance_online(&new_guard, &new_guard.endpoint().host));
                instance
                    .transport
                    .swap_to_live(endpoint_target(new_guard.endpoint().clone()));
                *instance
                    .guard
                    .lock()
                    .expect("the fleet guard lock is never poisoned") = Some(new_guard);
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
                match assess(self.store, self.lock.run(), &self.fleet.budget, now_ms)? {
                    Verdict::Exhausted(exhaustion) => {
                        self.wind_down_for_budget(exhaustion);
                        instance.transport.retire(false);
                    }
                    Verdict::Within { .. } => {
                        let fatal = matches!(self.fleet.fill, FillPolicy::Strict);
                        instance.transport.retire(fatal);
                    }
                }
                Ok(())
            }
        }
    }
}

/// The `InstanceOnline` event for a guarded instance: the rental's tag, the
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

/// The supervisor's exit guard, run on every path out of
/// [`Supervisor::run`]: it clears the run's emitter clone so the collector can
/// join, and retires the transports as fatal on an unwind so a worker blocked
/// in `Replacing` never waits forever on a target the panicked supervisor will
/// never swap. A clean return disarms the retirement; the emitter clearing is
/// unconditional.
struct SupervisorExit<'a, 'b> {
    instances: &'b [FleetInstance<'a>],
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
            for instance in self.instances {
                instance.transport.retire(true);
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
    use std::time::Duration;

    use sima_model::{GeneratorConfig, GeneratorId, Params, RunConfig, RunId};
    use sima_provider::stub::StubProvider;
    use sima_provider::{InstanceStatus, Provision};
    use sima_trace::{Collector, DurableSink, Record};
    use tempfile::TempDir;

    use super::*;
    use crate::config::{FillPolicy, FleetConfig};

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

    /// A stub fleet requesting `count` instances, permissive constraints.
    fn stub_fleet(count: usize) -> FleetConfig {
        fleet_config(count, FillPolicy::Strict)
    }

    /// A stub fleet requesting `count` instances under `fill`.
    fn fleet_config(count: usize, fill: FillPolicy) -> FleetConfig {
        FleetConfig {
            provider: FleetProvider::Stub,
            count,
            fill,
            image: "ghcr.io/alvatar/sima-worker:latest".to_string(),
            disk_gb: 32,
            // Poll without waiting so a probe retry never sleeps in tests.
            ready_timeout: Duration::from_millis(500),
            ready_poll: Duration::ZERO,
            constraints: sima_provider::Constraints::default(),
            budget: sima_provider::Budget::default(),
        }
    }

    /// A generous stub offer at `price` micro-USD/hour, distinct by `id`.
    fn offer(id: &str, price: u64) -> Offer {
        Offer {
            id: OfferId(id.to_string()),
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

    /// A local probe that enumerates no device, so every acquired instance
    /// derives a single deviceless slot without a real worker binary or GPU.
    fn deviceless_probe() -> FleetMode {
        FleetMode::Local(PathBuf::from("/bin/true"))
    }

    #[test]
    fn a_strict_shortfall_tears_down_what_was_acquired_and_fails() -> Result<()> {
        // One offer for two requested instances under strict fill: the run
        // fails, and the one instance that came up is torn down.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let result = acquire_fleet(
            &fleet_config(2, FillPolicy::Strict),
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
            "the acquired instance is torn down"
        );
        assert!(provider.live().is_empty(), "no instance is left running");
        Ok(())
    }

    #[test]
    fn a_best_effort_shortfall_proceeds_with_what_came_up() -> Result<()> {
        // One offer for two requested instances under best-effort: the run
        // proceeds with the one instance, torn down on release.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let instances = acquire_fleet(
            &fleet_config(2, FillPolicy::BestEffort),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        assert_eq!(instances.len(), 1, "best-effort runs on what came up");
        assert!(
            provider.destroyed().is_empty(),
            "still running before release"
        );
        release_all(instances)?;
        assert_eq!(
            provider.destroyed().len(),
            1,
            "release tears the instance down"
        );
        assert!(provider.live().is_empty());
        Ok(())
    }

    #[test]
    fn the_fleet_acquires_and_probes_every_instance() -> Result<()> {
        // Two offers for two instances: both acquire, each probed into a single
        // deviceless slot, all torn down on release.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let instances = acquire_fleet(
            &fleet_config(2, FillPolicy::Strict),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        assert_eq!(instances.len(), 2);
        for instance in &instances {
            assert_eq!(
                instance.slots,
                vec![None],
                "a probe reporting no GPU is one slot"
            );
        }
        release_all(instances)?;
        assert_eq!(provider.destroyed().len(), 2);
        assert!(provider.live().is_empty());
        Ok(())
    }

    #[test]
    fn a_probe_failure_tears_the_instance_down() -> Result<()> {
        // The instance acquires but its probe never runs: the instance is torn
        // down rather than left running with no slots.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let result = acquire_fleet(
            &fleet_config(1, FillPolicy::Strict),
            &provider,
            &store,
            &lock,
            &FleetMode::Local(PathBuf::from("/nonexistent/sima-worker")),
            &format,
            &exec(),
        );
        assert!(result.is_err(), "a probe failure fails the acquisition");
        assert_eq!(provider.destroyed().len(), 1, "the instance is torn down");
        assert!(provider.live().is_empty());
        Ok(())
    }

    #[test]
    fn a_vast_fleet_is_reached_over_ssh() -> Result<()> {
        // The transport mode is a pure function of the provider: vast over ssh,
        // read without touching the environment (only the provider itself reads
        // the key).
        let mut fleet = stub_fleet(1);
        fleet.provider = FleetProvider::Vast;
        assert!(matches!(transport_mode(&fleet)?, FleetMode::Ssh));
        Ok(())
    }

    #[test]
    fn the_stub_provider_lists_an_offer_per_requested_instance() -> Result<()> {
        let provider = provider_for(&stub_fleet(3))?;
        assert_eq!(provider.id(), "stub");
        assert_eq!(provider.offers()?.len(), 3);
        Ok(())
    }

    #[test]
    fn the_stub_provider_acquires_an_instance_that_reaches_ready() -> Result<()> {
        // The stub acquires: provisioning an offer yields an instance that its
        // own status call reports Ready with an ssh endpoint, which maps to a
        // transport target.
        let provider = provider_for(&stub_fleet(1))?;
        let offer = provider.offers()?.into_iter().next().expect("an offer");
        let Provision::Provisioned(instance) = provider.provision(&offer.id, "tag-0")? else {
            panic!("the stub provisions an always-available offer");
        };
        let InstanceStatus::Ready(endpoint) = provider.instance(&instance.id)? else {
            panic!("the stub instance is ready at once");
        };
        let target = endpoint_target(endpoint.clone());
        assert_eq!(target.host, endpoint.host);
        assert_eq!(target.port, endpoint.port);
        assert_eq!(target.user, endpoint.user);
        Ok(())
    }

    /// A fleet under `budget` requesting one instance.
    fn budgeted_fleet(budget: sima_provider::Budget) -> FleetConfig {
        FleetConfig {
            budget,
            ..fleet_config(1, FillPolicy::Strict)
        }
    }

    #[test]
    fn budget_spend_exhaustion_sets_the_interrupt() -> Result<()> {
        // One rental admitted under a small spend cap; a heartbeat at a far
        // future time sees its accrual cross the cap and winds the run down.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = budgeted_fleet(sima_provider::Budget {
            max_spend: Some(sima_provider::Cost(50_000)),
            max_wall_clock: None,
        });
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let interrupt = AtomicBool::new(false);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(
            &provider, &store, &lock, &fleet, &instances, &interrupt, &emitter,
        );
        // A far-future heartbeat: the open rental's accrual is well past the cap.
        supervisor.tick(u64::MAX)?;
        assert!(
            interrupt.load(Ordering::Relaxed),
            "spend exhaustion interrupts"
        );
        release_all(instances)?;
        Ok(())
    }

    #[test]
    fn wall_clock_exhaustion_sets_the_interrupt() -> Result<()> {
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = budgeted_fleet(sima_provider::Budget {
            max_spend: None,
            max_wall_clock: Some(Duration::from_millis(1)),
        });
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let interrupt = AtomicBool::new(false);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(
            &provider, &store, &lock, &fleet, &instances, &interrupt, &emitter,
        );
        supervisor.tick(u64::MAX)?;
        assert!(
            interrupt.load(Ordering::Relaxed),
            "wall-clock exhaustion interrupts"
        );
        release_all(instances)?;
        Ok(())
    }

    #[test]
    fn a_healthy_fleet_makes_no_teardown_or_replacement() -> Result<()> {
        // A heartbeat over a healthy fleet polls status and assesses budget, and
        // does nothing else: no instance is destroyed, none replaced.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = stub_fleet(1);
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let live_before = provider.live();
        let interrupt = AtomicBool::new(false);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(
            &provider, &store, &lock, &fleet, &instances, &interrupt, &emitter,
        );
        supervisor.tick(now_ms())?;
        assert!(
            !interrupt.load(Ordering::Relaxed),
            "a healthy fleet is not interrupted"
        );
        assert!(provider.destroyed().is_empty(), "no instance is torn down");
        assert_eq!(provider.live(), live_before, "no instance is replaced");
        release_all(instances)?;
        Ok(())
    }

    #[test]
    fn a_lost_instance_is_replaced_and_the_target_swaps() -> Result<()> {
        // Two offers, one instance: the acquired instance is killed on the
        // provider side, and a heartbeat replaces it — the dead one torn down, a
        // new one acquired, and the transport target swapped to the new host.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = stub_fleet(1);
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let original_host = instances[0].transport.live_host().expect("a live host");
        // Kill the instance on the provider side: its next status is Gone.
        let dead_id = instances[0]
            .guard
            .lock()
            .expect("the fleet guard lock is never poisoned")
            .as_ref()
            .expect("a live guard")
            .id()
            .clone();
        provider.destroy(&dead_id)?;

        let interrupt = AtomicBool::new(false);
        let events = capture_events(|emitter| {
            let supervisor = Supervisor::new(
                &provider, &store, &lock, &fleet, &instances, &interrupt, emitter,
            );
            supervisor.tick(now_ms())
        })?;

        // The transport now points at a different, live host, and a fresh
        // instance is running.
        let new_host = instances[0]
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
            "exactly one instance runs after replacement"
        );
        // The replacement's InstanceOnline reports the host it actually answers
        // on — the new endpoint's host — not the dead instance's original host.
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
            "the replacement announces its own host, not the dead instance's"
        );
        release_all(instances)?;
        assert!(
            provider.live().is_empty(),
            "release tears the replacement down"
        );
        Ok(())
    }

    #[test]
    fn a_replacement_that_cannot_be_made_retires_the_transport() -> Result<()> {
        // One offer, one instance, killed on the provider side: no offer remains
        // for a replacement, so the transport retires and points at no host.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = fleet_config(1, FillPolicy::Strict);
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let dead_id = instances[0]
            .guard
            .lock()
            .expect("the fleet guard lock is never poisoned")
            .as_ref()
            .expect("a live guard")
            .id()
            .clone();
        provider.destroy(&dead_id)?;

        let interrupt = AtomicBool::new(false);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(
            &provider, &store, &lock, &fleet, &instances, &interrupt, &emitter,
        );
        supervisor.tick(now_ms())?;

        assert!(
            instances[0].transport.live_host().is_none(),
            "a transport with no replacement retires"
        );
        release_all(instances)?;
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
        // Two offers, one instance: a Gone instance is normally replaced from
        // the second offer. With the interrupt already set, the tick reads it
        // first and does nothing — no paid replacement while the run winds
        // down.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = stub_fleet(1);
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        let original_host = instances[0].transport.live_host().expect("a live host");
        let dead_id = instances[0]
            .guard
            .lock()
            .expect("the fleet guard lock is never poisoned")
            .as_ref()
            .expect("a live guard")
            .id()
            .clone();
        provider.destroy(&dead_id)?;

        let interrupt = AtomicBool::new(true);
        let emitter = Mutex::new(None);
        let supervisor = Supervisor::new(
            &provider, &store, &lock, &fleet, &instances, &interrupt, &emitter,
        );
        supervisor.tick(now_ms())?;

        // Nothing new was provisioned, and the transport is untouched — still
        // naming the original host, not swapped or retired.
        assert!(provider.live().is_empty(), "a wind-down tick rents nothing");
        assert_eq!(
            instances[0].transport.live_host().as_deref(),
            Some(original_host.as_str()),
            "the transport is left as the wind-down found it"
        );
        release_all(instances)?;
        Ok(())
    }

    #[test]
    fn a_replacement_refused_for_budget_winds_down_rather_than_faulting() -> Result<()> {
        // A strict-fill fleet whose replacement cannot be paid for: exhaustion
        // must surface as the resumable interrupt, not a strict-fill fault.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = budgeted_fleet(sima_provider::Budget {
            max_spend: Some(sima_provider::Cost(50_000)),
            max_wall_clock: None,
        });
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        // A prior closed rental already past the cap, so any replacement
        // acquisition is refused for want of budget whatever the clock reads.
        store.put_spend(&closed_spend(&run, 120_000))?;
        let dead_id = instances[0]
            .guard
            .lock()
            .expect("the fleet guard lock is never poisoned")
            .as_ref()
            .expect("a live guard")
            .id()
            .clone();
        provider.destroy(&dead_id)?;

        let interrupt = AtomicBool::new(false);
        let events = capture_events(|emitter| {
            let supervisor = Supervisor::new(
                &provider, &store, &lock, &fleet, &instances, &interrupt, emitter,
            );
            // Drive the replacement directly: acquisition is refused for
            // budget, and the failure must classify as exhaustion.
            supervisor.replace(&instances[0], u64::MAX)
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
        assert!(instances[0].transport.live_host().is_none());
        release_all(instances)?;
        Ok(())
    }

    #[test]
    fn a_clean_supervisor_exit_clears_the_emitter_and_leaves_transports_alive() -> Result<()> {
        // The run's own wind-down tears the fleet down, so a clean supervisor
        // exit must leave the transports alive — but it must still release the
        // emitter clone, or the collector never joins.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = stub_fleet(1);
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        assert!(instances[0].transport.live_host().is_some());
        let observer = |_: &Record| {};
        thread::scope(|scope| -> Result<()> {
            let collector = Collector::spawn(scope, NullSink, &observer);
            let emitter = Mutex::new(Some(collector.emitter()));
            let guard = SupervisorExit {
                instances: &instances,
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
            instances[0].transport.live_host().is_some(),
            "a clean exit leaves the transports alive for the run's wind-down"
        );
        release_all(instances)?;
        Ok(())
    }

    #[test]
    fn a_supervisor_panic_clears_the_emitter_and_retires_the_transports() -> Result<()> {
        // A tick panic unwinds past the emitter-clearing line the old `run`
        // held; the collector would then wait forever on the leaked clone and
        // the fleet would never tear down. The exit guard clears the emitter
        // and retires the transports on the unwind alike.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let fleet = stub_fleet(1);
        let instances = acquire_fleet(
            &fleet,
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        assert!(instances[0].transport.live_host().is_some());
        let observer = |_: &Record| {};
        thread::scope(|scope| -> Result<()> {
            let collector = Collector::spawn(scope, NullSink, &observer);
            let emitter = Mutex::new(Some(collector.emitter()));
            let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = SupervisorExit {
                    instances: &instances,
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
            instances[0].transport.live_host().is_none(),
            "the panic retires the transports so no worker waits on a dead supervisor"
        );
        release_all(instances)?;
        Ok(())
    }
}
