//! The supervisor that keeps a run's rented machines within budget and
//! replaces the ones that vanish.
//!
//! One thread per run, ticking on a heartbeat: it assesses the run's spend and
//! wall clock against the budget, then every machine's health. A machine the
//! control plane has lost is replaced and its transport re-targeted; a
//! replacement the budget cannot afford winds the run down instead of faulting
//! it, because a run that has spent its ceiling has finished spending, not
//! broken.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use sima_core::Result;
use sima_provider::{
    AcquireLimits, Budget, Exhaustion, IncidentKind, InstanceGuard, InstanceStatus, Objective,
    Provider, Verdict, acquire, assess, now_ms, record_incident,
};
use sima_scheduler::Event;
use sima_store::{Rental as RentalRole, RunLock, Store};
use sima_trace::Emitter;
use sima_transport::{SpawnOutcome, WorkerTransport};

use sima_contracts::DeviceBinding;

use crate::config::FillPolicy;
use crate::rental::acquire::{
    RentalGroup, RentedHost, budget_exhausted, endpoint_target, never_cancelled,
};

/// A rented pool's transport, wired to stop the run's supervisor when a spawn
/// fails.
///
/// A worker that cannot spawn its child holds no task, so it faults the run
/// without journaling anything: there is no task event, and no run-level one
/// either. The supervisor holds a clone of the run's emitter and drops it when
/// it stops, and the run's collector joins only once every clone is gone —
/// so without a signal here the supervisor would hold its clone until the
/// scheduler returned, and the scheduler would not return until the collector
/// joined. The failing spawn therefore raises the stop on its way out.
///
/// Only a rented pool needs it: it is the only kind with a supervisor beside
/// it.
pub(crate) struct StopOnSpawnFailure<'a> {
    /// The transport this stands in front of.
    pub(crate) inner: &'a dyn WorkerTransport,
    /// Raised on a spawn failure, so the supervisor winds down.
    pub(crate) stop: &'a StopSignal,
    /// Set with it, so a replacement acquisition already in flight is abandoned
    /// rather than finishing for a run that is over.
    pub(crate) cancel: &'a AtomicBool,
}

impl WorkerTransport for StopOnSpawnFailure<'_> {
    fn spawn(
        &self,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<SpawnOutcome> {
        self.inner.spawn(worker, device, events).inspect_err(|_| {
            self.cancel.store(true, Ordering::Relaxed);
            self.stop.raise();
        })
    }
}

/// The supervisor's heartbeat period. Rental health is a low-frequency concern
/// and the poll is cheap, so a fixed period suffices; no config knob.
const HEARTBEAT: Duration = Duration::from_secs(10);

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
    pub(crate) fn wait(&self, timeout: Duration) -> bool {
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
    /// journal boundary every other event does.
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
        // A replacement is a fresh machine being asked for, so its clock
        // starts here; nothing probes it, the transport reaches it.
        let limits = AcquireLimits {
            usable_by: Instant::now() + group.spec.ready_timeout,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::thread;

    use sima_model::{FormatId, RunId};
    use sima_provider::Cost;
    use sima_provider::stub::StubProvider;
    use sima_scheduler::Record;
    use sima_trace::{Collector, DurableSink};

    use super::*;
    use crate::rental::acquire::{acquire_hosts, release_all};
    use crate::rental::fixtures::{
        acquisition_env, deviceless_probe, exec, offer, one_group, rental, spec,
    };

    /// A journal sink that discards every line: tests capture events through
    /// the collector's observer, not its durable output.
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
                None,
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
            None,
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
            None,
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
            None,
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
            None,
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
