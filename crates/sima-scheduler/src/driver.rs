//! The driver: it sets up the run, drives the worker pool, and finalizes.
//!
//! `run` owns the whole run: it registers the run, materializes the frontier,
//! spawns the pool inside a scope so workers borrow the store and transport
//! without `Arc`, feeds the queue by polling the task source, and finalizes on
//! success. A definitive candidate failure terminates the run without writing a
//! manifest, leaving the store clean and resumable; an infrastructure fault
//! returns `Err`. The two mirror the executor's own `Ok(Outcome)`/`Err` split
//! one level up. A caller's interrupt winds the run down the same way a
//! failure does — in-flight attempts are abandoned and their worker processes
//! killed, queued tasks are abandoned, no manifest — but reports
//! [`RunOutcome::Interrupted`], the resumable outcome.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use sima_contracts::{DeviceBinding, DeviceClass, Generator, WorkerId};
use sima_core::{Error, Result};
use sima_model::{Environment, RunConfig, RunId, TaskKey};
use sima_store::Store;
use sima_trace::{Collector, Emitter, Event, Record};

use crate::config::ExecutionConfig;
use crate::control::RunControl;
use crate::coordinator::{Coordinator, Failure, Pending, RunState, Shared};
use crate::placement;
use crate::segment_chain::SegmentChain;
use crate::static_batch::StaticBatch;
use crate::task_source::{RunnableTask, TaskSource};
use crate::worker::{WorkerContext, worker_loop};
use crate::worker_pool::WorkerPool;

/// The result of a run.
#[derive(Debug)]
pub enum RunOutcome {
    /// Every task committed; the manifest is written.
    Finalized {
        /// The finalized run.
        run: RunId,
    },
    /// A task failed definitively; no manifest was written and the store is
    /// left clean and resumable.
    Failed {
        /// The task whose definitive failure ended the run.
        task: TaskKey,
        /// Why it failed.
        reason: String,
    },
    /// The caller interrupted the run: in-flight attempts drained and
    /// committed, queued tasks were abandoned, and no manifest was written,
    /// so the store is resumable.
    Interrupted {
        /// The interrupted run.
        run: RunId,
    },
}

/// Runs a search to completion.
///
/// Registers the run, materializes the runnable frontier from `(config,
/// environment, store state)`, and evaluates each task on the `pools`' worker
/// processes, committing successes through `store` and retrying transient
/// failures up to the cap. Each pool spawns its own slots through its own
/// transport; worker ids are global and sequential across pools, local first.
/// Returns [`RunOutcome::Finalized`] once every task is committed and the
/// manifest is written, or [`RunOutcome::Failed`] when a task fails
/// definitively, or [`RunOutcome::Interrupted`] when `control`'s interrupt flag
/// winds the run down — in the latter two cases no manifest is written and the
/// store stays resumable. `Err` signals an infrastructure fault. `control` also
/// carries the caller's event observer, invoked with each lifecycle event in
/// journal order.
pub fn run(
    store: &Store,
    config: &RunConfig,
    environment: &Environment,
    generator: &dyn Generator,
    pools: &[WorkerPool<'_>],
    exec: &ExecutionConfig,
    control: &RunControl,
) -> Result<RunOutcome> {
    // A run needs a worker somewhere: with every pool's slots empty, no one
    // would ever pull a task and the run would hang. This is the whole-run form
    // of the per-pool worker requirement, enforced where all pools are visible.
    if pools.iter().all(|pool| pool.slots.is_empty()) {
        return Err(Error::Validation(
            "a run needs at least one worker; every pool is empty".to_string(),
        ));
    }
    // Register the run; its id is the config object's address.
    let run = store.create_run(config)?;
    // Every committed record references the params and environment objects, so
    // they must be durable before any commit; the spec objects are stored as
    // the frontier materializes.
    store.put(&config.params.to_bytes())?;
    store.put(&environment.to_bytes())?;

    // The work-division quantity selects the source: a segment count means
    // per-candidate chains walked through committed state, its absence one
    // stateless task per candidate.
    let mut source: Box<dyn TaskSource + '_> = match config.segments {
        Some(_) => Box::new(SegmentChain::new(generator, config, environment, store)?),
        None => Box::new(StaticBatch::new(generator, config, environment, store)?),
    };
    let writer = store.journal_writer(&run)?;
    // Placement resumes from the store: a chain that already ran returns to
    // its class, and a slot naming a class the run no longer has rebinds when
    // a worker first pulls it.
    let mut chains = HashMap::new();
    for (chain, payload) in store.chain_bindings(&run)? {
        chains.insert(chain, placement::decode_class(&payload)?);
    }
    let coordinator = Coordinator::with_placement(eligible_classes(pools), chains);
    let coordinator = &coordinator;

    // Two nested scopes: the outer one holds the trace collector — a scoped
    // thread, so it can call the caller's borrowed observer — and the inner
    // one holds the pool, so workers borrow `&store` and the transport
    // without `Arc` and a worker panic re-raises at the pool's join, before
    // any finalize. The driver drives polling on this thread.
    let subscribers: [&(dyn Fn(&Record) + Sync); 1] = [control.observer];
    let (outcome, journal) = thread::scope(|scope| {
        let collector = Collector::spawn(scope, writer, &subscribers);
        let events = collector.emitter();
        events.emit(Event::RunStarted {
            run: run.to_string(),
            tasks: source.task_total(),
            committed: source.prior_committed(),
        });

        let drive_result = thread::scope(|scope| -> Result<DriveOutcome> {
            // Worker ids run global and sequential across pools, local first,
            // so every slot of every pool has a distinct id in the journal.
            let mut worker = 0u64;
            for pool in pools {
                for device in &pool.slots {
                    let ctx = WorkerContext {
                        coordinator,
                        store,
                        run,
                        config,
                        transport: pool.transport,
                        host: pool.host.clone(),
                        exec,
                        device: *device,
                        events: events.clone(),
                    };
                    scope.spawn(move || worker_loop(WorkerId(worker), ctx));
                    worker += 1;
                }
            }
            drive(coordinator, source.as_mut(), &events, control)
        });

        // Whatever the pool returned, decide the outcome, then always flush
        // and join the journal.
        let outcome = match drive_result {
            Ok(DriveOutcome::Finalize) => store.finalize_run(&run, source.all_keys()).map(|()| {
                events.emit(Event::RunFinalized {
                    run: run.to_string(),
                    committed: source.all_keys().len(),
                });
                RunOutcome::Finalized { run }
            }),
            Ok(DriveOutcome::Fail(failure)) => {
                events.emit(Event::RunFailed {
                    run: run.to_string(),
                    task: failure.task.to_string(),
                    reason: failure.reason.clone(),
                });
                Ok(RunOutcome::Failed {
                    task: failure.task,
                    reason: failure.reason,
                })
            }
            Ok(DriveOutcome::Interrupt) => {
                events.emit(Event::RunInterrupted {
                    run: run.to_string(),
                });
                Ok(RunOutcome::Interrupted { run })
            }
            Err(fault) => Err(fault),
        };

        drop(events);
        (outcome, collector.shutdown())
    });

    // The domain outcome wins: a definitive candidate failure or an interrupt
    // is returned even when the journal degraded, because the journal is
    // observational and the same fault resurfaces on the next run that
    // finalizes over this store. Only a Finalized outcome yields to the
    // journal fault — there it is the sole signal anything went wrong.
    let outcome = outcome?;
    if matches!(outcome, RunOutcome::Finalized { .. }) {
        journal?;
    }
    Ok(outcome)
}

/// The device each slot of one pool computes on, in slot order — the pool
/// assembly the pipeline calls per execution entry, local or remote.
///
/// One slot per (entry, worker of that entry); an entry set with no device
/// entries gets `exec.workers` unbound slots, leaving every child on the
/// backend's default selection. Slots of one entry round-robin over the class's
/// cards, so several workers share a card only once every card has one.
pub fn worker_slots(exec: &ExecutionConfig) -> Vec<Option<DeviceBinding>> {
    if exec.devices.is_empty() {
        return vec![None; exec.workers];
    }
    let mut slots = Vec::with_capacity(exec.workers);
    for entry in &exec.devices {
        for slot in 0..entry.workers {
            slots.push(Some(DeviceBinding {
                vendor_id: entry.class.vendor_id,
                device_id: entry.class.device_id,
                // An entry carries at least one card, which `ExecutionConfig`
                // validates, so the remainder is over a positive count.
                member: (slot as u32) % entry.members,
            }));
        }
    }
    slots
}

/// The device classes the run's pools carry, distinct, in pool-then-slot
/// order. This is placement's eligibility set: a class is global (decision
/// C3), so a class present on any pool is a class the run has, and a chain
/// bound to it runs on whichever pool holds it. For a single pool this is
/// exactly that pool's classes in slot order.
fn eligible_classes(pools: &[WorkerPool<'_>]) -> Vec<DeviceClass> {
    let mut classes = Vec::new();
    for pool in pools {
        for slot in pool.slots.iter().flatten() {
            let class = slot.class();
            if !classes.contains(&class) {
                classes.push(class);
            }
        }
    }
    classes
}

/// What the driver decided the run should do once its gate resolved.
enum DriveOutcome {
    /// Every task committed; finalize the run.
    Finalize,
    /// A task failed definitively; report it.
    Fail(Failure),
    /// The caller's interrupt wound the run down; report it.
    Interrupt,
}

/// How long the driver parks between wakeups: an upper bound on how long a
/// set interrupt flag goes unobserved, since the pool's own notifications
/// also wake the driver.
///
/// Only the driver thread takes this bounded wait; workers park on plain
/// condvar waits with no timeout. At 50 ms the cost is about 20 uncontended
/// lock acquisitions per second per run process — one per wakeup, to
/// re-check quiescence and the interrupt flag under the shared lock. The
/// poll is what carries a signal into the condvar: a signal handler may not
/// call `notify_all` (that function is not async-signal-safe), and the
/// alternative — a waker-registration protocol across the CLI/scheduler
/// boundary — is disproportionate for a flag this cheap to poll. The
/// constant can be raised (for example to 250 ms) if the wakeup churn ever
/// matters, trading interrupt latency up to that bound for fewer
/// acquisitions.
const INTERRUPT_POLL: Duration = Duration::from_millis(50);

/// Whether a wound-down pool has drained: no lease outstanding. Queued tasks
/// are then abandoned. Gates only the terminal states; a healthy run is
/// driven by the poll gate instead.
fn drained(shared: &Shared) -> bool {
    shared.leases.is_empty()
}

/// What one locked look at the shared state told the driver to do next.
enum Gate {
    /// Nothing to do until the next wakeup.
    Wait,
    /// The run wound down and the pool drained; the taken terminal reason.
    Terminal(RunState),
    /// Poll the source: the queue is empty and a lease release moved
    /// `settled` past the last poll. `idle` is whether the pool also holds
    /// no leases; `settled` is the release count the poll is current as of.
    Poll { idle: bool, settled: u64 },
}

/// Feeds the queue and waits for the pool, deciding the run's outcome.
/// Three ideas govern the loop:
///
/// - **Poll gate**: while the run is healthy, the source is polled when the
///   queue is empty and a lease has been released since the last poll. A
///   source reads committed records to decide what is runnable next, and
///   only a settling worker changes them, so releases are the poll trigger.
/// - **Finalize condition**: an empty poll at an idle pool — no queue, no
///   leases — means no task remains, and the run finalizes.
/// - **Terminal drain**: a terminal state (a caller interrupt, a definitive
///   failure, a fault) waits for the in-flight leases to drain, abandoning
///   queued tasks.
fn drive(
    coordinator: &Coordinator,
    source: &mut dyn TaskSource,
    events: &Emitter,
    control: &RunControl,
) -> Result<DriveOutcome> {
    // The release count the last poll was current as of; `None` before the
    // first poll, so it fires unconditionally.
    let mut polled_at: Option<u64> = None;
    loop {
        // Every wakeup — a pool notification or the bounded wait elapsing —
        // passes through here, so a set interrupt flag is observed within
        // `INTERRUPT_POLL`. It upgrades a healthy run to `Interrupted`, and
        // the wind-down rides the ordinary drain path below.
        if control.interrupt.load(Ordering::Relaxed) {
            coordinator.interrupt();
        }
        // Decide the next action in one block-scoped lock region; a poll runs
        // outside it. Without anything to do, park for one bounded wait —
        // under the same lock acquisition, so a notification between the
        // decision and the park is not missed — and loop around to re-check
        // the interrupt flag.
        let gate = {
            let mut shared = coordinator.lock();
            let decision = if !matches!(shared.state, RunState::Running) {
                if drained(&shared) {
                    // Take the terminal reason by value: the Error/Failure
                    // payload moves out to be returned, and the Finished left
                    // in its place is the signal that makes every worker's
                    // next_task exit.
                    let terminal = std::mem::replace(&mut shared.state, RunState::Finished);
                    coordinator.state_changed.notify_all();
                    Gate::Terminal(terminal)
                } else {
                    Gate::Wait
                }
            } else if shared.queue.is_empty() && polled_at != Some(shared.settled) {
                // The gate ignores outstanding leases, so a chain task's
                // successor is handed out the moment its own predecessor
                // commits, while other tasks still run.
                Gate::Poll {
                    idle: shared.leases.is_empty(),
                    settled: shared.settled,
                }
            } else {
                Gate::Wait
            };
            if matches!(decision, Gate::Wait) {
                drop(
                    coordinator
                        .state_changed
                        .wait_timeout(shared, INTERRUPT_POLL)
                        .unwrap_or_else(|p| p.into_inner()),
                );
                continue;
            }
            decision
        };
        match gate {
            Gate::Terminal(terminal) => {
                return match terminal {
                    RunState::Failed(failure) => Ok(DriveOutcome::Fail(failure)),
                    RunState::Fault(fault) => Err(fault),
                    RunState::Interrupted => Ok(DriveOutcome::Interrupt),
                    // Drained with the run already wound down: finalize.
                    RunState::Finished => Ok(DriveOutcome::Finalize),
                    // This arm is guarded by the `Running` check above.
                    RunState::Running => unreachable!("the terminal branch excludes Running"),
                };
            }
            Gate::Poll { idle, settled } => {
                polled_at = Some(settled);
                // A poll fault set the terminal state; the next iteration
                // drains the in-flight work and the terminal branch returns
                // it. An empty poll finalizes only at an idle pool; with
                // leases outstanding, their releases trigger the next poll.
                match poll_source(coordinator, source) {
                    None => {}
                    Some(more) if more.is_empty() => {
                        if idle {
                            let mut shared = coordinator.lock();
                            shared.state = RunState::Finished;
                            coordinator.state_changed.notify_all();
                            return Ok(DriveOutcome::Finalize);
                        }
                    }
                    Some(more) => enqueue(coordinator, events, more),
                }
            }
            Gate::Wait => unreachable!("a Wait decision parks and continues above"),
        }
    }
}

/// Polls the source. A poll error becomes the run's terminal fault so the pool
/// winds down and the driver reports it; `None` signals that routing, `Some`
/// carries the polled tasks.
fn poll_source(
    coordinator: &Coordinator,
    source: &mut dyn TaskSource,
) -> Option<Vec<RunnableTask>> {
    match source.poll() {
        Ok(tasks) => Some(tasks),
        Err(e) => {
            let mut shared = coordinator.lock();
            if matches!(shared.state, RunState::Running) {
                shared.state = RunState::Fault(e);
            }
            coordinator.state_changed.notify_all();
            None
        }
    }
}

/// Enqueues ready tasks at attempt 0 and wakes the workers. The `Queued` events
/// are journaled before the tasks become visible, so each task's events appear
/// in lifecycle order: a worker cannot lease a task before it is pushed, which
/// happens after the emits.
fn enqueue(coordinator: &Coordinator, events: &Emitter, tasks: Vec<RunnableTask>) {
    if tasks.is_empty() {
        return;
    }
    let pending: Vec<Pending> = tasks
        .into_iter()
        .map(|task| Pending {
            key: task.identity.key(),
            task,
            attempt: 0,
        })
        .collect();
    for p in &pending {
        events.emit(Event::Queued {
            task: p.key.to_string(),
        });
    }
    let mut shared = coordinator.lock();
    shared.queue.extend(pending);
    coordinator.state_changed.notify_all();
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::mpsc;

    use sima_contracts::DeviceClass;
    use sima_core::{Error, hash_bytes};
    use sima_domains::{StubBehavior, StubProgram};
    use sima_model::{EnvironmentId, FormatId, Params, Spec, TaskIdentity};

    use super::*;
    use crate::config::DeviceEntry;

    /// A resolved entry: `workers` workers over a class of `members` cards.
    fn entry(vendor_id: u32, workers: usize, members: u32) -> DeviceEntry {
        DeviceEntry {
            class: DeviceClass {
                vendor_id,
                device_id: 1,
            },
            name: format!("device {vendor_id:04x}"),
            workers,
            members,
        }
    }

    /// The slots as `(vendor id, member)` pairs, the part these tests pin.
    fn slot_shape(exec: &ExecutionConfig) -> Vec<Option<(u32, u32)>> {
        worker_slots(exec)
            .into_iter()
            .map(|slot| slot.map(|binding| (binding.vendor_id, binding.member)))
            .collect()
    }

    #[test]
    fn slots_round_robin_over_each_class_s_cards() -> Result<()> {
        // Three workers over a two-card class, then one worker over a
        // single-card class: the first class's third worker wraps back to its
        // first card, and each class counts its cards from zero.
        let exec = ExecutionConfig::with_devices(
            vec![entry(0x10de, 3, 2), entry(0x8086, 1, 1)],
            1,
            Duration::MAX,
            Duration::MAX,
            None,
        )?;
        assert_eq!(
            slot_shape(&exec),
            vec![
                Some((0x10de, 0)),
                Some((0x10de, 1)),
                Some((0x10de, 0)),
                Some((0x8086, 0)),
            ]
        );
        Ok(())
    }

    #[test]
    fn one_worker_per_card_shares_no_card() -> Result<()> {
        let exec = ExecutionConfig::with_devices(
            vec![entry(0x10de, 4, 4)],
            1,
            Duration::MAX,
            Duration::MAX,
            None,
        )?;
        assert_eq!(
            slot_shape(&exec),
            vec![
                Some((0x10de, 0)),
                Some((0x10de, 1)),
                Some((0x10de, 2)),
                Some((0x10de, 3)),
            ]
        );
        Ok(())
    }

    #[test]
    fn a_run_naming_no_device_leaves_every_slot_unbound() -> Result<()> {
        // The single implicit class: every child takes the backend's own
        // choice, and the pool is the plain worker count.
        let exec = ExecutionConfig::new(3, 1, Duration::MAX, Duration::MAX, None)?;
        assert_eq!(slot_shape(&exec), vec![None, None, None]);
        Ok(())
    }

    /// A task source whose poll always faults, standing in for a fallible
    /// dynamic source. Its key set is empty: it never yields runnable work.
    struct FailingSource;

    impl TaskSource for FailingSource {
        fn poll(&mut self) -> Result<Vec<RunnableTask>> {
            Err(Error::Validation("source poll failed".to_string()))
        }

        fn all_keys(&self) -> &[TaskKey] {
            &[]
        }

        fn task_total(&self) -> usize {
            0
        }

        fn prior_committed(&self) -> usize {
            0
        }
    }

    /// A task source that hands out scripted batches, one per poll, and yields
    /// nothing once the script is exhausted.
    #[derive(Default)]
    struct ScriptedSource {
        polls: VecDeque<Vec<RunnableTask>>,
    }

    impl TaskSource for ScriptedSource {
        fn poll(&mut self) -> Result<Vec<RunnableTask>> {
            Ok(self.polls.pop_front().unwrap_or_default())
        }

        fn all_keys(&self) -> &[TaskKey] {
            &[]
        }

        fn task_total(&self) -> usize {
            0
        }

        fn prior_committed(&self) -> usize {
            0
        }
    }

    /// A runnable task over a stub `Succeed` program, distinguished by `nonce`.
    fn runnable(nonce: u64) -> RunnableTask {
        let spec = Spec {
            format: FormatId::new("stub.v1").expect("format id"),
            bytes: StubProgram {
                behavior: StubBehavior::Succeed,
                nonce,
            }
            .to_bytes(),
        };
        let identity = TaskIdentity {
            spec: spec.id(),
            params: Params {
                bytes: vec![1, 2, 3],
            }
            .id(),
            seed: nonce,
            environment: EnvironmentId::from_hash(hash_bytes(b"unit-test environment")),
            input_state: None,
        };
        RunnableTask {
            spec,
            identity,
            chain: None,
        }
    }

    /// A throwaway task key.
    fn a_key() -> TaskKey {
        TaskKey::from_hash(hash_bytes(b"preset terminal task"))
    }

    #[test]
    fn a_poll_error_winds_the_run_down_instead_of_hanging() {
        let coordinator = Coordinator::new();
        let (tx, _rx) = mpsc::channel();
        let events = Emitter::from(tx);
        // With no live workers, the first poll faults immediately.
        let result = drive(
            &coordinator,
            &mut FailingSource,
            &events,
            &RunControl::detached(),
        );
        assert!(matches!(result, Err(Error::Validation(_))));
        // The terminal state is what a real pool observes to drain: a poll
        // error that left `Running` in place would park the workers forever.
        assert!(!matches!(coordinator.lock().state, RunState::Running));
    }

    #[test]
    fn an_empty_source_finalizes_immediately() {
        let coordinator = Coordinator::new();
        let (tx, _rx) = mpsc::channel();
        let events = Emitter::from(tx);
        // Nothing has been polled yet, so the first poll fires immediately;
        // it comes back empty at an idle pool, which is the whole run:
        // one poll, then finalize.
        let result = drive(
            &coordinator,
            &mut ScriptedSource::default(),
            &events,
            &RunControl::detached(),
        );
        assert!(matches!(result, Ok(DriveOutcome::Finalize)));
        assert!(matches!(coordinator.lock().state, RunState::Finished));
    }

    #[test]
    fn a_preset_failure_is_reported_and_the_pool_asked_to_exit() {
        let coordinator = Coordinator::new();
        coordinator.lock().state = RunState::Failed(Failure {
            task: a_key(),
            reason: "candidate rejected".to_string(),
        });
        let (tx, _rx) = mpsc::channel();
        let events = Emitter::from(tx);
        let result = drive(
            &coordinator,
            &mut ScriptedSource::default(),
            &events,
            &RunControl::detached(),
        );
        match result {
            Ok(DriveOutcome::Fail(failure)) => {
                assert_eq!(failure.task, a_key());
                assert_eq!(failure.reason, "candidate rejected");
            }
            _ => panic!("expected the preset failure to be reported"),
        }
        // Finished is left in the state's place: the pool's exit signal.
        assert!(matches!(coordinator.lock().state, RunState::Finished));
    }

    #[test]
    fn a_preset_fault_is_returned_as_the_run_error() {
        let coordinator = Coordinator::new();
        coordinator.lock().state = RunState::Fault(Error::Corruption("store broke".to_string()));
        let (tx, _rx) = mpsc::channel();
        let events = Emitter::from(tx);
        let result = drive(
            &coordinator,
            &mut ScriptedSource::default(),
            &events,
            &RunControl::detached(),
        );
        match result {
            Err(e) => assert_eq!(e.to_string(), "store corruption: store broke"),
            Ok(_) => panic!("expected the preset fault to be returned"),
        }
        assert!(matches!(coordinator.lock().state, RunState::Finished));
    }

    /// Drains `rx` until a `Queued` event for `key` arrives, bounded by
    /// `deadline`; returns whether it arrived.
    fn saw_queued(rx: &mpsc::Receiver<Event>, key: &TaskKey, deadline: Duration) -> bool {
        let end = std::time::Instant::now() + deadline;
        loop {
            let Some(remaining) = end.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            match rx.recv_timeout(remaining) {
                Ok(Event::Queued { task }) if task == key.to_string() => return true,
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    }

    #[test]
    fn a_successor_is_handed_out_while_another_lease_is_outstanding() {
        // A chain source derives new work from commits; each chain must
        // advance the moment its own predecessor commits, without waiting for
        // the whole pool to drain. Scripted: poll 1 yields tasks A and C,
        // poll 2 yields B — modeling C's successor. The fake worker resolves
        // C while holding A's lease; B must be queued before A releases,
        // which only a poll gate that runs with leases outstanding can do.
        let coordinator = Coordinator::new();
        let (tx, rx) = mpsc::channel();
        let events = Emitter::from(tx);
        let task_a = runnable(1);
        let task_c = runnable(3);
        let task_b = runnable(2);
        let key_b = task_b.identity.key();
        let mut source = ScriptedSource {
            polls: VecDeque::from([vec![task_a, task_c], vec![task_b]]),
        };
        let queued_during_lease = std::sync::atomic::AtomicBool::new(false);
        let outcome = thread::scope(|scope| {
            // The receiver moves into the worker thread; the rest is borrowed.
            let coordinator = &coordinator;
            let queued_during_lease = &queued_during_lease;
            scope.spawn(move || {
                // FIFO queue: the first lease is A, the second is C.
                let a = coordinator.next_task(None).expect("task A");
                let c = coordinator.next_task(None).expect("task C");
                coordinator.resolve(c.pending.key);
                // C's commit derives B; it must arrive while A is still held.
                if saw_queued(&rx, &key_b, Duration::from_secs(5)) {
                    queued_during_lease.store(true, Ordering::Relaxed);
                }
                coordinator.resolve(a.pending.key);
                // Drain whatever else the driver hands out, so the run can
                // finalize in both the passing and the failing sequence.
                while let Some(t) = coordinator.next_task(None) {
                    coordinator.resolve(t.pending.key);
                }
            });
            drive(coordinator, &mut source, &events, &RunControl::detached())
        });
        assert!(matches!(outcome, Ok(DriveOutcome::Finalize)));
        assert!(
            queued_during_lease.load(Ordering::Relaxed),
            "task B must be queued while task A's lease is outstanding"
        );
    }

    #[test]
    fn an_empty_poll_with_a_lease_outstanding_does_not_finalize() {
        // Finalize requires an empty poll and an idle pool. The fake worker
        // holds the sole task's lease across an empty poll; drive must wait
        // for the release instead of finalizing under the lease.
        let coordinator = Coordinator::new();
        let (tx, _rx) = mpsc::channel();
        let events = Emitter::from(tx);
        let task_a = runnable(1);
        let mut source = ScriptedSource {
            polls: VecDeque::from([vec![task_a]]),
        };
        let resolved = std::sync::atomic::AtomicBool::new(false);
        let outcome = thread::scope(|scope| {
            scope.spawn(|| {
                let a = coordinator.next_task(None).expect("task A");
                // Long enough for several empty polls under the 50 ms cadence.
                thread::sleep(Duration::from_millis(300));
                resolved.store(true, Ordering::Relaxed);
                coordinator.resolve(a.pending.key);
                while let Some(t) = coordinator.next_task(None) {
                    coordinator.resolve(t.pending.key);
                }
            });
            drive(&coordinator, &mut source, &events, &RunControl::detached())
        });
        assert!(matches!(outcome, Ok(DriveOutcome::Finalize)));
        assert!(
            resolved.load(Ordering::Relaxed),
            "drive returned before the outstanding lease was released"
        );
    }

    #[test]
    fn a_poll_fault_under_an_outstanding_lease_drains_and_reports() {
        // The second poll faults while the fake worker still holds task A.
        // The run must wind down through the ordinary drain — wait for the
        // lease, then report the fault — instead of hanging or returning
        // under the lease.
        struct FaultAfterFirst {
            first: Option<Vec<RunnableTask>>,
        }
        impl TaskSource for FaultAfterFirst {
            fn poll(&mut self) -> Result<Vec<RunnableTask>> {
                match self.first.take() {
                    Some(tasks) => Ok(tasks),
                    None => Err(Error::Validation("source poll failed".to_string())),
                }
            }
            fn all_keys(&self) -> &[TaskKey] {
                &[]
            }
            fn task_total(&self) -> usize {
                0
            }
            fn prior_committed(&self) -> usize {
                0
            }
        }
        let coordinator = Coordinator::new();
        let (tx, _rx) = mpsc::channel();
        let events = Emitter::from(tx);
        let mut source = FaultAfterFirst {
            first: Some(vec![runnable(1)]),
        };
        let resolved = std::sync::atomic::AtomicBool::new(false);
        let outcome = thread::scope(|scope| {
            scope.spawn(|| {
                let a = coordinator.next_task(None).expect("task A");
                thread::sleep(Duration::from_millis(200));
                resolved.store(true, Ordering::Relaxed);
                coordinator.resolve(a.pending.key);
                while let Some(t) = coordinator.next_task(None) {
                    coordinator.resolve(t.pending.key);
                }
            });
            drive(&coordinator, &mut source, &events, &RunControl::detached())
        });
        assert!(matches!(outcome, Err(Error::Validation(_))));
        assert!(
            resolved.load(Ordering::Relaxed),
            "the fault must drain the in-flight lease before reporting"
        );
    }

    #[test]
    fn an_interrupt_mid_chain_drains_and_reports_interrupted() {
        // The interrupt lands while task A is leased and more chain work
        // remains scripted. The in-flight attempt drains; the pending
        // successor is never handed out; the run reports Interrupted.
        let coordinator = Coordinator::new();
        let (tx, _rx) = mpsc::channel();
        let events = Emitter::from(tx);
        // A local flag: `detached()`'s flag is a process-wide static shared
        // by every detached control, so setting it would poison other tests.
        let interrupt = std::sync::atomic::AtomicBool::new(false);
        let control = RunControl {
            observer: &|_| {},
            interrupt: &interrupt,
        };
        let mut source = ScriptedSource {
            polls: VecDeque::from([vec![runnable(1)], vec![runnable(2)]]),
        };
        let outcome = thread::scope(|scope| {
            scope.spawn(|| {
                let a = coordinator.next_task(None).expect("task A");
                control.interrupt.store(true, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(200));
                coordinator.resolve(a.pending.key);
                while let Some(t) = coordinator.next_task(None) {
                    coordinator.resolve(t.pending.key);
                }
            });
            drive(&coordinator, &mut source, &events, &control)
        });
        assert!(matches!(outcome, Ok(DriveOutcome::Interrupt)));
    }

    #[test]
    fn enqueue_journals_each_task_and_publishes_it() {
        let coordinator = Coordinator::new();
        let (tx, rx) = mpsc::channel();
        let events = Emitter::from(tx);
        let tasks = vec![runnable(1), runnable(2)];
        let keys: Vec<TaskKey> = tasks.iter().map(|t| t.identity.key()).collect();
        enqueue(&coordinator, &events, tasks);
        drop(events);
        // Exactly one Queued event per task, in queue order.
        let queued: Vec<Event> = rx.into_iter().collect();
        assert_eq!(queued.len(), 2);
        for (event, key) in queued.iter().zip(&keys) {
            assert!(
                matches!(event, Event::Queued { task } if *task == key.to_string()),
                "expected a Queued event for {key}"
            );
        }
        // Both tasks sit in the queue at attempt 0 with their key precomputed.
        let shared = coordinator.lock();
        assert_eq!(shared.queue.len(), 2);
        for (pending, key) in shared.queue.iter().zip(&keys) {
            assert_eq!(pending.key, *key);
            assert_eq!(pending.key, pending.task.identity.key());
            assert_eq!(pending.attempt, 0);
        }
    }

    #[test]
    fn enqueue_with_nothing_is_a_no_op() {
        let coordinator = Coordinator::new();
        let (tx, rx) = mpsc::channel();
        let events = Emitter::from(tx);
        enqueue(&coordinator, &events, Vec::new());
        drop(events);
        assert_eq!(rx.into_iter().count(), 0);
        assert!(coordinator.lock().queue.is_empty());
    }
}
