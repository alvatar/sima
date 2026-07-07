//! The driver: it sets up the run, drives the worker pool, and finalizes.
//!
//! `run` owns the whole run: it registers the run, materializes the frontier,
//! spawns the pool inside a scope so workers borrow the store and executor
//! without `Arc`, feeds the queue by polling the task source, and finalizes on
//! success. A definitive candidate failure terminates the run without writing a
//! manifest, leaving the store clean and resumable; an infrastructure fault
//! returns `Err`. The two mirror the executor's own `Ok(Outcome)`/`Err` split
//! one level up. A caller's interrupt winds the run down the same way a
//! failure does — in-flight attempts drain and commit, queued tasks are
//! abandoned, no manifest — but reports [`RunOutcome::Interrupted`], the
//! resumable outcome.

use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use sima_contracts::{Executor, Generator, WorkerId};
use sima_core::Result;
use sima_model::{Environment, RunConfig, RunId, TaskKey};
use sima_store::Store;

use crate::config::ExecutionConfig;
use crate::control::RunControl;
use crate::coord::{Coord, Failure, Pending, Shared, Stop};
use crate::event::LifecycleEvent;
use crate::journal_sink::{JournalSink, emit};
use crate::static_batch::StaticBatch;
use crate::task_source::{RunnableTask, TaskSource};
use crate::watchdog::watchdog_loop;
use crate::worker::{WorkerContext, worker_loop};

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
/// environment, store state)`, and evaluates each task on a pool of `exec`
/// worker threads, committing successes through `store` and retrying transient
/// failures up to the cap. Returns [`RunOutcome::Finalized`] once every task is
/// committed and the manifest is written, or [`RunOutcome::Failed`] when a task
/// fails definitively, or [`RunOutcome::Interrupted`] when `control`'s
/// interrupt flag winds the run down — in the latter two cases no manifest is
/// written and the store stays resumable. `Err` signals an infrastructure
/// fault. `control` also carries the caller's event observer, invoked with
/// each lifecycle event in journal order.
pub fn run(
    store: &Store,
    config: &RunConfig,
    environment: &Environment,
    generator: &dyn Generator,
    executor: &(dyn Executor + Sync),
    exec: &ExecutionConfig,
    control: &RunControl,
) -> Result<RunOutcome> {
    // Register the run; its id is the config object's address.
    let run = store.create_run(config)?;
    // Every committed record references the params and environment objects, so
    // they must be durable before any commit; the spec objects are stored as
    // the frontier materializes.
    store.put(&config.params.to_bytes())?;
    store.put(&environment.to_bytes())?;

    let mut source = StaticBatch::new(generator, config, environment, store)?;
    let writer = store.journal_writer(&run)?;
    let coord = Coord::new();
    let coord = &coord;

    // Two nested scopes: the outer one holds the journal sink — a scoped
    // thread, so it can call the caller's borrowed observer — and the inner
    // one holds the pool, so workers borrow `&store` and the executor without
    // `Arc` and a worker panic re-raises at the pool's join, before any
    // finalize. The driver drives polling on this thread.
    let (outcome, journal) = thread::scope(|scope| {
        let sink = JournalSink::spawn(scope, writer, control.observer);
        let events = sink.sender();
        emit(
            &events,
            LifecycleEvent::RunStarted {
                run: run.to_string(),
                tasks: source.all_keys().len(),
            },
        );

        let drive_result = thread::scope(|pool| -> Result<DriveOutcome> {
            for w in 0..exec.workers {
                let ctx = WorkerContext {
                    coord,
                    store,
                    config,
                    executor,
                    exec,
                    events: events.clone(),
                };
                pool.spawn(move || worker_loop(WorkerId(w as u64), ctx));
            }
            {
                let events = events.clone();
                pool.spawn(move || watchdog_loop(coord, exec.attempt_timeout, &events));
            }
            drive(coord, &mut source, &events, control)
        });

        // Whatever the pool returned, decide the outcome, then always flush
        // and join the journal.
        let outcome = match drive_result {
            Ok(DriveOutcome::Finalize) => store.finalize_run(&run, source.all_keys()).map(|()| {
                emit(
                    &events,
                    LifecycleEvent::RunFinalized {
                        run: run.to_string(),
                        committed: source.all_keys().len(),
                    },
                );
                RunOutcome::Finalized { run }
            }),
            Ok(DriveOutcome::Fail(failure)) => {
                emit(
                    &events,
                    LifecycleEvent::RunFailed {
                        run: run.to_string(),
                        task: failure.task.to_string(),
                        reason: failure.reason.clone(),
                    },
                );
                Ok(RunOutcome::Failed {
                    task: failure.task,
                    reason: failure.reason,
                })
            }
            Ok(DriveOutcome::Interrupt) => {
                emit(
                    &events,
                    LifecycleEvent::RunInterrupted {
                        run: run.to_string(),
                    },
                );
                Ok(RunOutcome::Interrupted { run })
            }
            Err(fault) => Err(fault),
        };

        drop(events);
        (outcome, sink.shutdown())
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

/// What the driver decided the run should do once the pool went quiescent.
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
const INTERRUPT_POLL: Duration = Duration::from_millis(50);

/// Whether the pool is quiescent: no lease outstanding and, while the run is
/// healthy, nothing queued. A terminal state only waits for the in-flight
/// work to drain, and queued tasks are then abandoned.
fn quiescent(state: &Shared) -> bool {
    state.leases.is_empty() && (!matches!(state.stop, Stop::Running) || state.queue.is_empty())
}

/// Feeds the queue and waits for the pool: each time the pool goes quiescent
/// it polls the source — the first loop iteration is trivially quiescent, so
/// the first poll happens immediately, and a source that derives new tasks
/// from committed results hands them out at exactly those points — until a
/// poll yields nothing more (finalize) or an interrupt, a definitive failure,
/// or a fault ends the run. Every wakeup — a pool notification or the bounded
/// wait elapsing — re-checks `control`'s interrupt flag, so an interrupt is
/// observed within [`INTERRUPT_POLL`] and upgrades a healthy run to
/// `Interrupted`; the wind-down then rides the ordinary drain path.
fn drive(
    coord: &Coord,
    source: &mut dyn TaskSource,
    events: &Sender<LifecycleEvent>,
    control: &RunControl,
) -> Result<DriveOutcome> {
    loop {
        if control.interrupt.load(Ordering::Relaxed) {
            coord.interrupt();
        }
        // Observe quiescence and take the terminal reason, if any, in one
        // block-scoped lock region; without quiescence, park for one bounded
        // wait and loop around to re-check the interrupt flag.
        let stop = {
            let mut state = coord.lock();
            if !quiescent(&state) {
                drop(
                    coord
                        .idle
                        .wait_timeout(state, INTERRUPT_POLL)
                        .unwrap_or_else(|p| p.into_inner()),
                );
                continue;
            }
            if matches!(state.stop, Stop::Running) {
                None
            } else {
                // Take the terminal reason by value: the Error/Failure payload
                // moves out to be returned, and the Finished left in its place
                // is the signal that makes every worker's next_task exit.
                let stop = std::mem::replace(&mut state.stop, Stop::Finished);
                coord.idle.notify_all();
                Some(stop)
            }
        };
        if let Some(stop) = stop {
            return match stop {
                Stop::Failed(failure) => Ok(DriveOutcome::Fail(failure)),
                Stop::Fault(fault) => Err(fault),
                Stop::Interrupted => Ok(DriveOutcome::Interrupt),
                // Quiescent with the run already wound down: finalize.
                Stop::Finished => Ok(DriveOutcome::Finalize),
                // This arm is guarded by the `Running` check above.
                Stop::Running => unreachable!("the terminal branch excludes Running"),
            };
        }
        // Healthy and quiescent: poll. A poll fault set the terminal state;
        // the next iteration's wait drains in-flight work and the terminal
        // branch returns it. Nothing more means the run is done.
        match poll_source(coord, source) {
            None => {}
            Some(more) if more.is_empty() => {
                let mut state = coord.lock();
                state.stop = Stop::Finished;
                coord.idle.notify_all();
                return Ok(DriveOutcome::Finalize);
            }
            Some(more) => enqueue(coord, events, more),
        }
    }
}

/// Polls the source. A poll error becomes the run's terminal fault so the pool
/// winds down and the driver reports it; `None` signals that routing, `Some`
/// carries the polled tasks.
fn poll_source(coord: &Coord, source: &mut dyn TaskSource) -> Option<Vec<RunnableTask>> {
    match source.poll() {
        Ok(tasks) => Some(tasks),
        Err(e) => {
            let mut state = coord.lock();
            if matches!(state.stop, Stop::Running) {
                state.stop = Stop::Fault(e);
            }
            coord.idle.notify_all();
            None
        }
    }
}

/// Enqueues ready tasks at attempt 0 and wakes the workers. The `Queued` events
/// are journaled before the tasks become visible, so each task's events appear
/// in lifecycle order: a worker cannot lease a task before it is pushed, which
/// happens after the emits.
fn enqueue(coord: &Coord, events: &Sender<LifecycleEvent>, tasks: Vec<RunnableTask>) {
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
        emit(
            events,
            LifecycleEvent::Queued {
                task: p.key.to_string(),
            },
        );
    }
    let mut state = coord.lock();
    state.queue.extend(pending);
    coord.idle.notify_all();
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::mpsc;

    use sima_contracts::{StubBehavior, StubProgram};
    use sima_core::{Error, hash_bytes};
    use sima_model::{EnvironmentId, FormatId, Params, Spec, TaskIdentity};

    use super::*;

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
        RunnableTask { spec, identity }
    }

    /// A throwaway task key.
    fn a_key() -> TaskKey {
        TaskKey::from_hash(hash_bytes(b"preset terminal task"))
    }

    #[test]
    fn a_poll_error_winds_the_run_down_instead_of_hanging() {
        let coord = Coord::new();
        let (events, _rx) = mpsc::channel();
        // With no live workers, the first poll faults immediately.
        let result = drive(&coord, &mut FailingSource, &events, &RunControl::detached());
        assert!(matches!(result, Err(Error::Validation(_))));
        // The terminal state is what a real pool observes to drain: a poll
        // error that left `Running` in place would park the workers forever.
        assert!(!matches!(coord.lock().stop, Stop::Running));
    }

    #[test]
    fn an_empty_source_finalizes_immediately() {
        let coord = Coord::new();
        let (events, _rx) = mpsc::channel();
        // The first iteration is trivially quiescent, so the empty poll is the
        // whole run: one poll, then finalize.
        let result = drive(
            &coord,
            &mut ScriptedSource::default(),
            &events,
            &RunControl::detached(),
        );
        assert!(matches!(result, Ok(DriveOutcome::Finalize)));
        assert!(matches!(coord.lock().stop, Stop::Finished));
    }

    #[test]
    fn a_preset_failure_is_reported_and_the_pool_asked_to_exit() {
        let coord = Coord::new();
        coord.lock().stop = Stop::Failed(Failure {
            task: a_key(),
            reason: "candidate rejected".to_string(),
        });
        let (events, _rx) = mpsc::channel();
        let result = drive(
            &coord,
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
        assert!(matches!(coord.lock().stop, Stop::Finished));
    }

    #[test]
    fn a_preset_fault_is_returned_as_the_run_error() {
        let coord = Coord::new();
        coord.lock().stop = Stop::Fault(Error::Corruption("store broke".to_string()));
        let (events, _rx) = mpsc::channel();
        let result = drive(
            &coord,
            &mut ScriptedSource::default(),
            &events,
            &RunControl::detached(),
        );
        match result {
            Err(e) => assert_eq!(e.to_string(), "store corruption: store broke"),
            Ok(_) => panic!("expected the preset fault to be returned"),
        }
        assert!(matches!(coord.lock().stop, Stop::Finished));
    }

    #[test]
    fn enqueue_journals_each_task_and_publishes_it() {
        let coord = Coord::new();
        let (events, rx) = mpsc::channel();
        let tasks = vec![runnable(1), runnable(2)];
        let keys: Vec<TaskKey> = tasks.iter().map(|t| t.identity.key()).collect();
        enqueue(&coord, &events, tasks);
        drop(events);
        // Exactly one Queued event per task, in queue order.
        let queued: Vec<LifecycleEvent> = rx.into_iter().collect();
        assert_eq!(queued.len(), 2);
        for (event, key) in queued.iter().zip(&keys) {
            assert!(
                matches!(event, LifecycleEvent::Queued { task } if *task == key.to_string()),
                "expected a Queued event for {key}"
            );
        }
        // Both tasks sit in the queue at attempt 0 with their key precomputed.
        let state = coord.lock();
        assert_eq!(state.queue.len(), 2);
        for (pending, key) in state.queue.iter().zip(&keys) {
            assert_eq!(pending.key, *key);
            assert_eq!(pending.key, pending.task.identity.key());
            assert_eq!(pending.attempt, 0);
        }
    }

    #[test]
    fn enqueue_with_nothing_is_a_no_op() {
        let coord = Coord::new();
        let (events, rx) = mpsc::channel();
        enqueue(&coord, &events, Vec::new());
        drop(events);
        assert_eq!(rx.into_iter().count(), 0);
        assert!(coord.lock().queue.is_empty());
    }
}
