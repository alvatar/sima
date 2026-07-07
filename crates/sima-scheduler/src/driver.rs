//! The driver: it sets up the run, drives the worker pool, and finalizes.
//!
//! `run` owns the whole run: it registers the run, materializes the frontier,
//! spawns the pool inside a scope so workers borrow the store and executor
//! without `Arc`, feeds the queue by polling the task source, and finalizes on
//! success. A definitive candidate failure terminates the run without writing a
//! manifest, leaving the store clean and resumable; an infrastructure fault
//! returns `Err`. The two mirror the executor's own `Ok(Outcome)`/`Err` split
//! one level up.

use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::Sender;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::thread;

use sima_contracts::{Executor, Generator, WorkerId};
use sima_core::{Error, Result};
use sima_model::{Environment, RunConfig, RunId, TaskKey};
use sima_store::Store;

use crate::config::ExecutionConfig;
use crate::event::LifecycleEvent;
use crate::journal_sink::{JournalSink, emit};
use crate::lease::Lease;
use crate::static_batch::StaticBatch;
use crate::task_source::{RunnableTask, TaskSource};
use crate::watchdog::watchdog_loop;
use crate::worker::{WorkerContext, worker_loop};

/// The result of a run.
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
}

/// A task waiting in the ready queue: the runnable task and the attempt it is
/// queued for.
pub(crate) struct Pending {
    pub(crate) task: RunnableTask,
    /// The task key, equal to `task.identity.key()`, computed once where the
    /// `Pending` is built so no lifecycle step recomputes it under the lock.
    pub(crate) key: TaskKey,
    pub(crate) attempt: u32,
}

/// A definitive candidate failure: the task that could not produce a result,
/// and why.
#[derive(Clone)]
pub(crate) struct Failure {
    pub(crate) task: TaskKey,
    pub(crate) reason: String,
}

/// Why the run is winding down. `Running` is the steady state; every other
/// variant is terminal and makes each worker stop pulling new work.
pub(crate) enum Stop {
    /// Work is proceeding.
    Running,
    /// A definitive candidate failure; the run returns [`RunOutcome::Failed`].
    Failed(Failure),
    /// An infrastructure fault; the run returns `Err`.
    Fault(Error),
    /// The driver saw the work through and asked the pool to exit.
    Finished,
}

/// The mutable state every scheduler thread shares.
pub(crate) struct Shared {
    /// FIFO of tasks ready to lease.
    pub(crate) queue: VecDeque<Pending>,
    /// The in-memory lease table, keyed by task.
    pub(crate) leases: HashMap<TaskKey, Lease>,
    /// Tasks leased and not yet resolved.
    pub(crate) in_flight: usize,
    /// The run's wind-down state.
    pub(crate) stop: Stop,
}

/// The shared state plus the condition every thread waits on.
pub(crate) struct Coord {
    pub(crate) state: Mutex<Shared>,
    pub(crate) idle: Condvar,
}

impl Coord {
    /// Locks the shared state, recovering a poisoned lock. Poisoning would mean
    /// a thread panicked holding the lock; the scheduler panics only inside the
    /// worker's `catch_unwind`, which holds no lock, so the lock is not
    /// poisoned in practice.
    pub(crate) fn lock(&self) -> MutexGuard<'_, Shared> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// Runs a search to completion.
///
/// Registers the run, materializes the runnable frontier from `(config,
/// environment, store state)`, and evaluates each task on a pool of `exec`
/// worker threads, committing successes through `store` and retrying transient
/// failures up to the cap. Returns [`RunOutcome::Finalized`] once every task is
/// committed and the manifest is written, or [`RunOutcome::Failed`] when a task
/// fails definitively — in which case no manifest is written and the store
/// stays resumable. `Err` signals an infrastructure fault.
pub fn run(
    store: &Store,
    config: &RunConfig,
    environment: &Environment,
    generator: &dyn Generator,
    executor: &(dyn Executor + Sync),
    exec: &ExecutionConfig,
) -> Result<RunOutcome> {
    // Register the run; its id is the config object's address.
    let run = store.create_run(config)?;
    // Every committed record references the params and environment objects, so
    // they must be durable before any commit; the spec objects are stored as
    // the frontier materializes.
    store.put(&config.params.to_bytes())?;
    store.put(&environment.to_bytes())?;

    let mut source = StaticBatch::new(generator, config, environment, store)?;
    let sink = JournalSink::spawn(store.journal_writer(&run)?);
    let events = sink.sender();
    emit(
        &events,
        LifecycleEvent::RunStarted {
            run: run.to_string(),
            tasks: source.all_keys().len(),
        },
    );

    let coord = Coord {
        state: Mutex::new(Shared {
            queue: VecDeque::new(),
            leases: HashMap::new(),
            in_flight: 0,
            stop: Stop::Running,
        }),
        idle: Condvar::new(),
    };

    // The pool runs inside a scope so workers borrow `&store` and the executor
    // without `Arc`; the driver drives polling on this thread.
    let coord = &coord;
    let drive_result = thread::scope(|scope| -> Result<DriveOutcome> {
        for w in 0..exec.workers {
            let ctx = WorkerContext {
                coord,
                store,
                config,
                executor,
                exec,
                events: events.clone(),
            };
            scope.spawn(move || worker_loop(WorkerId(w as u64), ctx));
        }
        {
            let events = events.clone();
            scope.spawn(move || watchdog_loop(coord, exec.attempt_timeout, &events));
        }
        drive(coord, &mut source, &events)
    });

    // Whatever the pool returned, decide the outcome, then always flush and
    // join the journal.
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
        Err(fault) => Err(fault),
    };

    drop(events);
    let journal = sink.shutdown();
    // The domain outcome wins: a definitive candidate failure is returned even
    // when the journal degraded, because the journal is observational and the
    // same fault resurfaces on the next run that finalizes over this store.
    // Only a Finalized outcome yields to the journal fault — there it is the
    // sole signal anything went wrong.
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
}

/// Feeds the queue and waits for the pool: seeds the queue from the first poll,
/// then polls again each time the pool goes quiescent — a source that derives
/// new tasks from committed results hands them out at those points — until a
/// poll yields nothing more (finalize) or a definitive failure or fault ends
/// the run.
fn drive(
    coord: &Coord,
    source: &mut dyn TaskSource,
    events: &Sender<LifecycleEvent>,
) -> Result<DriveOutcome> {
    if let Some(tasks) = poll_source(coord, source) {
        enqueue(coord, events, tasks);
    }
    loop {
        let mut state = coord.lock();
        // Wait for the pool to go quiescent: nothing in flight and, while the
        // run is healthy, nothing queued. A terminal state only waits for the
        // in-flight work to drain; queued tasks are then abandoned.
        while !(state.in_flight == 0
            && (!matches!(state.stop, Stop::Running) || state.queue.is_empty()))
        {
            state = coord.idle.wait(state).unwrap_or_else(|p| p.into_inner());
        }
        if !matches!(state.stop, Stop::Running) {
            // Terminal: take the reason, ask the pool to exit, and report.
            let stop = std::mem::replace(&mut state.stop, Stop::Finished);
            coord.idle.notify_all();
            drop(state);
            return match stop {
                Stop::Failed(failure) => Ok(DriveOutcome::Fail(failure)),
                Stop::Fault(fault) => Err(fault),
                // Quiescent with the run already wound down: finalize.
                Stop::Finished => Ok(DriveOutcome::Finalize),
                // This arm is guarded by the `!Running` check above.
                Stop::Running => unreachable!("the terminal branch excludes Running"),
            };
        }
        drop(state);
        // Healthy and quiescent: re-poll. A poll fault set the terminal state,
        // so the next iteration's wait drains in-flight work and the terminal
        // branch returns the fault; nothing more means the run is done.
        match poll_source(coord, source) {
            None => continue,
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
    use std::sync::mpsc;

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

    /// A fresh coordinator in the steady `Running` state, for driving a source
    /// with no worker pool attached.
    fn idle_coord() -> Coord {
        Coord {
            state: Mutex::new(Shared {
                queue: VecDeque::new(),
                leases: HashMap::new(),
                in_flight: 0,
                stop: Stop::Running,
            }),
            idle: Condvar::new(),
        }
    }

    #[test]
    fn a_poll_error_winds_the_run_down_instead_of_hanging() {
        let coord = idle_coord();
        let (events, _rx) = mpsc::channel();
        // With no live workers, the seed poll faults on the first call.
        let result = drive(&coord, &mut FailingSource, &events);
        assert!(matches!(result, Err(Error::Validation(_))));
        // The terminal state is what a real pool observes to drain: a poll
        // error that left `Running` in place would park the workers forever.
        assert!(!matches!(coord.lock().stop, Stop::Running));
    }
}
