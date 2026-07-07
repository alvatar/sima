//! The worker loop: it leases a task, runs the executor, classifies the
//! outcome, and commits or retries.
//!
//! This is the interim in-process transport: a fixed pool of threads pulling
//! from the shared queue. It is deliberately narrow — lease, execute, classify,
//! commit, retry — so a subprocess-based worker can replace the execution
//! transport (the `execute` call and its panic classification) while the lease,
//! retry, and commit logic around it stays in place. The executor trust
//! boundary lives here: the worker holds the only store handle, so a result
//! reaches durable state only by passing through this commit path.

use std::any::Any;
use std::sync::mpsc::Sender;
use std::time::Instant;

use sima_contracts::{Artifact, ExecutionContext, Executor, Outcome, TaskInput, WorkerId};
use sima_core::{Error, Hash, Result};
use sima_model::{ArtifactRef, RunConfig, TaskIdentity, TaskKey, TaskRecord};
use sima_store::Store;

use crate::config::ExecutionConfig;
use crate::driver::{Coord, Failure, Pending, Stop};
use crate::event::LifecycleEvent;
use crate::journal_sink::emit;
use crate::lease::Lease;
use crate::task_source::RunnableTask;

/// The run-wide context one worker borrows for its whole life: the shared
/// coordination, the store it commits through, the run config and executor it
/// evaluates against, the execution settings, and its own journal sender.
pub(crate) struct WorkerContext<'a> {
    pub(crate) coord: &'a Coord,
    pub(crate) store: &'a Store,
    pub(crate) config: &'a RunConfig,
    pub(crate) executor: &'a (dyn Executor + Sync),
    pub(crate) exec: &'a ExecutionConfig,
    pub(crate) events: Sender<LifecycleEvent>,
}

/// Runs the worker: lease a task, evaluate it, resolve the outcome, repeat
/// until the run winds down.
pub(crate) fn worker_loop(worker: WorkerId, ctx: WorkerContext<'_>) {
    while let Some(pending) = next_task(ctx.coord, worker, ctx.exec) {
        process(&ctx, worker, pending);
    }
}

/// Leases the next ready task, inserting its lease and counting it in flight;
/// returns `None` once the run is winding down and this worker should exit.
fn next_task(coord: &Coord, worker: WorkerId, exec: &ExecutionConfig) -> Option<Pending> {
    let mut state = coord.lock();
    loop {
        if !matches!(state.stop, Stop::Running) {
            // Winding down: pull no more work. Wake peers and the driver so
            // they observe the state and drain.
            coord.idle.notify_all();
            return None;
        }
        if let Some(pending) = state.queue.pop_front() {
            let key = pending.task.identity.key();
            let deadline = Instant::now() + exec.attempt_timeout;
            state.leases.insert(
                key,
                Lease {
                    worker,
                    attempt: pending.attempt,
                    deadline,
                },
            );
            state.in_flight += 1;
            return Some(pending);
        }
        // The queue is empty: this worker may be the one making the pool
        // quiescent, so wake the driver before parking for new work.
        coord.idle.notify_all();
        state = coord.idle.wait(state).unwrap_or_else(|p| p.into_inner());
    }
}

/// Evaluates one leased task and resolves its outcome: commit, retry, reject,
/// or record an infrastructure fault.
fn process(ctx: &WorkerContext<'_>, worker: WorkerId, pending: Pending) {
    let RunnableTask { spec, identity } = pending.task;
    let attempt = pending.attempt;
    let key = identity.key();
    let task = key.to_string();
    emit(
        &ctx.events,
        LifecycleEvent::Leased {
            task: task.clone(),
            worker: worker.0,
            attempt,
        },
    );

    let exec_ctx = ExecutionContext { attempt, worker };
    let input = TaskInput {
        spec: &spec,
        params: &ctx.config.params,
        seed: identity.seed,
        environment: identity.environment,
        input_state: None,
    };
    // The panic handler wraps only the executor call: a panic escaping it was
    // raised inside the candidate's execution, so the worker classifies it as a
    // definitive rejection. A panic anywhere else is a scheduler bug and
    // propagates as one.
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ctx.executor.execute(&input, &exec_ctx)
    }));
    // `input` (and its borrow of `spec`) is unused past this point, so the
    // retry path below is free to move `spec` back into a re-enqueued task.

    match caught {
        Ok(Ok(Outcome::Completed { artifacts, stats })) => {
            match commit(ctx.store, identity, artifacts) {
                Ok(record) => {
                    emit(
                        &ctx.events,
                        LifecycleEvent::Committed {
                            task,
                            record: record.to_string(),
                            stats_hex: to_hex(&stats.bytes),
                        },
                    );
                    resolve(ctx.coord, key);
                }
                Err(e) => task_fault(ctx, task, attempt, key, e),
            }
        }
        Ok(Ok(Outcome::Failed { reason, stats })) => {
            emit(
                &ctx.events,
                LifecycleEvent::Failed {
                    task: task.clone(),
                    attempt,
                    reason: reason.clone(),
                    stats_hex: to_hex(&stats.bytes),
                },
            );
            if attempt + 1 < ctx.exec.max_attempts {
                if requeue(ctx.coord, key, RunnableTask { spec, identity }, attempt + 1) {
                    emit(
                        &ctx.events,
                        LifecycleEvent::Retried {
                            task,
                            next_attempt: attempt + 1,
                        },
                    );
                }
            } else {
                // Retries exhausted: the transient failure is now definitive.
                terminate(ctx.coord, key, reason);
            }
        }
        Ok(Ok(Outcome::Rejected { reason, stats })) => {
            emit(
                &ctx.events,
                LifecycleEvent::Rejected {
                    task,
                    attempt,
                    reason: reason.clone(),
                    stats_hex: to_hex(&stats.bytes),
                },
            );
            terminate(ctx.coord, key, reason);
        }
        // An infrastructure fault from the executor (e.g. a structurally
        // invalid spec) fails the whole run, distinct from a candidate that
        // merely evaluated badly.
        Ok(Err(e)) => task_fault(ctx, task, attempt, key, e),
        Err(payload) => {
            let reason = panic_reason(payload);
            emit(
                &ctx.events,
                LifecycleEvent::Rejected {
                    task,
                    attempt,
                    reason: reason.clone(),
                    stats_hex: String::new(),
                },
            );
            terminate(ctx.coord, key, reason);
        }
    }
}

/// Commits a completed result: stores each artifact object, then the record,
/// through the store's single commit path. Every referenced identity object is
/// already durable (the driver stored params and environment; the task source
/// stored the spec), so the commit's only writes are the artifacts and record.
fn commit(store: &Store, identity: TaskIdentity, artifacts: Vec<Artifact>) -> Result<Hash> {
    let mut refs = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let object = store.put(&artifact.bytes)?;
        refs.push(ArtifactRef::new(artifact.name, object)?);
    }
    let record = TaskRecord::new(identity, refs)?;
    store.commit_record(&record)
}

/// Clears a resolved lease and counts its task no longer in flight.
fn resolve(coord: &Coord, key: TaskKey) {
    let mut state = coord.lock();
    state.leases.remove(&key);
    state.in_flight -= 1;
    coord.idle.notify_all();
}

/// Clears the lease and, while the run is still healthy, re-enqueues the task
/// at the next attempt. Returns whether it was re-enqueued: a run already
/// winding down abandons the task rather than queueing work no worker will take.
fn requeue(coord: &Coord, key: TaskKey, task: RunnableTask, next_attempt: u32) -> bool {
    let mut state = coord.lock();
    state.leases.remove(&key);
    state.in_flight -= 1;
    let running = matches!(state.stop, Stop::Running);
    if running {
        state.queue.push_back(Pending {
            task,
            attempt: next_attempt,
        });
    }
    coord.idle.notify_all();
    running
}

/// Records a definitive candidate failure: it decides the run's outcome only
/// from the healthy state, so it never downgrades an infrastructure fault; the
/// first candidate failure among candidate failures wins.
fn terminate(coord: &Coord, key: TaskKey, reason: String) {
    let mut state = coord.lock();
    state.leases.remove(&key);
    state.in_flight -= 1;
    if matches!(state.stop, Stop::Running) {
        state.stop = Stop::Failed(Failure { task: key, reason });
    }
    coord.idle.notify_all();
}

/// Emits the task's `Faulted` event and records the infrastructure fault, so
/// the run surfaces the error. One classification site for every fault: the
/// executor-error path, the commit-error path, and the input-state load path.
fn task_fault(ctx: &WorkerContext<'_>, task: String, attempt: u32, key: TaskKey, err: Error) {
    emit(
        &ctx.events,
        LifecycleEvent::Faulted {
            task,
            attempt,
            error: err.to_string(),
        },
    );
    fault(ctx.coord, key, err);
}

/// Records an infrastructure fault: it outranks a definitive candidate failure,
/// because the result path itself broke and the run must surface the error. The
/// first fault wins; a later candidate failure never displaces it.
fn fault(coord: &Coord, key: TaskKey, err: Error) {
    let mut state = coord.lock();
    state.leases.remove(&key);
    state.in_flight -= 1;
    if !matches!(state.stop, Stop::Fault(_)) {
        state.stop = Stop::Fault(err);
    }
    coord.idle.notify_all();
}

/// Renders a caught panic payload as a rejection reason, recovering the common
/// `&str` and `String` payloads.
fn panic_reason(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        format!("panic: {message}")
    } else if let Some(message) = payload.downcast_ref::<String>() {
        format!("panic: {message}")
    } else {
        "panic: non-string payload".to_string()
    }
}

/// Renders opaque stats bytes as a lowercase-hex journal field.
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a String is infallible.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Condvar, Mutex};

    use sima_core::hash_bytes;

    use super::*;
    use crate::driver::{Failure, Shared};

    /// A coordinator in a given stop state, with one lease in flight so a
    /// single settle balances the in-flight count.
    fn coord_with(stop: Stop) -> Coord {
        Coord {
            state: Mutex::new(Shared {
                queue: VecDeque::new(),
                leases: HashMap::new(),
                in_flight: 1,
                stop,
            }),
            idle: Condvar::new(),
        }
    }

    /// A throwaway task key.
    fn a_key() -> TaskKey {
        TaskKey::from_hash(hash_bytes(b"fault-upgrade task"))
    }

    #[test]
    fn a_fault_upgrades_a_definitive_candidate_failure() {
        let coord = coord_with(Stop::Failed(Failure {
            task: a_key(),
            reason: "candidate rejected".to_string(),
        }));
        fault(&coord, a_key(), Error::Corruption("store broke".to_string()));
        assert!(matches!(coord.lock().stop, Stop::Fault(_)));
    }

    #[test]
    fn the_first_fault_is_kept_over_a_later_one() {
        let coord = coord_with(Stop::Fault(Error::Corruption("first fault".to_string())));
        fault(&coord, a_key(), Error::Validation("second fault".to_string()));
        match &coord.lock().stop {
            Stop::Fault(e) => assert_eq!(e.to_string(), "store corruption: first fault"),
            _ => panic!("expected a fault stop state"),
        }
    }
}
