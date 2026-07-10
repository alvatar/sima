//! The worker loop: it leases a task, runs the executor, classifies the
//! outcome, and commits or retries.
//!
//! This is the interim in-process transport: a fixed pool of threads pulling
//! from the shared queue. It is deliberately narrow — evaluate, classify,
//! commit — with the lease and settlement bookkeeping on [`Coordinator`], so a
//! subprocess-based worker can replace the execution transport (the `execute`
//! call and its panic classification) while the coordination around it stays
//! in place. The executor trust boundary lives here: the worker holds the only
//! store handle, so a result reaches durable state only by passing through
//! this commit path.

use std::any::Any;
use std::sync::mpsc::Sender;

use sima_contracts::{Artifact, ExecutionContext, Executor, Outcome, TaskInput, WorkerId};
use sima_core::{Error, Hash, Result, to_hex};
use sima_model::{ArtifactRef, RunConfig, TaskIdentity, TaskKey, TaskRecord};
use sima_store::Store;

use crate::config::ExecutionConfig;
use crate::coordinator::{Coordinator, Pending};
use crate::event::LifecycleEvent;
use crate::journal_sink::emit;
use crate::task_source::RunnableTask;

/// The run-wide context one worker borrows for its whole life: the shared
/// coordination, the store it commits through, the run config and executor it
/// evaluates against, the execution settings, and its own journal sender.
pub(crate) struct WorkerContext<'a> {
    pub(crate) coordinator: &'a Coordinator,
    pub(crate) store: &'a Store,
    pub(crate) config: &'a RunConfig,
    pub(crate) executor: &'a (dyn Executor + Sync),
    pub(crate) exec: &'a ExecutionConfig,
    pub(crate) events: Sender<LifecycleEvent>,
}

/// Runs the worker: lease a task, evaluate it, resolve the outcome, repeat
/// until the run winds down.
pub(crate) fn worker_loop(worker: WorkerId, ctx: WorkerContext<'_>) {
    while let Some(pending) = ctx.coordinator.next_task(worker) {
        // A panic escaping process() outside the executor's own catch_unwind —
        // the commit path, a store read, the settle code — would leak the
        // task's lease, and drive() would then block forever on
        // leases.is_empty() inside thread::scope, which never reaches its join
        // phase, so the panic would be swallowed and the process would hang.
        // The guard releases the lease as a fault during unwind so the pool
        // winds down; thread::scope still re-raises the panic at join, so the
        // fault content is never observed and the panic surfaces as the bug it
        // is. Re-raising preserves the meaning of the Err vocabulary: every Err
        // a caller receives is an expected, describable fault it can act on,
        // while a bug arrives as an abnormal death, so a supervising caller can
        // distinguish retry-after-fixing-the-environment from the-code-is-wrong.
        // Executor panics are unaffected: process()'s inner handler catches
        // them and settles the lease, so the guard is disarmed before it drops.
        let guard = PanicGuard::arm(ctx.coordinator, pending.key);
        process(&ctx, worker, pending);
        guard.disarm();
    }
}

/// A liveness guard over one leased task. While armed, its `Drop` releases the
/// lease as a fault, so a panic escaping `process` outside the executor's
/// `catch_unwind` cannot strand the lease and hang the driver. A normal
/// `process` return disarms it, since the lease is already settled by then.
struct PanicGuard<'a> {
    coordinator: &'a Coordinator,
    key: TaskKey,
    armed: bool,
}

impl<'a> PanicGuard<'a> {
    /// Arms the guard over `key`'s lease on `coordinator`.
    fn arm(coordinator: &'a Coordinator, key: TaskKey) -> PanicGuard<'a> {
        PanicGuard {
            coordinator,
            key,
            armed: true,
        }
    }

    /// Disarms the guard: `process` returned normally and has already settled
    /// the lease, so nothing is owed.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PanicGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            // Reached only during unwind. Release the lease so drive() observes
            // quiescence; the fault's content is unobservable because
            // thread::scope re-raises the worker's panic at join, so this is
            // purely a liveness release.
            self.coordinator.fault(
                self.key,
                Error::Validation(format!(
                    "worker panicked while processing task {}",
                    self.key
                )),
            );
        }
    }
}

/// Evaluates one leased task and resolves its outcome: commit, retry, reject,
/// or record an infrastructure fault.
fn process(ctx: &WorkerContext<'_>, worker: WorkerId, pending: Pending) {
    let key = pending.key;
    let attempt = pending.attempt;
    let RunnableTask { spec, identity } = pending.task;
    let task = key.to_string();
    emit(
        &ctx.events,
        LifecycleEvent::Leased {
            task: task.clone(),
            worker: worker.0,
            attempt,
        },
    );
    // A death here strands nothing: the lease is in-memory only, and the
    // kernel releases the orchestrator lock with the process, so a resumed
    // run re-derives the task in its frontier.
    sima_core::crashpoint("lease.held");

    // Resolve the input-state object the identity references: the key carries
    // its digest, the executor receives its bytes. A load failure is an
    // infrastructure fault.
    let input_state = match identity.input_state {
        Some(hash) => match ctx.store.get(&hash) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                task_fault(ctx, task, attempt, key, e);
                return;
            }
        },
        None => None,
    };

    let exec_ctx = ExecutionContext { attempt, worker };
    let input = TaskInput {
        spec: &spec,
        params: &ctx.config.params,
        seed: identity.seed,
        environment: identity.environment,
        input_state: input_state.as_deref(),
    };
    // The panic handler wraps only the executor call: a panic escaping it was
    // raised inside the candidate's execution, so the worker classifies it as a
    // definitive rejection. A panic anywhere else is a scheduler bug and
    // propagates as one. `catch_unwind` intercepts only unwinding panics:
    // under `panic = "abort"` this handler is unreachable and an executor
    // panic kills the process instead — a crash the store's recovery
    // guarantee covers, so no correctness contract depends on unwinding.
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ctx.executor.execute(&input, &exec_ctx)
    }));
    // `input` (and its borrow of `spec`) is unused past this point, so the
    // retry path below is free to move `spec` back into a re-enqueued task.

    // `caught` is `Result<Result<Outcome, Error>, Box<dyn Any + Send>>`. The
    // outer layer comes from `catch_unwind` and is `Err` exactly when the
    // executor panicked; the inner layer is `execute`'s own result, whose
    // `Err` is an infrastructure fault. The three `Ok(Ok(..))` arms are the
    // normal outcomes; only the last arm is the panic path.
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
                    ctx.coordinator.resolve(key);
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
                if ctx
                    .coordinator
                    .requeue(key, RunnableTask { spec, identity }, attempt + 1)
                {
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
                ctx.coordinator.terminate(key, reason);
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
            ctx.coordinator.terminate(key, reason);
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
            ctx.coordinator.terminate(key, reason);
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
    ctx.coordinator.fault(key, err);
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

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use sima_core::hash_bytes;
    use sima_domains::{StubBehavior, StubExecutor, StubProgram};
    use sima_model::{EnvironmentId, FormatId, GeneratorConfig, GeneratorId, Params, Spec, SpecId};

    use super::*;
    use crate::coordinator::RunState;
    use crate::lease::Lease;

    /// A throwaway task key.
    fn a_key() -> TaskKey {
        TaskKey::from_hash(hash_bytes(b"panic-guard task"))
    }

    /// A lease held by `worker`, leased now.
    fn a_lease(worker: u64) -> Lease {
        Lease {
            worker: WorkerId(worker),
            attempt: 0,
            leased_at: Instant::now(),
        }
    }

    #[test]
    fn a_panicking_worker_releases_its_lease_as_a_fault() {
        let coordinator = Coordinator::new();
        let key = a_key();
        coordinator.lock().leases.insert(key, a_lease(0));
        // A panic escaping the guarded region unwinds through the guard's Drop.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = PanicGuard::arm(&coordinator, key);
            panic!("worker body panicked");
        }));
        assert!(result.is_err());
        let shared = coordinator.lock();
        // The lease is released and the run winds down, so drive() can observe
        // quiescence instead of blocking forever.
        assert!(!shared.leases.contains_key(&key));
        assert!(matches!(shared.state, RunState::Fault(_)));
    }

    #[test]
    fn a_disarmed_guard_leaves_the_run_running() {
        let coordinator = Coordinator::new();
        let key = a_key();
        coordinator.lock().leases.insert(key, a_lease(0));
        {
            let guard = PanicGuard::arm(&coordinator, key);
            guard.disarm();
        }
        let shared = coordinator.lock();
        // Disarm settles nothing itself — the normal process() path does — so
        // the state is untouched: no fault, and the lease still stands.
        assert!(matches!(shared.state, RunState::Running));
        assert!(shared.leases.contains_key(&key));
    }

    /// The outcome of running `process` once against a stub `Succeed` candidate
    /// whose identity references an input-state object.
    struct ProcessRun {
        _dir: tempfile::TempDir,
        store: Store,
        identity: TaskIdentity,
        coordinator: Coordinator,
        events: Vec<LifecycleEvent>,
        /// The artifact bytes the stub executor produces for this identity when
        /// it receives `state`'s bytes directly.
        expected_artifact: Vec<u8>,
    }

    /// Builds a task whose identity references `state` and runs `process` once.
    /// The state object is stored only when `store_state` is set, so a caller
    /// can exercise both the resolved-state and missing-state paths. Every other
    /// identity component is stored, so a successful commit's only open question
    /// is the input state.
    fn run_process(state: &[u8], store_state: bool) -> Result<ProcessRun> {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");

        // Each identity component is stored as its own object so the commit's
        // durability check passes; the object address is the component id.
        let params = Params {
            bytes: vec![1, 2, 3],
        };
        store.put(&params.to_bytes())?;
        let environment = EnvironmentId::from_hash(store.put(b"unit-test environment")?);
        let spec = Spec {
            format: FormatId::new("stub.v1")?,
            bytes: StubProgram {
                behavior: StubBehavior::Succeed,
                nonce: 7,
            }
            .to_bytes(),
        };
        let spec_id = SpecId::from_hash(store.put(&spec.to_bytes())?);
        if store_state {
            store.put(state)?;
        }
        let identity = TaskIdentity {
            spec: spec_id,
            params: params.id(),
            seed: 5,
            environment,
            input_state: Some(hash_bytes(state)),
        };

        let config = RunConfig {
            root_seed: 0,
            segments: None,
            format: FormatId::new("stub.v1")?,
            generator: GeneratorConfig {
                id: GeneratorId::new("stub.v1")?,
                params: Vec::new(),
            },
            params,
        };
        let executor = StubExecutor::new()?;
        let exec = ExecutionConfig::new(1, 1, Duration::from_secs(1))?;

        // The stub artifact this identity commits when the state bytes reach the
        // executor: computed by calling the executor directly with the bytes.
        let expected_artifact = match executor.execute(
            &TaskInput {
                spec: &spec,
                params: &config.params,
                seed: identity.seed,
                environment,
                input_state: Some(state),
            },
            &ExecutionContext {
                attempt: 0,
                worker: WorkerId(0),
            },
        )? {
            Outcome::Completed { artifacts, .. } => {
                artifacts.into_iter().next().expect("one artifact").bytes
            }
            _ => panic!("a stub Succeed candidate completes"),
        };

        let coordinator = Coordinator::new();
        let (tx, rx) = mpsc::channel();
        {
            let ctx = WorkerContext {
                coordinator: &coordinator,
                store: &store,
                config: &config,
                executor: &executor,
                exec: &exec,
                events: tx,
            };
            let pending = Pending {
                key: identity.key(),
                task: RunnableTask {
                    spec: spec.clone(),
                    identity,
                },
                attempt: 0,
            };
            process(&ctx, WorkerId(0), pending);
        }
        let events = rx.into_iter().collect();
        Ok(ProcessRun {
            _dir: dir,
            store,
            identity,
            coordinator,
            events,
            expected_artifact,
        })
    }

    #[test]
    fn the_worker_resolves_input_state_and_commits_the_state_dependent_artifact() -> Result<()> {
        let run = run_process(b"input state blob", true)?;
        let record = run
            .store
            .record(&run.identity.key())?
            .expect("the task committed a record");
        assert_eq!(record.artifacts().len(), 1);
        let object = record.artifacts()[0].object();
        // The committed artifact is the digest that folds in the input-state
        // bytes the worker resolved and passed to the executor.
        assert_eq!(run.store.get(object)?, run.expected_artifact);
        Ok(())
    }

    #[test]
    fn a_missing_input_state_object_is_a_fault() -> Result<()> {
        let run = run_process(b"never stored", false)?;
        assert!(run.store.record(&run.identity.key())?.is_none());
        assert!(matches!(run.coordinator.lock().state, RunState::Fault(_)));
        assert!(
            run.events
                .iter()
                .any(|e| matches!(e, LifecycleEvent::Faulted { .. })),
            "the load failure emits a Faulted event"
        );
        Ok(())
    }
}
