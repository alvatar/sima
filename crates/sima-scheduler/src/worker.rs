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
use std::cell::Cell;
use std::num::NonZeroU64;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use sima_contracts::{
    Artifact, Checkpoint, ExecutionContext, Executor, NoCheckpoint, Outcome, TaskInput, WorkerId,
};
use sima_core::{Error, Hash, Result, to_hex};
use sima_model::{ArtifactRef, RunConfig, RunId, TaskIdentity, TaskKey, TaskRecord};
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
    /// The run the worker commits under; keys the checkpoint slots.
    pub(crate) run: RunId,
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

/// The worker's store-side arm of the checkpoint contract for one attempt of
/// one chain task: it owns all slot I/O, enforces the wall-clock cadence, and
/// serves the resume bytes loaded before execution started. The executor on
/// the other side of the seam only offers and adopts bytes — it never touches
/// the store.
struct SlotCheckpoint<'a> {
    store: &'a Store,
    run: RunId,
    slot: u64,
    key: TaskKey,
    task: &'a str,
    interval: Duration,
    /// Step-count cadence: a save is due every `n`th offer since the last save.
    /// `None` leaves the wall-clock `interval` as the only cadence.
    step_interval: Option<NonZeroU64>,
    /// When the last save happened — initialized to the attempt's start, so
    /// the first save becomes due one full interval in.
    last_saved: Cell<Instant>,
    /// Offers seen since the last save, driving the step-count cadence.
    offers_since_save: Cell<u64>,
    resume: Option<Vec<u8>>,
    events: &'a Sender<LifecycleEvent>,
}

impl Checkpoint for SlotCheckpoint<'_> {
    fn resume(&self) -> Option<&[u8]> {
        self.resume.as_deref()
    }

    fn offer(&self, produce: &dyn Fn() -> Vec<u8>) {
        if !self.save_due() {
            return;
        }
        // Both cadences reset before the save is attempted — chosen over
        // retrying at the next offer, so a persistently failing slot degrades
        // once per cadence period instead of once per offer.
        self.offers_since_save.set(0);
        self.last_saved.set(Instant::now());
        if let Err(e) = self
            .store
            .save_checkpoint(&self.run, self.slot, &self.key, &produce())
        {
            // Checkpointing is an optimization, never a task outcome: the
            // failure is journaled and execution continues.
            emit(
                self.events,
                LifecycleEvent::CheckpointDegraded {
                    task: self.task.to_string(),
                    error: e.to_string(),
                },
            );
        }
    }
}

/// Whether either checkpoint cadence axis is set, so a chain task gets a slot
/// handle. With both axes disabled the inert handle is used and no slot is
/// touched.
fn checkpointing_enabled(exec: &ExecutionConfig) -> bool {
    exec.checkpoint_interval != Duration::MAX || exec.checkpoint_interval_steps.is_some()
}

impl SlotCheckpoint<'_> {
    /// Whether this offer triggers a save, under either cadence axis. The
    /// step-count axis advances its offer counter here, so every offer is
    /// counted exactly once; the wall-clock axis reads the elapsed time since
    /// the last save. A save is due when either axis fires.
    fn save_due(&self) -> bool {
        let step_due = match self.step_interval {
            Some(n) => {
                let count = self.offers_since_save.get() + 1;
                self.offers_since_save.set(count);
                count >= n.get()
            }
            None => false,
        };
        let clock_due =
            self.interval != Duration::MAX && self.last_saved.get().elapsed() >= self.interval;
        step_due || clock_due
    }
}

/// Evaluates one leased task and resolves its outcome: commit, retry, reject,
/// or record an infrastructure fault.
fn process(ctx: &WorkerContext<'_>, worker: WorkerId, pending: Pending) {
    let key = pending.key;
    let attempt = pending.attempt;
    let RunnableTask {
        spec,
        identity,
        chain,
    } = pending.task;
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

    // Select the attempt's checkpoint handle. The run keeps one checkpoint
    // slot per chain — mutable scratch storage for the continuation state a
    // running segment offers. A chain task under an enabled interval gets
    // the handle that reads and writes its chain's slot, loading whatever
    // the slot holds for this key — a slot that is missing, torn, or keyed
    // to another segment loads as nothing. Stateless tasks and disabled
    // checkpointing get the inert handle, and the slot is never read.
    let slot_checkpoint = match chain {
        Some(slot) if checkpointing_enabled(ctx.exec) => {
            let resume = match ctx.store.checkpoint(&ctx.run, slot, &key) {
                Ok(resume) => resume,
                Err(e) => {
                    // A checkpoint is disposable, so a load failure degrades
                    // to a fresh start — chosen over faulting the attempt:
                    // the resume is lost, the task still runs.
                    emit(
                        &ctx.events,
                        LifecycleEvent::CheckpointDegraded {
                            task: task.clone(),
                            error: e.to_string(),
                        },
                    );
                    None
                }
            };
            Some(SlotCheckpoint {
                store: ctx.store,
                run: ctx.run,
                slot,
                key,
                task: &task,
                interval: ctx.exec.checkpoint_interval,
                step_interval: ctx.exec.checkpoint_interval_steps,
                last_saved: Cell::new(Instant::now()),
                offers_since_save: Cell::new(0),
                resume,
                events: &ctx.events,
            })
        }
        _ => None,
    };
    let checkpoint: &dyn Checkpoint = match &slot_checkpoint {
        Some(slot_checkpoint) => slot_checkpoint,
        None => &NoCheckpoint,
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
        ctx.executor.execute(&input, &exec_ctx, checkpoint)
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
                if ctx.coordinator.requeue(
                    key,
                    RunnableTask {
                        spec,
                        identity,
                        chain,
                    },
                    attempt + 1,
                ) {
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
        let run = store.create_run(&config)?;
        let executor = StubExecutor::new()?;
        let exec = ExecutionConfig::new(1, 1, Duration::from_secs(1), Duration::MAX, None)?;

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
            &NoCheckpoint,
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
                run,
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
                    chain: None,
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

    /// A one-task `accumulate:k` fixture: store, registered run, and the
    /// segment-0 identity, ready for `process` runs with checkpoint knobs.
    struct AccumulateFixture {
        _dir: tempfile::TempDir,
        store: Store,
        run: sima_model::RunId,
        config: RunConfig,
        spec: Spec,
        identity: TaskIdentity,
    }

    impl AccumulateFixture {
        fn new(k: u64) -> Result<AccumulateFixture> {
            let dir = tempfile::tempdir().expect("temp dir");
            let store = Store::open(dir.path()).expect("open store");
            let params = Params {
                bytes: vec![1, 2, 3],
            };
            store.put(&params.to_bytes())?;
            let environment = EnvironmentId::from_hash(store.put(b"unit-test environment")?);
            let spec = Spec {
                format: FormatId::new("stub.v1")?,
                bytes: StubProgram {
                    behavior: StubBehavior::Accumulate(k),
                    nonce: 7,
                }
                .to_bytes(),
            };
            let spec_id = SpecId::from_hash(store.put(&spec.to_bytes())?);
            let identity = TaskIdentity {
                spec: spec_id,
                params: params.id(),
                seed: 42,
                environment,
                input_state: None,
            };
            let config = RunConfig {
                root_seed: 0,
                segments: std::num::NonZeroU64::new(1),
                format: FormatId::new("stub.v1")?,
                generator: GeneratorConfig {
                    id: GeneratorId::new("stub.v1")?,
                    params: Vec::new(),
                },
                params,
            };
            let run = store.create_run(&config)?;
            Ok(AccumulateFixture {
                _dir: dir,
                store,
                run,
                config,
                spec,
                identity,
            })
        }

        fn key(&self) -> TaskKey {
            self.identity.key()
        }

        /// Runs `process` once over the fixture task with the given chain
        /// slot and wall-clock checkpoint interval, the step axis disabled.
        fn process(&self, chain: Option<u64>, interval: Duration) -> Result<Vec<LifecycleEvent>> {
            self.process_with(chain, interval, None)
        }

        /// Runs `process` once with both checkpoint cadence axes set explicitly;
        /// returns the emitted events.
        fn process_with(
            &self,
            chain: Option<u64>,
            interval: Duration,
            step_interval: Option<NonZeroU64>,
        ) -> Result<Vec<LifecycleEvent>> {
            let executor = StubExecutor::new()?;
            let exec = ExecutionConfig::new(1, 1, Duration::from_secs(5), interval, step_interval)?;
            let coordinator = Coordinator::new();
            let (tx, rx) = mpsc::channel();
            {
                let ctx = WorkerContext {
                    coordinator: &coordinator,
                    store: &self.store,
                    run: self.run,
                    config: &self.config,
                    executor: &executor,
                    exec: &exec,
                    events: tx,
                };
                let pending = Pending {
                    key: self.key(),
                    task: RunnableTask {
                        spec: self.spec.clone(),
                        identity: self.identity,
                        chain,
                    },
                    attempt: 1,
                };
                process(&ctx, WorkerId(0), pending);
            }
            Ok(rx.into_iter().collect())
        }

        /// The committed `state` artifact bytes.
        fn state_artifact(&self) -> Result<Vec<u8>> {
            let record = self
                .store
                .record(&self.key())?
                .expect("the task committed a record");
            assert_eq!(record.artifacts().len(), 1);
            self.store.get(record.artifacts()[0].object())
        }
    }

    /// The steps the committed attempt executed, from the stub's stats in
    /// the `Committed` event: `(u32 attempt, u64 steps)`.
    fn committed_steps(events: &[LifecycleEvent]) -> u64 {
        let stats_hex = events
            .iter()
            .find_map(|e| match e {
                LifecycleEvent::Committed { stats_hex, .. } => Some(stats_hex.clone()),
                _ => None,
            })
            .expect("a Committed event");
        let bytes: Vec<u8> = (0..stats_hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&stats_hex[i..i + 2], 16).expect("hex"))
            .collect();
        let mut dec = sima_core::Dec::new(&bytes);
        dec.u32().expect("attempt");
        let steps = dec.u64().expect("steps");
        dec.finish().expect("stats end");
        steps
    }

    /// The stub trajectory from `{step: 0, acc: seed}` through `steps` steps.
    fn folded_state(seed: u64, steps: u64) -> sima_domains::StubState {
        let mut state = sima_domains::StubState { step: 0, acc: seed };
        for _ in 0..steps {
            state.acc = sima_core::prng::derive(state.acc, state.step);
            state.step += 1;
        }
        state
    }

    #[test]
    fn a_preseeded_checkpoint_shortens_reexecution() -> Result<()> {
        let fixture = AccumulateFixture::new(5)?;
        // A valid checkpoint three steps in, keyed to this task.
        fixture.store.save_checkpoint(
            &fixture.run,
            0,
            &fixture.key(),
            &folded_state(42, 3).to_bytes(),
        )?;
        let events = fixture.process(Some(0), Duration::ZERO)?;
        assert_eq!(
            committed_steps(&events),
            2,
            "the resumed attempt executes only the remaining steps"
        );
        // The committed state is the full trajectory regardless.
        assert_eq!(fixture.state_artifact()?, folded_state(42, 5).to_bytes());
        Ok(())
    }

    #[test]
    fn a_checkpoint_keyed_to_another_task_is_ignored() -> Result<()> {
        let fixture = AccumulateFixture::new(5)?;
        // The stale previous-segment case: the slot holds valid-looking state
        // under a different task key.
        let other = TaskKey::from_hash(hash_bytes(b"the previous segment's key"));
        fixture
            .store
            .save_checkpoint(&fixture.run, 0, &other, &folded_state(42, 3).to_bytes())?;
        let events = fixture.process(Some(0), Duration::ZERO)?;
        assert_eq!(committed_steps(&events), 5, "the task runs fully");
        assert_eq!(fixture.state_artifact()?, folded_state(42, 5).to_bytes());
        Ok(())
    }

    #[test]
    fn a_disabled_interval_writes_no_slot() -> Result<()> {
        let fixture = AccumulateFixture::new(3)?;
        fixture.process(Some(0), Duration::MAX)?;
        assert_eq!(
            fixture.store.checkpoint(&fixture.run, 0, &fixture.key())?,
            None
        );
        Ok(())
    }

    #[test]
    fn an_enabled_interval_writes_the_slot() -> Result<()> {
        let fixture = AccumulateFixture::new(3)?;
        // A zero interval makes every offer due, so the last save carries
        // the final step's state.
        fixture.process(Some(0), Duration::ZERO)?;
        let saved = fixture
            .store
            .checkpoint(&fixture.run, 0, &fixture.key())?
            .expect("the slot was written");
        assert_eq!(saved, folded_state(42, 3).to_bytes());
        Ok(())
    }

    #[test]
    fn a_save_failure_degrades_and_the_task_still_commits() -> Result<()> {
        let fixture = AccumulateFixture::new(3)?;
        // Occupy the checkpoint directory's path with a file, so creating
        // the directory — and thus every save — fails.
        let blocker = fixture
            ._dir
            .path()
            .join("runs")
            .join(fixture.run.to_string())
            .join("checkpoint");
        std::fs::write(&blocker, b"not a directory").expect("write blocker");
        let events = fixture.process(Some(0), Duration::ZERO)?;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, LifecycleEvent::CheckpointDegraded { .. })),
            "a save failure emits CheckpointDegraded"
        );
        assert_eq!(committed_steps(&events), 3, "the task still commits");
        assert_eq!(fixture.state_artifact()?, folded_state(42, 3).to_bytes());
        Ok(())
    }

    #[test]
    fn the_step_axis_alone_saves_every_nth_offer() -> Result<()> {
        // Wall-clock disabled, step cadence 2, over 5 offers (one per step). The
        // accumulate offer runs after each step, so saves land at offers 2 and 4;
        // offer 5 does not reach the third multiple. The slot holds step 4, not
        // the final step 5 — proving the step axis alone drives checkpointing.
        let fixture = AccumulateFixture::new(5)?;
        fixture.process_with(Some(0), Duration::MAX, NonZeroU64::new(2))?;
        let saved = fixture
            .store
            .checkpoint(&fixture.run, 0, &fixture.key())?
            .expect("the step axis wrote the slot");
        assert_eq!(saved, folded_state(42, 4).to_bytes());
        Ok(())
    }

    #[test]
    fn the_step_axis_fires_first_when_the_clock_is_far_off() -> Result<()> {
        // Both axes set, but the wall-clock interval is far larger than the run
        // takes, so only the step axis fires: the union saves at the step cadence.
        let fixture = AccumulateFixture::new(5)?;
        fixture.process_with(Some(0), Duration::from_secs(3600), NonZeroU64::new(2))?;
        let saved = fixture
            .store
            .checkpoint(&fixture.run, 0, &fixture.key())?
            .expect("the step axis wrote the slot");
        assert_eq!(saved, folded_state(42, 4).to_bytes());
        Ok(())
    }

    #[test]
    fn the_clock_axis_fires_first_when_the_step_cadence_is_far_off() -> Result<()> {
        // Both axes set, but the step cadence is larger than the run's offer
        // count, so only the wall-clock axis fires: a zero interval saves every
        // offer, and the last save carries the final step.
        let fixture = AccumulateFixture::new(3)?;
        fixture.process_with(Some(0), Duration::ZERO, NonZeroU64::new(1000))?;
        let saved = fixture
            .store
            .checkpoint(&fixture.run, 0, &fixture.key())?
            .expect("the clock axis wrote the slot");
        assert_eq!(saved, folded_state(42, 3).to_bytes());
        Ok(())
    }

    #[test]
    fn both_axes_disabled_writes_no_slot() -> Result<()> {
        // The default: neither axis set, so a chain task gets the inert handle
        // and the slot is never written.
        let fixture = AccumulateFixture::new(3)?;
        fixture.process_with(Some(0), Duration::MAX, None)?;
        assert_eq!(
            fixture.store.checkpoint(&fixture.run, 0, &fixture.key())?,
            None
        );
        Ok(())
    }

    #[test]
    fn committed_bytes_are_identical_with_checkpointing_on_off_and_resumed() -> Result<()> {
        let off = AccumulateFixture::new(4)?;
        off.process(None, Duration::MAX)?;
        let on = AccumulateFixture::new(4)?;
        on.process(Some(0), Duration::ZERO)?;
        let resumed = AccumulateFixture::new(4)?;
        resumed.store.save_checkpoint(
            &resumed.run,
            0,
            &resumed.key(),
            &folded_state(42, 2).to_bytes(),
        )?;
        resumed.process(Some(0), Duration::ZERO)?;
        assert_eq!(off.state_artifact()?, on.state_artifact()?);
        assert_eq!(off.state_artifact()?, resumed.state_artifact()?);
        Ok(())
    }
}
