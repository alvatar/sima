//! The worker loop: it leases a task, drives its execution on a worker
//! process over the transport link, classifies what comes back, and commits
//! or retries.
//!
//! Each worker thread owns one long-lived child process — a transport shim
//! over the lease/retry/settle bookkeeping on [`Coordinator`]. The
//! thread pulls a task, sends the child everything the attempt needs as
//! loaded values, and waits on the link with the attempt deadline: due
//! checkpoint saves are persisted as they arrive, the parent classifies the
//! outcome it receives, and a child death or deadline expiry becomes a
//! transient failure with the child replaced. The
//! executor trust boundary lives here: the parent holds the only store
//! handle, so a result reaches durable state only by passing through this
//! commit path — the child is never given the store.

use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use sima_contracts::{Artifact, DeviceBinding, Outcome, WorkerId};
use sima_core::{Error, Hash, Result, to_hex};
use sima_model::{ArtifactRef, RunConfig, RunId, TaskIdentity, TaskKey, TaskRecord};
use sima_store::Store;
use sima_transport::protocol::Assignment;
use sima_transport::{LinkEvent, WorkerLink, WorkerTransport};

use crate::config::ExecutionConfig;
use crate::coordinator::{Coordinator, Pending, RunState};
use crate::event::LifecycleEvent;
use crate::journal_sink::emit;
use crate::placement::{self, ChainPlacement};
use crate::task_source::RunnableTask;

/// The run-wide context one worker borrows for its whole life: the shared
/// coordination, the store it commits through, the run config, the transport
/// it spawns its child through, the execution settings, and its own journal
/// sender.
pub(crate) struct WorkerContext<'a> {
    pub(crate) coordinator: &'a Coordinator,
    pub(crate) store: &'a Store,
    /// The run the worker commits under; keys the checkpoint slots.
    pub(crate) run: RunId,
    pub(crate) config: &'a RunConfig,
    pub(crate) transport: &'a dyn WorkerTransport,
    pub(crate) exec: &'a ExecutionConfig,
    /// The device this slot's children compute on; `None` leaves the choice to
    /// the backend's default selection, the single-class case.
    pub(crate) device: Option<DeviceBinding>,
    pub(crate) events: Sender<LifecycleEvent>,
}

/// Runs the worker: spawn the child, then lease a task, drive it on the
/// child, resolve the outcome, and repeat until the run winds down. The
/// child lives as long as the worker; it is replaced only when it dies, and
/// dropping the last link at exit is the graceful shutdown — stdin closes,
/// the child exits on end-of-stream, and the parent reaps it.
pub(crate) fn worker_loop(worker: WorkerId, ctx: WorkerContext<'_>) {
    // A worker that cannot spawn its child cannot take work: the run faults.
    let mut link = match spawn_bound(&ctx, worker) {
        Ok(link) => link,
        Err(e) => return ctx.coordinator.fault_run(e),
    };
    while let Some(leased) = ctx.coordinator.next_task(ctx.device.map(|d| d.class())) {
        // The pull's placement decision reaches the store and the journal
        // before the assignment goes out, so a chain's binding is durable
        // before any work runs under it.
        if let Err(e) = record_placement(&ctx, &leased.placement) {
            ctx.coordinator.fault(leased.pending.key, e);
            break;
        }
        let pending = leased.pending;
        // A panic escaping process() — the commit path, a store read, the
        // settle code — would leak the task's lease, and drive() would then
        // block forever on leases.is_empty() inside thread::scope, which
        // never reaches its join phase, so the panic would be swallowed and
        // the process would hang. The guard releases the lease as a fault
        // during unwind so the pool winds down; thread::scope still
        // re-raises the panic at join, so the fault content is never
        // observed and the panic surfaces as the bug it is. Re-raising
        // preserves the meaning of the Err vocabulary: every Err a caller
        // receives is an expected, describable fault it can act on, while a
        // bug arrives as an abnormal death. Executor panics are unaffected:
        // the child catches them and reports a Panicked frame, which
        // process() settles before the guard drops.
        let guard = PanicGuard::arm(ctx.coordinator, pending.key);
        let child = process(&ctx, worker, pending, link.as_mut());
        guard.disarm();
        match child {
            ChildState::Alive => {}
            ChildState::Dead => {
                // Kill is idempotent: a child already dead is reaped, one
                // still dying is finished off, before the replacement spawns.
                link.kill();
                link = match spawn_bound(&ctx, worker) {
                    Ok(link) => link,
                    Err(e) => return ctx.coordinator.fault_run(e),
                };
            }
            // The run is winding down; the child was killed and no
            // replacement is owed — next_task would return None.
            ChildState::WindingDown => break,
        }
    }
}

/// Spawns this slot's child on its device and journals the device the child
/// reports, at every spawn and respawn.
///
/// The event carries the child's own answer, so the journal records where the
/// work actually ran rather than what the parent asked for.
fn spawn_bound<'a>(ctx: &WorkerContext<'a>, worker: WorkerId) -> Result<Box<dyn WorkerLink + 'a>> {
    let link = ctx.transport.spawn(ctx.device.as_ref())?;
    emit(
        &ctx.events,
        LifecycleEvent::WorkerBound {
            worker: worker.0,
            device: link.device_name().to_string(),
            driver: link.driver().to_string(),
            // Empty for a local slot; a pooled run names the host it spawned on.
            host: String::new(),
        },
    );
    Ok(link)
}

/// Persists and journals what a pull decided about its chain's placement.
///
/// A failed slot write is a store fault: the run stops on it like any other.
/// The binding is durable before the assignment goes out, so a chain that ran
/// on a class resumes there.
fn record_placement(ctx: &WorkerContext<'_>, placement: &ChainPlacement) -> Result<()> {
    match placement {
        ChainPlacement::Settled => Ok(()),
        ChainPlacement::Bound { chain, to } => {
            ctx.store
                .bind_chain(&ctx.run, *chain, &placement::encode_class(*to)?)
        }
        ChainPlacement::Rebound { chain, from, to } => {
            ctx.store
                .bind_chain(&ctx.run, *chain, &placement::encode_class(*to)?)?;
            // The rebind is loud: the hardware changed under a running search,
            // and the journal is where that shows.
            emit(
                &ctx.events,
                LifecycleEvent::ChainRebound {
                    chain: *chain,
                    from: from.to_string(),
                    to: to.to_string(),
                },
            );
            Ok(())
        }
    }
}

/// What `process` left behind: whether the worker's child can take another
/// task.
enum ChildState {
    /// The child survives and takes the next task.
    Alive,
    /// The child is dead; the worker replaces it before the next task.
    Dead,
    /// The run is winding down; the in-flight attempt was abandoned, its
    /// child killed, and the worker exits.
    WindingDown,
}

/// A liveness guard over one leased task. While armed, its `Drop` releases the
/// lease as a fault, so a panic escaping `process` cannot strand the lease and
/// hang the driver. A normal `process` return disarms it, since the lease is
/// already settled by then.
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

/// Whether either checkpoint cadence axis is set, so a chain task
/// checkpoints: its slot is read for resume bytes, and the child evaluates
/// the cadence and sends due saves. With both axes disabled the slot is
/// never touched.
fn checkpointing_enabled(exec: &ExecutionConfig) -> bool {
    exec.checkpoint_interval != Duration::MAX || exec.checkpoint_interval_steps.is_some()
}

/// How long one wait on the link lasts at most, so a run winding down is
/// observed within this bound and the in-flight attempt abandoned. The same
/// cost trade as the driver's interrupt poll: about 20 uncontended lock
/// acquisitions per second per worker.
const WINDDOWN_POLL: Duration = Duration::from_millis(50);

/// Drives one leased task on the worker's child and resolves its outcome:
/// commit, retry, reject, or record an infrastructure fault. Returns whether
/// the child survives for the next task.
fn process(
    ctx: &WorkerContext<'_>,
    worker: WorkerId,
    pending: Pending,
    link: &mut dyn WorkerLink,
) -> ChildState {
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
    // its digest, the child receives its bytes. A load failure is an
    // infrastructure fault.
    let input_state = match identity.input_state {
        Some(hash) => match ctx.store.get(&hash) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                task_fault(ctx, task, attempt, key, e);
                return ChildState::Alive;
            }
        },
        None => None,
    };

    // The run keeps one checkpoint slot per chain — mutable scratch storage
    // for the continuation state a running segment offers. A chain task
    // under an enabled cadence resumes from whatever the slot holds for this
    // key — a slot that is missing, torn, or keyed to another segment loads
    // as nothing — and its due saves are persisted back as they arrive.
    // Stateless tasks and disabled checkpointing never touch the slot.
    let slot = if checkpointing_enabled(ctx.exec) {
        chain
    } else {
        None
    };
    let resume = slot.and_then(|slot| {
        match ctx.store.checkpoint(&ctx.run, slot, &key) {
            Ok(resume) => resume,
            Err(e) => {
                // A checkpoint is disposable, so a load failure degrades to a
                // fresh start — chosen over faulting the attempt: the resume
                // is lost, the task still runs.
                emit(
                    &ctx.events,
                    LifecycleEvent::CheckpointDegraded {
                        task: task.clone(),
                        error: e.to_string(),
                    },
                );
                None
            }
        }
    });

    // Everything the attempt needs crosses as loaded values; the spec and
    // params bytes are cloned so a retry can re-enqueue the task.
    let assignment = Assignment {
        spec: spec.bytes.clone(),
        params: ctx.config.params.bytes.clone(),
        seed: identity.seed,
        environment: identity.environment,
        input_state,
        resume,
        attempt,
        worker: worker.0,
        checkpointing: slot.is_some(),
    };
    let started = Instant::now();
    // The enforced attempt deadline; a timeout too large to land on the
    // clock (Duration::MAX) disables enforcement.
    let deadline = started.checked_add(ctx.exec.attempt_timeout);

    if let Err(e) = link.assign(&assignment) {
        // The pipe broke mid-write: the child is dead or dying.
        let reason = format!("worker {} died taking the task: {e}", worker.0);
        fail_transiently(
            ctx,
            key,
            task,
            attempt,
            retry(spec, identity, chain),
            reason,
        );
        return ChildState::Dead;
    }

    // The conversation: saves persist as they arrive, one terminal frame
    // settles the attempt. Every wait is bounded by WINDDOWN_POLL so a run
    // winding down abandons the attempt, and by the attempt deadline so an
    // overrunning child is preempted.
    loop {
        let poll = Instant::now() + WINDDOWN_POLL;
        let event = match link.next(Some(deadline.map_or(poll, |d| d.min(poll)))) {
            Ok(event) => event,
            Err(e) => {
                // Frame hygiene: a child whose bytes violate the protocol is
                // never trusted further — kill it, fail the attempt
                // transiently, replace it.
                link.kill();
                let reason = format!("worker {} violated the transport protocol: {e}", worker.0);
                fail_transiently(
                    ctx,
                    key,
                    task,
                    attempt,
                    retry(spec, identity, chain),
                    reason,
                );
                return ChildState::Dead;
            }
        };
        match event {
            LinkEvent::Save(payload) => {
                // The parent's persist half of the checkpoint contract; a
                // failure degrades, execution in the child continues
                // unaffected. A save from a task that does not checkpoint is
                // dropped: no slot was selected for it.
                if let Some(slot) = slot
                    && let Err(e) = ctx.store.save_checkpoint(&ctx.run, slot, &key, &payload)
                {
                    emit(
                        &ctx.events,
                        LifecycleEvent::CheckpointDegraded {
                            task: task.clone(),
                            error: e.to_string(),
                        },
                    );
                }
            }
            LinkEvent::Done(Outcome::Completed { artifacts, stats }) => {
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
                return ChildState::Alive;
            }
            LinkEvent::Done(Outcome::Failed { reason, stats }) => {
                emit(
                    &ctx.events,
                    LifecycleEvent::Failed {
                        task: task.clone(),
                        attempt,
                        reason: reason.clone(),
                        stats_hex: to_hex(&stats.bytes),
                    },
                );
                retry_or_terminate(
                    ctx,
                    key,
                    task,
                    attempt,
                    retry(spec, identity, chain),
                    reason,
                );
                return ChildState::Alive;
            }
            LinkEvent::Done(Outcome::Rejected { reason, stats }) => {
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
                return ChildState::Alive;
            }
            LinkEvent::Panicked(reason) => {
                // A panic raised inside the candidate's execution, caught by
                // the child: a definitive rejection. The child survives.
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
                return ChildState::Alive;
            }
            LinkEvent::Fault(message) => {
                // An infrastructure fault from the executor (e.g. a
                // structurally invalid spec) fails the whole run, distinct
                // from a candidate that merely evaluated badly. The wire
                // flattens the executor's error to its rendered message.
                task_fault(ctx, task, attempt, key, Error::Validation(message));
                return ChildState::Alive;
            }
            LinkEvent::Died(death) => {
                // Any death without an outcome — crash, OOM kill, externally
                // killed — classifies identically: transient, retried.
                let reason = format!("worker {} died without an outcome: {death}", worker.0);
                fail_transiently(
                    ctx,
                    key,
                    task,
                    attempt,
                    retry(spec, identity, chain),
                    reason,
                );
                return ChildState::Dead;
            }
            LinkEvent::DeadlineExpired => {
                if !matches!(ctx.coordinator.lock().state, RunState::Running) {
                    // The run is winding down: kill the child and abandon the
                    // attempt. The store's crash-safety makes the abandoned
                    // attempt free — resume re-derives it in the frontier.
                    link.kill();
                    ctx.coordinator.resolve(key);
                    return ChildState::WindingDown;
                }
                match deadline {
                    Some(deadline) if Instant::now() >= deadline => {
                        // Preemption: the attempt outlived attempt_timeout.
                        let elapsed = started.elapsed();
                        emit(
                            &ctx.events,
                            LifecycleEvent::LeaseExpired {
                                task: task.clone(),
                                worker: worker.0,
                                elapsed_ms: elapsed.as_millis() as u64,
                            },
                        );
                        link.kill();
                        let reason = format!(
                            "attempt preempted after {}ms (attempt_timeout {}ms)",
                            elapsed.as_millis(),
                            ctx.exec.attempt_timeout.as_millis()
                        );
                        fail_transiently(
                            ctx,
                            key,
                            task,
                            attempt,
                            retry(spec, identity, chain),
                            reason,
                        );
                        return ChildState::Dead;
                    }
                    // Only the wind-down poll slice elapsed; keep waiting.
                    _ => {}
                }
            }
        }
    }
}

/// The task, reassembled for a retry's re-enqueue.
fn retry(spec: sima_model::Spec, identity: TaskIdentity, chain: Option<u64>) -> RunnableTask {
    RunnableTask {
        spec,
        identity,
        chain,
    }
}

/// Journals a transient failure and applies the retry policy. One settlement
/// for every way an attempt fails transiently without stats: a child death,
/// a preemption, a broken pipe, a protocol violation.
fn fail_transiently(
    ctx: &WorkerContext<'_>,
    key: TaskKey,
    task: String,
    attempt: u32,
    task_to_retry: RunnableTask,
    reason: String,
) {
    emit(
        &ctx.events,
        LifecycleEvent::Failed {
            task: task.clone(),
            attempt,
            reason: reason.clone(),
            stats_hex: String::new(),
        },
    );
    retry_or_terminate(ctx, key, task, attempt, task_to_retry, reason);
}

/// The retry policy shared by every transient failure: re-enqueue at the
/// next attempt while attempts remain, otherwise the transient failure is
/// definitive and terminates the run.
fn retry_or_terminate(
    ctx: &WorkerContext<'_>,
    key: TaskKey,
    task: String,
    attempt: u32,
    task_to_retry: RunnableTask,
    reason: String,
) {
    if attempt + 1 < ctx.exec.max_attempts {
        if ctx.coordinator.requeue(key, task_to_retry, attempt + 1) {
            emit(
                &ctx.events,
                LifecycleEvent::Retried {
                    task,
                    next_attempt: attempt + 1,
                },
            );
        }
    } else {
        ctx.coordinator.terminate(key, reason);
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
/// executor-fault path, the commit-error path, and the input-state load path.
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use sima_contracts::{ExecutionContext, Executor, NoCheckpoint, Outcome, TaskInput};
    use sima_core::hash_bytes;
    use sima_domains::{StubBehavior, StubExecutor, StubProgram};
    use sima_model::{EnvironmentId, FormatId, GeneratorConfig, GeneratorId, Params, Spec, SpecId};
    use sima_transport::loopback::LoopbackTransport;

    use super::*;

    /// A throwaway task key.
    fn a_key() -> TaskKey {
        TaskKey::from_hash(hash_bytes(b"panic-guard task"))
    }

    /// A loopback transport hosting the stub executor under `exec`'s
    /// checkpoint cadence.
    fn stub_transport(exec: &ExecutionConfig) -> LoopbackTransport {
        LoopbackTransport::new(
            FormatId::new("stub.v1").expect("format id"),
            exec.checkpoint_interval,
            exec.checkpoint_interval_steps,
            // The stub uses no device: it ignores the binding and names none.
            Arc::new(|_, _| {
                let executor: Box<dyn Executor> = Box::new(StubExecutor::new()?);
                Ok((executor, String::new(), String::new()))
            }),
        )
    }

    #[test]
    fn a_panicking_worker_releases_its_lease_as_a_fault() {
        let coordinator = Coordinator::new();
        let key = a_key();
        coordinator.lock().leases.insert(key);
        // A panic escaping the guarded region unwinds through the guard's Drop.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = PanicGuard::arm(&coordinator, key);
            panic!("worker body panicked");
        }));
        assert!(result.is_err());
        let shared = coordinator.lock();
        // The lease is released and the run winds down, so drive() can observe
        // quiescence instead of blocking forever.
        assert!(!shared.leases.contains(&key));
        assert!(matches!(shared.state, RunState::Fault(_)));
    }

    #[test]
    fn a_disarmed_guard_leaves_the_run_running() {
        let coordinator = Coordinator::new();
        let key = a_key();
        coordinator.lock().leases.insert(key);
        {
            let guard = PanicGuard::arm(&coordinator, key);
            guard.disarm();
        }
        let shared = coordinator.lock();
        // Disarm settles nothing itself — the normal process() path does — so
        // the state is untouched: no fault, and the lease still stands.
        assert!(matches!(shared.state, RunState::Running));
        assert!(shared.leases.contains(&key));
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

    /// Builds a task whose identity references `state` and runs `process` once
    /// over a loopback worker. The state object is stored only when
    /// `store_state` is set, so a caller can exercise both the resolved-state
    /// and missing-state paths. Every other identity component is stored, so a
    /// successful commit's only open question is the input state.
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
        let exec = ExecutionConfig::new(1, 1, Duration::from_secs(5), Duration::MAX, None)?;

        // The stub artifact this identity commits when the state bytes reach the
        // executor: computed by calling the executor directly with the bytes.
        let executor = StubExecutor::new()?;
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
            let transport = stub_transport(&exec);
            let ctx = WorkerContext {
                coordinator: &coordinator,
                store: &store,
                run,
                config: &config,
                transport: &transport,
                exec: &exec,
                device: None,
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
            let mut link = transport.spawn(None)?;
            process(&ctx, WorkerId(0), pending, link.as_mut());
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
        // bytes the worker resolved and sent to the child.
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
            let exec = ExecutionConfig::new(1, 1, Duration::from_secs(5), interval, step_interval)?;
            let coordinator = Coordinator::new();
            let (tx, rx) = mpsc::channel();
            {
                let transport = stub_transport(&exec);
                let ctx = WorkerContext {
                    coordinator: &coordinator,
                    store: &self.store,
                    run: self.run,
                    config: &self.config,
                    transport: &transport,
                    exec: &exec,
                    device: None,
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
                let mut link = transport.spawn(None)?;
                process(&ctx, WorkerId(0), pending, link.as_mut());
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
        // The default: neither axis set, so a chain task never checkpoints
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
