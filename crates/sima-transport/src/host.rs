//! The child side of the transport: [`serve`] hosts a domain executor over a
//! frame pipe.
//!
//! `serve` is what a worker process searches for its whole life: read the
//! [`Hello`](super::protocol::Hello), resolve the executor for the announced
//! format and device, reply [`ToParent::Ready`], then execute one
//! [`Assignment`](super::protocol::Assignment) after another until the parent
//! closes the pipe — end-of-stream at a frame boundary is the shutdown
//! signal, and `serve` returns `Ok`. The child never touches a store: inputs
//! arrive as loaded bytes and results leave as frames.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::sync::Mutex;
use std::thread::ThreadId;
use std::time::Duration;

use sima_contracts::{
    Checkpoint, DeviceBinding, ExecutionContext, Executor, NoCheckpoint, TaskInput, WorkerId,
};
use sima_core::{Error, Result, hash_bytes, read_frame, write_frame};
use sima_model::{FormatId, Params, Spec, TaskIdentity};
use sima_trace::{Event, Level};

use crate::checkpoint_cadence::CheckpointCadence;
use crate::protocol::{Assignment, Hello, PROGRAM_DIGEST_VAR, PROTOCOL_VERSION, ToChild, ToParent};

/// The most recent panic's message and backtrace, latched by the hook
/// [`capture_panics`] installs, under the thread that panicked. The executor
/// catch in [`serve`] takes its own thread's entry, so the correlated
/// diagnostic carries the backtrace the default hook would only print to
/// stderr.
///
/// Keyed by thread because the hook is process-global while the catch is not:
/// an executor that spawns threads of its own, or a host serving more than one
/// slot in process, would otherwise attribute a foreign thread's panic to
/// whichever task happened to be catching. A capture is taken by the thread
/// that made it and by nothing else.
static CAPTURED_PANIC: Mutex<Option<HashMap<ThreadId, String>>> = Mutex::new(None);

/// Installs a process-global panic hook that latches each panic's message and
/// backtrace into a slot the serve loop reads, then delegates to the
/// previously installed hook, so stderr output is unchanged. The worker
/// binary installs it at startup; an in-process host (the loopback) does not,
/// leaving the test harness's hook alone — its diagnostics then carry the
/// panic message without a backtrace.
pub fn capture_panics() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let message = if let Some(text) = info.payload().downcast_ref::<&str>() {
            (*text).to_string()
        } else if let Some(text) = info.payload().downcast_ref::<String>() {
            text.clone()
        } else {
            "non-string payload".to_string()
        };
        let backtrace = std::backtrace::Backtrace::force_capture();
        CAPTURED_PANIC
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(HashMap::new)
            .insert(
                std::thread::current().id(),
                format!("panic: {message}\n{backtrace}"),
            );
        previous(info);
    }));
}

/// Takes the panic message-and-backtrace the hook latched for this thread, if
/// the capture hook is installed and this thread panicked since the last take.
///
/// Removing rather than reading is what keeps a capture from outliving the
/// attempt that made it: the next panic-free attempt on this thread finds
/// nothing and falls back to its own rendered reason.
fn take_captured_panic() -> Option<String> {
    CAPTURED_PANIC
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_mut()?
        .remove(&std::thread::current().id())
}

/// Turns a handshake into the executor a host serves: the format id and the
/// device binding in, the executor and the name of the device it opened out.
///
/// The name and driver version are what the host answers `Ready` with, so they
/// are the child's own account of where it computes. A domain that uses no
/// device names neither.
pub type Resolver<'a> =
    dyn Fn(&FormatId, Option<&DeviceBinding>) -> Result<(Box<dyn Executor>, String, String)> + 'a;

/// Hosts an executor over the transport: handshake, then the assign loop.
///
/// `resolve` maps the [`Hello`]'s format id and device binding to the executor
/// this process hosts; the scheduler never names a domain. Returns `Ok` when
/// the parent closes the pipe at a frame boundary; a protocol-version
/// mismatch, a resolver failure, a frame violation, or a broken pipe is `Err`
/// — the caller maps it to a nonzero exit with a stderr diagnostic.
pub fn serve<R: Read, W: Write>(mut reader: R, writer: W, resolve: &Resolver<'_>) -> Result<()> {
    // The handshake: the first frame must be a Hello at this protocol
    // version. Refusal happens before Ready, so the parent's missing Ready is
    // its spawn-failure signal.
    let Some(payload) = read_frame(&mut reader)? else {
        return Err(Error::Transport(
            "the pipe closed before the handshake".to_string(),
        ));
    };
    let ToChild::Hello(hello) = ToChild::decode(&payload)? else {
        return Err(Error::Transport(
            "expected the Hello handshake as the first frame".to_string(),
        ));
    };
    if hello.protocol != PROTOCOL_VERSION {
        return Err(Error::Transport(format!(
            "protocol version mismatch: the parent speaks {}, this worker speaks {PROTOCOL_VERSION}",
            hello.protocol
        )));
    }
    // Resolving here, before Ready, is what makes a binding that names an
    // absent device fail the handshake rather than the first task.
    let (executor, device_name, driver) = resolve(&hello.format, hello.device.as_ref())?;

    // The executor's offer channel borrows the writer during execute while
    // serve writes the outcome after it; the RefCell arbitrates the two
    // single-threaded borrows.
    let writer = RefCell::new(writer);
    write_frame(
        &mut *writer.borrow_mut(),
        &ToParent::Ready {
            protocol: PROTOCOL_VERSION,
            device_name,
            driver,
            // The spawn's own claim about which program this is, answered
            // unread: the value is the environment's, not the format's, so the
            // resolver never sees it.
            program: std::env::var(PROGRAM_DIGEST_VAR).unwrap_or_default(),
        }
        .encode(),
    )?;

    // The assign loop: one task per frame until the parent closes the pipe.
    loop {
        let Some(payload) = read_frame(&mut reader)? else {
            return Ok(());
        };
        let ToChild::Assign(assignment) = ToChild::decode(&payload)? else {
            return Err(Error::Transport(
                "unexpected second Hello after the handshake".to_string(),
            ));
        };
        execute_assignment(&hello, executor.as_ref(), assignment, &writer)?;
    }
}

/// Executes one assignment and writes its terminal frame: `Done` for an
/// executor outcome, `Panicked` for a caught panic, `Fault` for an executor
/// `Err`. Classification authority stays with the parent — this reports.
fn execute_assignment<W: Write>(
    hello: &Hello,
    executor: &dyn Executor,
    assignment: Assignment,
    writer: &RefCell<W>,
) -> Result<()> {
    // The spec's format travels once in the handshake; every assignment of
    // the search reassembles under it.
    let spec = Spec {
        format: hello.format.clone(),
        bytes: assignment.spec,
    };
    let params = Params {
        bytes: assignment.params,
    };
    let input = TaskInput {
        spec: &spec,
        params: &params,
        seed: assignment.seed,
        environment: assignment.environment,
        input_state: assignment.input_state.as_deref(),
    };
    let ctx = ExecutionContext {
        attempt: assignment.attempt,
        worker: WorkerId(assignment.worker),
    };

    // The checkpointing flag selects the live offer channel or the inert
    // handle; resume bytes are served only through the live channel.
    let channel = assignment.checkpointing.then(|| SaveChannel {
        cadence: cadence(hello),
        resume: assignment.resume,
        writer,
        failed: RefCell::new(None),
    });
    let checkpoint: &dyn Checkpoint = match &channel {
        Some(channel) => channel,
        None => &NoCheckpoint,
    };

    // The panic handler wraps only the executor call: a panic raised inside
    // the candidate's execution crosses as a Panicked frame; a panic anywhere
    // else in the host is a bug and propagates as one.
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.execute(&input, &ctx, checkpoint)
    }));

    // A save that tore the stream poisons every later frame; surface it
    // instead of writing an outcome onto a broken pipe.
    if let Some(channel) = &channel
        && let Some(error) = channel.failed.borrow_mut().take()
    {
        return Err(error);
    }
    let reply = match caught {
        Ok(Ok(outcome)) => ToParent::Done(outcome),
        Ok(Err(e)) => ToParent::Fault(e.to_string()),
        Err(payload) => {
            let reason = panic_reason(payload);
            // The correlated backtrace crosses first, as a structured event
            // tied to the task and this slot's worker id, then the Panicked
            // frame settles the attempt exactly as before. The hook's
            // capture carries the backtrace; without the hook installed the
            // rendered reason stands in.
            let message = take_captured_panic().unwrap_or_else(|| reason.clone());
            let diagnostic = Event::Diagnostic {
                level: Level::Error,
                source: "panic".to_string(),
                message,
                worker: Some(hello.worker),
                host: None,
                task: Some(task_key(&input).to_string()),
            };
            write_event(writer, &diagnostic)?;
            ToParent::Panicked(reason)
        }
    };
    write_frame(&mut *writer.borrow_mut(), &reply.encode())
}

/// The task key of an attempt, rebuilt from its identity inputs: every
/// component of [`TaskIdentity`] crosses the wire as loaded values, so the
/// child derives the same key the parent leased — the spec and params hash to
/// their content ids, and the input-state bytes hash back to the store
/// address the identity referenced.
fn task_key(input: &TaskInput<'_>) -> sima_model::TaskKey {
    TaskIdentity {
        spec: input.spec.id(),
        params: input.params.id(),
        seed: input.seed,
        environment: input.environment,
        input_state: input.input_state.map(hash_bytes),
    }
    .key()
}

/// Frames one structured event onto the parent pipe. Written from the serve
/// thread under the same `RefCell` discipline as every other frame — the
/// serve loop is single-threaded, and the checkpoint `Save` callback writes
/// only inside `execute`, never concurrently with this. An event that fails
/// to serialize is dropped: observational data never decides the
/// conversation's fate. A broken pipe still surfaces.
fn write_event<W: Write>(writer: &RefCell<W>, event: &Event) -> Result<()> {
    let Ok(bytes) = serde_json::to_vec(event) else {
        return Ok(());
    };
    write_frame(&mut *writer.borrow_mut(), &ToParent::Event(bytes).encode())
}

/// The search's checkpoint cadence, decoded from the handshake's settings:
/// `u64::MAX` milliseconds disables the wall-clock axis, `0` steps disables
/// the step axis.
fn cadence(hello: &Hello) -> CheckpointCadence {
    let interval = if hello.checkpoint_interval_ms == u64::MAX {
        Duration::MAX
    } else {
        Duration::from_millis(hello.checkpoint_interval_ms)
    };
    CheckpointCadence::new(interval, NonZeroU64::new(hello.checkpoint_interval_steps))
}

/// The child side of the checkpoint contract: the cadence decides whether an
/// offer is written, and a due offer crosses the pipe as a `Save` frame for
/// the parent to persist. The executor never touches a store.
struct SaveChannel<'a, W: Write> {
    cadence: CheckpointCadence,
    resume: Option<Vec<u8>>,
    writer: &'a RefCell<W>,
    /// The first save-write failure, latched: `offer` cannot fail, so the
    /// host surfaces it after execute returns instead of losing it.
    failed: RefCell<Option<Error>>,
}

impl<W: Write> Checkpoint for SaveChannel<'_, W> {
    fn resume(&self) -> Option<&[u8]> {
        self.resume.as_deref()
    }

    fn offer(&self, produce: &dyn Fn() -> Vec<u8>) {
        if self.failed.borrow().is_some() || !self.cadence.advance_due() {
            return;
        }
        // The cadence resets before the write is attempted, matching the
        // parent-side slot handle: a persistently failing pipe degrades once
        // per cadence period instead of once per offer.
        self.cadence.reset();
        let frame = ToParent::Save(produce()).encode();
        if let Err(e) = write_frame(&mut *self.writer.borrow_mut(), &frame) {
            *self.failed.borrow_mut() = Some(e);
        }
    }
}

/// Renders a caught panic payload as a reason string, recovering the common
/// `&str` and `String` payloads.
pub(crate) fn panic_reason(payload: Box<dyn std::any::Any + Send>) -> String {
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
    use std::cell::Cell;

    use sima_contracts::{Artifact, DeviceClass, Outcome, Stats};
    use sima_core::{Enc, hash_bytes};
    use sima_model::EnvironmentId;

    use super::*;

    /// What the test executor does with an assignment.
    #[derive(Clone, Copy)]
    enum Behavior {
        /// Complete with one artifact folding every input the executor saw —
        /// spec, params, seed, environment, input state, resume — so a test
        /// asserts arrival by decoding the artifact.
        Echo,
        /// Offer a checkpoint at each of `n` steps, then complete.
        OfferSteps(u64),
        /// Panic with a fixed message.
        Panic,
        /// Return `Err`: an infrastructure fault.
        Fault,
        /// Return `Outcome::Failed`.
        Fail,
        /// Return `Outcome::Rejected`.
        Reject,
    }

    /// The in-test executor `serve` hosts; behavior is fixed per test.
    struct TestExecutor {
        format: FormatId,
        behavior: Behavior,
    }

    impl Executor for TestExecutor {
        fn format(&self) -> &FormatId {
            &self.format
        }

        fn execute(
            &self,
            input: &TaskInput<'_>,
            ctx: &ExecutionContext,
            checkpoint: &dyn Checkpoint,
        ) -> Result<Outcome> {
            match self.behavior {
                Behavior::Echo => {
                    let mut enc = Enc::new();
                    enc.bytes(&input.spec.bytes)
                        .bytes(&input.params.bytes)
                        .u64(input.seed)
                        .hash(input.environment.as_hash());
                    match input.input_state {
                        None => enc.u8(0),
                        Some(bytes) => enc.u8(1).bytes(bytes),
                    };
                    match checkpoint.resume() {
                        None => enc.u8(0),
                        Some(bytes) => enc.u8(1).bytes(bytes),
                    };
                    Ok(Outcome::Completed {
                        artifacts: vec![Artifact {
                            name: "echo".to_string(),
                            bytes: enc.finish(),
                        }],
                        stats: Stats {
                            scalars: vec![("attempt".to_string(), f64::from(ctx.attempt))],
                            blob: Vec::new(),
                        },
                    })
                }
                Behavior::OfferSteps(n) => {
                    for step in 0..n {
                        checkpoint.offer(&|| vec![step as u8]);
                    }
                    Ok(Outcome::Completed {
                        artifacts: Vec::new(),
                        stats: Stats::empty(),
                    })
                }
                Behavior::Panic => panic!("programmed panic"),
                Behavior::Fault => Err(Error::Validation("programmed fault".to_string())),
                Behavior::Fail => Ok(Outcome::Failed {
                    reason: "programmed failure".to_string(),
                    stats: Stats::empty(),
                }),
                Behavior::Reject => Ok(Outcome::Rejected {
                    reason: "programmed rejection".to_string(),
                    stats: Stats::empty(),
                }),
            }
        }
    }

    /// A resolver serving `TestExecutor` for the given behavior, counting
    /// invocations.
    fn resolver(behavior: Behavior, calls: &Cell<u32>) -> Box<Resolver<'_>> {
        Box::new(move |format, _| {
            calls.set(calls.get() + 1);
            let executor: Box<dyn Executor> = Box::new(TestExecutor {
                format: format.clone(),
                behavior,
            });
            Ok((executor, String::new(), String::new()))
        })
    }

    /// A `Hello` at the current protocol version for the `host-test.v1`
    /// format, with the given cadence settings and no device binding.
    fn hello(interval_ms: u64, steps: u64) -> ToChild {
        ToChild::Hello(Hello {
            protocol: PROTOCOL_VERSION,
            worker: 7,
            format: FormatId::new("host-test.v1").expect("format id"),
            checkpoint_interval_ms: interval_ms,
            checkpoint_interval_steps: steps,
            device: None,
        })
    }

    /// A `Hello` binding the child to `device`.
    fn hello_on(device: Option<DeviceBinding>) -> ToChild {
        let ToChild::Hello(hello) = hello(u64::MAX, 0) else {
            unreachable!("hello builds a Hello");
        };
        ToChild::Hello(Hello { device, ..hello })
    }

    /// Serves one `Hello` and reports what the resolver saw and answered.
    fn handshake(device: Option<DeviceBinding>) -> (Option<DeviceBinding>, Vec<ToParent>) {
        let seen = Cell::new(None);
        let resolve = |format: &FormatId, device: Option<&DeviceBinding>| {
            seen.set(device.cloned());
            let executor: Box<dyn Executor> = Box::new(TestExecutor {
                format: format.clone(),
                behavior: Behavior::Echo,
            });
            Ok((
                executor,
                "test device".to_string(),
                "test driver".to_string(),
            ))
        };
        let mut input = Vec::new();
        write_frame(&mut input, &hello_on(device).encode()).expect("frame the input");
        let mut output = Vec::new();
        serve(input.as_slice(), &mut output, &resolve).expect("serve to end of stream");
        let mut frames = Vec::new();
        let mut reader = output.as_slice();
        while let Some(payload) = read_frame(&mut reader).expect("well-formed output") {
            frames.push(ToParent::decode(&payload).expect("decodable output"));
        }
        (seen.take(), frames)
    }

    #[test]
    fn the_resolver_receives_the_handshake_binding() {
        let binding = DeviceBinding {
            class: DeviceClass::new("10de:2d39").expect("class id"),
            member: 1,
        };
        let (seen, frames) = handshake(Some(binding.clone()));
        assert_eq!(seen, Some(binding), "the binding reaches the resolver");
        // Ready reports the device and driver the resolver named, so the parent
        // journals what the child resolved rather than what it assumed.
        assert!(matches!(
            frames.as_slice(),
            [ToParent::Ready { device_name, driver, .. }]
                if device_name == "test device" && driver == "test driver"
        ));
    }

    #[test]
    fn an_unbound_handshake_leaves_the_device_to_the_resolver() {
        let (seen, frames) = handshake(None);
        assert_eq!(seen, None);
        assert!(matches!(
            frames.as_slice(),
            [ToParent::Ready { device_name, .. }] if device_name == "test device"
        ));
    }

    /// A default assignment; tests override fields as needed.
    fn assignment() -> Assignment {
        Assignment {
            spec: vec![1, 2, 3],
            params: vec![4, 5],
            seed: 42,
            environment: EnvironmentId::from_hash(hash_bytes(b"env")),
            input_state: None,
            resume: None,
            attempt: 0,
            worker: 7,
            checkpointing: false,
        }
    }

    /// Frames `messages` into one input buffer, searches `serve` over it with a
    /// `behavior` executor, and returns serve's result plus the decoded
    /// output frames.
    fn drive(behavior: Behavior, messages: &[ToChild]) -> (Result<()>, Vec<ToParent>) {
        let mut input = Vec::new();
        for message in messages {
            write_frame(&mut input, &message.encode()).expect("frame the input");
        }
        let mut output = Vec::new();
        let calls = Cell::new(0);
        let result = serve(input.as_slice(), &mut output, &resolver(behavior, &calls));
        let mut frames = Vec::new();
        let mut reader = output.as_slice();
        while let Some(payload) = read_frame(&mut reader).expect("well-formed output") {
            frames.push(ToParent::decode(&payload).expect("decodable output"));
        }
        (result, frames)
    }

    #[test]
    fn the_handshake_replies_ready_and_eof_ends_serve_cleanly() {
        let (result, frames) = drive(Behavior::Echo, &[hello(u64::MAX, 0)]);
        assert!(result.is_ok(), "clean EOF is a clean exit: {result:?}");
        assert_eq!(
            frames,
            vec![ToParent::Ready {
                protocol: PROTOCOL_VERSION,
                device_name: String::new(),
                driver: String::new(),
                // This process was not spawned with a program digest, so the
                // serve answers none. The echo itself is proven over a real
                // child in `sima-worker`'s smoke tests, where the variable can
                // be set on the spawn rather than on this process.
                program: String::new(),
            }]
        );
    }

    #[test]
    fn a_version_mismatch_is_refused_before_ready() {
        let opening = ToChild::Hello(Hello {
            protocol: PROTOCOL_VERSION + 1,
            worker: 0,
            format: FormatId::new("host-test.v1").expect("format id"),
            checkpoint_interval_ms: u64::MAX,
            checkpoint_interval_steps: 0,
            device: None,
        });
        let (result, frames) = drive(Behavior::Echo, &[opening]);
        assert!(matches!(result, Err(Error::Transport(_))));
        assert!(frames.is_empty(), "no Ready crosses a refused handshake");
    }

    #[test]
    fn a_resolver_failure_is_an_error_before_ready() {
        let mut input = Vec::new();
        write_frame(&mut input, &hello(u64::MAX, 0).encode()).expect("frame the input");
        let mut output = Vec::new();
        let result = serve(input.as_slice(), &mut output, &|format, _| {
            Err(Error::Validation(format!(
                "unknown format id {:?}",
                format.as_str()
            )))
        });
        assert!(matches!(result, Err(Error::Validation(_))));
        assert!(output.is_empty(), "no Ready crosses a failed resolution");
    }

    #[test]
    fn a_missing_hello_is_an_error() {
        let (result, frames) = drive(Behavior::Echo, &[ToChild::Assign(assignment())]);
        assert!(matches!(result, Err(Error::Transport(_))));
        assert!(frames.is_empty());
    }

    #[test]
    fn a_second_hello_is_an_error() {
        let (result, frames) = drive(Behavior::Echo, &[hello(u64::MAX, 0), hello(u64::MAX, 0)]);
        assert!(matches!(result, Err(Error::Transport(_))));
        assert_eq!(frames.len(), 1, "only the handshake Ready was written");
    }

    /// The executor resolves once, at the handshake — not per assignment.
    #[test]
    fn the_executor_resolves_once_for_many_assignments() {
        let mut input = Vec::new();
        write_frame(&mut input, &hello(u64::MAX, 0).encode()).expect("frame");
        for _ in 0..3 {
            write_frame(&mut input, &ToChild::Assign(assignment()).encode()).expect("frame");
        }
        let mut output = Vec::new();
        let calls = Cell::new(0);
        let result = serve(
            input.as_slice(),
            &mut output,
            &resolver(Behavior::Echo, &calls),
        );
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn a_completed_task_rounds_trip_with_every_input_delivered() {
        let mut assign = assignment();
        assign.input_state = Some(vec![9, 9, 9]);
        assign.resume = Some(vec![8, 8]);
        assign.checkpointing = true;
        let (result, frames) = drive(
            Behavior::Echo,
            &[hello(u64::MAX, 0), ToChild::Assign(assign.clone())],
        );
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(frames.len(), 2, "Ready then Done: {frames:?}");
        let ToParent::Done(Outcome::Completed { artifacts, stats }) = &frames[1] else {
            panic!("expected Done(Completed), got {:?}", frames[1]);
        };
        assert_eq!(
            stats.scalars,
            vec![("attempt".to_string(), f64::from(assign.attempt))]
        );
        // The echo artifact folds exactly what the child handed the executor.
        let mut expected = Enc::new();
        expected
            .bytes(&assign.spec)
            .bytes(&assign.params)
            .u64(assign.seed)
            .hash(assign.environment.as_hash());
        expected.u8(1).bytes(&[9, 9, 9]);
        expected.u8(1).bytes(&[8, 8]);
        assert_eq!(artifacts[0].bytes, expected.finish());
    }

    #[test]
    fn resume_bytes_are_withheld_without_checkpointing() {
        // The checkpointing flag gates the whole channel: a resume payload on
        // a non-checkpointing assignment never reaches the executor.
        let mut assign = assignment();
        assign.resume = Some(vec![8, 8]);
        assign.checkpointing = false;
        let (result, frames) = drive(
            Behavior::Echo,
            &[hello(u64::MAX, 0), ToChild::Assign(assign)],
        );
        assert!(result.is_ok(), "{result:?}");
        let ToParent::Done(Outcome::Completed { artifacts, .. }) = &frames[1] else {
            panic!("expected Done(Completed), got {:?}", frames[1]);
        };
        // The artifact's trailing resume flag byte is 0: nothing served.
        assert_eq!(artifacts[0].bytes.last(), Some(&0u8));
    }

    #[test]
    fn due_saves_cross_the_pipe_at_the_step_cadence() {
        // Five offers under a step cadence of 2: saves at offers 2 and 4,
        // carrying the state offered at those steps, then Done.
        let mut assign = assignment();
        assign.checkpointing = true;
        let (result, frames) = drive(
            Behavior::OfferSteps(5),
            &[hello(u64::MAX, 2), ToChild::Assign(assign)],
        );
        assert!(result.is_ok(), "{result:?}");
        let saves: Vec<&Vec<u8>> = frames
            .iter()
            .filter_map(|f| match f {
                ToParent::Save(bytes) => Some(bytes),
                _ => None,
            })
            .collect();
        // Steps are zero-based: offers 2 and 4 carry payloads [1] and [3].
        assert_eq!(saves, vec![&vec![1u8], &vec![3u8]]);
        assert!(
            matches!(frames.last(), Some(ToParent::Done(_))),
            "the outcome follows the saves: {frames:?}"
        );
    }

    #[test]
    fn offers_below_the_cadence_send_nothing() {
        let mut assign = assignment();
        assign.checkpointing = true;
        let (result, frames) = drive(
            Behavior::OfferSteps(1),
            &[hello(u64::MAX, 2), ToChild::Assign(assign)],
        );
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(frames.len(), 2, "Ready then Done, no Save: {frames:?}");
        assert!(matches!(frames[1], ToParent::Done(_)));
    }

    #[test]
    fn offers_without_the_checkpointing_flag_send_nothing() {
        // Even a due cadence stays silent when the assignment does not
        // checkpoint: the flag selects the inert handle.
        let (result, frames) = drive(
            Behavior::OfferSteps(5),
            &[hello(u64::MAX, 1), ToChild::Assign(assignment())],
        );
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(frames.len(), 2, "Ready then Done, no Save: {frames:?}");
    }

    #[test]
    fn an_executor_panic_emits_a_correlated_diagnostic_before_panicked() {
        let assign = assignment();
        let (result, frames) = drive(
            Behavior::Panic,
            &[hello(u64::MAX, 0), ToChild::Assign(assign.clone())],
        );
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(frames.len(), 3, "Ready, Event, Panicked: {frames:?}");
        // The diagnostic frame precedes the Panicked frame and carries the
        // panic's message, the worker id from the handshake, and the task
        // key derived from the assignment's identity inputs.
        let ToParent::Event(bytes) = &frames[1] else {
            panic!("expected an Event frame, got {:?}", frames[1]);
        };
        let event: sima_trace::Event = serde_json::from_slice(bytes).expect("event bytes parse");
        let sima_trace::Event::Diagnostic {
            level,
            source,
            message,
            worker,
            task,
            ..
        } = event
        else {
            panic!("expected a Diagnostic, got another event");
        };
        assert_eq!(level, sima_trace::Level::Error);
        assert_eq!(source, "panic");
        assert!(message.contains("programmed panic"), "{message}");
        assert_eq!(worker, Some(7), "the handshake's worker id");
        let expected = sima_model::TaskIdentity {
            spec: sima_model::Spec {
                format: FormatId::new("host-test.v1").expect("format id"),
                bytes: assign.spec.clone(),
            }
            .id(),
            params: sima_model::Params {
                bytes: assign.params.clone(),
            }
            .id(),
            seed: assign.seed,
            environment: assign.environment,
            input_state: None,
        }
        .key();
        assert_eq!(task, Some(expected.to_string()));
        assert!(matches!(frames[2], ToParent::Panicked(_)), "{frames:?}");
    }

    #[test]
    fn a_panicking_executor_reports_panicked_and_serve_continues() {
        // The panic crosses as a frame, and the child survives to take the
        // next assignment: two panicking tasks, two Panicked frames.
        let mut input = Vec::new();
        write_frame(&mut input, &hello(u64::MAX, 0).encode()).expect("frame");
        for _ in 0..2 {
            write_frame(&mut input, &ToChild::Assign(assignment()).encode()).expect("frame");
        }
        let mut output = Vec::new();
        let calls = Cell::new(0);
        let result = serve(
            input.as_slice(),
            &mut output,
            &resolver(Behavior::Panic, &calls),
        );
        assert!(result.is_ok(), "{result:?}");
        let mut frames = Vec::new();
        let mut reader = output.as_slice();
        while let Some(payload) = read_frame(&mut reader).expect("well-formed output") {
            frames.push(ToParent::decode(&payload).expect("decodable output"));
        }
        // Each panic crosses as its correlated diagnostic then the Panicked
        // frame, and the child survives to take the next assignment.
        let panicked: Vec<&ToParent> = frames
            .iter()
            .filter(|f| matches!(f, ToParent::Panicked(_)))
            .collect();
        assert_eq!(
            panicked,
            [
                &ToParent::Panicked("panic: programmed panic".to_string()),
                &ToParent::Panicked("panic: programmed panic".to_string()),
            ]
        );
    }

    #[test]
    fn an_executor_error_reports_fault() {
        let (result, frames) = drive(
            Behavior::Fault,
            &[hello(u64::MAX, 0), ToChild::Assign(assignment())],
        );
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(
            frames[1],
            ToParent::Fault("validation error: programmed fault".to_string())
        );
    }

    #[test]
    fn failed_and_rejected_outcomes_cross_verbatim() {
        for (behavior, expected_reason) in [
            (Behavior::Fail, "programmed failure"),
            (Behavior::Reject, "programmed rejection"),
        ] {
            let (result, frames) = drive(
                behavior,
                &[hello(u64::MAX, 0), ToChild::Assign(assignment())],
            );
            assert!(result.is_ok(), "{result:?}");
            let ToParent::Done(outcome) = &frames[1] else {
                panic!("expected Done, got {:?}", frames[1]);
            };
            let reason = match outcome {
                Outcome::Failed { reason, .. } | Outcome::Rejected { reason, .. } => reason,
                Outcome::Completed { .. } => panic!("expected a failure outcome"),
            };
            assert_eq!(reason, expected_reason);
        }
    }

    #[test]
    fn a_torn_input_frame_is_an_error() {
        let mut input = Vec::new();
        write_frame(&mut input, &hello(u64::MAX, 0).encode()).expect("frame");
        write_frame(&mut input, &ToChild::Assign(assignment()).encode()).expect("frame");
        input.truncate(input.len() - 1);
        let mut output = Vec::new();
        let calls = Cell::new(0);
        let result = serve(
            input.as_slice(),
            &mut output,
            &resolver(Behavior::Echo, &calls),
        );
        assert!(matches!(result, Err(Error::Transport(_))));
    }

    #[test]
    fn panic_reason_recovers_common_payloads() {
        assert_eq!(
            panic_reason(Box::new("a str payload")),
            "panic: a str payload"
        );
        assert_eq!(
            panic_reason(Box::new("a String payload".to_string())),
            "panic: a String payload"
        );
        assert_eq!(panic_reason(Box::new(7u32)), "panic: non-string payload");
    }
}
