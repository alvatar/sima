//! The child side of the transport: [`serve`] hosts a domain executor over a
//! frame pipe.
//!
//! `serve` is what a worker process runs for its whole life: read the
//! [`Hello`](super::protocol::Hello), resolve the executor for the announced
//! format, reply [`ToParent::Ready`], then execute one
//! [`Assignment`](super::protocol::Assignment) after another until the parent
//! closes the pipe — end-of-stream at a frame boundary is the shutdown
//! signal, and `serve` returns `Ok`. The child never touches a store: inputs
//! arrive as loaded bytes and results leave as frames.

use std::cell::RefCell;
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::time::Duration;

use sima_contracts::{Checkpoint, ExecutionContext, Executor, NoCheckpoint, TaskInput, WorkerId};
use sima_core::{Error, Result};
use sima_model::{FormatId, Params, Spec};

use super::checkpoint_cadence::CheckpointCadence;
use super::protocol::{
    Assignment, Hello, PROTOCOL_VERSION, ToChild, ToParent, read_frame, write_frame,
};

/// Hosts an executor over the transport: handshake, then the assign loop.
///
/// `resolve` maps the [`Hello`]'s format id to the executor this process
/// hosts; the scheduler never names a domain. Returns `Ok` when the parent
/// closes the pipe at a frame boundary; a protocol-version mismatch, a
/// resolver failure, a frame violation, or a broken pipe is `Err` — the
/// caller maps it to a nonzero exit with a stderr diagnostic.
pub fn serve<R: Read, W: Write>(
    mut reader: R,
    writer: W,
    resolve: &dyn Fn(&FormatId) -> Result<Box<dyn Executor>>,
) -> Result<()> {
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
    let executor = resolve(&hello.format)?;

    // The executor's offer channel borrows the writer during execute while
    // serve writes the outcome after it; the RefCell arbitrates the two
    // single-threaded borrows.
    let writer = RefCell::new(writer);
    write_frame(
        &mut *writer.borrow_mut(),
        &ToParent::Ready {
            protocol: PROTOCOL_VERSION,
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
    // the run reassembles under it.
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

    // The panic handler wraps only the executor call, exactly as the parent's
    // in-process worker did: a panic raised inside the candidate's execution
    // crosses as a Panicked frame; a panic anywhere else in the host is a bug
    // and propagates as one.
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
        Err(payload) => ToParent::Panicked(panic_reason(payload)),
    };
    write_frame(&mut *writer.borrow_mut(), &reply.encode())
}

/// The run's checkpoint cadence, decoded from the handshake's settings:
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
        if self.failed.borrow().is_some() || !self.cadence.save_due() {
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

    use sima_contracts::{Artifact, Outcome, Stats};
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
                            bytes: vec![ctx.attempt as u8],
                        },
                    })
                }
                Behavior::OfferSteps(n) => {
                    for step in 0..n {
                        checkpoint.offer(&|| vec![step as u8]);
                    }
                    Ok(Outcome::Completed {
                        artifacts: Vec::new(),
                        stats: Stats { bytes: Vec::new() },
                    })
                }
                Behavior::Panic => panic!("programmed panic"),
                Behavior::Fault => Err(Error::Validation("programmed fault".to_string())),
                Behavior::Fail => Ok(Outcome::Failed {
                    reason: "programmed failure".to_string(),
                    stats: Stats { bytes: Vec::new() },
                }),
                Behavior::Reject => Ok(Outcome::Rejected {
                    reason: "programmed rejection".to_string(),
                    stats: Stats { bytes: Vec::new() },
                }),
            }
        }
    }

    /// A resolver serving `TestExecutor` for the given behavior, counting
    /// invocations.
    fn resolver(
        behavior: Behavior,
        calls: &Cell<u32>,
    ) -> impl Fn(&FormatId) -> Result<Box<dyn Executor>> + '_ {
        move |format| {
            calls.set(calls.get() + 1);
            Ok(Box::new(TestExecutor {
                format: format.clone(),
                behavior,
            }))
        }
    }

    /// A `Hello` at the current protocol version for the `host-test.v1`
    /// format, with the given cadence settings.
    fn hello(interval_ms: u64, steps: u64) -> ToChild {
        ToChild::Hello(Hello {
            protocol: PROTOCOL_VERSION,
            format: FormatId::new("host-test.v1").expect("format id"),
            checkpoint_interval_ms: interval_ms,
            checkpoint_interval_steps: steps,
        })
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

    /// Frames `messages` into one input buffer, runs `serve` over it with a
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
                protocol: PROTOCOL_VERSION
            }]
        );
    }

    #[test]
    fn a_version_mismatch_is_refused_before_ready() {
        let opening = ToChild::Hello(Hello {
            protocol: PROTOCOL_VERSION + 1,
            format: FormatId::new("host-test.v1").expect("format id"),
            checkpoint_interval_ms: u64::MAX,
            checkpoint_interval_steps: 0,
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
        let result = serve(input.as_slice(), &mut output, &|format| {
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
        assert_eq!(stats.bytes, vec![assign.attempt as u8]);
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
        assert_eq!(
            &frames[1..],
            &[
                ToParent::Panicked("panic: programmed panic".to_string()),
                ToParent::Panicked("panic: programmed panic".to_string()),
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
