//! The loopback transport: the scheduler's test double for the worker
//! transport.
//!
//! It exercises the real wire protocol and the real host loop — every frame
//! is encoded, framed, and decoded exactly as production does — with the
//! process boundary replaced by in-memory pipes and a thread running
//! [`host::serve`]. What it cannot exercise is the OS: process isolation,
//! SIGKILL preemption, and orphan behavior are tested with the real binaries
//! in the CLI crate's test suites.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use sima_contracts::{DeviceBinding, Executor};
use sima_core::{Error, Result, write_frame};
use sima_model::FormatId;
use sima_trace::Emitter;

use crate::host;
use crate::link::{LinkEvent, SpawnOutcome, WorkerLink, WorkerTransport};
use crate::protocol::{Assignment, Hello, ToChild, ToParent};
use crate::subprocess::{self, next_event, read_events};

/// A [`host::Resolver`] the loopback shares across its host threads: one
/// transport spawns many, each moving in a handle of its own.
pub type SharedResolver = Arc<
    dyn Fn(&FormatId, Option<&DeviceBinding>) -> Result<(Box<dyn Executor>, String, String)>
        + Send
        + Sync,
>;

/// Spawns in-process workers: each is a thread running the real host loop
/// over in-memory pipes, hosting the executor the resolver supplies.
pub struct LoopbackTransport {
    hello: Hello,
    resolver: SharedResolver,
}

impl LoopbackTransport {
    /// A transport hosting `resolver`'s executor for `format` with the given
    /// checkpoint cadence ([`Duration::MAX`] and `None` disable an axis).
    pub fn new(
        format: FormatId,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<NonZeroU64>,
        resolver: SharedResolver,
    ) -> LoopbackTransport {
        LoopbackTransport {
            hello: Hello::for_run(format, checkpoint_interval, checkpoint_interval_steps),
            resolver,
        }
    }
}

impl WorkerTransport for LoopbackTransport {
    fn spawn(
        &self,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<SpawnOutcome> {
        let (mut stdin, host_reader) = pipe();
        let (host_writer, stdout) = pipe();
        let resolver = self.resolver.clone();
        // The host thread is the "child": serve's return value has no one to
        // go to — its ending, whatever the reason, is what the link observes
        // as the event stream closing, exactly like a process death.
        let host = std::thread::spawn(move || {
            let _ = host::serve(host_reader, host_writer, resolver.as_ref());
        });
        let (sender, link_events) = channel();
        // The loopback host runs in-process on this machine: Event frames
        // forward under the local pool's empty host label; there is no
        // stderr to capture.
        let context = subprocess::EventContext {
            events,
            worker,
            host: String::new(),
        };
        let reader = std::thread::spawn(move || read_events(stdout, sender, Some(context)));
        // The handshake, over the real wire protocol.
        let hello = Hello {
            worker,
            device: device.cloned(),
            ..self.hello.clone()
        };
        write_frame(&mut stdin, &ToChild::Hello(hello).encode())?;
        let (device_name, driver) =
            subprocess::ready_desc("loopback host", link_events.recv().ok())?;
        Ok(SpawnOutcome::Link(Box::new(LoopbackLink {
            stdin: Some(stdin),
            events: link_events,
            host: Some(host),
            reader: Some(reader),
            device_name,
            driver,
        })))
    }
}

/// The parent's conversation with one loopback worker thread.
struct LoopbackLink {
    /// The host's stdin; dropping it — the kill — is also the shutdown
    /// signal, after which the host thread exits and the event stream ends.
    stdin: Option<PipeWriter>,
    events: Receiver<Result<ToParent>>,
    host: Option<JoinHandle<()>>,
    reader: Option<JoinHandle<()>>,
    /// The device the host reported at the handshake.
    device_name: String,
    /// The driver version the host reported at the handshake.
    driver: String,
}

impl WorkerLink for LoopbackLink {
    fn device_name(&self) -> &str {
        &self.device_name
    }

    fn driver(&self) -> &str {
        &self.driver
    }

    fn assign(&mut self, assignment: &Assignment) -> Result<()> {
        let stdin = self.stdin.as_mut().ok_or_else(|| {
            Error::Transport("the loopback host's stdin is already closed".to_string())
        })?;
        write_frame(stdin, &ToChild::Assign(assignment.clone()).encode())
    }

    fn next(&mut self, deadline: Option<Instant>) -> Result<LinkEvent> {
        next_event(&self.events, deadline)
    }

    fn kill(&mut self) {
        // A thread cannot be killed the way a process can; closing its stdin
        // makes the host exit at its next frame boundary, and the event
        // stream then ends exactly as a dead process's would.
        self.stdin = None;
    }
}

impl Drop for LoopbackLink {
    fn drop(&mut self) {
        self.stdin = None;
        if let Some(host) = self.host.take() {
            let _ = host.join();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

/// An in-memory unidirectional pipe: `Write` chunks cross a channel to the
/// `Read` side. Dropping the writer ends the stream, mirroring a closed OS
/// pipe.
fn pipe() -> (PipeWriter, PipeReader) {
    let (tx, rx) = channel();
    (
        PipeWriter { tx },
        PipeReader {
            rx,
            pending: VecDeque::new(),
        },
    )
}

/// The write end: each `write` sends its buffer as one chunk.
struct PipeWriter {
    tx: Sender<Vec<u8>>,
}

impl Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tx
            .send(buf.to_vec())
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The read end: chunks queue until consumed; a disconnected channel with an
/// empty queue is end-of-stream.
struct PipeReader {
    rx: Receiver<Vec<u8>>,
    pending: VecDeque<u8>,
}

impl Read for PipeReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pending.is_empty() {
            match self.rx.recv() {
                Ok(chunk) => self.pending.extend(chunk),
                Err(_) => return Ok(0),
            }
        }
        let n = buf.len().min(self.pending.len());
        for slot in buf.iter_mut().take(n) {
            *slot = self.pending.pop_front().expect("n is bounded by len");
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sima_contracts::{DeviceClass, Outcome};
    use sima_core::hash_bytes;
    use sima_domains::{StubBehavior, StubExecutor, StubProgram, StubState};
    use sima_model::EnvironmentId;

    use super::*;

    /// A loopback transport hosting the stub executor under the given step
    /// cadence. The stub uses no device, so it reports no device name.
    fn stub_transport(steps: Option<NonZeroU64>) -> LoopbackTransport {
        LoopbackTransport::new(
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            steps,
            Arc::new(|_, _| {
                let executor: Box<dyn Executor> = Box::new(StubExecutor::new()?);
                Ok((executor, String::new(), String::new()))
            }),
        )
    }

    /// An emitter that discards every event, for these link-level tests,
    /// which observe the link's frames rather than its emissions.
    fn discarding_emitter() -> Emitter {
        // The channel's receiver is dropped at the end of this expression, so
        // the emitter holds a sender with no receiver and each send is a no-op.
        Emitter::from(channel().0)
    }

    /// An assignment over a stub program.
    fn assignment(behavior: StubBehavior, checkpointing: bool) -> Assignment {
        Assignment {
            spec: StubProgram { behavior, nonce: 7 }.to_bytes(),
            params: vec![1, 2, 3],
            seed: 42,
            environment: EnvironmentId::from_hash(hash_bytes(b"env")),
            input_state: None,
            resume: None,
            attempt: 0,
            worker: 0,
            checkpointing,
        }
    }

    #[test]
    fn a_host_speaking_another_version_is_refused_as_a_mismatch() {
        // Every transport refuses a version mismatch the same way, and says
        // which two versions disagree rather than reporting an unexpected
        // message.
        let error = subprocess::ready_desc(
            "loopback host",
            Some(Ok(ToParent::Ready {
                protocol: crate::protocol::PROTOCOL_VERSION - 1,
                device_name: String::new(),
                driver: String::new(),
            })),
        )
        .expect_err("a host at another version");
        let Error::Transport(message) = error else {
            panic!("expected a transport error");
        };
        assert!(
            message.contains("loopback host protocol version mismatch"),
            "{message}"
        );
        assert!(message.contains("loopback host speaks"), "{message}");
    }

    #[test]
    fn a_spawn_binding_reaches_the_resolver() -> Result<()> {
        // The loopback's executors ignore the binding; what it proves is that
        // the parameter travels the real handshake to the host's resolver.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let transport = LoopbackTransport::new(
            FormatId::new("stub.v1")?,
            Duration::MAX,
            None,
            Arc::new(move |_, device: Option<&DeviceBinding>| {
                recorder
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(device.cloned());
                let executor: Box<dyn Executor> = Box::new(StubExecutor::new()?);
                Ok((executor, "loopback device".to_string(), String::new()))
            }),
        );
        let binding = DeviceBinding {
            class: DeviceClass::new("8086:7d51").expect("class id"),
            member: 0,
        };
        // The handshake completes inside spawn, so the resolver has already run.
        let link = transport
            .spawn(0, Some(&binding), discarding_emitter())?
            .into_link();
        assert_eq!(link.device_name(), "loopback device");
        assert_eq!(
            *seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![Some(binding)]
        );
        Ok(())
    }

    /// The stub trajectory from `{step: 0, acc: seed}` through `steps` steps.
    fn folded_state(seed: u64, steps: u64) -> StubState {
        let mut state = StubState { step: 0, acc: seed };
        for _ in 0..steps {
            state.acc = sima_core::prng::derive(state.acc, state.step);
            state.step += 1;
        }
        state
    }

    #[test]
    fn a_task_round_trips_with_saves_in_order() -> Result<()> {
        // Three accumulate steps under a save-every-offer cadence: the link
        // yields the three saves in step order, then the outcome.
        let transport = stub_transport(NonZeroU64::new(1));
        let mut link = transport.spawn(0, None, discarding_emitter())?.into_link();
        link.assign(&assignment(StubBehavior::Accumulate(3), true))?;
        for step in 1..=3u64 {
            match link.next(None)? {
                LinkEvent::Save(bytes) => {
                    assert_eq!(bytes, folded_state(42, step).to_bytes(), "save {step}");
                }
                other => panic!("expected Save at step {step}, got {other:?}"),
            }
        }
        match link.next(None)? {
            LinkEvent::Done(Outcome::Completed { artifacts, .. }) => {
                assert_eq!(artifacts[0].bytes, folded_state(42, 3).to_bytes());
            }
            other => panic!("expected Done(Completed), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn the_same_link_executes_a_second_task() -> Result<()> {
        // The worker is long-lived: one link, two assignments, two outcomes.
        let transport = stub_transport(None);
        let mut link = transport.spawn(0, None, discarding_emitter())?.into_link();
        for _ in 0..2 {
            link.assign(&assignment(StubBehavior::Succeed, false))?;
            match link.next(None)? {
                LinkEvent::Done(Outcome::Completed { .. }) => {}
                other => panic!("expected Done(Completed), got {other:?}"),
            }
        }
        Ok(())
    }

    #[test]
    fn a_deadline_expiry_consumes_nothing_and_the_outcome_still_arrives() -> Result<()> {
        let transport = stub_transport(None);
        let mut link = transport.spawn(0, None, discarding_emitter())?.into_link();
        link.assign(&assignment(StubBehavior::Sleep(300), false))?;
        // The deadline lands mid-sleep: expiry, with nothing consumed.
        match link.next(Some(Instant::now() + Duration::from_millis(30)))? {
            LinkEvent::DeadlineExpired => {}
            other => panic!("expected DeadlineExpired, got {other:?}"),
        }
        // The attempt is still running; an unbounded wait yields its outcome.
        match link.next(None)? {
            LinkEvent::Done(Outcome::Completed { .. }) => {}
            other => panic!("expected Done(Completed), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn a_panicking_executor_surfaces_panicked() -> Result<()> {
        let transport = stub_transport(None);
        let mut link = transport.spawn(0, None, discarding_emitter())?.into_link();
        link.assign(&assignment(StubBehavior::Panic, false))?;
        match link.next(None)? {
            LinkEvent::Panicked(reason) => {
                assert!(reason.contains("programmed panic"), "{reason}");
            }
            other => panic!("expected Panicked, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn a_kill_ends_the_event_stream_with_died() -> Result<()> {
        let transport = stub_transport(None);
        let mut link = transport.spawn(0, None, discarding_emitter())?.into_link();
        // The kill closes the host's stdin; the host exits, its writer drops,
        // and the event stream ends — the same signal a dead process leaves.
        link.kill();
        match link.next(None)? {
            LinkEvent::Died(_) => {}
            other => panic!("expected Died, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn assigning_to_a_killed_worker_is_an_error() -> Result<()> {
        let transport = stub_transport(None);
        let mut link = transport.spawn(0, None, discarding_emitter())?.into_link();
        link.kill();
        assert!(
            link.assign(&assignment(StubBehavior::Succeed, false))
                .is_err(),
            "an assign onto a closed pipe must error"
        );
        Ok(())
    }
}
