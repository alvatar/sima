//! [`SubprocessTransport`]: the production transport — one OS process per
//! worker.
//!
//! Each spawn pipes the child's stdin, stdout, and stderr, and performs the
//! handshake. A reader thread per child decodes stdout frames into a
//! channel, so the caller's deadline wait is a plain `recv_timeout` and a
//! kill never races a blocking read; Event frames fork off to the run's
//! emitter on that thread. A second thread per child captures stderr line by
//! line and emits each line as an info diagnostic attributed to the worker
//! and host, so nothing a child prints lands uncorrelated.
//! Process isolation is what makes preemption enforceable: `kill` is SIGKILL
//! plus reap, and a child's death — for any reason — surfaces as the channel
//! closing, which [`WorkerLink::next`] reports as [`LinkEvent::Died`].

use std::io::Read;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result, read_frame, write_frame};
use sima_model::FormatId;
use sima_trace::{Emitter, Event, Level};

use crate::link::{LinkEvent, WorkerLink, WorkerTransport};
use crate::protocol::{Assignment, Hello, PROTOCOL_VERSION, ToChild, ToParent};

/// Spawns worker processes for one run: the command vector to run — a program
/// and its arguments — plus the handshake every child receives, the run's
/// format id and checkpoint cadence.
///
/// The command vector is `sima-worker` with no arguments for a local worker,
/// and a wrapper that ultimately execs a worker for anything longer-lived: the
/// arguments carry the whole invocation, so the same spawn, handshake, and
/// kill machinery serves a bare local child and a container client alike.
pub struct SubprocessTransport {
    program: PathBuf,
    args: Vec<String>,
    hello: Hello,
}

impl SubprocessTransport {
    /// A transport spawning `program args...` for a run over `format` with the
    /// given checkpoint cadence ([`Duration::MAX`] and `None` disable an axis).
    /// A local worker passes an empty argument vector.
    pub fn new(
        program: PathBuf,
        args: Vec<String>,
        format: FormatId,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<NonZeroU64>,
    ) -> SubprocessTransport {
        SubprocessTransport {
            program,
            args,
            hello: hello(format, checkpoint_interval, checkpoint_interval_steps),
        }
    }
}

/// The handshake frame for a run's settings, in the wire's cadence encoding:
/// a disabled wall-clock axis is `u64::MAX` milliseconds — an interval too
/// large for the u64 saturates there, since a cadence beyond the address
/// space of milliseconds is disabled in effect — and a disabled step axis
/// is `0`.
///
/// The worker id and device are left unbound: they vary per worker, so each
/// spawn sets them on its own copy of this frame.
pub(crate) fn hello(
    format: FormatId,
    checkpoint_interval: Duration,
    checkpoint_interval_steps: Option<NonZeroU64>,
) -> Hello {
    let checkpoint_interval_ms = if checkpoint_interval == Duration::MAX {
        u64::MAX
    } else {
        u64::try_from(checkpoint_interval.as_millis()).unwrap_or(u64::MAX)
    };
    Hello {
        protocol: PROTOCOL_VERSION,
        worker: 0,
        format,
        checkpoint_interval_ms,
        checkpoint_interval_steps: checkpoint_interval_steps.map_or(0, NonZeroU64::get),
        device: None,
    }
}

impl WorkerTransport for SubprocessTransport {
    fn spawn(
        &self,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<Box<dyn WorkerLink>> {
        // The subprocess transport runs on this machine, so its diagnostics
        // carry the local pool's empty host label.
        let context = EventContext {
            events,
            worker,
            host: String::new(),
        };
        spawn_worker(
            &self.program,
            &self.args,
            &self.hello,
            worker,
            device,
            context,
        )
    }
}

/// Attribution for one child's reader threads: the run's emitter plus the
/// identity the parent knows about the child — its slot's worker id and the
/// pool's host label (empty for a local pool).
#[derive(Clone)]
pub(crate) struct EventContext {
    pub(crate) events: Emitter,
    pub(crate) worker: u64,
    pub(crate) host: String,
}

/// Spawns `program args...` as a worker child, pipes its stdio, runs the
/// reader thread, and performs the handshake bound to `device`. The returned
/// link owns the child; a handshake failure kills and reaps it before the
/// error returns. Shared by every transport that runs a worker over a local
/// process — a bare `sima-worker` or a container client wrapping one.
pub(crate) fn spawn_worker(
    program: &Path,
    args: &[String],
    hello: &Hello,
    worker: u64,
    device: Option<&DeviceBinding>,
    context: EventContext,
) -> Result<Box<dyn WorkerLink>> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            Error::Transport(format!("spawning worker {} failed: {e}", program.display()))
        })?;
    // The pipes exist iff the spawn configured them; taking them cannot
    // fail past a successful spawn.
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::Transport("the spawned worker has no piped stdin".to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Transport("the spawned worker has no piped stdout".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::Transport("the spawned worker has no piped stderr".to_string()))?;
    let (sender, events) = channel();
    let stdout_context = context.clone();
    let reader = std::thread::spawn(move || read_events(stdout, sender, Some(stdout_context)));
    let stderr_reader = std::thread::spawn(move || read_stderr(stderr, context));
    let mut link = SubprocessLink {
        child,
        stdin: Some(stdin),
        events,
        reader: Some(reader),
        stderr_reader: Some(stderr_reader),
        device_name: String::new(),
        driver: String::new(),
    };
    // The handshake: Hello out, Ready back. Any other answer — silence ended
    // by death, a wrong version, an undecodable echo — is a spawn failure, and
    // the misbehaving child is killed and reaped before the error returns.
    let hello = Hello {
        worker,
        device: device.copied(),
        ..hello.clone()
    };
    match handshake(&mut link, &hello) {
        Ok((device_name, driver)) => {
            link.device_name = device_name;
            link.driver = driver;
        }
        Err(e) => {
            link.kill();
            return Err(e);
        }
    }
    Ok(Box::new(link))
}

/// Performs the parent's half of the handshake over a fresh link, returning
/// the device name and driver version the worker reported.
fn handshake(link: &mut SubprocessLink, hello: &Hello) -> Result<(String, String)> {
    link.write(&ToChild::Hello(hello.clone()))?;
    ready_desc("worker", link.events.recv().ok())
}

/// Classifies a peer's answer to `Hello`: the device name and driver version
/// it reported, or why the handshake failed. `answer` is `None` when the event
/// stream ended first.
///
/// The parent's half of the handshake, shared by every transport and pure over
/// the answer, so each refusal is verifiable without a peer to produce it.
/// `peer` names the far side in the diagnostics.
pub(crate) fn ready_desc(peer: &str, answer: Option<Result<ToParent>>) -> Result<(String, String)> {
    match answer {
        Some(Ok(ToParent::Ready {
            protocol,
            device_name,
            driver,
        })) if protocol == PROTOCOL_VERSION => Ok((device_name, driver)),
        // A Ready at another version is a version mismatch, not an unexpected
        // message: say which two versions disagree.
        Some(Ok(ToParent::Ready { protocol, .. })) => Err(Error::Transport(format!(
            "{peer} protocol version mismatch: parent speaks {PROTOCOL_VERSION}, \
             {peer} speaks {protocol}"
        ))),
        Some(Ok(other)) => Err(Error::Transport(format!(
            "expected Ready from the {peer}, got {other:?}"
        ))),
        Some(Err(e)) => Err(e),
        None => Err(Error::Transport(format!(
            "the {peer} exited before completing the handshake"
        ))),
    }
}

/// The live conversation with one child process. The link owns the child's
/// stdin and the process handle; the reader thread owns its stdout.
struct SubprocessLink {
    child: Child,
    /// The child's stdin; dropping it is the shutdown signal.
    stdin: Option<ChildStdin>,
    events: Receiver<Result<ToParent>>,
    reader: Option<JoinHandle<()>>,
    /// The stderr capture thread; it exits on the pipe's EOF, which the
    /// child's death closes.
    stderr_reader: Option<JoinHandle<()>>,
    /// The device the child reported, set once the handshake answers.
    device_name: String,
    /// The driver version the child reported, set once the handshake answers.
    driver: String,
}

impl SubprocessLink {
    /// Writes one frame to the child's stdin; a closed pipe is `Err`.
    fn write(&mut self, message: &ToChild) -> Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| Error::Transport("the worker's stdin is already closed".to_string()))?;
        write_frame(stdin, &message.encode())
    }
}

impl WorkerLink for SubprocessLink {
    fn device_name(&self) -> &str {
        &self.device_name
    }

    fn driver(&self) -> &str {
        &self.driver
    }

    fn assign(&mut self, assignment: &Assignment) -> Result<()> {
        self.write(&ToChild::Assign(assignment.clone()))
    }

    fn next(&mut self, deadline: Option<Instant>) -> Result<LinkEvent> {
        match next_event(&self.events, deadline)? {
            // The event stream ended: reap the child so the death carries
            // its exit status or signal.
            LinkEvent::Died(_) => Ok(LinkEvent::Died(self.reap())),
            event => Ok(event),
        }
    }

    fn kill(&mut self) {
        // Close stdin first so a child mid-read is not owed anything, then
        // SIGKILL and reap. Errors are ignored: the child may already be
        // dead, which is the state this call exists to guarantee.
        self.stdin = None;
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.join_reader();
    }
}

/// How long the graceful shutdown waits for the child to exit on the
/// stdin-close signal before escalating to SIGKILL.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

impl SubprocessLink {
    /// Reaps the child gracefully: closing stdin is the shutdown signal, the
    /// child exits on end-of-stream, and the parent collects it. A child
    /// that does not exit within [`SHUTDOWN_GRACE`] is killed. Idempotent —
    /// a reaped child's cached status is returned. The returned string
    /// describes the death: an exit status or a signal.
    fn reap(&mut self) -> String {
        self.stdin = None;
        let waited = Instant::now();
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return status.to_string(),
                Ok(None) if waited.elapsed() < SHUTDOWN_GRACE => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                // Out of grace, or the wait itself failed: force the exit.
                Ok(None) | Err(_) => {
                    let _ = self.child.kill();
                    return match self.child.wait() {
                        Ok(status) => status.to_string(),
                        Err(e) => format!("unreapable: {e}"),
                    };
                }
            }
        }
    }

    /// Joins the reader threads; they exit when the child's stdout and
    /// stderr end, which a reaped child's already have.
    fn join_reader(&mut self) {
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
    }
}

impl Drop for SubprocessLink {
    /// The graceful shutdown at the end of the worker's life: reap the child
    /// off the stdin-close signal, then collect the reader thread.
    fn drop(&mut self) {
        self.reap();
        self.join_reader();
    }
}

/// Decodes one child-to-parent frame stream into `events` until it ends.
/// Runs on the per-child reader thread: end-of-stream simply ends the
/// thread — the dropped sender is the death signal the link observes — and
/// a torn frame or an undecodable payload is sent as the stream's final
/// `Err` before the thread ends.
pub(crate) fn read_events(
    mut reader: impl Read,
    events: Sender<Result<ToParent>>,
    context: Option<EventContext>,
) {
    loop {
        let message = match read_frame(&mut reader) {
            Ok(Some(payload)) => match ToParent::decode(&payload) {
                // Event frames belong to the collector, never to the lease
                // loop: forward them to the run's emitter and keep reading.
                Ok(ToParent::Event(bytes)) => {
                    if let Some(context) = &context {
                        forward_event(context, &bytes);
                    }
                    continue;
                }
                message => message,
            },
            Ok(None) => return,
            Err(e) => Err(e),
        };
        let failed = message.is_err();
        // A send failure means the link is gone; nothing is owed.
        if events.send(message).is_err() || failed {
            return;
        }
    }
}

/// Parses one Event frame's bytes and emits the event, filling a
/// diagnostic's worker and host attribution where the child left them unset
/// — the child knows its worker id from the handshake but never the pool's
/// host label. Malformed bytes never kill the worker: they degrade to a
/// warning diagnostic naming the decode failure.
fn forward_event(context: &EventContext, bytes: &[u8]) {
    match serde_json::from_slice::<Event>(bytes) {
        Ok(mut event) => {
            if let Event::Diagnostic { worker, host, .. } = &mut event {
                if worker.is_none() {
                    *worker = Some(context.worker);
                }
                if host.is_none() {
                    *host = pool_host(context);
                }
            }
            context.events.emit(event);
        }
        Err(e) => context.events.emit(Event::Diagnostic {
            level: Level::Warn,
            source: "transport".to_string(),
            message: format!("undecodable event frame: {e}"),
            worker: Some(context.worker),
            host: pool_host(context),
            task: None,
        }),
    }
}

/// The host key of a diagnostic under `context`: the pool's label, or `None`
/// for a local pool, matching the journal's empty-host convention.
fn pool_host(context: &EventContext) -> Option<String> {
    (!context.host.is_empty()).then(|| context.host.clone())
}

/// How many bytes of one captured stderr line a diagnostic carries; the rest
/// is dropped, with the truncation noted by a trailing marker.
const STDERR_LINE_CAP: usize = 4096;

/// Consumes a child's stderr line by line until EOF — the child's death
/// closes the pipe — emitting each line as an info diagnostic attributed to
/// the worker and host. Runs on its own thread per child; a read error ends
/// the capture the same way EOF does. Blank lines carry nothing and are
/// skipped; invalid UTF-8 is replaced, not refused — this is capture, and
/// capture never fails the worker.
fn read_stderr(stderr: impl Read, context: EventContext) {
    let mut reader = std::io::BufReader::new(stderr);
    let mut line = Vec::new();
    loop {
        line.clear();
        match std::io::BufRead::read_until(&mut reader, b'\n', &mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        while line.last().is_some_and(|b| *b == b'\n' || *b == b'\r') {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        let truncated = line.len() > STDERR_LINE_CAP;
        if truncated {
            line.truncate(STDERR_LINE_CAP);
        }
        let mut message = String::from_utf8_lossy(&line).into_owned();
        if truncated {
            message.push_str(" [truncated]");
        }
        context.events.emit(Event::Diagnostic {
            level: Level::Info,
            source: "worker stderr".to_string(),
            message,
            worker: Some(context.worker),
            host: pool_host(&context),
            task: None,
        });
    }
}

/// The generic death event a closed event stream maps to. A link that can
/// observe the actual exit status replaces the description with it.
fn died() -> LinkEvent {
    LinkEvent::Died("the worker's event stream ended".to_string())
}

/// Maps one received channel event to a [`LinkEvent`] for [`WorkerLink::next`]:
/// waits up to `deadline` when given, reporting expiry and channel
/// disconnection — the child's death — as events. A frame violation or an
/// unexpected `Ready` is `Err`; the caller kills the child.
pub(crate) fn next_event(
    events: &Receiver<Result<ToParent>>,
    deadline: Option<Instant>,
) -> Result<LinkEvent> {
    let received = match deadline {
        Some(deadline) => {
            // A deadline already past still polls the queue once: an outcome
            // that beat the deadline is preferred over expiring it.
            let timeout = deadline.saturating_duration_since(Instant::now());
            match events.recv_timeout(timeout) {
                Ok(message) => message,
                Err(RecvTimeoutError::Timeout) => return Ok(LinkEvent::DeadlineExpired),
                Err(RecvTimeoutError::Disconnected) => return Ok(died()),
            }
        }
        None => match events.recv() {
            Ok(message) => message,
            Err(_) => return Ok(died()),
        },
    };
    Ok(match received? {
        ToParent::Save(bytes) => LinkEvent::Save(bytes),
        ToParent::Done(outcome) => LinkEvent::Done(outcome),
        ToParent::Panicked(reason) => LinkEvent::Panicked(reason),
        ToParent::Fault(message) => LinkEvent::Fault(message),
        ToParent::Ready { .. } => {
            return Err(Error::Transport(
                "unexpected Ready after the handshake".to_string(),
            ));
        }
        // Event frames belong to the transport's reader thread, which
        // forwards them to the run's collector; one on the lease loop is a
        // routing violation.
        ToParent::Event(_) => {
            return Err(Error::Transport(
                "unexpected Event frame on the lease loop".to_string(),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A transport over the given program for the stub format with
    /// checkpointing disabled.
    fn transport(program: &str) -> SubprocessTransport {
        SubprocessTransport::new(
            PathBuf::from(program),
            Vec::new(),
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None,
        )
    }

    /// An emitter whose receiver is dropped: emissions vanish, which these
    /// spawn-failure tests do not observe.
    fn drop_emitter() -> Emitter {
        Emitter::from(channel().0)
    }

    /// The device name and driver a `Ready` at `protocol` resolves to, or the
    /// refusal.
    fn answer_ready(protocol: u32) -> Result<(String, String)> {
        ready_desc(
            "worker",
            Some(Ok(ToParent::Ready {
                protocol,
                device_name: "a device".to_string(),
                driver: "a driver".to_string(),
            })),
        )
    }

    #[test]
    fn a_ready_at_this_version_carries_the_device_through() -> Result<()> {
        assert_eq!(
            answer_ready(PROTOCOL_VERSION)?,
            ("a device".to_string(), "a driver".to_string())
        );
        Ok(())
    }

    #[test]
    fn a_ready_at_another_version_is_refused_naming_both_versions() {
        // The two binaries are built apart, so the mismatch is the one thing
        // the handshake exists to catch; the message names each side.
        let error = answer_ready(PROTOCOL_VERSION - 1).expect_err("a stale worker");
        let Error::Transport(message) = error else {
            panic!("expected a transport error");
        };
        assert!(message.contains("version mismatch"), "{message}");
        assert!(
            message.contains(&format!("parent speaks {PROTOCOL_VERSION}")),
            "names the parent's version: {message}"
        );
        assert!(
            message.contains(&format!("worker speaks {}", PROTOCOL_VERSION - 1)),
            "names the worker's version: {message}"
        );
    }

    #[test]
    fn an_answer_that_is_not_ready_is_refused() {
        let error = ready_desc("worker", Some(Ok(ToParent::Save(vec![1]))))
            .expect_err("the handshake takes Ready alone");
        assert!(matches!(error, Error::Transport(_)));
    }

    #[test]
    fn a_stream_that_ends_before_the_answer_is_refused() {
        // The child died during its own startup: no answer is coming.
        let error = ready_desc("worker", None).expect_err("nothing answered");
        let Error::Transport(message) = error else {
            panic!("expected a transport error");
        };
        assert!(message.contains("exited before completing the handshake"));
    }

    #[test]
    fn a_frame_violation_during_the_handshake_surfaces_verbatim() {
        let error = ready_desc(
            "worker",
            Some(Err(Error::Encoding("unknown tag 9".to_string()))),
        )
        .expect_err("the frame never decoded");
        assert!(matches!(error, Error::Encoding(_)), "{error:?}");
    }

    /// Runs `read_events` over pre-framed input, returning what reached the
    /// link channel; the caller observes emissions through its own receiver
    /// behind the context's emitter.
    fn read_framed(frames: &[ToParent], context: EventContext) -> Vec<Result<ToParent>> {
        let mut pipe = Vec::new();
        for frame in frames {
            write_frame(&mut pipe, &frame.encode()).expect("frame the input");
        }
        let (sender, link_events) = channel();
        read_events(pipe.as_slice(), sender, Some(context));
        link_events.into_iter().collect()
    }

    #[test]
    fn a_malformed_event_frame_degrades_to_a_warning_and_the_stream_continues() {
        let (tx, emitted) = channel();
        let context = EventContext {
            events: Emitter::from(tx),
            worker: 9,
            host: "gpubox".to_string(),
        };
        let link_events = read_framed(
            &[
                ToParent::Event(b"not json".to_vec()),
                ToParent::Save(vec![1]),
            ],
            context,
        );
        // The malformed frame degraded to a warning naming the failure...
        let events: Vec<Event> = emitted.into_iter().collect();
        assert_eq!(events.len(), 1, "{events:?}");
        let Event::Diagnostic {
            level,
            source,
            message,
            worker,
            host,
            task,
        } = &events[0]
        else {
            panic!("expected a diagnostic, got {:?}", events[0]);
        };
        assert_eq!(*level, Level::Warn);
        assert_eq!(source, "transport");
        assert!(message.contains("undecodable event frame"), "{message}");
        assert_eq!(*worker, Some(9));
        assert_eq!(host.as_deref(), Some("gpubox"));
        assert_eq!(*task, None);
        // ...and the following frame still reached the lease loop: the
        // worker is never killed over an observational line.
        assert!(
            matches!(link_events.as_slice(), [Ok(ToParent::Save(bytes))] if bytes == &[1]),
            "{link_events:?}"
        );
    }

    #[test]
    fn a_forwarded_diagnostic_gains_the_attribution_the_child_left_unset() {
        let unattributed = Event::Diagnostic {
            level: Level::Info,
            source: "worker stderr".to_string(),
            message: "from the child".to_string(),
            worker: None,
            host: None,
            task: None,
        };
        let bytes = serde_json::to_vec(&unattributed).expect("serialize the event");
        let (tx, emitted) = channel();
        let context = EventContext {
            events: Emitter::from(tx),
            worker: 4,
            host: "gpubox".to_string(),
        };
        read_framed(&[ToParent::Event(bytes)], context);
        let events: Vec<Event> = emitted.into_iter().collect();
        assert!(
            matches!(
                events.as_slice(),
                [Event::Diagnostic { worker: Some(4), host: Some(host), .. }] if host == "gpubox"
            ),
            "{events:?}"
        );
    }

    #[test]
    fn a_missing_program_is_a_clean_spawn_error_naming_the_path() {
        let result = transport("/nonexistent/sima-worker").spawn(0, None, drop_emitter());
        let error = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("spawning a missing program must fail"),
        };
        assert!(
            error.contains("/nonexistent/sima-worker"),
            "the error names the searched path: {error}"
        );
    }

    #[test]
    fn a_program_that_is_not_a_worker_fails_the_handshake() {
        // cat echoes the Hello frame back; the echoed payload decodes as a
        // malformed child message, so the handshake fails cleanly instead of
        // hanging or panicking.
        let result = transport("/bin/cat").spawn(0, None, drop_emitter());
        assert!(result.is_err(), "the handshake against cat must fail");
    }
}
