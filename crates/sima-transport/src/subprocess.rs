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
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result, read_frame, write_frame};
use sima_trace::{Emitter, Event, Level};
use tempfile::TempDir;

use crate::answer_deadline::receive_within;
use crate::link::{LinkEvent, SpawnOutcome, WorkerLink, WorkerTransport};
use crate::protocol::{Assignment, Hello, PROTOCOL_VERSION, ToChild, ToParent, encode_assign};
use crate::spawn_settings::SpawnSettings;

/// Spawns worker processes for one run: the command vector to run — a program
/// and its arguments — plus the settings every child of this pool is spawned
/// and greeted under.
///
/// The command vector is `sima-worker` with no arguments for a local worker,
/// and a wrapper that ultimately execs a worker for anything longer-lived: the
/// arguments carry the whole invocation, so the same spawn, handshake, and
/// kill machinery serves a bare local child and a container client alike.
pub struct SubprocessTransport {
    program: PathBuf,
    args: Vec<String>,
    settings: SpawnSettings,
}

impl SubprocessTransport {
    /// A transport spawning `program args...` under `settings`. A local worker
    /// passes an empty argument vector.
    pub fn new(
        program: PathBuf,
        args: Vec<String>,
        settings: SpawnSettings,
    ) -> SubprocessTransport {
        SubprocessTransport {
            program,
            args,
            settings,
        }
    }
}

impl WorkerTransport for SubprocessTransport {
    fn spawn(
        &self,
        worker: u64,
        device: Option<&DeviceBinding>,
        events: Emitter,
    ) -> Result<SpawnOutcome> {
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
            &self.settings,
            worker,
            device,
            context,
        )
        .map(SpawnOutcome::Link)
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

/// Spawns `program args...` as a worker child under `settings`, pipes its
/// stdio, runs the reader thread, and performs the handshake bound to
/// `device`. The returned link owns the child and the scratch directory an
/// explicit policy gave it; a handshake failure kills and reaps it before the
/// error returns. Shared by every transport that runs a worker over a local
/// process — a bare `sima-worker` or a container client wrapping one.
pub(crate) fn spawn_worker(
    program: &Path,
    args: &[String],
    settings: &SpawnSettings,
    worker: u64,
    device: Option<&DeviceBinding>,
    context: EventContext,
) -> Result<Box<dyn WorkerLink>> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let scratch = settings.policy.apply(&mut command, std::env::vars_os)?;
    let mut child = command.spawn().map_err(|e| {
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
        scratch,
        stdin: Some(stdin),
        events,
        reader: Some(reader),
        stderr_reader: Some(stderr_reader),
        device_name: String::new(),
        driver: String::new(),
    };
    // The handshake: Hello out, Ready back. Any other answer — silence ended
    // by death, silence outlasting the answer deadline, a wrong version, an
    // undecodable echo — is a spawn failure, and the misbehaving child is
    // killed and reaped before the error returns.
    let hello = Hello {
        worker,
        device: device.cloned(),
        ..settings.hello.clone()
    };
    match handshake(&mut link, &hello, settings.answer_timeout, program) {
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
///
/// The wait is startup plus one answer, so `answer_timeout` bounds it: a
/// child wedged before `Ready` — a broken driver hanging device
/// initialization, a wrapper that never execs — is a spawn failure naming the
/// program rather than a worker thread stopped forever.
fn handshake(
    link: &mut SubprocessLink,
    hello: &Hello,
    answer_timeout: Duration,
    program: &Path,
) -> Result<(String, String)> {
    link.write(&ToChild::Hello(hello.clone()))?;
    match receive_within(&link.events, answer_timeout) {
        Ok(answer) => ready_desc("worker", Some(answer)),
        Err(RecvTimeoutError::Timeout) => Err(Error::Transport(format!(
            "the worker {} exceeded the {}ms answer deadline awaiting Ready",
            program.display(),
            answer_timeout.as_millis()
        ))),
        Err(RecvTimeoutError::Disconnected) => ready_desc("worker", None),
    }
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
            ..
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
    /// The scratch working directory an explicit spawn gave the child, held so
    /// it lives exactly as long as the process; `None` under an inheriting
    /// policy. Cleared once the child is reaped, so the directory is removed
    /// with nothing still writing into it.
    scratch: Option<TempDir>,
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
        self.write_payload(&message.encode())
    }

    /// Writes one already-encoded frame payload to the child's stdin.
    fn write_payload(&mut self, payload: &[u8]) -> Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| Error::Transport("the worker's stdin is already closed".to_string()))?;
        write_frame(stdin, payload)
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
        // Encoded from the borrow: an assignment carries the candidate's state,
        // which for a grid domain is megabytes, and it would be copied once per
        // attempt only to be wrapped in a message value and dropped.
        self.write_payload(&encode_assign(assignment))
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
        // The reader threads are released rather than joined. They end when
        // the child's pipes close, which is when the last holder of them
        // exits — the child, and anything it left running behind it. Waiting
        // on them would put the caller back at the mercy of the process this
        // call exists to stop; each thread ends on its own, with nothing owed
        // to it.
        self.reader = None;
        self.stderr_reader = None;
        // The child is gone, so the directory it ran in goes with it.
        self.scratch = None;
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
    /// off the stdin-close signal, collect the reader thread, then remove the
    /// scratch directory the child ran in.
    fn drop(&mut self) {
        self.reap();
        self.join_reader();
        self.scratch = None;
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

/// Assembles a stderr byte stream into lines, retaining at most
/// [`STDERR_LINE_CAP`] bytes of any one line: the bytes of an overlong line
/// past the cap are discarded as they arrive, so a child that streams without
/// newlines costs a bounded buffer, never one that grows for the life of the
/// run.
struct LineCapture {
    /// The retained prefix of the line being assembled; never past the cap.
    retained: Vec<u8>,
    /// Whether the current line overflowed the cap and lost bytes.
    truncated: bool,
}

impl LineCapture {
    fn new() -> LineCapture {
        LineCapture {
            retained: Vec::new(),
            truncated: false,
        }
    }

    /// Feeds one chunk of the stream, calling `emit(line, truncated)` for
    /// each line the chunk completes.
    ///
    /// Only `\n` — or the end of the stream, via [`finish`](Self::finish) —
    /// terminates a line. A lone `\r` does not: carriage returns are how a
    /// progress bar repaints one logical line, and journaling every repaint
    /// would flood the journal with near-duplicates; the cap bounds what one
    /// such line retains instead.
    fn feed(&mut self, mut chunk: &[u8], emit: &mut impl FnMut(&[u8], bool)) {
        while let Some(newline) = chunk.iter().position(|byte| *byte == b'\n') {
            self.retain(&chunk[..newline]);
            self.complete(emit);
            chunk = &chunk[newline + 1..];
        }
        self.retain(chunk);
    }

    /// Flushes an unterminated final line at the end of the stream.
    fn finish(mut self, emit: &mut impl FnMut(&[u8], bool)) {
        self.complete(emit);
    }

    /// Keeps `bytes` up to the room left under the cap, marking the line
    /// truncated when anything is discarded.
    fn retain(&mut self, bytes: &[u8]) {
        let room = STDERR_LINE_CAP - self.retained.len();
        if bytes.len() > room {
            self.truncated = true;
        }
        self.retained
            .extend_from_slice(&bytes[..room.min(bytes.len())]);
    }

    /// Emits the assembled line and resets for the next one. A trailing `\r`
    /// run is part of the terminator (CRLF), stripped when the line fit —
    /// past the cap every retained byte is content. A line empty after
    /// stripping carries nothing and is skipped.
    fn complete(&mut self, emit: &mut impl FnMut(&[u8], bool)) {
        if !self.truncated {
            while self.retained.last() == Some(&b'\r') {
                self.retained.pop();
            }
        }
        if self.truncated || !self.retained.is_empty() {
            emit(&self.retained, self.truncated);
        }
        self.retained.clear();
        self.truncated = false;
    }
}

/// Consumes a child's stderr line by line until EOF — the child's death
/// closes the pipe — emitting each line as an info diagnostic attributed to
/// the worker and host. Runs on its own thread per child; a read error ends
/// the capture the same way EOF does, flushing what was retained. Invalid
/// UTF-8 is replaced with the Unicode replacement character — this is
/// capture, and capture never fails the worker.
fn read_stderr(stderr: impl Read, context: EventContext) {
    let mut reader = std::io::BufReader::new(stderr);
    let mut lines = LineCapture::new();
    let mut emit = |line: &[u8], truncated: bool| {
        let mut message = String::from_utf8_lossy(line).into_owned();
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
    };
    loop {
        let chunk = match std::io::BufRead::fill_buf(&mut reader) {
            Ok([]) | Err(_) => break,
            Ok(chunk) => chunk,
        };
        let consumed = chunk.len();
        lines.feed(chunk, &mut emit);
        std::io::BufRead::consume(&mut reader, consumed);
    }
    lines.finish(&mut emit);
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
    use std::num::NonZeroU64;

    use sima_model::FormatId;

    use super::*;
    use crate::spawn_policy::{SpawnPolicy, fixture};

    /// The settings a builtin worker of the stub format is spawned under:
    /// checkpointing disabled, and every answer awaited for as long as the
    /// child lives.
    fn settings(policy: SpawnPolicy) -> SpawnSettings {
        settings_within(policy, Duration::MAX)
    }

    /// The same settings under an explicit answer deadline.
    fn settings_within(policy: SpawnPolicy, answer_timeout: Duration) -> SpawnSettings {
        SpawnSettings::new(
            policy,
            answer_timeout,
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None::<NonZeroU64>,
        )
    }

    /// A transport over the given program for the stub format with
    /// checkpointing disabled, spawning the way a builtin worker is spawned.
    fn transport(program: &str) -> SubprocessTransport {
        SubprocessTransport::new(
            PathBuf::from(program),
            Vec::new(),
            settings(SpawnPolicy::Inherit),
        )
    }

    /// An emitter that discards every event, for these spawn-failure tests,
    /// which observe the spawn's error rather than its emissions.
    fn discarding_emitter() -> Emitter {
        // The channel's receiver is dropped at the end of this expression, so
        // the emitter holds a sender with no receiver and each send is a no-op.
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
                program: String::new(),
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

    /// Feeds `input` through `read_stderr` as one child's whole stderr
    /// stream, returning the emitted events.
    fn capture_stderr(input: &[u8]) -> Vec<Event> {
        let (tx, emitted) = channel();
        let context = EventContext {
            events: Emitter::from(tx),
            worker: 7,
            host: String::new(),
        };
        read_stderr(input, context);
        emitted.into_iter().collect()
    }

    /// The messages of `events`, all of which must be diagnostics.
    fn messages(events: &[Event]) -> Vec<&str> {
        events
            .iter()
            .map(|event| match event {
                Event::Diagnostic { message, .. } => message.as_str(),
                other => panic!("expected a diagnostic, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn an_unterminated_stream_past_the_cap_retains_at_most_the_cap() {
        let mut lines: Vec<(Vec<u8>, bool)> = Vec::new();
        let mut emit = |line: &[u8], truncated: bool| lines.push((line.to_vec(), truncated));
        let mut capture = LineCapture::new();
        // A newline-free stream many times the cap, arriving chunk by chunk:
        // the retained prefix stays at the cap while the rest is discarded.
        for _ in 0..8 {
            capture.feed(&[b'y'; STDERR_LINE_CAP], &mut emit);
            assert!(
                capture.retained.len() <= STDERR_LINE_CAP,
                "retained {} bytes",
                capture.retained.len()
            );
        }
        capture.finish(&mut emit);
        assert_eq!(lines.len(), 1, "{}", lines.len());
        assert_eq!(lines[0].0, [b'y'; STDERR_LINE_CAP]);
        assert!(lines[0].1, "the discarded tail is marked");
    }

    #[test]
    fn an_unterminated_stream_past_the_cap_yields_one_truncated_diagnostic() {
        let input = vec![b'x'; STDERR_LINE_CAP * 8];
        let events = capture_stderr(&input);
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
        assert_eq!(*level, Level::Info);
        assert_eq!(source, "worker stderr");
        assert_eq!(message.len(), STDERR_LINE_CAP + " [truncated]".len());
        assert!(message.ends_with(" [truncated]"), "{message}");
        assert_eq!(*worker, Some(7));
        assert_eq!(*host, None);
        assert_eq!(*task, None);
    }

    #[test]
    fn the_discarded_tail_of_an_overlong_line_ends_at_its_newline() {
        let mut input = vec![b'x'; STDERR_LINE_CAP * 2];
        input.extend_from_slice(b"\nsecond line\n");
        let events = capture_stderr(&input);
        let messages = messages(&events);
        assert_eq!(messages.len(), 2, "{messages:?}");
        assert!(messages[0].ends_with(" [truncated]"), "{}", messages[0]);
        assert_eq!(messages[1], "second line");
    }

    #[test]
    fn carriage_return_repaints_stay_one_line() {
        // A progress bar repaints with lone `\r`: one logical line, with the
        // terminating CRLF stripped and the interior returns kept verbatim.
        let events = capture_stderr(b"step 1\rstep 2\rstep 3\r\n");
        assert_eq!(messages(&events), ["step 1\rstep 2\rstep 3"]);
    }

    #[test]
    fn a_missing_program_is_a_clean_spawn_error_naming_the_path() {
        let result = transport("/nonexistent/sima-worker").spawn(0, None, discarding_emitter());
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
        let result = transport("/bin/cat").spawn(0, None, discarding_emitter());
        assert!(result.is_err(), "the handshake against cat must fail");
    }

    #[test]
    fn a_worker_silent_past_the_deadline_fails_the_spawn_naming_it() {
        // Every worker spawn is bounded, builtin or configured: a child
        // wedged before Ready — a broken driver hanging device
        // initialization — is a spawn failure rather than a worker thread
        // stopped forever. `exec` puts the sleep in the shell's place, so the
        // process holding the pipes is the one the kill reaches.
        let dir = tempfile::tempdir().expect("temp dir");
        let program = fixture::program(dir.path(), "wedged-worker.sh", "exec sleep 300");
        let transport = SubprocessTransport::new(
            program,
            Vec::new(),
            settings_within(SpawnPolicy::Inherit, Duration::from_millis(300)),
        );
        let started = Instant::now();
        let error = match transport.spawn(0, None, discarding_emitter()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a silent child completes no handshake"),
        };
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{:?}",
            started.elapsed()
        );
        assert!(error.contains("wedged-worker.sh"), "{error}");
        assert!(error.contains("Ready"), "names the answer: {error}");
        assert!(error.contains("300ms"), "names the deadline: {error}");
    }

    #[test]
    fn a_worker_on_an_explicit_surface_runs_in_a_scratch_directory_that_dies_with_it() {
        // The scratch directory's life is the child's: the program records
        // where it ran, and by the time the spawn returns — the child killed
        // and reaped over its refused handshake — that directory is gone.
        let dir = tempfile::tempdir().expect("temp dir");
        let report = dir.path().join("cwd");
        let program = fixture::cwd_reporting_program(dir.path(), &report);
        let transport = SubprocessTransport::new(
            program,
            Vec::new(),
            settings(SpawnPolicy::Explicit {
                passthrough: Vec::new(),
                prepend: Vec::new(),
            }),
        );
        assert!(
            transport.spawn(0, None, discarding_emitter()).is_err(),
            "a program that exits is no worker"
        );
        let scratch = fixture::reported_cwd(&report);
        assert_ne!(
            scratch,
            dir.path(),
            "the child ran in a directory of its own"
        );
        assert!(
            !scratch.exists(),
            "{} outlived its child",
            scratch.display()
        );
    }
}
