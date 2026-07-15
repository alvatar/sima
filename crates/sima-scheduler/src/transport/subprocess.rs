//! [`SubprocessTransport`]: the production transport — one OS process per
//! worker.
//!
//! Each spawn pipes the child's stdin and stdout, inherits stderr for
//! human-readable diagnostics, and performs the handshake. A reader thread
//! per child decodes stdout frames into a channel, so the caller's deadline
//! wait is a plain `recv_timeout` and a kill never races a blocking read.
//! Process isolation is what makes preemption enforceable: `kill` is SIGKILL
//! plus reap, and a child's death — for any reason — surfaces as the channel
//! closing, which [`WorkerLink::next`] reports as [`LinkEvent::Died`].

use std::io::Read;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use sima_core::{Error, Result};
use sima_model::FormatId;

use super::link::{LinkEvent, WorkerLink, WorkerTransport};
use super::protocol::{
    Assignment, Hello, PROTOCOL_VERSION, ToChild, ToParent, read_frame, write_frame,
};

/// Spawns `sima-worker` processes for one run: the worker binary's path plus
/// the handshake every child receives — the run's format id and checkpoint
/// cadence.
pub struct SubprocessTransport {
    program: PathBuf,
    hello: Hello,
}

impl SubprocessTransport {
    /// A transport spawning `program` for a run over `format` with the given
    /// checkpoint cadence ([`Duration::MAX`] and `None` disable an axis).
    pub fn new(
        program: PathBuf,
        format: FormatId,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<NonZeroU64>,
    ) -> SubprocessTransport {
        SubprocessTransport {
            program,
            hello: hello(format, checkpoint_interval, checkpoint_interval_steps),
        }
    }
}

/// The handshake frame for a run's settings, in the wire's cadence encoding:
/// a disabled wall-clock axis is `u64::MAX` milliseconds — an interval too
/// large for the u64 saturates there, since a cadence beyond the address
/// space of milliseconds is disabled in effect — and a disabled step axis
/// is `0`.
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
        format,
        checkpoint_interval_ms,
        checkpoint_interval_steps: checkpoint_interval_steps.map_or(0, NonZeroU64::get),
    }
}

impl WorkerTransport for SubprocessTransport {
    fn spawn(&self) -> Result<Box<dyn WorkerLink>> {
        let mut child = Command::new(&self.program)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| {
                Error::Transport(format!(
                    "spawning worker {} failed: {e}",
                    self.program.display()
                ))
            })?;
        // The pipes exist iff the spawn configured them; taking them cannot
        // fail past a successful spawn.
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Transport("the spawned worker has no piped stdin".to_string()))?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::Transport("the spawned worker has no piped stdout".to_string())
        })?;
        let (sender, events) = channel();
        let reader = std::thread::spawn(move || read_events(stdout, sender));
        let mut link = SubprocessLink {
            child,
            stdin: Some(stdin),
            events,
            reader: Some(reader),
        };
        // The handshake: Hello out, Ready back. Any other answer — silence
        // ended by death, a wrong version, an undecodable echo — is a spawn
        // failure, and the misbehaving child is killed and reaped before the
        // error returns.
        if let Err(e) = handshake(&mut link, &self.hello) {
            link.kill();
            return Err(e);
        }
        Ok(Box::new(link))
    }
}

/// Performs the parent's half of the handshake over a fresh link.
fn handshake(link: &mut SubprocessLink, hello: &Hello) -> Result<()> {
    link.write(&ToChild::Hello(hello.clone()))?;
    match link.events.recv() {
        Ok(Ok(ToParent::Ready { protocol })) if protocol == PROTOCOL_VERSION => Ok(()),
        Ok(Ok(ToParent::Ready { protocol })) => Err(Error::Transport(format!(
            "worker protocol version mismatch: parent speaks {PROTOCOL_VERSION}, worker speaks {protocol}"
        ))),
        Ok(Ok(other)) => Err(Error::Transport(format!(
            "expected Ready from the worker, got {other:?}"
        ))),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Error::Transport(
            "the worker exited before completing the handshake".to_string(),
        )),
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

    /// Joins the reader thread; it exits when the child's stdout ends, which
    /// a reaped child's already has.
    fn join_reader(&mut self) {
        if let Some(reader) = self.reader.take() {
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
pub(crate) fn read_events(mut reader: impl Read, events: Sender<Result<ToParent>>) {
    loop {
        let message = match read_frame(&mut reader) {
            Ok(Some(payload)) => ToParent::decode(&payload),
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
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None,
        )
    }

    #[test]
    fn a_missing_program_is_a_clean_spawn_error_naming_the_path() {
        let result = transport("/nonexistent/sima-worker").spawn();
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
        let result = transport("/bin/cat").spawn();
        assert!(result.is_err(), "the handshake against cat must fail");
    }
}
