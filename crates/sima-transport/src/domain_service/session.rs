//! The parent's side of the domain service: [`DomainService`] holds the
//! conversation with one program for the life of a run.
//!
//! The program is spawned once, in its domain-service role, and answers every
//! question the run asks about its format: what environment its results depend
//! on, what devices its work runs on, how its configuration translates, and
//! what specs its generators produce. Holding the session open is what lets a
//! program pay its startup cost once.
//!
//! A failure the program renders crosses as [`Error::Reported`], so the
//! classification stays with the process that made it.
//!
//! A reader thread decodes the program's frames into a channel, so a question
//! waits on the channel rather than on the pipe: a program that goes silent
//! is bounded by the run's answer deadline instead of stopping the
//! orchestrator. [`DomainService::generate`] is the one question left
//! unbounded — it is computation proportional to the batch, the analog of a
//! task attempt rather than an answer.

use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::Duration;

use sima_contracts::DeviceInfo;
use sima_core::{Error, Result, read_frame, write_frame};
use sima_model::{Environment, FormatId, GeneratorId, Params, Spec};
use tempfile::TempDir;

use crate::answer_deadline::receive_within;
use crate::domain_service::protocol::{FromDomain, PROTOCOL_VERSION, ToDomain};
use crate::spawn_policy::SpawnPolicy;

/// The flag that puts a program in its domain-service role.
const SERVE_DOMAIN: &str = "--serve-domain";

/// One program, spawned to answer for one format.
#[derive(Debug)]
pub struct DomainService {
    child: Child,
    /// The scratch working directory a scrubbed spawn gave the program, held
    /// so it lives exactly as long as the session; `None` under an inheriting
    /// policy. Cleared once the program is reaped, so the directory is removed
    /// with nothing still writing into it.
    scratch: Option<TempDir>,
    /// The child's stdin; dropping it is the shutdown signal that follows the
    /// farewell.
    stdin: Option<ChildStdin>,
    /// The program's answers, as its reader thread decodes them.
    answers: Receiver<Result<FromDomain>>,
    /// The reader thread; it exits on the pipe's end, which the program's
    /// death closes.
    reader: Option<JoinHandle<()>>,
    /// The program, for diagnostics: a failure names which binary produced it.
    binary: PathBuf,
    /// How long each bounded question waits; [`Duration::MAX`] leaves every
    /// wait for as long as the program lives.
    answer_timeout: Duration,
}

impl DomainService {
    /// Spawns `binary` in its domain-service role for `format` under `policy`
    /// and completes the handshake, so a program that cannot be run, cannot
    /// speak this protocol version, or does not serve the format fails here
    /// rather than at the first question. `answer_timeout` bounds the
    /// handshake and every later question but `Generate`.
    pub fn spawn(
        binary: &Path,
        format: &FormatId,
        policy: &SpawnPolicy,
        answer_timeout: Duration,
    ) -> Result<DomainService> {
        let mut command = Command::new(binary);
        command
            .arg(SERVE_DOMAIN)
            .arg(format.as_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        let scratch = policy.apply(&mut command, std::env::vars())?;
        let mut child = command.spawn().map_err(|e| {
            Error::Transport(format!(
                "spawning the domain service {} failed: {e}",
                binary.display()
            ))
        })?;
        // The pipes exist iff the spawn configured them; taking them cannot
        // fail past a successful spawn.
        let stdin = child.stdin.take().ok_or_else(|| {
            Error::Transport("the spawned domain service has no piped stdin".to_string())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::Transport("the spawned domain service has no piped stdout".to_string())
        })?;
        let (sender, answers) = channel();
        let reader = std::thread::spawn(move || read_answers(stdout, &sender));
        let mut service = DomainService {
            child,
            scratch,
            stdin: Some(stdin),
            answers,
            reader: Some(reader),
            binary: binary.to_path_buf(),
            answer_timeout,
        };
        // The handshake: Hello out, Ready back. Any other answer is a spawn
        // failure, and the misbehaving program is killed and reaped before the
        // error returns. A program that refuses the format it was spawned for
        // exits at once, which the parent meets as a torn write or as an
        // unanswered read depending on which side moved first; both are the
        // handshake refused, so both read as that, with the cause behind it.
        match service.handshake() {
            Ok(()) => Ok(service),
            Err(e) => {
                service.kill();
                Err(Error::Transport(format!(
                    "the domain service {} refused the handshake: {e}",
                    binary.display()
                )))
            }
        }
    }

    /// The environment the format's results depend on.
    pub fn environment(&mut self, format: &FormatId) -> Result<Environment> {
        let within = self.answer_timeout;
        match self.ask(
            &ToDomain::Describe {
                format: format.clone(),
            },
            "Described",
            within,
        )? {
            FromDomain::Described { environment } => Ok(environment),
            other => Err(self.unexpected("Described", &other)),
        }
    }

    /// The devices the format's work can run on.
    pub fn enumerate_devices(&mut self, format: &FormatId) -> Result<Vec<DeviceInfo>> {
        let within = self.answer_timeout;
        match self.ask(
            &ToDomain::EnumerateDevices {
                format: format.clone(),
            },
            "EnumeratedDevices",
            within,
        )? {
            FromDomain::EnumeratedDevices { devices } => Ok(devices),
            other => Err(self.unexpected("EnumeratedDevices", &other)),
        }
    }

    /// The `[run.params]` section, as text, translated into the format's
    /// canonical params bytes.
    pub fn translate_config(
        &mut self,
        format: &FormatId,
        toml: &str,
        segmented: bool,
    ) -> Result<Params> {
        let within = self.answer_timeout;
        match self.ask(
            &ToDomain::TranslateConfig {
                format: format.clone(),
                toml: toml.to_string(),
                segmented,
            },
            "TranslatedConfig",
            within,
        )? {
            FromDomain::TranslatedConfig { bytes } => Ok(Params { bytes }),
            other => Err(self.unexpected("TranslatedConfig", &other)),
        }
    }

    /// The `[run.generator]` section, as text, translated into the generator's
    /// opaque params blob.
    pub fn translate_generator_config(
        &mut self,
        generator: &GeneratorId,
        toml: &str,
    ) -> Result<Vec<u8>> {
        let within = self.answer_timeout;
        match self.ask(
            &ToDomain::TranslateGeneratorConfig {
                generator: generator.clone(),
                toml: toml.to_string(),
            },
            "TranslatedConfig",
            within,
        )? {
            FromDomain::TranslatedConfig { bytes } => Ok(bytes),
            other => Err(self.unexpected("TranslatedConfig", &other)),
        }
    }

    /// The run's candidate specs.
    ///
    /// The one question with no deadline: generation is computation
    /// proportional to the batch, so a bound sized for answers would kill a
    /// legitimate large batch. A generator computes under the same trust as an
    /// executor, and a runaway one is interrupted the way any run is — Ctrl-C,
    /// with the store losing nothing.
    pub fn generate(
        &mut self,
        generator: &GeneratorId,
        format: &FormatId,
        root_seed: u64,
        params: &[u8],
    ) -> Result<Vec<Spec>> {
        match self.ask(
            &ToDomain::Generate {
                generator: generator.clone(),
                format: format.clone(),
                root_seed,
                params: params.to_vec(),
            },
            "Generated",
            Duration::MAX,
        )? {
            FromDomain::Generated { specs } => Ok(specs),
            other => Err(self.unexpected("Generated", &other)),
        }
    }

    /// Opens the conversation, refusing a program that speaks another version.
    fn handshake(&mut self) -> Result<()> {
        let within = self.answer_timeout;
        match self.ask(
            &ToDomain::Hello {
                protocol: PROTOCOL_VERSION,
            },
            "Ready",
            within,
        )? {
            FromDomain::Ready { protocol } if protocol == PROTOCOL_VERSION => Ok(()),
            FromDomain::Ready { protocol } => Err(Error::Transport(format!(
                "domain service {} protocol version mismatch: parent speaks \
                 {PROTOCOL_VERSION}, it speaks {protocol}",
                self.binary.display()
            ))),
            other => Err(self.unexpected("Ready", &other)),
        }
    }

    /// Asks one question and takes its answer off the reader thread's channel,
    /// waiting at most `within` ([`Duration::MAX`] waits for as long as the
    /// program lives). `answer` names the message expected back, so an expiry
    /// says which question went unanswered.
    ///
    /// A failure the program rendered crosses verbatim. An expiry kills and
    /// reaps the program: it owes an answer it will not give, and the session
    /// has no other way back to a known state.
    fn ask(&mut self, question: &ToDomain, answer: &str, within: Duration) -> Result<FromDomain> {
        let stdin = self.stdin.as_mut().ok_or_else(|| {
            Error::Transport(format!(
                "the domain service {} is already closed",
                self.binary.display()
            ))
        })?;
        write_frame(stdin, &question.encode())?;
        let received = match receive_within(&self.answers, within) {
            Ok(received) => received,
            Err(RecvTimeoutError::Timeout) => {
                let expired = Error::Transport(format!(
                    "the domain service {} exceeded the {}ms answer deadline awaiting {answer}",
                    self.binary.display(),
                    within.as_millis()
                ));
                self.kill();
                return Err(expired);
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(Error::Transport(format!(
                    "the domain service {} ended before answering",
                    self.binary.display()
                )));
            }
        };
        match received? {
            FromDomain::Failed { message } => Err(Error::Reported(message)),
            answer => Ok(answer),
        }
    }

    /// An answer of the wrong shape: a protocol violation naming what was
    /// expected and which program sent it.
    fn unexpected(&self, expected: &str, answer: &FromDomain) -> Error {
        Error::Transport(format!(
            "expected {expected} from the domain service {}, got {answer:?}",
            self.binary.display()
        ))
    }

    /// Kills the program and reaps it, then releases the reader thread and
    /// removes the directory it ran in. Best effort: one already dead is fine.
    ///
    /// The reader is released rather than joined. It ends when the program's
    /// stdout closes, which is when the last holder of that pipe exits — the
    /// program, and anything it left running behind it. Waiting on it would
    /// put the caller back at the mercy of the process this call exists to
    /// stop; the thread ends on its own, with nothing owed to it.
    fn kill(&mut self) {
        self.stdin = None;
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.reader = None;
        self.scratch = None;
    }

    /// Joins the reader thread at the end of a session that ended on its own
    /// terms; it exits when the program's stdout ends, which a program reaped
    /// off the farewell has already closed.
    fn join_reader(&mut self) {
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

impl Drop for DomainService {
    /// Says goodbye, then closes the pipe and reaps the program. A farewell
    /// that cannot be written means the program is already gone, so the close
    /// and the reap are what settle it. The reader thread and the scratch
    /// directory go last, once nothing is left running in it.
    fn drop(&mut self) {
        if let Some(stdin) = self.stdin.as_mut() {
            let _ = write_frame(stdin, &ToDomain::Goodbye.encode());
        }
        self.stdin = None;
        let _ = self.child.wait();
        self.join_reader();
        self.scratch = None;
    }
}

/// Decodes one program's answer stream into `answers` until it ends.
///
/// Runs on the session's reader thread: end-of-stream simply ends the thread —
/// the dropped sender is what the session observes as the program ending — and
/// a torn frame or an undecodable payload is sent as the stream's final `Err`
/// before the thread ends.
fn read_answers(mut reader: impl std::io::Read, answers: &Sender<Result<FromDomain>>) {
    loop {
        let message = match read_frame(&mut reader) {
            Ok(Some(payload)) => FromDomain::decode(&payload),
            Ok(None) => return,
            Err(e) => Err(e),
        };
        let failed = message.is_err();
        // A send failure means the session is gone; nothing is owed.
        if answers.send(message).is_err() || failed {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::spawn_policy::fixture;

    /// A deadline short enough that a wedged program expires promptly, and
    /// long enough that a process start never races it.
    const DEADLINE: Duration = Duration::from_millis(300);

    /// How long a fake pauses to outlast [`DEADLINE`] without making a test
    /// that waits it out slow.
    const PAST_THE_DEADLINE: &str = "0.9";

    /// A ceiling on a bounded wait, generous enough to survive a loaded
    /// machine while still failing a wait that never ends.
    const WELL_WITHIN: Duration = Duration::from_secs(20);

    /// The tail of a fake that has said all it will say: it reads until the
    /// parent closes its stdin — the shutdown signal a real program obeys —
    /// and exits then.
    const AWAIT_SHUTDOWN: &str = "exec cat > /dev/null";

    /// The tail of a fake that answers nothing further: it holds its pipes
    /// open and outlives any deadline, so the only thing that ends it is the
    /// kill an expiry fires. `exec` puts it in the shell's place, so that kill
    /// reaches the process holding the pipes.
    const WEDGE: &str = "exec sleep 300";

    /// The shell command writing `message` to stdout as one frame.
    ///
    /// The bytes come from the real encoder and travel through a file, so a
    /// fake speaks the protocol exactly rather than from a second copy of its
    /// rules, and `cat` puts them on the pipe as a process that writes and
    /// exits — with none of a shell builtin's buffering between the frame and
    /// the parent waiting for it.
    fn emit(dir: &Path, name: &str, message: &FromDomain) -> String {
        let mut frame = Vec::new();
        write_frame(&mut frame, &message.encode()).expect("frame the message");
        let path = dir.join(name);
        std::fs::write(&path, &frame).expect("write the frame");
        format!("cat {}", path.display())
    }

    /// The `Ready` a program answers the handshake with.
    fn ready(dir: &Path) -> String {
        emit(
            dir,
            "ready.frame",
            &FromDomain::Ready {
                protocol: PROTOCOL_VERSION,
            },
        )
    }

    /// A session against the program `body` describes, spawned under `within`.
    fn session(dir: &Path, body: &str, within: Duration) -> Result<DomainService> {
        DomainService::spawn(
            &fixture::program(dir, "fake-domain.sh", body),
            &FormatId::new("stub.v1").expect("format id"),
            &SpawnPolicy::Inherit,
            within,
        )
    }

    #[test]
    fn a_program_silent_past_the_deadline_fails_the_handshake_naming_it() {
        // The measure: a program wedged before its first answer is a config
        // failure naming what was awaited, not an orchestrator stopped
        // forever.
        let dir = tempfile::tempdir().expect("temp dir");
        let started = Instant::now();
        let error = session(dir.path(), WEDGE, DEADLINE).expect_err("a silent program");
        assert!(started.elapsed() < WELL_WITHIN, "{:?}", started.elapsed());
        let message = error.to_string();
        assert!(message.contains("fake-domain.sh"), "{message}");
        assert!(message.contains("Ready"), "names the answer: {message}");
        assert!(message.contains("300ms"), "names the deadline: {message}");
    }

    #[test]
    fn a_program_slow_past_the_deadline_answers_when_no_deadline_is_set() {
        // The absent key leaves the wait exactly as it was: the same program,
        // taking the same time, is a session.
        let dir = tempfile::tempdir().expect("temp dir");
        let body = format!(
            "sleep {PAST_THE_DEADLINE}\n{}\n{AWAIT_SHUTDOWN}",
            ready(dir.path())
        );
        session(dir.path(), &body, Duration::MAX).expect("a slow program still answers");
    }

    #[test]
    fn a_question_wedged_mid_session_expires_naming_it() {
        // The handshake passed, so what expires here is the question after
        // it: the error says which one went unanswered.
        let dir = tempfile::tempdir().expect("temp dir");
        let body = format!("{}\n{WEDGE}", ready(dir.path()));
        let mut service = session(dir.path(), &body, DEADLINE).expect("the handshake answers");
        let started = Instant::now();
        let error = service
            .environment(&FormatId::new("stub.v1").expect("format id"))
            .expect_err("a wedged question");
        assert!(started.elapsed() < WELL_WITHIN, "{:?}", started.elapsed());
        let message = error.to_string();
        assert!(message.contains("fake-domain.sh"), "{message}");
        assert!(
            message.contains("Described"),
            "names the question: {message}"
        );
    }

    #[test]
    fn generation_outlasts_the_deadline_every_other_question_is_held_to() {
        // Generation is computation proportional to the batch, so it answers
        // whenever it is done — under the very deadline that bounds the
        // questions around it.
        let dir = tempfile::tempdir().expect("temp dir");
        let body = format!(
            "{}\nsleep {PAST_THE_DEADLINE}\n{}\n{AWAIT_SHUTDOWN}",
            ready(dir.path()),
            emit(
                dir.path(),
                "generated.frame",
                &FromDomain::Generated { specs: Vec::new() }
            )
        );
        let mut service = session(dir.path(), &body, DEADLINE).expect("the handshake answers");
        let specs = service
            .generate(
                &GeneratorId::new("stub.v1").expect("generator id"),
                &FormatId::new("stub.v1").expect("format id"),
                42,
                &[],
            )
            .expect("generation answers past the deadline");
        assert!(specs.is_empty());
    }

    #[test]
    fn a_scrubbed_program_runs_in_a_scratch_directory_that_dies_with_it() {
        // The scratch directory's life is the program's: it records where it
        // ran, and by the time the spawn returns — the program killed and
        // reaped over its refused handshake — that directory is gone.
        let dir = tempfile::tempdir().expect("temp dir");
        let report = dir.path().join("cwd");
        let program = fixture::cwd_reporting_program(dir.path(), &report);
        DomainService::spawn(
            &program,
            &FormatId::new("stub.v1").expect("format id"),
            &SpawnPolicy::Scrubbed {
                passthrough: Vec::new(),
            },
            Duration::MAX,
        )
        .expect_err("a program that exits serves no domain");
        let scratch = fixture::reported_cwd(&report);
        assert_ne!(
            scratch,
            dir.path(),
            "the program ran in a directory of its own"
        );
        assert!(
            !scratch.exists(),
            "{} outlived its program",
            scratch.display()
        );
    }
}
