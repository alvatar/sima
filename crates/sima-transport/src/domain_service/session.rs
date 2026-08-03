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
use std::time::{Duration, Instant};

use sima_contracts::DeviceInfo;
use sima_core::{Error, Result, read_frame, write_frame};
use sima_model::{Environment, FormatId, GeneratorId, Params, Spec};
use tempfile::TempDir;

use crate::answer_deadline::receive_within;
use crate::domain_service::protocol::{FromDomain, PROTOCOL_VERSION, ToDomain};
use crate::serve::SERVE_DOMAIN;
use crate::spawn_policy::SpawnPolicy;

/// One program, spawned to answer for one format.
#[derive(Debug)]
pub struct DomainService {
    child: Child,
    /// The scratch working directory an explicit spawn gave the program, held
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
        let scratch = policy.apply(&mut command, std::env::vars_os)?;
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
        match self.ask(
            &ToDomain::Describe {
                format: format.clone(),
            },
            "Described",
            self.answer_timeout,
        )? {
            FromDomain::Described { environment } => Ok(environment),
            other => Err(self.unexpected("Described", &other)),
        }
    }

    /// The devices the format's work can run on.
    pub fn enumerate_devices(&mut self, format: &FormatId) -> Result<Vec<DeviceInfo>> {
        match self.ask(
            &ToDomain::EnumerateDevices {
                format: format.clone(),
            },
            "EnumeratedDevices",
            self.answer_timeout,
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
        match self.ask(
            &ToDomain::TranslateConfig {
                format: format.clone(),
                toml: toml.to_string(),
                segmented,
            },
            "TranslatedConfig",
            self.answer_timeout,
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
        match self.ask(
            &ToDomain::TranslateGeneratorConfig {
                generator: generator.clone(),
                toml: toml.to_string(),
            },
            "TranslatedConfig",
            self.answer_timeout,
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
        match self.ask(
            &ToDomain::Hello {
                protocol: PROTOCOL_VERSION,
            },
            "Ready",
            self.answer_timeout,
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
                // The answer stream ended, which means the program's stdout
                // closed. The process itself may still be there — a program
                // that closed its output and stayed — so it is settled here
                // rather than left for the drop to wait on.
                self.kill();
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

    /// How long the farewell waits for the program to leave: the session's own
    /// answer deadline where it states one, and a fixed few seconds otherwise.
    ///
    /// A session with no deadline is one whose questions may take as long as
    /// the program lives; the farewell still needs a bound, because it runs
    /// where a run is being torn down.
    fn farewell_bound(&self) -> Duration {
        bounded_farewell(self.answer_timeout)
    }
}

impl Drop for DomainService {
    /// Says goodbye, then closes the pipe and reaps the program within a bound.
    /// A farewell that cannot be written means the program is already gone, so
    /// the close and the reap are what settle it. The reader thread and the
    /// scratch directory go last, once nothing is left running in it.
    ///
    /// The wait is bounded because a program that ignores its closed stdin
    /// would otherwise hold this drop forever — and this drop runs on the
    /// thread tearing a run down, so a program that will not leave would keep
    /// the process alive after the run ended. Past the bound it is killed and
    /// reaped, which is what frees the scratch directory too.
    ///
    /// The reader handle is released on every path, never joined: the thread
    /// ends when the program's stdout closes, and anything the program spawned
    /// may hold that pipe past the program's own exit — even past a clean one —
    /// so a join anywhere here would wait on a process this drop does not
    /// control. [`kill`](Self::kill) releases for the same reason.
    fn drop(&mut self) {
        if let Some(stdin) = self.stdin.as_mut() {
            let _ = write_frame(stdin, &ToDomain::Goodbye.encode());
        }
        self.stdin = None;
        let bound = self.farewell_bound();
        if !reaped_within(&mut self.child, bound) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.reader = None;
        self.scratch = None;
    }
}

/// How long a program gets to leave on its own once its stdin is closed, when
/// the session states no answer deadline of its own.
const FAREWELL_BOUND: Duration = Duration::from_secs(5);

/// The farewell bound for a session whose questions wait `answer_timeout`.
///
/// The shorter of the two: a session may wait minutes on a question and still
/// owe a teardown a prompt exit, and one whose questions wait as long as the
/// program lives owes it the fixed bound.
fn bounded_farewell(answer_timeout: Duration) -> Duration {
    answer_timeout.min(FAREWELL_BOUND)
}

/// How often the farewell wait looks at the child. Short enough that an
/// ordinary exit is noticed at once, long enough that the wait is not a spin.
const REAP_POLL: Duration = Duration::from_millis(20);

/// Waits up to `bound` for `child` to exit, reporting whether it did.
///
/// `try_wait` rather than `wait`, because the point is to give up: a program
/// that will not leave is killed by the caller rather than waited on.
fn reaped_within(child: &mut Child, bound: Duration) -> bool {
    let deadline = Instant::now() + bound;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            // A child that cannot be waited on is one this side cannot settle
            // by waiting; the caller's kill is the remaining move.
            Err(_) => return false,
            Ok(None) => {}
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return false;
        }
        std::thread::sleep(REAP_POLL.min(left));
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

    /// The deadline of a session whose program answers nothing: short, so a
    /// test that waits the expiry out is quick. Nothing a correct fake does
    /// has to fit inside it.
    const EXPIRES_PROMPTLY: Duration = Duration::from_millis(300);

    /// The deadline of a session whose program must answer first: generous,
    /// because a process start and a frame on a machine running the whole
    /// suite at once share one wall clock.
    const ANSWERS_WITHIN: Duration = Duration::from_secs(2);

    /// How long a fake pauses to outlast [`EXPIRES_PROMPTLY`].
    const PAST_A_PROMPT_DEADLINE: &str = "0.9";

    /// How long a fake pauses to outlast [`ANSWERS_WITHIN`], with the margin
    /// a loaded machine takes out of it.
    const PAST_A_GENEROUS_DEADLINE: &str = "2.6";

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
    fn a_program_that_ignores_its_closed_stdin_is_killed_within_the_bound() {
        // The drop runs on the thread tearing a run down, so a program that
        // will not leave must not hold it: past the bound it is killed and
        // reaped. `sleep` ignores its stdin entirely, which is exactly the
        // shape that used to hang.
        let mut child = Command::new("sleep")
            .arg("300")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn a program that ignores its stdin");
        let bound = Duration::from_millis(100);
        let started = Instant::now();
        assert!(
            !reaped_within(&mut child, bound),
            "a program that ignores its stdin does not leave on its own"
        );
        // The wait gave up rather than blocking: comfortably inside the sleep.
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the wait is bounded"
        );
        child.kill().expect("kill the program");
        child.wait().expect("reap the program");
    }

    #[test]
    fn a_program_that_has_already_left_is_reaped_at_once() {
        let mut child = Command::new("true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn a program that exits");
        assert!(reaped_within(&mut child, Duration::from_secs(5)));
    }

    #[test]
    fn the_farewell_bound_is_the_shorter_of_the_deadline_and_the_fixed_bound() {
        // A deadline shorter than the fixed bound is the farewell's; a longer
        // one is not, because a teardown owes a prompt exit however long a
        // question may wait. A session with no deadline takes the fixed bound.
        assert_eq!(
            bounded_farewell(Duration::from_millis(250)),
            Duration::from_millis(250)
        );
        assert_eq!(
            bounded_farewell(FAREWELL_BOUND + Duration::from_secs(60)),
            FAREWELL_BOUND
        );
        assert_eq!(bounded_farewell(Duration::MAX), FAREWELL_BOUND);
    }

    #[test]
    fn a_program_silent_past_the_deadline_fails_the_handshake_naming_it() {
        // The measure: a program wedged before its first answer is a config
        // failure naming what was awaited, not an orchestrator stopped
        // forever.
        let dir = tempfile::tempdir().expect("temp dir");
        let started = Instant::now();
        let error = session(dir.path(), WEDGE, EXPIRES_PROMPTLY).expect_err("a silent program");
        assert!(started.elapsed() < WELL_WITHIN, "{:?}", started.elapsed());
        let message = error.to_string();
        assert!(message.contains("fake-domain.sh"), "{message}");
        assert!(message.contains("Ready"), "names the answer: {message}");
        assert!(
            message.contains(&format!("{}ms", EXPIRES_PROMPTLY.as_millis())),
            "names the deadline: {message}"
        );
    }

    #[test]
    fn a_program_at_another_protocol_version_is_refused_naming_both_versions() {
        // The two binaries are built apart, so the mismatch is the one thing
        // the handshake exists to catch; the parent refuses it here, where the
        // answer arrives.
        let dir = tempfile::tempdir().expect("temp dir");
        let stale = emit(
            dir.path(),
            "stale-ready.frame",
            &FromDomain::Ready {
                protocol: PROTOCOL_VERSION - 1,
            },
        );
        let error = session(
            dir.path(),
            &format!("{stale}\n{AWAIT_SHUTDOWN}"),
            Duration::MAX,
        )
        .expect_err("a program at another version");
        let message = error.to_string();
        assert!(message.contains("version mismatch"), "{message}");
        assert!(
            message.contains(&format!("parent speaks {PROTOCOL_VERSION}")),
            "names the parent's version: {message}"
        );
        assert!(
            message.contains(&format!("it speaks {}", PROTOCOL_VERSION - 1)),
            "names the program's version: {message}"
        );
    }

    #[test]
    fn a_program_slow_past_the_deadline_answers_when_no_deadline_is_set() {
        // The absent key leaves the wait exactly as it was: the same program,
        // taking the same time, is a session.
        let dir = tempfile::tempdir().expect("temp dir");
        let body = format!(
            "sleep {PAST_A_PROMPT_DEADLINE}\n{}\n{AWAIT_SHUTDOWN}",
            ready(dir.path())
        );
        let started = Instant::now();
        session(dir.path(), &body, Duration::MAX).expect("a slow program still answers");
        // The pause is what makes the session mean something: a program that
        // answered inside the deadline it is meant to outlast would have been
        // a session under either key.
        assert!(
            started.elapsed() > EXPIRES_PROMPTLY,
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_question_wedged_mid_session_expires_naming_it() {
        // The handshake passed, so what expires here is the question after
        // it: the error says which one went unanswered.
        let dir = tempfile::tempdir().expect("temp dir");
        let body = format!("{}\n{WEDGE}", ready(dir.path()));
        let mut service =
            session(dir.path(), &body, ANSWERS_WITHIN).expect("the handshake answers");
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
            "{}\nsleep {PAST_A_GENEROUS_DEADLINE}\n{}\n{AWAIT_SHUTDOWN}",
            ready(dir.path()),
            emit(
                dir.path(),
                "generated.frame",
                &FromDomain::Generated { specs: Vec::new() }
            )
        );
        let mut service =
            session(dir.path(), &body, ANSWERS_WITHIN).expect("the handshake answers");
        let started = Instant::now();
        let specs = service
            .generate(
                &GeneratorId::new("stub.v1").expect("generator id"),
                &FormatId::new("stub.v1").expect("format id"),
                42,
                &[],
            )
            .expect("generation answers past the deadline");
        assert!(specs.is_empty());
        // The wait is what is being measured, so it has to be a wait the
        // deadline would have cut: an answer arriving inside it proves
        // nothing about the question being unbounded.
        assert!(
            started.elapsed() > ANSWERS_WITHIN,
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_program_on_an_explicit_surface_runs_in_a_scratch_directory_that_dies_with_it() {
        // The scratch directory's life is the program's: it records where it
        // ran, and by the time the spawn returns — the program killed and
        // reaped over its refused handshake — that directory is gone.
        let dir = tempfile::tempdir().expect("temp dir");
        let report = dir.path().join("cwd");
        let program = fixture::cwd_reporting_program(dir.path(), &report);
        DomainService::spawn(
            &program,
            &FormatId::new("stub.v1").expect("format id"),
            &SpawnPolicy::Explicit {
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
