//! Smoke tests over the built `sima-worker` binary: the handshake and one
//! stub-format task driven directly over its pipes. The full run path over
//! subprocess workers is exercised end-to-end in the CLI crate's suites.

use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use sima_contracts::Outcome;
use sima_core::{hash_bytes, read_frame, write_frame};
use sima_domains::{StubBehavior, StubProgram};
use sima_model::{EnvironmentId, FormatId};
use sima_transport::protocol::{Assignment, Hello, PROTOCOL_VERSION, ToChild, ToParent};

/// A spawned worker with its pipes taken; stderr is inherited so a failure
/// diagnostic lands in the test output.
struct Worker {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: ChildStdout,
}

impl Worker {
    fn spawn() -> Worker {
        Worker::spawn_with(&[])
    }

    /// A worker spawned with `env` set on top of the inherited environment,
    /// and with the program digest variable cleared first so what the child
    /// answers is exactly what `env` states.
    fn spawn_with(env: &[(&str, &str)]) -> Worker {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sima-worker"));
        command
            .env_remove("SIMA_PROGRAM_DIGEST")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        for (name, value) in env {
            command.env(name, value);
        }
        let mut child = command.spawn().expect("spawn sima-worker");
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        Worker {
            child,
            stdin: Some(stdin),
            stdout,
        }
    }

    fn send(&mut self, message: &ToChild) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        write_frame(stdin, &message.encode()).expect("write a frame");
    }

    fn receive(&mut self) -> ToParent {
        let payload = read_frame(&mut self.stdout)
            .expect("read a frame")
            .expect("a frame before end-of-stream");
        ToParent::decode(&payload).expect("decode the frame")
    }

    /// Closes stdin — the shutdown signal — and returns the exit code.
    fn shutdown(mut self) -> Option<i32> {
        self.stdin = None;
        self.child.wait().expect("wait for the worker").code()
    }
}

/// A `Hello` for the stub format at the given protocol version, leaving the
/// device to the domain — the stub uses none.
fn hello(protocol: u32) -> ToChild {
    ToChild::Hello(Hello {
        protocol,
        worker: 0,
        format: FormatId::new("stub.v1").expect("format id"),
        checkpoint_interval_ms: u64::MAX,
        checkpoint_interval_steps: 0,
        device: None,
    })
}

/// An assignment over a stub `Succeed` program.
fn assignment() -> ToChild {
    ToChild::Assign(Assignment {
        spec: StubProgram {
            behavior: StubBehavior::Succeed,
            nonce: 7,
        }
        .to_bytes(),
        params: vec![1, 2, 3],
        seed: 42,
        environment: EnvironmentId::from_hash(hash_bytes(b"env")),
        input_state: None,
        resume: None,
        attempt: 0,
        worker: 0,
        checkpointing: false,
    })
}

#[test]
fn the_worker_serves_a_stub_task_and_exits_cleanly_on_eof() {
    let mut worker = Worker::spawn();
    worker.send(&hello(PROTOCOL_VERSION));
    assert_eq!(
        worker.receive(),
        ToParent::Ready {
            protocol: PROTOCOL_VERSION,
            // The stub domain uses no device, so it names neither device nor
            // driver.
            device_name: String::new(),
            driver: String::new(),
            // Spawned without a program digest, so the child answers none.
            program: String::new(),
        }
    );
    worker.send(&assignment());
    match worker.receive() {
        ToParent::Done(Outcome::Completed { artifacts, .. }) => {
            // The stub's Succeed artifact: the 32-byte identity digest under
            // the name "output".
            assert_eq!(artifacts.len(), 1);
            assert_eq!(artifacts[0].name, "output");
            assert_eq!(artifacts[0].bytes.len(), 32);
        }
        other => panic!("expected Done(Completed), got {other:?}"),
    }
    assert_eq!(worker.shutdown(), Some(0), "clean EOF exits 0");
}

#[test]
fn the_worker_echoes_the_program_digest_its_environment_holds() {
    // A program cannot hash itself — a script's executable is its interpreter,
    // and a built entry point is not the payload that travelled — so the
    // spawner states which program it sent and the child answers that value
    // back verbatim, unread.
    let digest = "c".repeat(64);
    let mut worker = Worker::spawn_with(&[("SIMA_PROGRAM_DIGEST", digest.as_str())]);
    worker.send(&hello(PROTOCOL_VERSION));
    match worker.receive() {
        ToParent::Ready { program, .. } => assert_eq!(program, digest),
        other => panic!("expected Ready, got {other:?}"),
    }
    assert_eq!(worker.shutdown(), Some(0), "clean EOF exits 0");
}

/// Spawns one worker through a wrapper that exports `held` as the program
/// digest before exec, under a run expecting `sent`, and answers the spawn's
/// result.
///
/// The wrapper is the test's stand-in for a machine whose installed tree
/// drifted: sima injects the digest it sent, and overriding the variable is
/// how a machine holding another program is produced without one.
fn spawn_expecting(sent: Option<&str>, held: &str) -> sima_core::Result<String> {
    use std::path::PathBuf;
    use std::time::Duration;

    use sima_transport::{SpawnPolicy, SpawnSettings, SubprocessTransport, WorkerTransport};

    let worker = env!("CARGO_BIN_EXE_sima-worker");
    let transport = SubprocessTransport::new(
        PathBuf::from("sh"),
        vec![
            "-c".to_string(),
            format!("SIMA_PROGRAM_DIGEST={held} exec {worker}"),
        ],
        SpawnSettings::new(
            SpawnPolicy::Inherit,
            Duration::MAX,
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None,
        )
        .expecting_program(sent.map(str::to_string)),
    );
    transport
        .spawn(
            0,
            None,
            sima_trace::Emitter::from(std::sync::mpsc::channel().0),
        )
        .map(|outcome| outcome.into_link().program().to_string())
}

/// The digest a run in these tests sent, and the one a drifted machine holds.
const SENT: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const HELD: &str = "2222222222222222222222222222222222222222222222222222222222222222";

#[test]
fn a_worker_answering_the_digest_the_run_sent_binds() {
    let answered = spawn_expecting(Some(SENT), SENT).expect("the agreement holds");
    assert_eq!(
        answered, SENT,
        "the answered digest reaches the link for the journal"
    );
}

#[test]
fn a_worker_holding_another_program_fails_the_spawn_naming_both_digests() {
    // The drifted machine, through a real child and the real wire: what
    // answered is a program, and it is not the one this run sent.
    let error = spawn_expecting(Some(SENT), HELD).expect_err("a program disagreement");
    let message = format!("{error}");
    assert!(message.contains("program digest mismatch"), "{message}");
    assert!(message.contains(SENT), "names what the run sent: {message}");
    assert!(
        message.contains(HELD),
        "names what the worker answered: {message}"
    );
}

#[test]
fn a_worker_naming_a_program_the_run_never_sent_fails_the_spawn() {
    // The symmetric direction: a run answering for its format in process sent
    // no program at all, so a worker that names one is not the one it spawned.
    let error = spawn_expecting(None, HELD).expect_err("a program disagreement");
    let message = format!("{error}");
    assert!(message.contains("program digest mismatch"), "{message}");
    assert!(message.contains(HELD), "{message}");
}

#[test]
fn a_command_vector_spawn_reaches_the_worker_through_a_wrapper() {
    // The generalized command vector: program `sh`, arguments that exec the
    // worker. This is the shape a container invocation takes — a wrapper
    // process that ultimately execs `sima-worker` — proven here over the
    // `WorkerLink` API without a container, so the run path is exercised by the
    // vector form the remote transport also spawns.
    use std::path::PathBuf;
    use std::time::Duration;

    use sima_transport::{
        LinkEvent, SpawnPolicy, SpawnSettings, SubprocessTransport, WorkerTransport,
    };

    let worker = env!("CARGO_BIN_EXE_sima-worker");
    let transport = SubprocessTransport::new(
        PathBuf::from("sh"),
        vec!["-c".to_string(), format!("exec {worker}")],
        SpawnSettings::new(
            SpawnPolicy::Inherit,
            Duration::MAX,
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None,
        ),
    );
    let mut link = transport
        .spawn(
            0,
            None,
            sima_trace::Emitter::from(std::sync::mpsc::channel().0),
        )
        .expect("spawn through the wrapper")
        .into_link();
    // The handshake completed through the wrapper: the stub names no device.
    assert_eq!(link.device_name(), "");
    let ToChild::Assign(task) = assignment() else {
        unreachable!("assignment builds an Assign");
    };
    link.assign(&task).expect("assign one task");
    match link.next(None).expect("await the outcome") {
        LinkEvent::Done(Outcome::Completed { artifacts, .. }) => {
            assert_eq!(artifacts.len(), 1, "the stub's single output artifact");
        }
        other => panic!("expected a completed outcome, got {other:?}"),
    }
}

#[test]
fn worker_stderr_lines_arrive_as_correlated_diagnostics() {
    // A wrapper that writes one stderr line before becoming the worker: the
    // transport captures it and emits an info diagnostic attributed to the
    // worker id, with no host key for a local pool.
    use std::path::PathBuf;
    use std::time::Duration;

    use sima_trace::{Emitter, Event, Level};
    use sima_transport::{SpawnPolicy, SpawnSettings, SubprocessTransport, WorkerTransport};

    let worker = env!("CARGO_BIN_EXE_sima-worker");
    let transport = SubprocessTransport::new(
        PathBuf::from("sh"),
        vec![
            "-c".to_string(),
            format!("echo 'a stderr line' >&2; exec {worker}"),
        ],
        SpawnSettings::new(
            SpawnPolicy::Inherit,
            Duration::MAX,
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None,
        ),
    );
    let (tx, rx) = std::sync::mpsc::channel();
    let _link = transport
        .spawn(3, None, Emitter::from(tx))
        .expect("spawn through the wrapper")
        .into_link();
    let event = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("a captured stderr diagnostic");
    assert_eq!(
        event,
        Event::Diagnostic {
            level: Level::Info,
            source: "worker stderr".to_string(),
            message: "a stderr line".to_string(),
            worker: Some(3),
            host: None,
            task: None,
        }
    );
}

#[test]
fn an_overlong_stderr_line_is_truncated_with_a_marker() {
    use std::path::PathBuf;
    use std::time::Duration;

    use sima_trace::{Emitter, Event};
    use sima_transport::{SpawnPolicy, SpawnSettings, SubprocessTransport, WorkerTransport};

    let worker = env!("CARGO_BIN_EXE_sima-worker");
    let transport = SubprocessTransport::new(
        PathBuf::from("sh"),
        vec![
            "-c".to_string(),
            // 5000 x's on one stderr line, then the worker.
            format!("printf 'x%.0s' $(seq 1 5000) >&2; echo >&2; exec {worker}"),
        ],
        SpawnSettings::new(
            SpawnPolicy::Inherit,
            Duration::MAX,
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None,
        ),
    );
    let (tx, rx) = std::sync::mpsc::channel();
    let _link = transport
        .spawn(0, None, Emitter::from(tx))
        .expect("spawn through the wrapper")
        .into_link();
    let event = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("a captured stderr diagnostic");
    let Event::Diagnostic { message, .. } = event else {
        panic!("expected a diagnostic, got {event:?}");
    };
    assert!(message.ends_with("[truncated]"), "{message}");
    assert!(
        message.starts_with("xxxx"),
        "the capped prefix survives: {message}"
    );
    let payload = message.strip_suffix(" [truncated]").expect("the marker");
    assert_eq!(payload.len(), 4096, "the line is capped at 4096 bytes");
}

#[test]
fn an_executor_panic_crosses_as_a_correlated_diagnostic_event() {
    // The full child-to-parent leg over the real binary: the worker's panic
    // hook captures the backtrace, the host emits the Event frame, and the
    // transport's reader forwards it to the emitter before the Panicked
    // frame settles the attempt.
    use std::path::PathBuf;
    use std::time::Duration;

    use sima_trace::{Emitter, Event, Level};
    use sima_transport::{
        LinkEvent, SpawnPolicy, SpawnSettings, SubprocessTransport, WorkerTransport,
    };

    let transport = SubprocessTransport::new(
        PathBuf::from(env!("CARGO_BIN_EXE_sima-worker")),
        Vec::new(),
        SpawnSettings::new(
            SpawnPolicy::Inherit,
            Duration::MAX,
            FormatId::new("stub.v1").expect("format id"),
            Duration::MAX,
            None,
        ),
    );
    let (tx, rx) = std::sync::mpsc::channel();
    let mut link = transport
        .spawn(5, None, Emitter::from(tx))
        .expect("spawn the worker")
        .into_link();
    let task = ToChild::Assign(Assignment {
        spec: StubProgram {
            behavior: StubBehavior::Panic,
            nonce: 7,
        }
        .to_bytes(),
        params: vec![1, 2, 3],
        seed: 42,
        environment: EnvironmentId::from_hash(hash_bytes(b"env")),
        input_state: None,
        resume: None,
        attempt: 0,
        worker: 5,
        checkpointing: false,
    });
    let ToChild::Assign(task) = task else {
        unreachable!("built as Assign");
    };
    link.assign(&task).expect("assign the panicking task");
    match link.next(None).expect("await the outcome") {
        LinkEvent::Panicked(reason) => {
            assert!(reason.contains("programmed panic"), "{reason}");
        }
        other => panic!("expected Panicked, got {other:?}"),
    }
    // The stdout reader forwards frames in order, so the structured
    // diagnostic preceded the Panicked frame the link just returned. The
    // captured stderr of the default panic hook races it on its own thread,
    // so the panic-source diagnostic is selected, not assumed first.
    let mut panic_diagnostic = None;
    while let Ok(event) = rx.try_recv() {
        if matches!(&event, Event::Diagnostic { source, .. } if source == "panic") {
            panic_diagnostic = Some(event);
            break;
        }
    }
    let Some(Event::Diagnostic {
        level,
        message,
        worker,
        task: task_key,
        ..
    }) = panic_diagnostic
    else {
        panic!("no panic-source diagnostic arrived");
    };
    assert_eq!(level, Level::Error);
    assert!(message.contains("programmed panic"), "{message}");
    assert_eq!(worker, Some(5), "the handshake's worker id");
    assert!(task_key.is_some(), "the diagnostic names the task");
}

/// Runs the enumeration probe for `format` and returns the devices it printed,
/// asserting it exited zero and that every line is a well-formed device.
fn probe(format: &str) -> Vec<serde_json::Value> {
    let output = Command::new(env!("CARGO_BIN_EXE_sima-worker"))
        .args(["--enumerate-devices", format])
        .output()
        .expect("run the probe");
    assert!(
        output.status.success(),
        "the probe exits zero: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).expect("probe output is UTF-8");
    let devices: Vec<serde_json::Value> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("each line is a JSON device"))
        .collect();
    for device in &devices {
        assert!(
            device
                .get("class")
                .and_then(serde_json::Value::as_str)
                .is_some(),
            "a device names the class its backend minted"
        );
    }
    devices
}

#[test]
fn the_enumerate_probe_answers_per_format_rather_than_per_machine() {
    // A device list is a claim about what a program can run on. The stub
    // computes in the worker process and opens no device, so it enumerates
    // none however much hardware this machine has — and the orchestrator reads
    // that as a deviceless worker rather than as a bare host.
    assert!(
        probe("stub.v1").is_empty(),
        "a program that opens no device enumerates none"
    );
}

#[test]
fn the_enumerate_probe_refuses_a_format_it_cannot_resolve() {
    // The backend to ask comes from the format, so an unknown one has no
    // answer: it fails loudly rather than reporting an empty device list, which
    // the orchestrator would read as a machine with no hardware.
    let output = Command::new(env!("CARGO_BIN_EXE_sima-worker"))
        .args(["--enumerate-devices", "no-such-domain.v1"])
        .output()
        .expect("run the probe");
    assert!(!output.status.success(), "the probe exits nonzero");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no-such-domain.v1"),
        "the diagnostic names the format"
    );
}

#[test]
fn the_enumerate_probe_needs_the_format_to_answer_for() {
    let output = Command::new(env!("CARGO_BIN_EXE_sima-worker"))
        .arg("--enumerate-devices")
        .output()
        .expect("run the probe");
    assert!(!output.status.success(), "the probe exits nonzero");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("format id"),
        "the diagnostic says what is missing"
    );
}

#[test]
fn the_enumerate_probe_answers_when_the_backend_finds_no_driver() {
    // A backend whose driver search comes up empty — a CI runner, a rented
    // instance with a broken driver — reports no devices and no failure: the
    // probe still exits zero, and the rental derives one deviceless worker from
    // an empty answer. `VK_DRIVER_FILES` naming a nonexistent manifest makes
    // the Vulkan loader's driver search come up empty, the same condition as a
    // machine with no Vulkan driver installed.
    let output = Command::new(env!("CARGO_BIN_EXE_sima-worker"))
        .args(["--enumerate-devices", "ca_evolution.gray_scott.v1"])
        .env("VK_DRIVER_FILES", "/nonexistent/no_driver.json")
        .output()
        .expect("run the probe");
    assert!(
        output.status.success(),
        "the probe exits zero: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8(output.stdout)
            .expect("probe output is UTF-8")
            .trim()
            .is_empty(),
        "a driverless backend enumerates nothing"
    );
}

#[test]
fn a_protocol_version_mismatch_exits_nonzero_before_ready() {
    let mut worker = Worker::spawn();
    worker.send(&hello(PROTOCOL_VERSION + 1));
    // The refusal: no Ready, end-of-stream, nonzero exit.
    let answer = read_frame(&mut worker.stdout).expect("a clean end-of-stream");
    assert_eq!(answer, None, "no frame crosses a refused handshake");
    let code = worker.shutdown();
    assert_ne!(code, Some(0), "a refused handshake exits nonzero");
}

#[test]
fn an_unknown_format_exits_nonzero_before_ready() {
    let mut worker = Worker::spawn();
    worker.send(&ToChild::Hello(Hello {
        protocol: PROTOCOL_VERSION,
        worker: 0,
        format: FormatId::new("no-such-domain.v1").expect("format id"),
        checkpoint_interval_ms: u64::MAX,
        checkpoint_interval_steps: 0,
        device: None,
    }));
    let answer = read_frame(&mut worker.stdout).expect("a clean end-of-stream");
    assert_eq!(answer, None, "no frame crosses a failed resolution");
    let code = worker.shutdown();
    assert_ne!(code, Some(0), "an unresolvable format exits nonzero");
}

/// The probe answers from the Vulkan loader, so it needs a device.
mod on_device {
    use super::*;

    #[test]
    fn the_enumerate_probe_prints_one_json_line_per_device() {
        // The remote-resolution probe: `--enumerate-devices <format>` prints the devices
        // that format's program can run on as JSON, one per line, and exits zero.
        assert!(
            !probe("ca_evolution.gray_scott.v1").is_empty(),
            "this machine has a Vulkan device"
        );
    }
}
