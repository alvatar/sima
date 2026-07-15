//! Smoke tests over the built `sima-worker` binary: the handshake and one
//! stub-format task driven directly over its pipes. The full run path over
//! subprocess workers is exercised end-to-end in the CLI crate's suites.

use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use sima_contracts::Outcome;
use sima_core::hash_bytes;
use sima_domains::{StubBehavior, StubProgram};
use sima_model::{EnvironmentId, FormatId};
use sima_scheduler::transport::protocol::{
    Assignment, Hello, PROTOCOL_VERSION, ToChild, ToParent, read_frame, write_frame,
};

/// A spawned worker with its pipes taken; stderr is inherited so a failure
/// diagnostic lands in the test output.
struct Worker {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: ChildStdout,
}

impl Worker {
    fn spawn() -> Worker {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sima-worker"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn sima-worker");
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

/// A `Hello` for the stub format at the given protocol version.
fn hello(protocol: u32) -> ToChild {
    ToChild::Hello(Hello {
        protocol,
        format: FormatId::new("stub.v1").expect("format id"),
        checkpoint_interval_ms: u64::MAX,
        checkpoint_interval_steps: 0,
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
            protocol: PROTOCOL_VERSION
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
        format: FormatId::new("no-such-domain.v1").expect("format id"),
        checkpoint_interval_ms: u64::MAX,
        checkpoint_interval_steps: 0,
    }));
    let answer = read_frame(&mut worker.stdout).expect("a clean end-of-stream");
    assert_eq!(answer, None, "no frame crosses a failed resolution");
    let code = worker.shutdown();
    assert_ne!(code, Some(0), "an unresolvable format exits nonzero");
}
