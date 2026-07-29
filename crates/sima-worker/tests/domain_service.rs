//! The domain-service role of the built `sima-worker` binary: what the in-tree
//! formats bind, asked over a pipe and compared against the same answers
//! reached by direct calls.

use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use sima_core::{read_frame, write_frame};
use sima_model::{FormatId, GeneratorId};
use sima_transport::domain_service::protocol::{FromDomain, PROTOCOL_VERSION, ToDomain};

/// Every format this build carries.
const FORMATS: [&str; 4] = [
    "stub.v1",
    "ca_evolution.gray_scott.v1",
    "ca_evolution.gray_scott_cuda.v1",
    "ca_evolution.nca.v1",
];

/// A spawned domain service with its pipes taken; stderr is inherited so a
/// failure diagnostic lands in the test output.
struct Service {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: ChildStdout,
}

impl Service {
    /// Spawns the worker in its domain-service role for `format` and completes
    /// the handshake.
    fn spawn(format: &str) -> Service {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sima-worker"))
            .arg("--serve-domain")
            .arg(format)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn sima-worker");
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let mut service = Service {
            child,
            stdin: Some(stdin),
            stdout,
        };
        service.send(&ToDomain::Hello {
            protocol: PROTOCOL_VERSION,
        });
        assert_eq!(
            service.receive(),
            FromDomain::Ready {
                protocol: PROTOCOL_VERSION
            }
        );
        service
    }

    fn send(&mut self, message: &ToDomain) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        write_frame(stdin, &message.encode()).expect("write a frame");
    }

    fn receive(&mut self) -> FromDomain {
        let payload = read_frame(&mut self.stdout)
            .expect("read a frame")
            .expect("a frame before end-of-stream");
        FromDomain::decode(&payload).expect("decode the frame")
    }

    /// Asks one question and returns its answer.
    fn ask(&mut self, question: ToDomain) -> FromDomain {
        self.send(&question);
        self.receive()
    }

    /// Says goodbye and returns the exit code.
    fn shutdown(mut self) -> Option<i32> {
        self.send(&ToDomain::Goodbye);
        self.stdin = None;
        self.child.wait().expect("wait for the worker").code()
    }
}

/// A validated format id.
fn format(name: &str) -> FormatId {
    FormatId::new(name).expect("format id")
}

#[test]
fn every_format_describes_the_environment_its_dispatch_supplies() {
    // The environment enters every task key, so a run driven through the
    // protocol keeps the keys it has by direct call.
    for name in FORMATS {
        let mut service = Service::spawn(name);
        let answer = service.ask(ToDomain::Describe {
            format: format(name),
        });
        let expected = sima_domains::domain_for(&format(name))
            .expect("a registered format")
            .environment;
        assert_eq!(
            answer,
            FromDomain::Described {
                environment: expected
            },
            "{name}"
        );
        assert_eq!(service.shutdown(), Some(0), "the farewell exits 0");
    }
}

#[test]
fn every_format_enumerates_the_devices_its_backend_reports() {
    // The answer travels as the enumeration the format's own backend gives, so
    // a machine with no device of that kind answers with an empty list rather
    // than failing.
    for name in FORMATS {
        let mut service = Service::spawn(name);
        let answer = service.ask(ToDomain::Enumerate {
            format: format(name),
        });
        let expected =
            sima_domains::devices::enumerate_devices(&format(name)).expect("enumeration answers");
        assert_eq!(
            answer,
            FromDomain::Enumerated { devices: expected },
            "{name}"
        );
        assert_eq!(service.shutdown(), Some(0));
    }
}

#[test]
fn params_translate_to_the_bytes_the_dispatch_produces() {
    // The section crosses as text and comes back as the canonical bytes that
    // enter the run id, so the two paths agree byte for byte.
    let text = "hex = \"00ff\"\n";
    let mut service = Service::spawn("stub.v1");
    let answer = service.ask(ToDomain::TranslateParams {
        format: format("stub.v1"),
        toml: text.to_string(),
        segmented: false,
    });
    let expected = sima_domains::params_for(
        &format("stub.v1"),
        &text.parse::<toml::Table>().expect("a table"),
        false,
    )
    .expect("the stub translates its params");
    assert_eq!(
        answer,
        FromDomain::Translated {
            bytes: expected.bytes
        }
    );
    assert_eq!(service.shutdown(), Some(0));
}

#[test]
fn generator_params_translate_to_the_bytes_the_dispatch_produces() {
    let text = "behaviors = [\"succeed\", \"reject\"]\n";
    let generator = GeneratorId::new("stub.v1").expect("generator id");
    let mut service = Service::spawn("stub.v1");
    let answer = service.ask(ToDomain::TranslateGeneratorParams {
        generator: generator.clone(),
        toml: text.to_string(),
    });
    let expected = sima_domains::generator_params_for(
        &generator,
        &text.parse::<toml::Table>().expect("a table"),
    )
    .expect("the stub translates its generator params");
    assert_eq!(answer, FromDomain::Translated { bytes: expected });
    assert_eq!(service.shutdown(), Some(0));
}

#[test]
fn generation_answers_the_specs_the_in_process_generator_produces() {
    // Generation is deterministic in the seed and the params, so the specs
    // that cross the pipe are the ones the in-process generator produces —
    // which is what keeps every task key of the run the same.
    let text = "behaviors = [\"succeed\", \"reject\", \"panic\"]\n";
    let generator = GeneratorId::new("stub.v1").expect("generator id");
    let params = sima_domains::generator_params_for(
        &generator,
        &text.parse::<toml::Table>().expect("a table"),
    )
    .expect("the stub translates its generator params");
    let mut service = Service::spawn("stub.v1");
    let answer = service.ask(ToDomain::Generate {
        generator: generator.clone(),
        format: format("stub.v1"),
        root_seed: 42,
        params: params.clone(),
    });
    let expected = sima_domains::generator_for(&generator)
        .expect("a registered generator")
        .generate(42, &params, &format("stub.v1"))
        .expect("the stub generates");
    assert_eq!(answer, FromDomain::Generated { specs: expected });
    assert_eq!(service.shutdown(), Some(0));
}

#[test]
fn a_question_about_another_format_fails_naming_the_id() {
    // One session serves one format, so a question about another is refused
    // rather than answered for the one it serves.
    let mut service = Service::spawn("stub.v1");
    let answer = service.ask(ToDomain::Describe {
        format: format("ca_evolution.nca.v1"),
    });
    let FromDomain::Failed { message } = answer else {
        panic!("expected Failed, got {answer:?}");
    };
    assert!(message.contains("ca_evolution.nca.v1"), "{message}");
    assert_eq!(service.shutdown(), Some(0), "the session continues");
}

#[test]
fn a_format_this_build_does_not_carry_exits_nonzero_naming_the_id() {
    // The role resolves its format before the handshake, so a format no build
    // carries fails at startup rather than at the first question.
    let output = Command::new(env!("CARGO_BIN_EXE_sima-worker"))
        .arg("--serve-domain")
        .arg("no-such-domain.v1")
        .stdin(Stdio::null())
        .output()
        .expect("run sima-worker");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no-such-domain.v1"), "{stderr}");
}

#[test]
fn the_flag_without_a_format_exits_nonzero_naming_what_it_takes() {
    let output = Command::new(env!("CARGO_BIN_EXE_sima-worker"))
        .arg("--serve-domain")
        .stdin(Stdio::null())
        .output()
        .expect("run sima-worker");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--serve-domain"), "{stderr}");
}

/// The parent-side session against the built binary: the same answers, reached
/// the way a run reaches them.
mod session {
    use sima_transport::domain_service::DomainService;

    use super::{FORMATS, format};

    /// The worker binary, in its domain-service role.
    fn service(name: &str) -> DomainService {
        DomainService::spawn(
            std::path::Path::new(env!("CARGO_BIN_EXE_sima-worker")),
            &format(name),
        )
        .expect("the worker serves its own formats")
    }

    #[test]
    fn a_session_answers_every_question_a_run_asks() {
        // One session, every question: a program is spawned once for a run and
        // answers each of them over the pipe it already holds.
        let mut service = service("stub.v1");
        let format = format("stub.v1");
        let generator = sima_model::GeneratorId::new("stub.v1").expect("generator id");
        assert_eq!(
            service.environment(&format).expect("an environment"),
            sima_domains::domain_for(&format)
                .expect("a registered format")
                .environment
        );
        assert_eq!(
            service.enumerate(&format).expect("an enumeration"),
            sima_domains::devices::enumerate_devices(&format).expect("enumeration answers")
        );
        let params = service
            .translate_generator_params(&generator, "behaviors = [\"succeed\"]\n")
            .expect("a translation");
        let specs = service
            .generate(&generator, &format, 42, &params)
            .expect("a generation");
        assert_eq!(specs.len(), 1, "one behavior, one candidate");
        assert_eq!(specs[0].format, format);
    }

    #[test]
    fn every_format_opens_a_session() {
        for name in FORMATS {
            let mut service = service(name);
            service
                .environment(&format(name))
                .unwrap_or_else(|e| panic!("{name} describes itself: {e}"));
        }
    }

    #[test]
    fn a_program_that_does_not_serve_the_format_fails_at_the_handshake() {
        // The format is settled when the program is spawned, so a binary that
        // cannot answer for it fails before a run reaches its first question.
        let error = DomainService::spawn(
            std::path::Path::new(env!("CARGO_BIN_EXE_sima-worker")),
            &format("no-such-domain.v1"),
        )
        .expect_err("a program that serves no such format");
        assert!(
            error.to_string().contains("refused the handshake"),
            "{error}"
        );
    }

    #[test]
    fn a_binary_that_cannot_be_run_names_itself() {
        let error = DomainService::spawn(
            std::path::Path::new("/no/such/domain/binary"),
            &format("stub.v1"),
        )
        .expect_err("no such binary");
        assert!(
            error.to_string().contains("/no/such/domain/binary"),
            "{error}"
        );
    }

    #[test]
    fn a_failure_the_program_rendered_crosses_verbatim() {
        // The program's own words reach the run: a question about another
        // format is refused by the program, and the parent surfaces that.
        let mut service = service("stub.v1");
        let error = service
            .environment(&format("ca_evolution.nca.v1"))
            .expect_err("a question about another format");
        // The program's rendering, prefix and all, with nothing added by the
        // parent: the classification belongs where the failure was raised.
        assert_eq!(
            error.to_string(),
            "validation error: unknown format id \"ca_evolution.nca.v1\"; \
             this program serves \"stub.v1\""
        );
    }
}
