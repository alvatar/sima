//! The child side of the domain service: [`serve`] answers what a format
//! binds.
//!
//! `serve` is what a program runs for the life of a session: read the
//! [`ToDomain::Hello`], answer [`FromDomain::Ready`], then answer one question
//! after another until the parent says goodbye or closes the pipe. The components
//! are built before the session opens, so a program that loads assets or opens
//! a device pays that cost once and every later question is a message.
//!
//! A question the program cannot answer is [`FromDomain::Failed`] carrying its
//! own rendering, and the session continues; an `Err` from `serve` is a
//! handshake refusal, a frame violation, or a broken pipe — the caller maps it
//! to a nonzero exit with a stderr diagnostic.

use std::io::{Read, Write};

use sima_contracts::{Domain, Generator};
use sima_core::{Error, Result, read_frame, write_frame};
use sima_model::{FormatId, GeneratorId};

use crate::domain_service::protocol::{FromDomain, PROTOCOL_VERSION, ToDomain};

/// Answers the domain service over a frame pipe: handshake, then the question
/// loop.
///
/// `domain` binds one format and `generators` are the generators that target
/// it, so a question about another id is refused rather than answered for the
/// ones this program serves. Returns `Ok` when the parent says goodbye or
/// closes the pipe at a frame boundary.
pub fn serve<R: Read, W: Write>(
    mut reader: R,
    mut writer: W,
    domain: &dyn Domain,
    generators: &[&dyn Generator],
) -> Result<()> {
    // The handshake: the first frame must be a Hello at this protocol version.
    // Refusal happens before Ready, so the parent's missing Ready is its
    // spawn-failure signal.
    let Some(payload) = read_frame(&mut reader)? else {
        return Err(Error::Transport(
            "the pipe closed before the handshake".to_string(),
        ));
    };
    let ToDomain::Hello { protocol } = ToDomain::decode(&payload)? else {
        return Err(Error::Transport(
            "expected the Hello handshake as the first frame".to_string(),
        ));
    };
    if protocol != PROTOCOL_VERSION {
        return Err(Error::Transport(format!(
            "protocol version mismatch: the parent speaks {protocol}, this domain service \
             speaks {PROTOCOL_VERSION}"
        )));
    }
    write_frame(
        &mut writer,
        &FromDomain::Ready {
            protocol: PROTOCOL_VERSION,
        }
        .encode(),
    )?;

    // The question loop: one answer per question until the farewell or the
    // pipe's end.
    loop {
        let Some(payload) = read_frame(&mut reader)? else {
            return Ok(());
        };
        match ToDomain::decode(&payload)? {
            ToDomain::Goodbye => return Ok(()),
            ToDomain::Hello { .. } => {
                return Err(Error::Transport(
                    "unexpected second Hello after the handshake".to_string(),
                ));
            }
            // What the program could not answer crosses as its own rendering,
            // so the parent surfaces the program's words rather than its own
            // guess.
            question => {
                let answer =
                    answer(domain, generators, question).unwrap_or_else(|e| FromDomain::Failed {
                        message: e.to_string(),
                    });
                write_frame(&mut writer, &answer.encode())?;
            }
        }
    }
}

/// Answers one question from the components. The handshake messages are handled by
/// the loop, so they never reach here.
fn answer(
    domain: &dyn Domain,
    generators: &[&dyn Generator],
    question: ToDomain,
) -> Result<FromDomain> {
    match question {
        ToDomain::Describe { format } => {
            served(domain, &format)?;
            Ok(FromDomain::Described {
                environment: domain.environment().clone(),
            })
        }
        ToDomain::Enumerate { format } => {
            served(domain, &format)?;
            Ok(FromDomain::Enumerated {
                devices: domain.enumerate()?,
            })
        }
        ToDomain::TranslateParams {
            format,
            toml,
            segmented,
        } => {
            served(domain, &format)?;
            Ok(FromDomain::Translated {
                bytes: domain.translate_params(&toml, segmented)?.bytes,
            })
        }
        ToDomain::TranslateGeneratorParams { generator, toml } => Ok(FromDomain::Translated {
            bytes: generator_for(generators, &generator)?.translate_params(&toml)?,
        }),
        ToDomain::Generate {
            generator,
            format,
            root_seed,
            params,
        } => {
            served(domain, &format)?;
            Ok(FromDomain::Generated {
                specs: generator_for(generators, &generator)?.generate(root_seed, &params)?,
            })
        }
        ToDomain::Hello { .. } | ToDomain::Goodbye => Err(Error::Transport(
            "the handshake messages carry no question".to_string(),
        )),
    }
}

/// Confirms `format` is the one this program serves. Shared with the role
/// entry, which checks the same thing about the format it was spawned for.
pub(crate) fn served(domain: &dyn Domain, format: &FormatId) -> Result<()> {
    if domain.format() == format {
        return Ok(());
    }
    Err(Error::Validation(format!(
        "unknown format id {:?}; this program serves {:?}",
        format.as_str(),
        domain.format().as_str()
    )))
}

/// The generator registered under `id`.
fn generator_for<'a>(
    generators: &[&'a dyn Generator],
    id: &GeneratorId,
) -> Result<&'a dyn Generator> {
    generators
        .iter()
        .find(|generator| generator.id() == id)
        .copied()
        .ok_or_else(|| Error::Validation(format!("unknown generator id {:?}", id.as_str())))
}

#[cfg(test)]
mod tests {
    use sima_contracts::{
        DeviceBinding, DeviceClass, DeviceInfo, DeviceType, Domain, Executor, Generator,
    };
    use sima_core::{Error, Result, read_frame, write_frame};
    use sima_model::{
        Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorId, Params, Spec,
    };

    use super::*;
    use crate::domain_service::protocol::{FromDomain, PROTOCOL_VERSION, ToDomain};

    /// The format the test domain serves.
    const FORMAT: &str = "host-test.v1";

    /// A validated format id.
    fn format(name: &str) -> FormatId {
        FormatId::new(name).expect("format id")
    }

    /// A validated generator id.
    fn generator(name: &str) -> GeneratorId {
        GeneratorId::new(name).expect("generator id")
    }

    /// A domain over one format: its translations merge their input so a test
    /// asserts arrival, and its enumeration answers one device.
    struct TestDomain {
        format: FormatId,
        environment: Environment,
        /// Whether every question answers a failure instead.
        failing: bool,
    }

    impl TestDomain {
        fn new(failing: bool) -> TestDomain {
            TestDomain {
                format: format(FORMAT),
                environment: Environment::new(vec![
                    EnvironmentComponent::new(
                        "host-test.executor",
                        EnvironmentValue::Version("v1".to_string()),
                    )
                    .expect("component"),
                ])
                .expect("environment"),
                failing,
            }
        }

        /// The programmed failure, so each arm refuses the same way.
        fn refusal(&self) -> Result<()> {
            if self.failing {
                return Err(Error::Validation("programmed refusal".to_string()));
            }
            Ok(())
        }
    }

    impl Domain for TestDomain {
        fn format(&self) -> &FormatId {
            &self.format
        }

        fn environment(&self) -> &Environment {
            &self.environment
        }

        fn executor(&self, _device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
            Err(Error::Validation("no executor here".to_string()))
        }

        fn device_desc(&self, _device: Option<&DeviceBinding>) -> Result<(String, String)> {
            Ok((String::new(), String::new()))
        }

        fn enumerate(&self) -> Result<Vec<DeviceInfo>> {
            self.refusal()?;
            Ok(vec![DeviceInfo {
                class: DeviceClass::new("8086:7d51").expect("class id"),
                name: "a test device".to_string(),
                device_type: DeviceType::Integrated,
                member: 0,
            }])
        }

        fn translate_params(&self, toml: &str, segmented: bool) -> Result<Params> {
            self.refusal()?;
            Ok(Params {
                bytes: format!("{toml}|{segmented}").into_bytes(),
            })
        }
    }

    /// A generator whose specs merge the seed and params it was given.
    struct TestGenerator {
        id: GeneratorId,
        format: FormatId,
    }

    impl Generator for TestGenerator {
        fn id(&self) -> &GeneratorId {
            &self.id
        }

        fn format(&self) -> &FormatId {
            &self.format
        }

        fn translate_params(&self, toml: &str) -> Result<Vec<u8>> {
            Ok(format!("gen:{toml}").into_bytes())
        }

        fn generate(&self, root_seed: u64, params: &[u8]) -> Result<Vec<Spec>> {
            let format = &self.format;
            let mut bytes = root_seed.to_le_bytes().to_vec();
            bytes.extend_from_slice(params);
            Ok(vec![Spec {
                format: format.clone(),
                bytes,
            }])
        }
    }

    /// Frames `messages` into one input buffer, serves them against a domain that
    /// fails or answers, and returns the result plus the decoded answers.
    fn drive(failing: bool, messages: &[ToDomain]) -> (Result<()>, Vec<FromDomain>) {
        let domain = TestDomain::new(failing);
        let test_generator = TestGenerator {
            id: generator("host-test.v1"),
            format: format(FORMAT),
        };
        let mut input = Vec::new();
        for message in messages {
            write_frame(&mut input, &message.encode()).expect("frame the input");
        }
        let mut output = Vec::new();
        let result = serve(
            input.as_slice(),
            &mut output,
            &domain,
            &[&test_generator as &dyn Generator],
        );
        let mut answers = Vec::new();
        let mut reader = output.as_slice();
        while let Some(payload) = read_frame(&mut reader).expect("well-formed output") {
            answers.push(FromDomain::decode(&payload).expect("decodable output"));
        }
        (result, answers)
    }

    /// The handshake, so each test states only the questions it asks.
    fn hello() -> ToDomain {
        ToDomain::Hello {
            protocol: PROTOCOL_VERSION,
        }
    }

    #[test]
    fn the_handshake_answers_ready_and_goodbye_ends_the_session() {
        let (result, answers) = drive(false, &[hello(), ToDomain::Goodbye]);
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(
            answers,
            vec![FromDomain::Ready {
                protocol: PROTOCOL_VERSION
            }],
            "the farewell is answered by ending, not by a frame"
        );
    }

    #[test]
    fn the_end_of_the_pipe_ends_the_session() {
        // The parent closing the pipe is a shutdown signal of its own, so a
        // session that ends without a farewell still ends cleanly.
        let (result, answers) = drive(false, &[hello()]);
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(answers.len(), 1, "the handshake alone: {answers:?}");
    }

    #[test]
    fn a_version_mismatch_is_refused_before_ready() {
        let (result, answers) = drive(
            false,
            &[ToDomain::Hello {
                protocol: PROTOCOL_VERSION + 1,
            }],
        );
        assert!(matches!(result, Err(Error::Transport(_))), "{result:?}");
        assert!(answers.is_empty(), "no Ready crosses a refused handshake");
    }

    #[test]
    fn a_missing_hello_is_an_error() {
        let (result, answers) = drive(
            false,
            &[ToDomain::Describe {
                format: format(FORMAT),
            }],
        );
        assert!(matches!(result, Err(Error::Transport(_))), "{result:?}");
        assert!(answers.is_empty());
    }

    #[test]
    fn describe_answers_the_formats_environment() {
        let (result, answers) = drive(
            false,
            &[
                hello(),
                ToDomain::Describe {
                    format: format(FORMAT),
                },
            ],
        );
        assert!(result.is_ok(), "{result:?}");
        let FromDomain::Described { environment } = &answers[1] else {
            panic!("expected Described, got {:?}", answers[1]);
        };
        assert_eq!(environment.components()[0].name(), "host-test.executor");
    }

    #[test]
    fn enumerate_answers_the_formats_devices() {
        let (result, answers) = drive(
            false,
            &[
                hello(),
                ToDomain::Enumerate {
                    format: format(FORMAT),
                },
            ],
        );
        assert!(result.is_ok(), "{result:?}");
        let FromDomain::Enumerated { devices } = &answers[1] else {
            panic!("expected Enumerated, got {:?}", answers[1]);
        };
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].class.as_str(), "8086:7d51");
    }

    #[test]
    fn translate_params_carries_the_section_text_and_the_segment_flag() {
        // Both inputs reach the domain: the section as written, and whether the
        // run divides candidates into segments.
        let (result, answers) = drive(
            false,
            &[
                hello(),
                ToDomain::TranslateParams {
                    format: format(FORMAT),
                    toml: "hex = \"00\"".to_string(),
                    segmented: true,
                },
            ],
        );
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(
            answers[1],
            FromDomain::Translated {
                bytes: b"hex = \"00\"|true".to_vec()
            }
        );
    }

    #[test]
    fn translate_generator_params_answers_through_the_named_generator() {
        let (result, answers) = drive(
            false,
            &[
                hello(),
                ToDomain::TranslateGeneratorParams {
                    generator: generator("host-test.v1"),
                    toml: "n = 1".to_string(),
                },
            ],
        );
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(
            answers[1],
            FromDomain::Translated {
                bytes: b"gen:n = 1".to_vec()
            }
        );
    }

    #[test]
    fn generate_answers_the_specs_the_generator_produced() {
        let (result, answers) = drive(
            false,
            &[
                hello(),
                ToDomain::Generate {
                    generator: generator("host-test.v1"),
                    format: format(FORMAT),
                    root_seed: 42,
                    params: vec![7],
                },
            ],
        );
        assert!(result.is_ok(), "{result:?}");
        let mut bytes = 42u64.to_le_bytes().to_vec();
        bytes.push(7);
        assert_eq!(
            answers[1],
            FromDomain::Generated {
                specs: vec![Spec {
                    format: format(FORMAT),
                    bytes
                }]
            }
        );
    }

    #[test]
    fn a_question_about_another_format_fails_naming_the_id() {
        // One binary serves one format, so a question about another is refused
        // rather than answered for the one it does serve.
        let (result, answers) = drive(
            false,
            &[
                hello(),
                ToDomain::Describe {
                    format: format("other.v1"),
                },
                ToDomain::Enumerate {
                    format: format("other.v1"),
                },
                ToDomain::Generate {
                    generator: generator("host-test.v1"),
                    format: format("other.v1"),
                    root_seed: 0,
                    params: Vec::new(),
                },
            ],
        );
        assert!(result.is_ok(), "{result:?}");
        for answer in &answers[1..] {
            let FromDomain::Failed { message } = answer else {
                panic!("expected Failed, got {answer:?}");
            };
            assert!(message.contains("other.v1"), "{message}");
        }
    }

    #[test]
    fn a_question_about_an_unknown_generator_fails_naming_the_id() {
        let (result, answers) = drive(
            false,
            &[
                hello(),
                ToDomain::TranslateGeneratorParams {
                    generator: generator("no-such-generator.v1"),
                    toml: String::new(),
                },
            ],
        );
        assert!(result.is_ok(), "{result:?}");
        let FromDomain::Failed { message } = &answers[1] else {
            panic!("expected Failed, got {:?}", answers[1]);
        };
        assert!(message.contains("no-such-generator.v1"), "{message}");
    }

    #[test]
    fn a_plug_failure_crosses_verbatim_and_the_session_continues() {
        // A question the program cannot answer is one failed answer, not the
        // end of the conversation: the next question is still answered.
        let (result, answers) = drive(
            true,
            &[
                hello(),
                ToDomain::Enumerate {
                    format: format(FORMAT),
                },
                ToDomain::Describe {
                    format: format(FORMAT),
                },
            ],
        );
        assert!(result.is_ok(), "{result:?}");
        let FromDomain::Failed { message } = &answers[1] else {
            panic!("expected Failed, got {:?}", answers[1]);
        };
        assert!(message.contains("programmed refusal"), "{message}");
        assert!(
            matches!(answers[2], FromDomain::Described { .. }),
            "the session continues: {:?}",
            answers[2]
        );
    }

    #[test]
    fn a_second_hello_is_an_error() {
        let (result, answers) = drive(false, &[hello(), hello()]);
        assert!(matches!(result, Err(Error::Transport(_))), "{result:?}");
        assert_eq!(answers.len(), 1, "only the handshake was answered");
    }

    #[test]
    fn a_torn_input_frame_is_an_error() {
        let domain = TestDomain::new(false);
        let mut input = Vec::new();
        write_frame(&mut input, &hello().encode()).expect("frame");
        write_frame(
            &mut input,
            &ToDomain::Describe {
                format: format(FORMAT),
            }
            .encode(),
        )
        .expect("frame");
        input.truncate(input.len() - 1);
        let mut output = Vec::new();
        let result = serve(input.as_slice(), &mut output, &domain, &[]);
        assert!(matches!(result, Err(Error::Transport(_))), "{result:?}");
    }
}
