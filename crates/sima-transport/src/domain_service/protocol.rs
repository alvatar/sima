//! The wire protocol between the orchestrator and a domain service.
//!
//! Frames travel on the child's stdin (parent → child) and stdout (child →
//! parent), framed by [`sima_core::frame`] and built with the canonical
//! [`Enc`]/[`Dec`] primitives, each payload starting with a `u8` message tag —
//! the shape the worker protocol uses, over a message set of its own. Frames
//! are transport encoding, never identity-bearing, and no frame is ever hashed.
//!
//! The conversation: the parent opens with [`ToDomain::Hello`] and the child
//! answers [`FromDomain::Ready`] (or refuses a protocol-version mismatch). Each
//! later message is one question answered by exactly one frame — the answer it
//! names, or [`FromDomain::Failed`] carrying the program's own rendering of what
//! went wrong. [`ToDomain::Goodbye`] ends the session; so does the parent
//! closing the pipe.
//!
//! Both endpoints of a binary speak one version: the constant is
//! [`crate::protocol::PROTOCOL_VERSION`], because one program answers both the
//! worker protocol and this one, and a parent that can speak to it at all
//! speaks both.

use sima_contracts::{DeviceClass, DeviceInfo, DeviceType};
use sima_core::{Dec, Enc, Error, Result};
use sima_model::{Environment, FormatId, GeneratorId, Spec};

pub use crate::protocol::PROTOCOL_VERSION;

// Parent → domain message tags.
const TAG_HELLO: u8 = 0;
const TAG_DESCRIBE: u8 = 1;
const TAG_ENUMERATE_DEVICES: u8 = 2;
const TAG_TRANSLATE_CONFIG: u8 = 3;
const TAG_TRANSLATE_GENERATOR_CONFIG: u8 = 4;
const TAG_GENERATE: u8 = 5;
const TAG_GOODBYE: u8 = 6;

// Domain → parent message tags.
const TAG_READY: u8 = 0;
const TAG_DESCRIBED: u8 = 1;
const TAG_ENUMERATED_DEVICES: u8 = 2;
const TAG_TRANSLATED_CONFIG: u8 = 3;
const TAG_GENERATED: u8 = 4;
const TAG_FAILED: u8 = 5;

// Device-category tags inside an `EnumeratedDevices` payload.
const DEVICE_DISCRETE: u8 = 0;
const DEVICE_INTEGRATED: u8 = 1;
const DEVICE_VIRTUAL: u8 = 2;
const DEVICE_CPU: u8 = 3;
const DEVICE_OTHER: u8 = 4;

/// A parent → domain message: the handshake, one question, or the farewell.
///
/// Every question names the id it is about, so a program serving one format
/// refuses a question about another rather than answering for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToDomain {
    /// The handshake opening: the protocol version the parent speaks.
    Hello {
        /// The parent's [`PROTOCOL_VERSION`]; the child refuses a mismatch.
        protocol: u32,
    },
    /// What environment the format's results depend on.
    Describe {
        /// The format asked about.
        format: FormatId,
    },
    /// What devices the format's work can run on.
    EnumerateDevices {
        /// The format asked about.
        format: FormatId,
    },
    /// The `[run.params]` section, as text, to be translated into the format's
    /// canonical params bytes.
    TranslateConfig {
        /// The format whose translation is asked for.
        format: FormatId,
        /// The section's TOML text; empty for a run that states no params.
        toml: String,
        /// Whether the run divides candidates into segments.
        segmented: bool,
    },
    /// The `[run.generator]` section minus `id`, as text, to be translated into
    /// the generator's opaque params blob.
    TranslateGeneratorConfig {
        /// The generator whose translation is asked for.
        generator: GeneratorId,
        /// The section's TOML text; empty for a run that states no settings.
        toml: String,
    },
    /// The run's candidate specs.
    Generate {
        /// The generator producing them.
        generator: GeneratorId,
        /// The format stamped into every produced spec.
        format: FormatId,
        /// The run's root seed.
        root_seed: u64,
        /// The generator's own settings blob.
        params: Vec<u8>,
    },
    /// The session's end. The child answers nothing and exits.
    Goodbye,
}

/// A domain → parent message: the handshake answer, one answer, or the failure
/// that replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromDomain {
    /// The handshake answer: the child speaks this protocol version.
    Ready {
        /// The child's [`PROTOCOL_VERSION`].
        protocol: u32,
    },
    /// The environment entering every task's identity.
    Described {
        /// The format's environment.
        environment: Environment,
    },
    /// The devices the format's work can run on; empty for a format that opens
    /// none.
    EnumeratedDevices {
        /// The devices, as the program's execution backend enumerates them.
        devices: Vec<DeviceInfo>,
    },
    /// TranslatedConfig configuration: the canonical params bytes, for either
    /// translation.
    TranslatedConfig {
        /// The opaque bytes the translation produced.
        bytes: Vec<u8>,
    },
    /// The run's candidate specs, in the order the generator produced them.
    Generated {
        /// The specs.
        specs: Vec<Spec>,
    },
    /// The question failed; the program's own rendering, which the parent
    /// surfaces verbatim.
    Failed {
        /// What went wrong, as the program says it.
        message: String,
    },
}

impl ToDomain {
    /// The message's frame payload: tag byte, then fields in wire order.
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        match self {
            ToDomain::Hello { protocol } => {
                enc.u8(TAG_HELLO).u32(*protocol);
            }
            ToDomain::Describe { format } => {
                enc.u8(TAG_DESCRIBE).str(format.as_str());
            }
            ToDomain::EnumerateDevices { format } => {
                enc.u8(TAG_ENUMERATE_DEVICES).str(format.as_str());
            }
            ToDomain::TranslateConfig {
                format,
                toml,
                segmented,
            } => {
                enc.u8(TAG_TRANSLATE_CONFIG)
                    .str(format.as_str())
                    .str(toml)
                    .u8(u8::from(*segmented));
            }
            ToDomain::TranslateGeneratorConfig { generator, toml } => {
                enc.u8(TAG_TRANSLATE_GENERATOR_CONFIG)
                    .str(generator.as_str())
                    .str(toml);
            }
            ToDomain::Generate {
                generator,
                format,
                root_seed,
                params,
            } => {
                enc.u8(TAG_GENERATE)
                    .str(generator.as_str())
                    .str(format.as_str())
                    .u64(*root_seed)
                    .bytes(params);
            }
            ToDomain::Goodbye => {
                enc.u8(TAG_GOODBYE);
            }
        }
        enc.finish()
    }

    /// Parses a frame payload written by [`ToDomain::encode`], rejecting
    /// unknown tags and trailing bytes. Ids revalidate here, so a name outside
    /// the rule fails at the frame rather than reaching a dispatch.
    pub fn decode(payload: &[u8]) -> Result<ToDomain> {
        let mut dec = Dec::new(payload);
        let message = match dec.u8()? {
            TAG_HELLO => ToDomain::Hello {
                protocol: dec.u32()?,
            },
            TAG_DESCRIBE => ToDomain::Describe {
                format: FormatId::new(dec.str()?)?,
            },
            TAG_ENUMERATE_DEVICES => ToDomain::EnumerateDevices {
                format: FormatId::new(dec.str()?)?,
            },
            TAG_TRANSLATE_CONFIG => ToDomain::TranslateConfig {
                format: FormatId::new(dec.str()?)?,
                toml: dec.str()?.to_string(),
                segmented: decode_flag(&mut dec)?,
            },
            TAG_TRANSLATE_GENERATOR_CONFIG => ToDomain::TranslateGeneratorConfig {
                generator: GeneratorId::new(dec.str()?)?,
                toml: dec.str()?.to_string(),
            },
            TAG_GENERATE => ToDomain::Generate {
                generator: GeneratorId::new(dec.str()?)?,
                format: FormatId::new(dec.str()?)?,
                root_seed: dec.u64()?,
                params: dec.bytes()?.to_vec(),
            },
            TAG_GOODBYE => ToDomain::Goodbye,
            tag => {
                return Err(Error::Encoding(format!(
                    "unknown parent-to-domain message tag {tag}"
                )));
            }
        };
        dec.finish()?;
        Ok(message)
    }
}

impl FromDomain {
    /// The message's frame payload: tag byte, then fields in wire order.
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        match self {
            FromDomain::Ready { protocol } => {
                enc.u8(TAG_READY).u32(*protocol);
            }
            FromDomain::Described { environment } => {
                enc.u8(TAG_DESCRIBED);
                environment.encode(&mut enc);
            }
            FromDomain::EnumeratedDevices { devices } => {
                enc.u8(TAG_ENUMERATED_DEVICES).u64(devices.len() as u64);
                for device in devices {
                    enc.str(device.class.as_str())
                        .str(&device.name)
                        .u8(device_tag(device.device_type))
                        .u32(device.member);
                }
            }
            FromDomain::TranslatedConfig { bytes } => {
                enc.u8(TAG_TRANSLATED_CONFIG).bytes(bytes);
            }
            FromDomain::Generated { specs } => {
                enc.u8(TAG_GENERATED).u64(specs.len() as u64);
                for spec in specs {
                    spec.encode(&mut enc);
                }
            }
            FromDomain::Failed { message } => {
                enc.u8(TAG_FAILED).str(message);
            }
        }
        enc.finish()
    }

    /// Parses a frame payload written by [`FromDomain::encode`], rejecting
    /// unknown tags and trailing bytes.
    pub fn decode(payload: &[u8]) -> Result<FromDomain> {
        let mut dec = Dec::new(payload);
        let message = match dec.u8()? {
            TAG_READY => FromDomain::Ready {
                protocol: dec.u32()?,
            },
            TAG_DESCRIBED => FromDomain::Described {
                environment: Environment::decode(&mut dec)?,
            },
            TAG_ENUMERATED_DEVICES => {
                let count = dec.u64()?;
                // No pre-allocation from the untrusted count: each device reads
                // two length-prefixed strings and five more bytes, so a lying
                // count fails on truncation before any oversized buffer exists.
                let mut devices = Vec::new();
                for _ in 0..count {
                    devices.push(DeviceInfo {
                        class: DeviceClass::new(dec.str()?)?,
                        name: dec.str()?.to_string(),
                        device_type: device_type(dec.u8()?)?,
                        member: dec.u32()?,
                    });
                }
                FromDomain::EnumeratedDevices { devices }
            }
            TAG_TRANSLATED_CONFIG => FromDomain::TranslatedConfig {
                bytes: dec.bytes()?.to_vec(),
            },
            TAG_GENERATED => {
                let count = dec.u64()?;
                // Untrusted count, read the same way: a spec is a tag, a
                // length-prefixed format, and length-prefixed bytes.
                let mut specs = Vec::new();
                for _ in 0..count {
                    specs.push(Spec::decode(&mut dec)?);
                }
                FromDomain::Generated { specs }
            }
            TAG_FAILED => FromDomain::Failed {
                message: dec.str()?.to_string(),
            },
            tag => {
                return Err(Error::Encoding(format!(
                    "unknown domain-to-parent message tag {tag}"
                )));
            }
        };
        dec.finish()?;
        Ok(message)
    }
}

/// The wire tag of a device category.
fn device_tag(device_type: DeviceType) -> u8 {
    match device_type {
        DeviceType::Discrete => DEVICE_DISCRETE,
        DeviceType::Integrated => DEVICE_INTEGRATED,
        DeviceType::Virtual => DEVICE_VIRTUAL,
        DeviceType::Cpu => DEVICE_CPU,
        DeviceType::Other => DEVICE_OTHER,
    }
}

/// The device category a wire tag names, rejecting a tag outside the set.
fn device_type(tag: u8) -> Result<DeviceType> {
    match tag {
        DEVICE_DISCRETE => Ok(DeviceType::Discrete),
        DEVICE_INTEGRATED => Ok(DeviceType::Integrated),
        DEVICE_VIRTUAL => Ok(DeviceType::Virtual),
        DEVICE_CPU => Ok(DeviceType::Cpu),
        DEVICE_OTHER => Ok(DeviceType::Other),
        tag => Err(Error::Encoding(format!(
            "unknown device category tag {tag}"
        ))),
    }
}

/// Reads a boolean flag byte, rejecting values other than 0 and 1.
fn decode_flag(dec: &mut Dec<'_>) -> Result<bool> {
    match dec.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        flag => Err(Error::Encoding(format!(
            "invalid flag byte {flag}, expected 0 or 1"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use sima_contracts::{DeviceClass, DeviceInfo, DeviceType};
    use sima_core::{Enc, Error, Result, read_frame, write_frame};
    use sima_model::{Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorId};

    use super::*;

    /// A validated format id.
    fn format(name: &str) -> FormatId {
        FormatId::new(name).expect("format id")
    }

    /// A validated generator id.
    fn generator(name: &str) -> GeneratorId {
        GeneratorId::new(name).expect("generator id")
    }

    /// A sample of every parent → domain message, each field shape included.
    fn to_domain_messages() -> Vec<ToDomain> {
        vec![
            ToDomain::Hello {
                protocol: PROTOCOL_VERSION,
            },
            ToDomain::Describe {
                format: format("stub.v1"),
            },
            ToDomain::EnumerateDevices {
                format: format("ca_evolution.gray_scott.v1"),
            },
            ToDomain::TranslateConfig {
                format: format("stub.v1"),
                toml: "hex = \"00ff\"\n".to_string(),
                segmented: true,
            },
            // An absent section crosses as empty text: a format whose params
            // are all defaulted still answers with its canonical bytes.
            ToDomain::TranslateConfig {
                format: format("stub.v1"),
                toml: String::new(),
                segmented: false,
            },
            ToDomain::TranslateGeneratorConfig {
                generator: generator("stub.v1"),
                toml: "behaviors = [\"succeed\"]\n".to_string(),
            },
            ToDomain::Generate {
                generator: generator("stub.v1"),
                format: format("stub.v1"),
                root_seed: 42,
                params: vec![1, 2, 3],
            },
            ToDomain::Generate {
                generator: generator("stub.v1"),
                format: format("stub.v1"),
                root_seed: 0,
                params: Vec::new(),
            },
            ToDomain::Goodbye,
        ]
    }

    /// A sample of every domain → parent message, empty and populated lists
    /// included.
    fn from_domain_messages() -> Vec<FromDomain> {
        vec![
            FromDomain::Ready {
                protocol: PROTOCOL_VERSION,
            },
            FromDomain::Described {
                environment: Environment::new(vec![
                    EnvironmentComponent::new(
                        "stub.executor",
                        EnvironmentValue::Version("v1".to_string()),
                    )
                    .expect("component"),
                    EnvironmentComponent::new(
                        "stub.kernel",
                        EnvironmentValue::Digest(sima_core::hash_bytes(b"kernel")),
                    )
                    .expect("component"),
                ])
                .expect("environment"),
            },
            FromDomain::EnumeratedDevices {
                devices: vec![
                    DeviceInfo {
                        class: DeviceClass::new("8086:7d51").expect("class id"),
                        name: "Intel(R) Graphics (ARL)".to_string(),
                        device_type: DeviceType::Integrated,
                        member: 0,
                    },
                    DeviceInfo {
                        class: DeviceClass::new("10de:2330:1g.10gb").expect("class id"),
                        name: "NVIDIA H100".to_string(),
                        device_type: DeviceType::Discrete,
                        member: 3,
                    },
                ],
            },
            // A format that opens no device answers with an empty list.
            FromDomain::EnumeratedDevices {
                devices: Vec::new(),
            },
            FromDomain::TranslatedConfig {
                bytes: vec![9, 9, 9],
            },
            FromDomain::TranslatedConfig { bytes: Vec::new() },
            FromDomain::Generated {
                specs: vec![
                    Spec {
                        format: format("stub.v1"),
                        bytes: vec![1],
                    },
                    Spec {
                        format: format("stub.v1"),
                        bytes: Vec::new(),
                    },
                ],
            },
            FromDomain::Generated { specs: Vec::new() },
            FromDomain::Failed {
                message: "unknown format id \"acme.thing.v1\"".to_string(),
            },
        ]
    }

    #[test]
    fn every_to_domain_message_round_trips() -> Result<()> {
        for message in to_domain_messages() {
            assert_eq!(ToDomain::decode(&message.encode())?, message);
        }
        Ok(())
    }

    #[test]
    fn every_from_domain_message_round_trips() -> Result<()> {
        for message in from_domain_messages() {
            assert_eq!(FromDomain::decode(&message.encode())?, message);
        }
        Ok(())
    }

    #[test]
    fn every_message_survives_a_frame_round_trip() -> Result<()> {
        // The full path both endpoints use: encode, frame, unframe, decode.
        let mut pipe = Vec::new();
        for message in to_domain_messages() {
            write_frame(&mut pipe, &message.encode())?;
        }
        let mut reader = pipe.as_slice();
        for message in to_domain_messages() {
            let payload = read_frame(&mut reader)?.expect("a frame");
            assert_eq!(ToDomain::decode(&payload)?, message);
        }
        assert_eq!(read_frame(&mut reader)?, None, "the stream ends cleanly");
        Ok(())
    }

    #[test]
    fn truncated_messages_are_rejected() {
        // Every proper prefix of every message must fail to decode, never
        // panic — the decoder trusts nothing beyond its checks.
        for message in to_domain_messages() {
            let payload = message.encode();
            for cut in 0..payload.len() {
                assert!(
                    ToDomain::decode(&payload[..cut]).is_err(),
                    "prefix of {cut} bytes must be rejected"
                );
            }
        }
        for message in from_domain_messages() {
            let payload = message.encode();
            for cut in 0..payload.len() {
                assert!(
                    FromDomain::decode(&payload[..cut]).is_err(),
                    "prefix of {cut} bytes must be rejected"
                );
            }
        }
    }

    #[test]
    fn trailing_bytes_after_a_message_are_rejected() {
        for message in to_domain_messages() {
            let mut payload = message.encode();
            payload.push(0);
            assert!(matches!(
                ToDomain::decode(&payload),
                Err(Error::Encoding(_))
            ));
        }
        for message in from_domain_messages() {
            let mut payload = message.encode();
            payload.push(0);
            assert!(matches!(
                FromDomain::decode(&payload),
                Err(Error::Encoding(_))
            ));
        }
    }

    #[test]
    fn unknown_message_tags_are_encoding_errors() {
        for payload in [[9u8].as_slice(), [255u8].as_slice()] {
            assert!(matches!(ToDomain::decode(payload), Err(Error::Encoding(_))));
            assert!(matches!(
                FromDomain::decode(payload),
                Err(Error::Encoding(_))
            ));
        }
    }

    #[test]
    fn an_unknown_device_type_tag_is_an_encoding_error() {
        // An EnumeratedDevices frame carrying one device whose type tag is 9.
        let mut enc = Enc::new();
        enc.u8(TAG_ENUMERATED_DEVICES)
            .u64(1)
            .str("8086:7d51")
            .str("a device")
            .u8(9)
            .u32(0);
        assert!(matches!(
            FromDomain::decode(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn a_message_naming_an_invalid_id_is_rejected() {
        // Ids revalidate on decode, so a name outside the rule fails at the
        // frame rather than reaching a dispatch.
        let mut enc = Enc::new();
        enc.u8(TAG_DESCRIBE).str("Bad Name");
        assert!(matches!(
            ToDomain::decode(&enc.finish()),
            Err(Error::Validation(_))
        ));
        let mut enc = Enc::new();
        enc.u8(TAG_TRANSLATE_GENERATOR_CONFIG)
            .str("Bad Name")
            .str("");
        assert!(matches!(
            ToDomain::decode(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn an_enumerated_device_carries_an_invalid_class_no_further() {
        // The class validates where it is read, so a name no backend could have
        // minted fails at the frame rather than travelling on as a class
        // nothing matches.
        let mut enc = Enc::new();
        enc.u8(TAG_ENUMERATED_DEVICES)
            .u64(1)
            .str("8086:7D51")
            .str("a device")
            .u8(0)
            .u32(0);
        assert!(matches!(
            FromDomain::decode(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn a_described_environment_carries_an_invalid_component_name_no_further() {
        // The environment enters every task's identity, so a component named
        // outside the rule fails at the frame rather than reaching a hash.
        let mut enc = Enc::new();
        enc.u8(TAG_DESCRIBED)
            .str("sima.environment.v1")
            .u64(1)
            .str("Bad Name")
            .u8(0)
            .str("1.0");
        assert!(
            matches!(FromDomain::decode(&enc.finish()), Err(Error::Validation(_))),
            "an out-of-rule component name crossed"
        );
    }

    #[test]
    fn a_generated_spec_carries_an_invalid_format_no_further() {
        // Every spec stamps the format its bytes are read under, so a name
        // outside the rule fails at the frame rather than reaching a task.
        let mut enc = Enc::new();
        enc.u8(TAG_GENERATED)
            .u64(1)
            .str("sima.spec.v1")
            .str("Bad Name")
            .bytes(&[0xAA]);
        assert!(
            matches!(FromDomain::decode(&enc.finish()), Err(Error::Validation(_))),
            "an out-of-rule spec format crossed"
        );
    }

    #[test]
    fn an_invalid_flag_byte_is_an_encoding_error() {
        // A TranslateConfig whose segmented flag byte is 2.
        let mut enc = Enc::new();
        enc.u8(TAG_TRANSLATE_CONFIG).str("stub.v1").str("").u8(2);
        assert!(matches!(
            ToDomain::decode(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn every_device_type_survives_the_wire() -> Result<()> {
        // The categories are a closed set both sides agree on, so each must
        // return as itself rather than collapsing onto a neighbour.
        for device_type in [
            DeviceType::Discrete,
            DeviceType::Integrated,
            DeviceType::Virtual,
            DeviceType::Cpu,
            DeviceType::Other,
        ] {
            let message = FromDomain::EnumeratedDevices {
                devices: vec![DeviceInfo {
                    class: DeviceClass::new("8086:7d51").expect("class id"),
                    name: "a device".to_string(),
                    device_type,
                    member: 0,
                }],
            };
            assert_eq!(FromDomain::decode(&message.encode())?, message);
        }
        Ok(())
    }
}
