//! The wire protocol between the orchestrator and a worker process.
//!
//! Frames travel on the child's stdin (parent → child) and stdout (child →
//! parent); stderr carries no frames — the parent captures it line by line
//! and journals each line as a correlated diagnostic. Framing is
//! [`sima_core::frame`]: a `u32` little-endian payload
//! length followed by the payload. Each payload is built with the canonical
//! [`Enc`]/[`Dec`] primitives and starts with a `u8` message tag. Frames are
//! transport encoding, never identity-bearing, and no frame is ever hashed.
//!
//! The conversation: the parent opens with [`Hello`] and the child answers
//! [`ToParent::Ready`] (or refuses a protocol-version mismatch). Each task is
//! one [`Assignment`], answered by zero or more [`ToParent::Save`] frames and
//! exactly one of [`ToParent::Done`], [`ToParent::Panicked`], or
//! [`ToParent::Fault`]; [`ToParent::Event`] frames may interleave anywhere
//! after `Ready`, carrying structured events for the run's collector. There
//! is no shutdown message: the parent closing the child's stdin is the
//! shutdown signal.

use sima_contracts::{Artifact, DeviceBinding, Outcome, Stats};
use sima_core::{Dec, Enc, Error, Result};
use sima_model::{EnvironmentId, FormatId};

/// Version of the wire protocol; the handshake refuses a mismatch.
pub const PROTOCOL_VERSION: u32 = 4;

// Parent → child message tags.
const TAG_HELLO: u8 = 0;
const TAG_ASSIGN: u8 = 1;

// Child → parent message tags.
const TAG_READY: u8 = 0;
const TAG_SAVE: u8 = 1;
const TAG_DONE: u8 = 2;
const TAG_PANICKED: u8 = 3;
const TAG_FAULT: u8 = 4;
const TAG_EVENT: u8 = 5;

// Outcome tags inside a `Done` payload.
const OUTCOME_COMPLETED: u8 = 0;
const OUTCOME_FAILED: u8 = 1;
const OUTCOME_REJECTED: u8 = 2;

/// The handshake opening, sent once after spawn: the protocol version the
/// parent speaks, the run's format id — the child resolves its executor from
/// it, once — the run's checkpoint cadence settings, and the device the child
/// computes on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    /// The parent's [`PROTOCOL_VERSION`]; the child refuses a mismatch.
    pub protocol: u32,
    /// The scheduler-assigned worker id of this child's slot, so the child
    /// and the transport's reader threads can attribute events without a
    /// side channel.
    pub worker: u64,
    /// The run's format id; every assignment's spec bytes are of this format.
    pub format: FormatId,
    /// Wall-clock checkpoint cadence in milliseconds; `u64::MAX` disables
    /// the axis.
    pub checkpoint_interval_ms: u64,
    /// Step-count checkpoint cadence; `0` disables the axis.
    pub checkpoint_interval_steps: u64,
    /// The device this worker's executor is built for; `None` leaves the
    /// choice to the execution backend's default selection.
    pub device: Option<DeviceBinding>,
}

/// One task handed to the child: the identity-bearing inputs of the attempt
/// plus the per-attempt facts. The store stays parent-side — the input-state
/// and resume bytes arrive loaded, and results travel back as values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    /// The candidate bytes; their format travels once in [`Hello`].
    pub spec: Vec<u8>,
    /// The run params blob.
    pub params: Vec<u8>,
    /// The task's deterministic seed.
    pub seed: u64,
    /// The environment id entering the task's identity.
    pub environment: EnvironmentId,
    /// Loaded bytes of the input-state object; `None` for a stateless task.
    pub input_state: Option<Vec<u8>>,
    /// Checkpoint bytes saved by a previous attempt, if any survive.
    pub resume: Option<Vec<u8>>,
    /// Zero-based attempt number.
    pub attempt: u32,
    /// The worker id running this attempt.
    pub worker: u64,
    /// Whether this task checkpoints: offers evaluate the cadence and due
    /// saves cross the pipe. Stateless tasks and disabled cadences clear it.
    pub checkpointing: bool,
}

/// A parent → child message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToChild {
    /// The handshake opening.
    Hello(Hello),
    /// One task to execute.
    Assign(Assignment),
}

/// A child → parent message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToParent {
    /// The handshake answer: the child resolved its executor and speaks
    /// this protocol version.
    Ready {
        /// The child's [`PROTOCOL_VERSION`].
        protocol: u32,
        /// The device the child's executor opened, as the execution backend
        /// reports it; empty for a domain that uses no device. Provenance the
        /// parent journals verbatim: what the child resolved, never what the
        /// parent assumed.
        device_name: String,
        /// The driver version of that device, as the backend reports it; empty
        /// for a domain that uses no device. The one variable an environment
        /// hash cannot see across machines of one class, carried so the journal
        /// makes a cross-machine divergence diagnosable.
        driver: String,
    },
    /// A due checkpoint save: the continuation-state payload to persist.
    Save(Vec<u8>),
    /// The attempt's outcome, verbatim from the executor.
    Done(Outcome),
    /// The executor panicked; the rendered reason. Classification stays with
    /// the parent.
    Panicked(String),
    /// The executor returned `Err` — an infrastructure fault; the message.
    Fault(String),
    /// A structured event from the child: the serde_json serialization of a
    /// `sima_trace::Event`, carried opaquely — observational serde-world
    /// bytes inside the canonical frame, the same way spec and params bytes
    /// travel opaquely elsewhere. The parent's reader thread parses and
    /// forwards it to the run's collector; it never reaches the lease loop.
    Event(Vec<u8>),
}

impl ToChild {
    /// The message's frame payload: tag byte, then fields in wire order.
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        match self {
            ToChild::Hello(hello) => {
                enc.u8(TAG_HELLO)
                    .u32(hello.protocol)
                    .u64(hello.worker)
                    .str(hello.format.as_str())
                    .u64(hello.checkpoint_interval_ms)
                    .u64(hello.checkpoint_interval_steps);
                opt_device(&mut enc, hello.device.as_ref());
            }
            ToChild::Assign(assignment) => {
                enc.u8(TAG_ASSIGN)
                    .bytes(&assignment.spec)
                    .bytes(&assignment.params)
                    .u64(assignment.seed)
                    .hash(assignment.environment.as_hash());
                opt_bytes(&mut enc, assignment.input_state.as_deref());
                opt_bytes(&mut enc, assignment.resume.as_deref());
                enc.u32(assignment.attempt)
                    .u64(assignment.worker)
                    .u8(assignment.checkpointing as u8);
            }
        }
        enc.finish()
    }

    /// Parses a frame payload written by [`ToChild::encode`], rejecting
    /// unknown tags and trailing bytes.
    pub fn decode(payload: &[u8]) -> Result<ToChild> {
        let mut dec = Dec::new(payload);
        let message = match dec.u8()? {
            TAG_HELLO => {
                let protocol = dec.u32()?;
                let worker = dec.u64()?;
                let format = FormatId::new(dec.str()?)?;
                let checkpoint_interval_ms = dec.u64()?;
                let checkpoint_interval_steps = dec.u64()?;
                let device = decode_opt_device(&mut dec)?;
                ToChild::Hello(Hello {
                    protocol,
                    worker,
                    format,
                    checkpoint_interval_ms,
                    checkpoint_interval_steps,
                    device,
                })
            }
            TAG_ASSIGN => {
                let spec = dec.bytes()?.to_vec();
                let params = dec.bytes()?.to_vec();
                let seed = dec.u64()?;
                let environment = EnvironmentId::from_hash(dec.hash()?);
                let input_state = decode_opt_bytes(&mut dec)?;
                let resume = decode_opt_bytes(&mut dec)?;
                let attempt = dec.u32()?;
                let worker = dec.u64()?;
                let checkpointing = decode_flag(&mut dec)?;
                ToChild::Assign(Assignment {
                    spec,
                    params,
                    seed,
                    environment,
                    input_state,
                    resume,
                    attempt,
                    worker,
                    checkpointing,
                })
            }
            tag => {
                return Err(Error::Encoding(format!(
                    "unknown parent-to-child message tag {tag}"
                )));
            }
        };
        dec.finish()?;
        Ok(message)
    }
}

impl ToParent {
    /// The message's frame payload: tag byte, then fields in wire order. A
    /// `Done` payload is one flat layout across the three outcome arms —
    /// outcome tag, artifacts, stats, reason — with the fields an arm does
    /// not carry written empty.
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        match self {
            ToParent::Ready {
                protocol,
                device_name,
                driver,
            } => {
                enc.u8(TAG_READY)
                    .u32(*protocol)
                    .str(device_name)
                    .str(driver);
            }
            ToParent::Save(payload) => {
                enc.u8(TAG_SAVE).bytes(payload);
            }
            ToParent::Done(outcome) => {
                let (tag, artifacts, stats, reason): (u8, &[Artifact], &Stats, &str) = match outcome
                {
                    Outcome::Completed { artifacts, stats } => {
                        (OUTCOME_COMPLETED, artifacts, stats, "")
                    }
                    Outcome::Failed { reason, stats } => (OUTCOME_FAILED, &[], stats, reason),
                    Outcome::Rejected { reason, stats } => (OUTCOME_REJECTED, &[], stats, reason),
                };
                enc.u8(TAG_DONE).u8(tag).u64(artifacts.len() as u64);
                for artifact in artifacts {
                    enc.str(&artifact.name).bytes(&artifact.bytes);
                }
                enc.bytes(&stats.bytes).str(reason);
            }
            ToParent::Panicked(reason) => {
                enc.u8(TAG_PANICKED).str(reason);
            }
            ToParent::Fault(message) => {
                enc.u8(TAG_FAULT).str(message);
            }
            ToParent::Event(payload) => {
                enc.u8(TAG_EVENT).bytes(payload);
            }
        }
        enc.finish()
    }

    /// Parses a frame payload written by [`ToParent::encode`], rejecting
    /// unknown tags and trailing bytes.
    pub fn decode(payload: &[u8]) -> Result<ToParent> {
        let mut dec = Dec::new(payload);
        let message = match dec.u8()? {
            TAG_READY => ToParent::Ready {
                protocol: dec.u32()?,
                device_name: dec.str()?.to_string(),
                driver: dec.str()?.to_string(),
            },
            TAG_SAVE => ToParent::Save(dec.bytes()?.to_vec()),
            TAG_DONE => {
                let outcome_tag = dec.u8()?;
                let count = dec.u64()?;
                // No pre-allocation from the untrusted count: each artifact
                // reads at least its two length prefixes, so a lying count
                // fails on truncation before any oversized buffer exists.
                let mut artifacts = Vec::new();
                for _ in 0..count {
                    let name = dec.str()?.to_string();
                    let bytes = dec.bytes()?.to_vec();
                    artifacts.push(Artifact { name, bytes });
                }
                let stats = Stats {
                    bytes: dec.bytes()?.to_vec(),
                };
                let reason = dec.str()?.to_string();
                let outcome = match outcome_tag {
                    OUTCOME_COMPLETED => Outcome::Completed { artifacts, stats },
                    OUTCOME_FAILED => Outcome::Failed { reason, stats },
                    OUTCOME_REJECTED => Outcome::Rejected { reason, stats },
                    tag => {
                        return Err(Error::Encoding(format!("unknown outcome tag {tag}")));
                    }
                };
                ToParent::Done(outcome)
            }
            TAG_PANICKED => ToParent::Panicked(dec.str()?.to_string()),
            TAG_FAULT => ToParent::Fault(dec.str()?.to_string()),
            TAG_EVENT => ToParent::Event(dec.bytes()?.to_vec()),
            tag => {
                return Err(Error::Encoding(format!(
                    "unknown child-to-parent message tag {tag}"
                )));
            }
        };
        dec.finish()?;
        Ok(message)
    }
}

/// Writes a present-flag byte, then the [`Enc::bytes`] framing when present.
fn opt_bytes(enc: &mut Enc, value: Option<&[u8]>) {
    match value {
        None => {
            enc.u8(0);
        }
        Some(bytes) => {
            enc.u8(1).bytes(bytes);
        }
    }
}

/// Reads a present-flag byte (0 or 1), then the framed bytes when present.
fn decode_opt_bytes(dec: &mut Dec<'_>) -> Result<Option<Vec<u8>>> {
    match dec.u8()? {
        0 => Ok(None),
        1 => Ok(Some(dec.bytes()?.to_vec())),
        flag => Err(Error::Encoding(format!(
            "invalid optional-bytes flag byte {flag}, expected 0 or 1"
        ))),
    }
}

/// Writes a present-flag byte, then the binding's three ids when present.
fn opt_device(enc: &mut Enc, device: Option<&DeviceBinding>) {
    match device {
        None => {
            enc.u8(0);
        }
        Some(device) => {
            enc.u8(1)
                .u32(device.vendor_id)
                .u32(device.device_id)
                .u32(device.member);
        }
    }
}

/// Reads a present-flag byte (0 or 1), then the binding's ids when present.
fn decode_opt_device(dec: &mut Dec<'_>) -> Result<Option<DeviceBinding>> {
    match dec.u8()? {
        0 => Ok(None),
        1 => Ok(Some(DeviceBinding {
            vendor_id: dec.u32()?,
            device_id: dec.u32()?,
            member: dec.u32()?,
        })),
        flag => Err(Error::Encoding(format!(
            "invalid optional-device flag byte {flag}, expected 0 or 1"
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
    use sima_core::{hash_bytes, read_frame, write_frame};

    use super::*;

    /// A sample of every parent → child message, options both present and
    /// absent.
    fn to_child_messages() -> Vec<ToChild> {
        vec![
            ToChild::Hello(Hello {
                protocol: PROTOCOL_VERSION,
                worker: 0,
                format: FormatId::new("stub.v1").expect("format id"),
                checkpoint_interval_ms: 250,
                checkpoint_interval_steps: 0,
                device: None,
            }),
            ToChild::Hello(Hello {
                protocol: PROTOCOL_VERSION,
                worker: 7,
                format: FormatId::new("stub.v1").expect("format id"),
                checkpoint_interval_ms: u64::MAX,
                checkpoint_interval_steps: 64,
                device: Some(DeviceBinding {
                    vendor_id: 0x10de,
                    device_id: 0x2d39,
                    member: 1,
                }),
            }),
            ToChild::Assign(Assignment {
                spec: vec![1, 2, 3],
                params: vec![4, 5],
                seed: 42,
                environment: EnvironmentId::from_hash(hash_bytes(b"env")),
                input_state: Some(vec![6; 100]),
                resume: Some(Vec::new()),
                attempt: 3,
                worker: 7,
                checkpointing: true,
            }),
            ToChild::Assign(Assignment {
                spec: Vec::new(),
                params: Vec::new(),
                seed: 0,
                environment: EnvironmentId::from_hash(hash_bytes(b"env2")),
                input_state: None,
                resume: None,
                attempt: 0,
                worker: 0,
                checkpointing: false,
            }),
        ]
    }

    /// A sample of every child → parent message, every outcome arm included.
    fn to_parent_messages() -> Vec<ToParent> {
        vec![
            ToParent::Ready {
                protocol: PROTOCOL_VERSION,
                device_name: "NVIDIA RTX PRO 2000 Blackwell Generation Laptop GPU".to_string(),
                driver: "580.65.6".to_string(),
            },
            // A domain that uses no device reports neither name nor driver.
            ToParent::Ready {
                protocol: PROTOCOL_VERSION,
                device_name: String::new(),
                driver: String::new(),
            },
            ToParent::Save(vec![9, 8, 7]),
            ToParent::Save(Vec::new()),
            ToParent::Done(Outcome::Completed {
                artifacts: vec![
                    Artifact {
                        name: "state".to_string(),
                        bytes: vec![1, 2],
                    },
                    Artifact {
                        name: "output".to_string(),
                        bytes: Vec::new(),
                    },
                ],
                stats: Stats { bytes: vec![0xAA] },
            }),
            ToParent::Done(Outcome::Failed {
                reason: "programmed failure".to_string(),
                stats: Stats { bytes: Vec::new() },
            }),
            ToParent::Done(Outcome::Rejected {
                reason: "programmed rejection".to_string(),
                stats: Stats {
                    bytes: vec![1, 2, 3],
                },
            }),
            ToParent::Panicked("panic: boom".to_string()),
            ToParent::Fault("spec is malformed".to_string()),
            // The bytes are opaque here: any payload must survive the frame.
            ToParent::Event(
                br#"{"event":"diagnostic","level":"error","source":"panic","message":"boom"}"#
                    .to_vec(),
            ),
            ToParent::Event(Vec::new()),
        ]
    }

    #[test]
    fn the_protocol_version_is_pinned() {
        // The handshake contract both binaries compile against; bumping it is
        // a deliberate act.
        assert_eq!(PROTOCOL_VERSION, 4);
    }

    #[test]
    fn every_to_child_message_round_trips() -> Result<()> {
        for message in to_child_messages() {
            let decoded = ToChild::decode(&message.encode())?;
            assert_eq!(decoded, message);
        }
        Ok(())
    }

    #[test]
    fn every_to_parent_message_round_trips() -> Result<()> {
        for message in to_parent_messages() {
            let decoded = ToParent::decode(&message.encode())?;
            assert_eq!(decoded, message);
        }
        Ok(())
    }

    #[test]
    fn every_message_survives_a_frame_round_trip() -> Result<()> {
        // The full path both endpoints use: encode, frame, unframe, decode.
        let mut pipe = Vec::new();
        for message in to_child_messages() {
            write_frame(&mut pipe, &message.encode())?;
        }
        let mut reader = pipe.as_slice();
        for message in to_child_messages() {
            let payload = read_frame(&mut reader)?.expect("a frame");
            assert_eq!(ToChild::decode(&payload)?, message);
        }
        assert_eq!(read_frame(&mut reader)?, None, "the stream ends cleanly");
        Ok(())
    }

    #[test]
    fn unknown_message_tags_are_encoding_errors() {
        for payload in [[9u8].as_slice(), [255u8].as_slice()] {
            assert!(matches!(ToChild::decode(payload), Err(Error::Encoding(_))));
            assert!(matches!(ToParent::decode(payload), Err(Error::Encoding(_))));
        }
    }

    #[test]
    fn an_unknown_outcome_tag_is_an_encoding_error() {
        // A Done frame whose outcome tag is 3: structure otherwise valid.
        let mut enc = Enc::new();
        enc.u8(TAG_DONE).u8(3).u64(0).bytes(&[]).str("");
        assert!(matches!(
            ToParent::decode(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn an_invalid_option_flag_is_an_encoding_error() {
        // An Assign whose input-state present-flag byte is 2.
        let mut enc = Enc::new();
        enc.u8(TAG_ASSIGN)
            .bytes(&[1])
            .bytes(&[2])
            .u64(0)
            .hash(&hash_bytes(b"env"));
        enc.u8(2);
        assert!(matches!(
            ToChild::decode(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn an_invalid_device_flag_is_an_encoding_error() {
        // A Hello whose device present-flag byte is 2.
        let mut enc = Enc::new();
        enc.u8(TAG_HELLO)
            .u32(PROTOCOL_VERSION)
            .u64(0)
            .str("stub.v1")
            .u64(0)
            .u64(0);
        enc.u8(2);
        assert!(matches!(
            ToChild::decode(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn trailing_bytes_after_a_message_are_rejected() {
        for message in to_child_messages() {
            let mut payload = message.encode();
            payload.push(0);
            assert!(matches!(ToChild::decode(&payload), Err(Error::Encoding(_))));
        }
        for message in to_parent_messages() {
            let mut payload = message.encode();
            payload.push(0);
            assert!(matches!(
                ToParent::decode(&payload),
                Err(Error::Encoding(_))
            ));
        }
    }

    #[test]
    fn truncated_messages_are_rejected() {
        // Every proper prefix of every message must fail to decode, never
        // panic — the decoder trusts nothing beyond its checks.
        for message in to_child_messages() {
            let payload = message.encode();
            for cut in 0..payload.len() {
                assert!(
                    ToChild::decode(&payload[..cut]).is_err(),
                    "prefix of {cut} bytes must be rejected"
                );
            }
        }
        for message in to_parent_messages() {
            let payload = message.encode();
            for cut in 0..payload.len() {
                assert!(
                    ToParent::decode(&payload[..cut]).is_err(),
                    "prefix of {cut} bytes must be rejected"
                );
            }
        }
    }

    #[test]
    fn a_hello_with_an_invalid_format_name_is_rejected() {
        // The format id revalidates on decode: the child never constructs an
        // executor from a name outside the rule.
        let mut enc = Enc::new();
        enc.u8(TAG_HELLO)
            .u32(PROTOCOL_VERSION)
            .u64(0)
            .str("Bad Name")
            .u64(u64::MAX)
            .u64(0);
        assert!(matches!(
            ToChild::decode(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }
}
