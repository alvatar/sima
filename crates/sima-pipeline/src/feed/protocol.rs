//! The wire protocol of the follow stream: what the host running a search's
//! orchestrator writes, and what the machine rendering the view reads.
//!
//! Framing is [`sima_core::frame`]: a `u32` little-endian payload length
//! followed by the payload. Each payload is built with the canonical
//! [`Enc`]/[`Dec`] primitives and starts with a `u8` frame tag, mirroring the
//! worker protocol's layout. Frames are transport encoding, never
//! identity-bearing, and no frame is ever hashed.
//!
//! The stream is one-directional: the far side writes, the near side reads.
//! [`FollowFrame::Hello`] opens it and carries the metadata the near side
//! cannot compute without the config, then records flow until the near side
//! closes the pipe or — in snapshot mode — [`FollowFrame::Complete`] ends it.

use sima_core::{Dec, Enc, Error, Result};
use sima_model::{FormatId, SearchId};

/// Version of the follow protocol; the near side refuses a mismatch rather
/// than interpreting foreign bytes.
///
/// It covers the journal events the records frame carries as well as the frame
/// layout, since a near side reading a record whose event it does not know
/// fails the whole stream as corruption. A build that adds an event therefore
/// moves the version, and the refusal at the handshake is what names the cause
/// in place of that corruption.
pub const FOLLOW_PROTOCOL_VERSION: u32 = 2;

const TAG_HELLO: u8 = 0;
const TAG_RECORDS: u8 = 1;
const TAG_HOLDER: u8 = 2;
const TAG_COMPLETE: u8 = 3;
const TAG_FAULT: u8 = 4;

/// One frame of the follow stream, written by the host the search's orchestrator
/// runs on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowFrame {
    /// The opening frame, always first: the protocol version and the search
    /// metadata the near side renders through.
    Hello {
        /// The far side's [`FOLLOW_PROTOCOL_VERSION`].
        protocol: u32,
        /// The search being followed, as the far side computed it from the
        /// config on its own disk.
        search: SearchId,
        /// The search's format id; the near side resolves the domain that
        /// renders stats from it.
        format: FormatId,
        /// The configured worker count, for the occupancy view.
        workers: u32,
        /// Who held the search's orchestrator lock when the stream opened.
        holder: Option<String>,
    },
    /// Raw journal lines, in append order, exactly as the journal stores them
    /// with the newline framing stripped. The near side parses each one, so
    /// the torn-write rule stays on the far side where the file is.
    Records(Vec<String>),
    /// The search's lock is held by this string, or free.
    Holder(Option<String>),
    /// Snapshot mode: the far side reached the journal's end and is exiting.
    Complete,
    /// The far side failed; the rendered error.
    Fault(String),
}

impl FollowFrame {
    /// The frame's payload: tag byte, then fields in wire order.
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        match self {
            FollowFrame::Hello {
                protocol,
                search,
                format,
                workers,
                holder,
            } => {
                enc.u8(TAG_HELLO)
                    .u32(*protocol)
                    .hash(search.as_hash())
                    .str(format.as_str())
                    .u32(*workers);
                opt_str(&mut enc, holder.as_deref());
            }
            FollowFrame::Records(lines) => {
                enc.u8(TAG_RECORDS).u64(lines.len() as u64);
                for line in lines {
                    enc.str(line);
                }
            }
            FollowFrame::Holder(holder) => {
                enc.u8(TAG_HOLDER);
                opt_str(&mut enc, holder.as_deref());
            }
            FollowFrame::Complete => {
                enc.u8(TAG_COMPLETE);
            }
            FollowFrame::Fault(message) => {
                enc.u8(TAG_FAULT).str(message);
            }
        }
        enc.finish()
    }

    /// Parses a payload written by [`encode`](FollowFrame::encode), rejecting
    /// unknown tags and trailing bytes.
    pub fn decode(payload: &[u8]) -> Result<FollowFrame> {
        let mut dec = Dec::new(payload);
        let frame = match dec.u8()? {
            TAG_HELLO => {
                let protocol = dec.u32()?;
                let search = SearchId::from_hash(dec.hash()?);
                let format = FormatId::new(dec.str()?)?;
                let workers = dec.u32()?;
                let holder = decode_opt_str(&mut dec)?;
                FollowFrame::Hello {
                    protocol,
                    search,
                    format,
                    workers,
                    holder,
                }
            }
            TAG_RECORDS => {
                let count = dec.u64()?;
                // No pre-allocation from the untrusted count: each line reads
                // at least its length prefix, so a lying count fails on
                // truncation before any oversized buffer exists.
                let mut lines = Vec::new();
                for _ in 0..count {
                    lines.push(dec.str()?.to_string());
                }
                FollowFrame::Records(lines)
            }
            TAG_HOLDER => FollowFrame::Holder(decode_opt_str(&mut dec)?),
            TAG_COMPLETE => FollowFrame::Complete,
            TAG_FAULT => FollowFrame::Fault(dec.str()?.to_string()),
            tag => {
                return Err(Error::Encoding(format!("unknown follow frame tag {tag}")));
            }
        };
        dec.finish()?;
        Ok(frame)
    }
}

/// Writes a present-flag byte, then the [`Enc::str`] framing when present.
fn opt_str(enc: &mut Enc, value: Option<&str>) {
    match value {
        None => {
            enc.u8(0);
        }
        Some(text) => {
            enc.u8(1).str(text);
        }
    }
}

/// Reads what [`opt_str`] wrote; any flag byte but 0 or 1 is a violation.
fn decode_opt_str(dec: &mut Dec<'_>) -> Result<Option<String>> {
    match dec.u8()? {
        0 => Ok(None),
        1 => Ok(Some(dec.str()?.to_string())),
        flag => Err(Error::Encoding(format!(
            "optional string present-flag {flag} is neither 0 nor 1"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::{Error, Result, hash_bytes, read_frame, write_frame};
    use sima_model::{FormatId, SearchId};

    /// One frame of each variant, covering the fields the wire carries.
    fn every_variant() -> Result<Vec<FollowFrame>> {
        Ok(vec![
            FollowFrame::Hello {
                protocol: FOLLOW_PROTOCOL_VERSION,
                search: SearchId::from_hash(hash_bytes(b"a followed search")),
                format: FormatId::new("stub.v1")?,
                workers: 4,
                holder: Some("4242 gpubox".to_string()),
            },
            FollowFrame::Hello {
                protocol: FOLLOW_PROTOCOL_VERSION,
                search: SearchId::from_hash(hash_bytes(b"an idle search")),
                format: FormatId::new("stub.v1")?,
                workers: 1,
                holder: None,
            },
            FollowFrame::Records(vec!["{\"a\":1}".to_string(), "{\"b\":2}".to_string()]),
            FollowFrame::Records(Vec::new()),
            FollowFrame::Holder(Some("7 host".to_string())),
            FollowFrame::Holder(None),
            FollowFrame::Complete,
            FollowFrame::Fault("the search was never started".to_string()),
        ])
    }

    #[test]
    fn the_follow_protocol_version_is_pinned() {
        // The handshake contract both machines compile against; bumping it is
        // a deliberate act, and the mismatch tests derive their foreign
        // version from this one.
        assert_eq!(FOLLOW_PROTOCOL_VERSION, 2);
    }

    #[test]
    fn every_frame_round_trips_through_its_payload() -> Result<()> {
        for frame in every_variant()? {
            assert_eq!(FollowFrame::decode(&frame.encode())?, frame);
        }
        Ok(())
    }

    #[test]
    fn every_frame_round_trips_through_the_framed_carrier() -> Result<()> {
        let frames = every_variant()?;
        let mut pipe = Vec::new();
        for frame in &frames {
            write_frame(&mut pipe, &frame.encode())?;
        }
        let mut reader = pipe.as_slice();
        for frame in &frames {
            let payload = read_frame(&mut reader)?.expect("a frame");
            assert_eq!(&FollowFrame::decode(&payload)?, frame);
        }
        assert_eq!(read_frame(&mut reader)?, None, "the stream ends cleanly");
        Ok(())
    }

    #[test]
    fn a_truncated_payload_is_an_encoding_error() -> Result<()> {
        // The far side died mid-frame: decoding the partial payload reports
        // the shortfall rather than panicking on the missing bytes.
        let payload = FollowFrame::Records(vec!["a line".to_string()]).encode();
        for cut in 1..payload.len() {
            assert!(
                matches!(
                    FollowFrame::decode(&payload[..cut]),
                    Err(Error::Encoding(_))
                ),
                "a payload cut to {cut} bytes must not decode"
            );
        }
        Ok(())
    }

    #[test]
    fn an_unknown_tag_is_an_encoding_error() {
        let unknown = [200u8];
        assert!(matches!(
            FollowFrame::decode(&unknown),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn trailing_bytes_past_a_frame_are_refused() {
        let mut payload = FollowFrame::Complete.encode();
        payload.push(0);
        assert!(matches!(
            FollowFrame::decode(&payload),
            Err(Error::Encoding(_))
        ));
    }
}
