//! The sync wire vocabulary: the messages a have/want session exchanges.
//!
//! Each message is one frame payload ([`sima_core::frame`]): a `u8` tag byte
//! followed by the message's fields in the canonical [`Enc`]/[`Dec`] encoding.
//! Frames are transport encoding, never identity-bearing — an object's bytes
//! carry their own advertised digest, and the receiver re-hashes to verify.

use sima_core::{Dec, Enc, Error, Hash, Result};
use sima_model::TaskKey;

/// Version of the sync protocol; the handshake refuses a mismatch.
///
/// Version 2 carries the inventory as a sequence of chunks rather than one
/// frame, so the two are not interchangeable on the wire.
pub const SYNC_PROTOCOL_VERSION: u32 = 2;

/// The most inventory entries one [`SyncMessage::Have`] or
/// [`SyncMessage::Want`] chunk carries.
///
/// A record entry is two digests and an object entry one, so a full chunk is
/// under 400 KiB — nearly three orders of magnitude below the frame cap, which
/// makes the bound obviously safe rather than tuned against it. The whole
/// inventory travels as a sequence of these, so what the cap bounds is one
/// frame rather than the size of a run: an inventory of any size syncs, where a
/// single frame made a run of about 1.3M tasks impossible to sync at all.
pub(crate) const INVENTORY_CHUNK: usize = 4096;

const TAG_HELLO: u8 = 0;
const TAG_HAVE: u8 = 1;
const TAG_WANT: u8 = 2;
const TAG_RECORD: u8 = 3;
const TAG_OBJECT: u8 = 4;
const TAG_DONE: u8 = 5;

/// One message in a sync session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SyncMessage {
    /// The handshake opening: the protocol version each side speaks.
    Hello {
        /// The sender's [`SYNC_PROTOCOL_VERSION`]; the peer refuses a mismatch.
        protocol: u32,
    },
    /// One chunk of the inventory a side holds within the run's task set: some
    /// of the records it holds as `(key, record digest)` pairs, and some of the
    /// objects those records reference. The record digests let the peer detect
    /// divergence — a key held on both sides under different bytes.
    ///
    /// The inventory is a sequence of these: `more` is set on every chunk but
    /// the last, so a receiver reads until it is clear. A side holding nothing
    /// still sends one empty chunk with `more` clear, which is what makes the
    /// end of the sequence explicit rather than inferred from a count nobody
    /// sent.
    Have {
        /// This chunk's held records as `(task key, record digest)` pairs.
        records: Vec<(TaskKey, Hash)>,
        /// This chunk's object digests.
        objects: Vec<Hash>,
        /// Whether another chunk of this inventory follows.
        more: bool,
    },
    /// One chunk of a request: some of the records and objects the sender lacks
    /// and the peer holds. Chunked exactly as [`SyncMessage::Have`] is.
    Want {
        /// This chunk's wanted task keys.
        records: Vec<TaskKey>,
        /// This chunk's wanted object digests.
        objects: Vec<Hash>,
        /// Whether another chunk of this request follows.
        more: bool,
    },
    /// A wanted record's canonical bytes, under the key it answers for.
    Record {
        /// The record's task key.
        key: TaskKey,
        /// The record's canonical bytes.
        bytes: Vec<u8>,
    },
    /// A wanted object's bytes, under its advertised digest; the receiver
    /// re-hashes to verify.
    Object {
        /// The object's advertised blake3 digest.
        hash: Hash,
        /// The object's bytes.
        bytes: Vec<u8>,
    },
    /// The session close, sent once each side has fulfilled the peer's want.
    Done,
}

impl SyncMessage {
    /// The message's frame payload: tag byte, then fields in wire order.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        match self {
            SyncMessage::Hello { protocol } => {
                enc.u8(TAG_HELLO).u32(*protocol);
            }
            SyncMessage::Have {
                records,
                objects,
                more,
            } => {
                enc.u8(TAG_HAVE).u64(records.len() as u64);
                for (key, record) in records {
                    enc.hash(key.as_hash()).hash(record);
                }
                enc.u64(objects.len() as u64);
                for object in objects {
                    enc.hash(object);
                }
                enc.u8(u8::from(*more));
            }
            SyncMessage::Want {
                records,
                objects,
                more,
            } => {
                enc.u8(TAG_WANT).u64(records.len() as u64);
                for key in records {
                    enc.hash(key.as_hash());
                }
                enc.u64(objects.len() as u64);
                for object in objects {
                    enc.hash(object);
                }
                enc.u8(u8::from(*more));
            }
            SyncMessage::Record { key, bytes } => {
                enc.u8(TAG_RECORD).hash(key.as_hash()).bytes(bytes);
            }
            SyncMessage::Object { hash, bytes } => {
                enc.u8(TAG_OBJECT).hash(hash).bytes(bytes);
            }
            SyncMessage::Done => {
                enc.u8(TAG_DONE);
            }
        }
        enc.finish()
    }

    /// A one-word name for the message kind, for protocol-sequencing
    /// diagnostics.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            SyncMessage::Hello { .. } => "hello",
            SyncMessage::Have { .. } => "have",
            SyncMessage::Want { .. } => "want",
            SyncMessage::Record { .. } => "record",
            SyncMessage::Object { .. } => "object",
            SyncMessage::Done => "done",
        }
    }

    /// Parses a frame payload written by [`SyncMessage::encode`], rejecting
    /// unknown tags and trailing bytes.
    pub(crate) fn decode(payload: &[u8]) -> Result<SyncMessage> {
        let mut dec = Dec::new(payload);
        let message = match dec.u8()? {
            TAG_HELLO => SyncMessage::Hello {
                protocol: dec.u32()?,
            },
            TAG_HAVE => {
                let records = decode_record_pairs(&mut dec)?;
                let objects = decode_hashes(&mut dec)?;
                SyncMessage::Have {
                    records,
                    objects,
                    more: decode_more(&mut dec)?,
                }
            }
            TAG_WANT => {
                let records = decode_keys(&mut dec)?;
                let objects = decode_hashes(&mut dec)?;
                SyncMessage::Want {
                    records,
                    objects,
                    more: decode_more(&mut dec)?,
                }
            }
            TAG_RECORD => SyncMessage::Record {
                key: TaskKey::from_hash(dec.hash()?),
                bytes: dec.bytes()?.to_vec(),
            },
            TAG_OBJECT => SyncMessage::Object {
                hash: dec.hash()?,
                bytes: dec.bytes()?.to_vec(),
            },
            TAG_DONE => SyncMessage::Done,
            tag => {
                return Err(Error::Encoding(format!("unknown sync message tag {tag}")));
            }
        };
        dec.finish()?;
        Ok(message)
    }
}

/// Reads the chunk-continuation flag: 0 or 1, and nothing else, so a byte no
/// encoder here writes fails to decode rather than reading as "another chunk
/// follows".
fn decode_more(dec: &mut Dec<'_>) -> Result<bool> {
    match dec.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(Error::Encoding(format!(
            "invalid sync chunk continuation byte {other}, expected 0 or 1"
        ))),
    }
}

/// Reads a `u64`-counted run of `(task key, record digest)` pairs. The count
/// is untrusted, so pairs accumulate without preallocation: each reads two
/// digests, so a lying count fails on truncation before any oversized buffer.
fn decode_record_pairs(dec: &mut Dec<'_>) -> Result<Vec<(TaskKey, Hash)>> {
    let count = dec.u64()?;
    let mut pairs = Vec::new();
    for _ in 0..count {
        let key = TaskKey::from_hash(dec.hash()?);
        let record = dec.hash()?;
        pairs.push((key, record));
    }
    Ok(pairs)
}

/// Reads a `u64`-counted run of task keys, without preallocating from the
/// untrusted count.
fn decode_keys(dec: &mut Dec<'_>) -> Result<Vec<TaskKey>> {
    let count = dec.u64()?;
    let mut keys = Vec::new();
    for _ in 0..count {
        keys.push(TaskKey::from_hash(dec.hash()?));
    }
    Ok(keys)
}

/// Reads a `u64`-counted run of digests, without preallocating from the
/// untrusted count.
fn decode_hashes(dec: &mut Dec<'_>) -> Result<Vec<Hash>> {
    let count = dec.u64()?;
    let mut hashes = Vec::new();
    for _ in 0..count {
        hashes.push(dec.hash()?);
    }
    Ok(hashes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::hash_bytes;

    fn key(seed: &[u8]) -> TaskKey {
        TaskKey::from_hash(hash_bytes(seed))
    }

    /// A sample of every message, empty and populated collections included.
    fn messages() -> Vec<SyncMessage> {
        vec![
            SyncMessage::Hello {
                protocol: SYNC_PROTOCOL_VERSION,
            },
            SyncMessage::Have {
                records: Vec::new(),
                objects: Vec::new(),
                more: false,
            },
            SyncMessage::Have {
                records: vec![
                    (key(b"k1"), hash_bytes(b"r1")),
                    (key(b"k2"), hash_bytes(b"r2")),
                ],
                objects: vec![hash_bytes(b"o1"), hash_bytes(b"o2"), hash_bytes(b"o3")],
                more: true,
            },
            SyncMessage::Want {
                records: Vec::new(),
                objects: Vec::new(),
                more: false,
            },
            SyncMessage::Want {
                records: vec![key(b"k3")],
                objects: vec![hash_bytes(b"o4")],
                more: true,
            },
            SyncMessage::Record {
                key: key(b"k4"),
                bytes: vec![1, 2, 3],
            },
            SyncMessage::Record {
                key: key(b"k5"),
                bytes: Vec::new(),
            },
            SyncMessage::Object {
                hash: hash_bytes(b"o5"),
                bytes: vec![9, 8, 7, 6],
            },
            SyncMessage::Object {
                hash: hash_bytes(b"o6"),
                bytes: Vec::new(),
            },
            SyncMessage::Done,
        ]
    }

    #[test]
    fn the_protocol_version_is_pinned() {
        // The handshake contract; bumping it is a deliberate act. Version 2
        // carries the inventory as a sequence of chunks, so a version-1 peer
        // and this one cannot read each other's frames.
        assert_eq!(SYNC_PROTOCOL_VERSION, 2);
    }

    #[test]
    fn every_message_round_trips() -> Result<()> {
        for message in messages() {
            assert_eq!(SyncMessage::decode(&message.encode())?, message);
        }
        Ok(())
    }

    #[test]
    fn unknown_message_tags_are_encoding_errors() {
        for payload in [[6u8].as_slice(), [255u8].as_slice()] {
            assert!(matches!(
                SyncMessage::decode(payload),
                Err(Error::Encoding(_))
            ));
        }
    }

    #[test]
    fn trailing_bytes_after_a_message_are_rejected() {
        for message in messages() {
            let mut payload = message.encode();
            payload.push(0);
            assert!(matches!(
                SyncMessage::decode(&payload),
                Err(Error::Encoding(_))
            ));
        }
    }

    #[test]
    fn every_truncation_is_rejected() {
        // Every proper prefix of every message must fail to decode, never
        // panic — the decoder trusts nothing beyond its checks.
        for message in messages() {
            let payload = message.encode();
            for cut in 0..payload.len() {
                assert!(
                    SyncMessage::decode(&payload[..cut]).is_err(),
                    "prefix of {cut} bytes must be rejected"
                );
            }
        }
    }
}
