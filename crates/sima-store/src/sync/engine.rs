//! The have/want sync engine: [`Store::sync`] and the session it drives.
//!
//! Both stores advertise the records they hold within the caller's task set
//! and the objects those records reference; each computes `want = theirs −
//! mine` and requests it; each fulfills the peer's request. Objects lead the
//! fulfillment stream so a record commits only once its referenced objects are
//! durable, and every write goes through the store's atomic path — a torn
//! session leaves the store valid.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};

use sima_core::{Error, Hash, Result, hash_bytes, read_frame, write_frame};
use sima_model::{TaskKey, TaskRecord};

use crate::catalog::referenced_objects;
use crate::store::Store;
use crate::sync::message::{SYNC_PROTOCOL_VERSION, SyncMessage};

/// Which side of a sync session a store plays. The roles interlock the
/// exchange so that at every step one side writes while the other reads,
/// leaving the session deadlock-free over an unbuffered duplex pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncRole {
    /// Opens the handshake and sends its want first.
    Initiator,
    /// Mirrors the initiator: reads the handshake, then fulfills first.
    Responder,
}

/// What a sync session transferred, from one side's view. Observational:
/// callers and tests read it; it enters no identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SyncReport {
    /// Records this side sent to the peer.
    pub records_sent: usize,
    /// Records this side received and committed.
    pub records_received: usize,
    /// Objects this side sent to the peer.
    pub objects_sent: usize,
    /// Objects this side received and committed.
    pub objects_received: usize,
}

/// A peer's advertised inventory: its held records as `(key, digest)` pairs
/// and the objects they reference.
type PeerInventory = (Vec<(TaskKey, Hash)>, Vec<Hash>);

/// A want request: the wanted record keys and object digests.
type Want = (Vec<TaskKey>, Vec<Hash>);

impl Store {
    /// Synchronizes this store with a peer over a byte pipe, transferring the
    /// task records within `keys` and the objects they reference so both sides
    /// end holding the union. `reader`/`writer` are the pipe halves to the
    /// peer, which runs `sync` with the opposite [`SyncRole`].
    ///
    /// Only records and objects move — checkpoints, placement, journals, and
    /// manifests stay with their orchestrator. A received object is re-hashed
    /// against its advertised digest ([`Error::Validation`] on mismatch, with
    /// nothing from that frame committed); a record held on both sides under
    /// one key with differing bytes is [`Error::Validation`] naming the key.
    /// Writes go through the store's atomic path, so a torn session leaves the
    /// store valid — some records and objects transferred, all intact. Run
    /// twice over identical stores it transfers nothing.
    ///
    /// The task-key set comes from the caller: the store cannot enumerate a
    /// run's tasks without the generator and config, which live above it.
    pub fn sync(
        &self,
        keys: &[TaskKey],
        reader: &mut dyn Read,
        writer: &mut dyn Write,
        role: SyncRole,
    ) -> Result<SyncReport> {
        let initiator = role == SyncRole::Initiator;

        // Handshake: the initiator opens, the responder answers.
        let hello = SyncMessage::Hello {
            protocol: SYNC_PROTOCOL_VERSION,
        };
        if initiator {
            send(writer, &hello)?;
            expect_hello(recv(reader)?)?;
        } else {
            expect_hello(recv(reader)?)?;
            send(writer, &hello)?;
        }

        // Advertise this side's inventory within the key set; read the peer's.
        let (my_records, my_objects) = self.inventory(keys)?;
        let my_have = SyncMessage::Have {
            records: my_records.iter().map(|(k, h)| (*k, *h)).collect(),
            objects: my_objects.iter().copied().collect(),
        };
        let (peer_records, peer_objects) = if initiator {
            send(writer, &my_have)?;
            expect_have(recv(reader)?)?
        } else {
            let peer = expect_have(recv(reader)?)?;
            send(writer, &my_have)?;
            peer
        };

        // Divergence: a key both sides hold under differing record bytes is a
        // determinism violation, surfaced before any transfer.
        for (key, peer_record) in &peer_records {
            if let Some(mine) = my_records.get(key)
                && mine != peer_record
            {
                return Err(Error::Validation(format!(
                    "sync record divergence under task {key}: this side holds {mine}, peer holds {peer_record}"
                )));
            }
        }

        // want = theirs − mine, in the peer's advertised order.
        let want_records: Vec<TaskKey> = peer_records
            .iter()
            .map(|(k, _)| *k)
            .filter(|k| !my_records.contains_key(k))
            .collect();
        let want_objects: Vec<Hash> = peer_objects
            .iter()
            .copied()
            .filter(|h| !my_objects.contains(h))
            .collect();
        let my_want = SyncMessage::Want {
            records: want_records.clone(),
            objects: want_objects.clone(),
        };

        // Want and fulfillment interlock: the initiator sends its want and
        // takes the peer's fulfillment, then serves the peer's want; the
        // responder mirrors. One side writes while the other reads throughout.
        let (peer_want_records, peer_want_objects): Want = if initiator {
            send(writer, &my_want)?;
            self.receive_fulfillment(reader, &want_objects, &want_records)?;
            let peer_want = expect_want(recv(reader)?)?;
            self.send_fulfillment(writer, &peer_want.1, &peer_want.0)?;
            peer_want
        } else {
            let peer_want = expect_want(recv(reader)?)?;
            self.send_fulfillment(writer, &peer_want.1, &peer_want.0)?;
            send(writer, &my_want)?;
            self.receive_fulfillment(reader, &want_objects, &want_records)?;
            peer_want
        };

        // Close: symmetric, initiator first.
        if initiator {
            send(writer, &SyncMessage::Done)?;
            expect_done(recv(reader)?)?;
        } else {
            expect_done(recv(reader)?)?;
            send(writer, &SyncMessage::Done)?;
        }

        Ok(SyncReport {
            records_sent: peer_want_records.len(),
            records_received: want_records.len(),
            objects_sent: peer_want_objects.len(),
            objects_received: want_objects.len(),
        })
    }

    /// This side's inventory within the key set: the held records as a
    /// key→digest map, and the set of every object those records reference.
    /// The object set is derived from the records, never a full CAS scan.
    fn inventory(&self, keys: &[TaskKey]) -> Result<(BTreeMap<TaskKey, Hash>, BTreeSet<Hash>)> {
        let mut records = BTreeMap::new();
        let mut objects = BTreeSet::new();
        // A BTreeSet dedups and orders the keys, so a caller's repeated key
        // advertises once and the inventory is deterministic.
        for key in keys.iter().copied().collect::<BTreeSet<_>>() {
            if let Some(record) = self.record(&key)? {
                objects.extend(referenced_objects(&record));
                records.insert(key, hash_bytes(&record.to_bytes()));
            }
        }
        Ok((records, objects))
    }

    /// Serves a peer's want: every requested object, then every requested
    /// record. Objects lead so a streaming receiver commits each record only
    /// after its referenced objects are durable.
    fn send_fulfillment(
        &self,
        writer: &mut dyn Write,
        objects: &[Hash],
        records: &[TaskKey],
    ) -> Result<()> {
        for hash in objects {
            let bytes = self.get(hash)?;
            send(writer, &SyncMessage::Object { hash: *hash, bytes })?;
        }
        for key in records {
            let record = self.record(key)?.ok_or_else(|| {
                Error::Validation(format!(
                    "peer requested a record for task {key}, which this side does not hold"
                ))
            })?;
            send(
                writer,
                &SyncMessage::Record {
                    key: *key,
                    bytes: record.to_bytes(),
                },
            )?;
        }
        Ok(())
    }

    /// Takes a fulfillment stream: objects first — each re-hashed and put —
    /// then records, each committed once its objects are durable. The counts
    /// are this side's own want, so the stream needs no terminator.
    fn receive_fulfillment(
        &self,
        reader: &mut dyn Read,
        objects: &[Hash],
        records: &[TaskKey],
    ) -> Result<()> {
        for _ in objects {
            match recv(reader)? {
                SyncMessage::Object { hash, bytes } => {
                    let actual = hash_bytes(&bytes);
                    if actual != hash {
                        return Err(Error::Validation(format!(
                            "sync object {hash} arrived with bytes hashing to {actual}"
                        )));
                    }
                    self.put(&bytes)?;
                }
                other => return Err(unexpected("object", &other)),
            }
        }
        for _ in records {
            match recv(reader)? {
                SyncMessage::Record { key, bytes } => {
                    let record = TaskRecord::from_bytes(&bytes)?;
                    if record.identity.key() != key {
                        return Err(Error::Validation(format!(
                            "sync record labelled task {key} answers for task {}",
                            record.identity.key()
                        )));
                    }
                    self.commit_record(&record)?;
                }
                other => return Err(unexpected("record", &other)),
            }
        }
        Ok(())
    }
}

/// Frames and writes one message.
fn send(writer: &mut dyn Write, message: &SyncMessage) -> Result<()> {
    write_frame(writer, &message.encode())
}

/// Reads and decodes one message; a clean end-of-stream mid-session is a
/// protocol violation, not a valid close (only [`SyncMessage::Done`] closes).
fn recv(reader: &mut dyn Read) -> Result<SyncMessage> {
    match read_frame(reader)? {
        Some(payload) => SyncMessage::decode(&payload),
        None => Err(Error::Validation(
            "sync peer closed the connection mid-session".to_string(),
        )),
    }
}

/// Accepts a [`SyncMessage::Hello`] at this protocol version, refusing a
/// mismatch by naming both versions.
fn expect_hello(message: SyncMessage) -> Result<()> {
    match message {
        SyncMessage::Hello { protocol } if protocol == SYNC_PROTOCOL_VERSION => Ok(()),
        SyncMessage::Hello { protocol } => Err(Error::Validation(format!(
            "sync protocol mismatch: this side speaks {SYNC_PROTOCOL_VERSION}, peer speaks {protocol}"
        ))),
        other => Err(unexpected("hello", &other)),
    }
}

/// Accepts a [`SyncMessage::Have`], returning its records and objects.
fn expect_have(message: SyncMessage) -> Result<PeerInventory> {
    match message {
        SyncMessage::Have { records, objects } => Ok((records, objects)),
        other => Err(unexpected("have", &other)),
    }
}

/// Accepts a [`SyncMessage::Want`], returning its records and objects.
fn expect_want(message: SyncMessage) -> Result<Want> {
    match message {
        SyncMessage::Want { records, objects } => Ok((records, objects)),
        other => Err(unexpected("want", &other)),
    }
}

/// Accepts a [`SyncMessage::Done`].
fn expect_done(message: SyncMessage) -> Result<()> {
    match message {
        SyncMessage::Done => Ok(()),
        other => Err(unexpected("done", &other)),
    }
}

/// A protocol-sequencing error: the phase expected one message and got
/// another.
fn unexpected(expected: &str, got: &SyncMessage) -> Error {
    Error::Validation(format!(
        "sync protocol error: expected a {expected} message, got a {} message",
        got.label()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    /// Opens a store over a fresh temporary directory, keeping the guard
    /// alive for the test's duration.
    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        (dir, store)
    }

    #[test]
    fn a_version_mismatch_is_refused_naming_both_versions() {
        // A hand-framed hello at a future version: the responder reads it
        // first and refuses before writing anything.
        let (_dir, store) = temp_store();
        let mut incoming = Vec::new();
        write_frame(
            &mut incoming,
            &SyncMessage::Hello { protocol: 999 }.encode(),
        )
        .expect("frame hello");
        let mut reader = incoming.as_slice();
        let mut sink = Vec::new();
        match store.sync(&[], &mut reader, &mut sink, SyncRole::Responder) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains("999") && msg.contains('1'), "{msg}");
            }
            other => panic!("expected a version-mismatch refusal, got {other:?}"),
        }
    }

    #[test]
    fn an_unexpected_first_message_is_a_protocol_error() {
        let (_dir, store) = temp_store();
        let mut incoming = Vec::new();
        write_frame(&mut incoming, &SyncMessage::Done.encode()).expect("frame");
        let mut reader = incoming.as_slice();
        let mut sink = Vec::new();
        assert!(matches!(
            store.sync(&[], &mut reader, &mut sink, SyncRole::Responder),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn a_tampered_object_is_rejected_and_nothing_is_committed() {
        // A hand-built initiator drives a real responder to want one object,
        // then serves a frame whose bytes do not hash to the advertised
        // digest. The responder must reject it and commit nothing. Every frame
        // the responder reads is precomputed, so the responder runs
        // synchronously against a byte slice — no peer thread.
        let (_dir, store) = temp_store();
        let advertised = hash_bytes(b"the real object bytes");
        let tampered = b"tampered".to_vec();
        assert_ne!(hash_bytes(&tampered), advertised);

        let mut incoming = Vec::new();
        // Handshake, then advertise the object the responder lacks with no
        // records, then a want of nothing, then the tampered fulfillment for
        // the responder's own want.
        for message in [
            SyncMessage::Hello {
                protocol: SYNC_PROTOCOL_VERSION,
            },
            SyncMessage::Have {
                records: Vec::new(),
                objects: vec![advertised],
            },
            SyncMessage::Want {
                records: Vec::new(),
                objects: Vec::new(),
            },
            SyncMessage::Object {
                hash: advertised,
                bytes: tampered.clone(),
            },
        ] {
            write_frame(&mut incoming, &message.encode()).expect("frame");
        }

        let mut reader = incoming.as_slice();
        let mut sink = Vec::new();
        match store.sync(&[], &mut reader, &mut sink, SyncRole::Responder) {
            Err(Error::Validation(msg)) => assert!(msg.contains("hashing to"), "{msg}"),
            other => panic!("expected a hash-mismatch rejection, got {other:?}"),
        }
        // Neither the advertised digest nor the tampered bytes were committed.
        assert!(!store.has(&advertised).expect("has advertised"));
        assert!(
            !store
                .has(&hash_bytes(&tampered))
                .expect("has tampered bytes")
        );
    }
}
