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

/// Which objects a side advertises, and therefore the most the peer can ask it
/// for: the scope bounds the peer's want, since a want is `theirs − mine` over
/// what was advertised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectScope<'a> {
    /// Every object the records in the key set reference. A side that holds a
    /// complete store advertises everything, which is what a pull wants: the
    /// store that comes home must be complete.
    Referenced,
    /// Exactly the listed objects, of those this side holds. A push uses it to
    /// send the records in full — a chain is traversable forward only, so
    /// without the prefix records the far side cannot locate the frontier at
    /// all — while sending only the state bytes the far side will actually
    /// read.
    Named(&'a [Hash]),
}

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
    /// against its advertised digest, and a received record is matched against
    /// the key and digest it was requested under, so bytes altered between the
    /// advertisement and the delivery fail the session
    /// ([`Error::Validation`], with nothing from that frame committed); a
    /// record held on both sides under one key with differing bytes is
    /// [`Error::Validation`] naming the key.
    /// Writes go through the store's atomic path, so a torn session leaves the
    /// store valid — some records and objects transferred, all intact. Run
    /// twice over identical stores it transfers nothing.
    ///
    /// The task-key set comes from the caller: the store cannot enumerate a
    /// run's tasks without the generator and config, which live above it.
    pub fn sync(
        &self,
        keys: &[TaskKey],
        scope: ObjectScope<'_>,
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
        let (my_records, my_objects) = self.have(keys, scope)?;
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

        // want = theirs − mine, in the peer's advertised order. The record
        // wants keep the digest each key was advertised under, which the
        // fulfillment is then checked against; the wire carries the keys alone.
        let want_records: Vec<(TaskKey, Hash)> = peer_records
            .iter()
            .copied()
            .filter(|(k, _)| !my_records.contains_key(k))
            .collect();
        let want_objects: Vec<Hash> = peer_objects
            .iter()
            .copied()
            .filter(|h| !my_objects.contains(h))
            .collect();
        let my_want = SyncMessage::Want {
            records: want_records.iter().map(|(k, _)| *k).collect(),
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

    /// What this side advertises, filling the protocol's `Have`: the records it
    /// holds within the key set as a key→digest map, and the objects it offers
    /// under `scope`.
    ///
    /// Records are always every one this side holds — they are what makes a
    /// chain traversable — and the object set is derived from them, never a
    /// full CAS scan. The advertised objects are filtered to what this side
    /// actually holds, so a store with gaps never offers bytes it cannot serve:
    /// a peer's want is bounded by this, and a want it could not fulfil would
    /// fail the session.
    fn have(
        &self,
        keys: &[TaskKey],
        scope: ObjectScope<'_>,
    ) -> Result<(BTreeMap<TaskKey, Hash>, BTreeSet<Hash>)> {
        let mut records = BTreeMap::new();
        let mut referenced = BTreeSet::new();
        // A BTreeSet dedups and orders the keys, so a caller's repeated key
        // advertises once and the advertisement is deterministic.
        for key in keys.iter().copied().collect::<BTreeSet<_>>() {
            if let Some(record) = self.record(&key)? {
                referenced.extend(referenced_objects(&record));
                records.insert(key, hash_bytes(&record.to_bytes()));
            }
        }
        let offered = match scope {
            ObjectScope::Referenced => referenced,
            // A named object outside the key set's references is not this
            // side's to offer under these keys, so the named set intersects
            // what the records reference.
            ObjectScope::Named(named) => named
                .iter()
                .copied()
                .filter(|hash| referenced.contains(hash))
                .collect(),
        };
        let mut objects = BTreeSet::new();
        for hash in offered {
            if self.has(&hash)? {
                objects.insert(hash);
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

    /// Takes a fulfillment stream: objects first — each re-hashed against the
    /// digest its frame carries and put — then records, each matched against the
    /// key and digest it was requested under before it is committed. Both loops
    /// walk this side's own want, in the order it was sent, so the stream needs
    /// no terminator and the records arrive in a known order.
    fn receive_fulfillment(
        &self,
        reader: &mut dyn Read,
        objects: &[Hash],
        records: &[(TaskKey, Hash)],
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
        for (requested, digest) in records {
            match recv(reader)? {
                SyncMessage::Record { key, bytes } => {
                    // The want list is this side's, so a key outside it is one
                    // the peer was never asked for.
                    if key != *requested {
                        return Err(Error::Validation(format!(
                            "sync record for task {key} arrived where task {requested} was requested"
                        )));
                    }
                    // The bytes must be the ones the digest was advertised
                    // for, which catches a channel that altered them in
                    // transit — the same guarantee the object loop gives.
                    let actual = hash_bytes(&bytes);
                    if actual != *digest {
                        return Err(Error::Validation(format!(
                            "sync record for task {key} was advertised as {digest} and arrived with bytes hashing to {actual}"
                        )));
                    }
                    let record = TaskRecord::from_bytes(&bytes)?;
                    // The digest is the peer's own claim, so the decoded record
                    // is still held to answering for the key it arrived under.
                    if record.identity.key() != key {
                        return Err(Error::Validation(format!(
                            "sync record labelled task {key} answers for task {}",
                            record.identity.key()
                        )));
                    }
                    self.replicate(&record)?;
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
    use crate::testutil::{
        record_with_stored_artifact, sample_identity, store_identity_components, temp_store,
    };

    /// Drives `store` as a responder against a hand-built peer whose every
    /// frame is precomputed, so the session runs synchronously against a byte
    /// slice with no peer thread. The responder holds no key set of its own, so
    /// it advertises nothing and the peer's `Have` alone decides its want.
    fn responder_reads(
        store: &Store,
        frames: impl IntoIterator<Item = SyncMessage>,
    ) -> Result<SyncReport> {
        let mut incoming = Vec::new();
        for message in frames {
            write_frame(&mut incoming, &message.encode()).expect("frame");
        }
        let mut reader = incoming.as_slice();
        let mut sink = Vec::new();
        store.sync(
            &[],
            ObjectScope::Referenced,
            &mut reader,
            &mut sink,
            SyncRole::Responder,
        )
    }

    /// The handshake and the empty want that precede a peer's fulfillment,
    /// advertising `records` and `objects` as the peer's inventory.
    fn peer_offering(records: Vec<(TaskKey, Hash)>, objects: Vec<Hash>) -> [SyncMessage; 3] {
        [
            SyncMessage::Hello {
                protocol: SYNC_PROTOCOL_VERSION,
            },
            SyncMessage::Have { records, objects },
            SyncMessage::Want {
                records: Vec::new(),
                objects: Vec::new(),
            },
        ]
    }

    #[test]
    fn a_version_mismatch_is_refused_naming_both_versions() {
        // A hand-framed hello at a future version: the responder reads it
        // first and refuses before writing anything.
        let (_dir, store) = temp_store();
        match responder_reads(&store, [SyncMessage::Hello { protocol: 999 }]) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains("999") && msg.contains('1'), "{msg}");
            }
            other => panic!("expected a version-mismatch refusal, got {other:?}"),
        }
    }

    #[test]
    fn an_unexpected_first_message_is_a_protocol_error() {
        let (_dir, store) = temp_store();
        assert!(matches!(
            responder_reads(&store, [SyncMessage::Done]),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn a_tampered_object_is_rejected_and_nothing_is_committed() {
        // The peer advertises one object the responder lacks, then serves a
        // frame whose bytes do not hash to the advertised digest.
        let (_dir, store) = temp_store();
        let advertised = hash_bytes(b"the real object bytes");
        let tampered = b"tampered".to_vec();
        assert_ne!(hash_bytes(&tampered), advertised);

        let frames = peer_offering(Vec::new(), vec![advertised])
            .into_iter()
            .chain([
                SyncMessage::Object {
                    hash: advertised,
                    bytes: tampered.clone(),
                },
                SyncMessage::Done,
            ]);
        match responder_reads(&store, frames) {
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

    #[test]
    fn a_record_that_misses_its_advertised_digest_is_rejected_and_nothing_is_committed() {
        // The peer advertises a record under a digest that is not the digest of
        // the bytes it then serves. The record itself is well formed and
        // answers for the key it arrives under, so the digest comparison is the
        // only check that can fire.
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(1));
        let key = record.identity.key();
        let bytes = record.to_bytes();
        let advertised = hash_bytes(b"a digest these bytes do not have");
        assert_ne!(hash_bytes(&bytes), advertised);

        let frames = peer_offering(vec![(key, advertised)], Vec::new())
            .into_iter()
            .chain([SyncMessage::Record { key, bytes }, SyncMessage::Done]);
        match responder_reads(&store, frames) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains("hashing to") && msg.contains(&key.to_string()),
                    "{msg}"
                );
            }
            other => panic!("expected a digest-mismatch rejection, got {other:?}"),
        }
        assert!(store.record(&key).expect("record lookup").is_none());
    }

    #[test]
    fn a_record_for_an_unwanted_key_is_rejected_and_nothing_is_committed() {
        // The peer advertises one record and serves another. The served record
        // is well formed and hashes to its own digest; what disqualifies it is
        // that this side never asked for its key.
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let wanted = record_with_stored_artifact(&store, sample_identity(1));
        let served = record_with_stored_artifact(&store, sample_identity(2));
        let (wanted_key, served_key) = (wanted.identity.key(), served.identity.key());
        assert_ne!(wanted_key, served_key);

        let frames = peer_offering(
            vec![(wanted_key, hash_bytes(&wanted.to_bytes()))],
            Vec::new(),
        )
        .into_iter()
        .chain([
            SyncMessage::Record {
                key: served_key,
                bytes: served.to_bytes(),
            },
            SyncMessage::Done,
        ]);
        match responder_reads(&store, frames) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains(&served_key.to_string()) && msg.contains(&wanted_key.to_string()),
                    "{msg}"
                );
            }
            other => panic!("expected an unwanted-key rejection, got {other:?}"),
        }
        for key in [wanted_key, served_key] {
            assert!(store.record(&key).expect("record lookup").is_none());
        }
    }
}
