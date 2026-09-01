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

use sima_core::{Codec, Error, Hash, Result, hash_bytes, read_frame, write_frame};
use sima_model::{TaskKey, TaskRecord};

use crate::catalog::referenced_objects;
use crate::store::Store;
use crate::sync::message::{INVENTORY_CHUNK, SYNC_PROTOCOL_VERSION, SyncMessage};

/// Which objects a side advertises, and therefore the most the peer can ask it
/// for: the scope bounds the peer's want, since a want is `theirs − mine` over
/// what was advertised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectScope<'a> {
    /// Every object the records in the key set reference. A side that holds a
    /// complete store advertises everything, which is what a pull wants: the
    /// store that comes home must be complete.
    Referenced,
    /// Exactly the listed objects, of those this side holds — whether or not a
    /// record in the key set references them. A push uses it to send the
    /// records in full — a chain is traversable forward only, so without the
    /// prefix records the far side cannot locate the frontier at all — while
    /// sending only the state bytes the far side will actually read, plus
    /// whatever else the search needs there: the program a config-routed format
    /// is served by travels this way, and no task record names it.
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

/// One chunk of a peer's inventory: its records, its objects, and whether
/// another chunk follows.
type HaveChunk = (Vec<(TaskKey, Hash)>, Vec<Hash>, bool);

/// One chunk of a peer's request, shaped as [`HaveChunk`] is.
type WantChunk = (Vec<TaskKey>, Vec<Hash>, bool);

/// A want request: the wanted record keys and object digests.
type Want = (Vec<TaskKey>, Vec<Hash>);

impl Store {
    /// Synchronizes this store with a peer over a byte pipe, transferring the
    /// task records within `keys` and the objects they reference so both sides
    /// end holding the union. `reader`/`writer` are the pipe halves to the
    /// peer, which searches `sync` with the opposite [`SyncRole`].
    ///
    /// Only records and objects move — checkpoints, placement, journals, and
    /// manifests stay with their orchestrator. Every arriving object and record
    /// is matched against the want it answers, both the item requested at that
    /// position and the digest it was advertised under, so an item this side
    /// never asked for and bytes altered after their advertisement each fail
    /// the session ([`Error::Validation`], with nothing from that frame
    /// committed); a record held on both sides under one key with differing
    /// bytes is [`Error::Validation`] naming the key.
    /// Writes go through the store's atomic path, so a torn session leaves the
    /// store valid — some records and objects transferred, all intact. Run
    /// twice over identical stores it transfers nothing.
    ///
    /// The task-key set comes from the caller: the store cannot enumerate a
    /// search's tasks without the generator and config, which live above it.
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
        // Each travels as a sequence of bounded chunks, so an inventory of any
        // size crosses rather than running into the frame cap.
        let (my_records, my_objects) = self.have(keys, scope)?;
        let advertised: Vec<(TaskKey, Hash)> = my_records.iter().map(|(k, h)| (*k, *h)).collect();
        let my_object_list: Vec<Hash> = my_objects.iter().copied().collect();
        let (peer_records, peer_objects) = if initiator {
            send_have(writer, &advertised, &my_object_list)?;
            recv_have(reader)?
        } else {
            let peer = recv_have(reader)?;
            send_have(writer, &advertised, &my_object_list)?;
            peer
        };

        // want = theirs − mine, in the peer's advertised order. The record
        // wants keep the digest each key was advertised under, which the
        // fulfillment is then checked against; the wire carries the keys alone.
        //
        // "Mine" is what this store **holds**, not what it advertised. The two
        // differ: advertising is bounded by the caller's key set, while a store
        // may hold a record or an object outside it — one it was sent under a
        // wider set in an earlier session. Asking for those back would re-search
        // every earlier transfer at each session. So a peer-advertised item
        // this side does not advertise is looked up before it is wanted.
        //
        // Divergence is checked over the same union: a key both sides hold
        // under differing record bytes is a determinism violation, surfaced
        // before any transfer.
        let mut want_records = Vec::new();
        for (key, peer_record) in &peer_records {
            let mine = match my_records.get(key) {
                Some(mine) => Some(*mine),
                None => self
                    .record(key)?
                    .map(|record| hash_bytes(&record.to_bytes())),
            };
            match mine {
                None => want_records.push((*key, *peer_record)),
                Some(mine) if mine != *peer_record => {
                    return Err(Error::Validation(format!(
                        "sync record divergence under task {key}: this side holds {mine}, \
                         peer holds {peer_record}"
                    )));
                }
                Some(_) => {}
            }
        }
        let mut want_objects = Vec::new();
        for hash in &peer_objects {
            if !my_objects.contains(hash) && !self.has(hash)? {
                want_objects.push(*hash);
            }
        }
        let want_keys: Vec<TaskKey> = want_records.iter().map(|(k, _)| *k).collect();

        // Want and fulfillment interlock: the initiator sends its want and
        // takes the peer's fulfillment, then serves the peer's want; the
        // responder mirrors. One side writes while the other reads throughout.
        let (peer_want_records, peer_want_objects): Want = if initiator {
            send_want(writer, &want_keys, &want_objects)?;
            self.receive_fulfillment(reader, &want_objects, &want_records)?;
            let peer_want = recv_want(reader)?;
            self.send_fulfillment(writer, &peer_want.1, &peer_want.0)?;
            peer_want
        } else {
            let peer_want = recv_want(reader)?;
            self.send_fulfillment(writer, &peer_want.1, &peer_want.0)?;
            send_want(writer, &want_keys, &want_objects)?;
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
        let offered: BTreeSet<Hash> = match scope {
            ObjectScope::Referenced => referenced,
            // Exactly what the caller named. Not every object a side needs to
            // offer is one a record references: a search whose format is served
            // by a program of its own carries that program as objects too, and
            // no task record names them.
            ObjectScope::Named(named) => named.iter().copied().collect(),
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

    /// Takes a fulfillment stream: objects first, then records, each committed
    /// once its objects are durable. Both loops walk this side's own want in
    /// the order it was sent, so the stream needs no terminator, and both hold
    /// every arrival to that want twice over: it must be the item requested at
    /// its position, and its bytes must hash to the digest it was advertised
    /// under. The want is this side's, so the first check binds what the peer
    /// may deliver; the digest is the peer's own advertisement, so the second
    /// catches bytes altered between the advertisement and the delivery.
    fn receive_fulfillment(
        &self,
        reader: &mut dyn Read,
        objects: &[Hash],
        records: &[(TaskKey, Hash)],
    ) -> Result<()> {
        for requested in objects {
            match recv(reader)? {
                SyncMessage::Object { hash, bytes } => {
                    // An unrequested object would consume the arrival the
                    // wanted one was to occupy, so the session would end with
                    // the store short of bytes it asked for and no error to say
                    // so — records replicate without their artifacts present.
                    if hash != *requested {
                        return Err(Error::Validation(format!(
                            "sync object {hash} arrived where object {requested} was requested"
                        )));
                    }
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
                    if key != *requested {
                        return Err(Error::Validation(format!(
                            "sync record for task {key} arrived where task {requested} was requested"
                        )));
                    }
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

/// Writes an inventory as a sequence of bounded chunks, the last with `more`
/// clear. An empty inventory is one empty chunk, so the end of the sequence is
/// always stated rather than inferred.
fn send_have(writer: &mut dyn Write, records: &[(TaskKey, Hash)], objects: &[Hash]) -> Result<()> {
    chunked(
        records,
        objects,
        INVENTORY_CHUNK,
        |records, objects, more| {
            send(
                writer,
                &SyncMessage::Have {
                    records: records.to_vec(),
                    objects: objects.to_vec(),
                    more,
                },
            )
        },
    )
}

/// Writes a request as a sequence of bounded chunks, exactly as [`send_have`]
/// writes an inventory.
fn send_want(writer: &mut dyn Write, records: &[TaskKey], objects: &[Hash]) -> Result<()> {
    chunked(
        records,
        objects,
        INVENTORY_CHUNK,
        |records, objects, more| {
            send(
                writer,
                &SyncMessage::Want {
                    records: records.to_vec(),
                    objects: objects.to_vec(),
                    more,
                },
            )
        },
    )
}

/// Splits two lists into chunks of at most `bound` entries each and hands every
/// chunk to `emit`, with `more` set on all but the last.
///
/// The two lists are drained independently and paired up, so a long record list
/// beside a short object list still fills its chunks rather than being paced by
/// the shorter one. `emit` sees at least one chunk however empty the lists are.
fn chunked<R, O>(
    records: &[R],
    objects: &[O],
    bound: usize,
    mut emit: impl FnMut(&[R], &[O], bool) -> Result<()>,
) -> Result<()> {
    let mut records = records;
    let mut objects = objects;
    loop {
        let (record_chunk, record_rest) = records.split_at(bound.min(records.len()));
        let (object_chunk, object_rest) = objects.split_at(bound.min(objects.len()));
        let more = !record_rest.is_empty() || !object_rest.is_empty();
        emit(record_chunk, object_chunk, more)?;
        if !more {
            return Ok(());
        }
        records = record_rest;
        objects = object_rest;
    }
}

/// The most chunks one inventory or request may span.
///
/// A peer sets `more` on every chunk but the last, so a peer that never clears
/// it keeps this side reading and accumulating forever. The ceiling is what
/// makes the loop terminate on a peer that will not. With each chunk's lists
/// capped at [`INVENTORY_CHUNK`] entries on receive, it admits about a billion
/// entries per list, past any real store and far short of exhausting memory.
const MAX_INVENTORY_CHUNKS: usize = 262_144;

/// Reads an inventory: one chunk after another until one clears `more`,
/// accumulating what they carry.
fn recv_have(reader: &mut dyn Read) -> Result<PeerInventory> {
    recv_chunks(reader, MAX_INVENTORY_CHUNKS, "inventory", expect_have)
}

/// Reads a request the same way [`recv_have`] reads an inventory.
fn recv_want(reader: &mut dyn Read) -> Result<Want> {
    recv_chunks(reader, MAX_INVENTORY_CHUNKS, "request", expect_want)
}

/// Reads chunks through `accept` until one clears `more`, accumulating both
/// lists, and refuses a peer that passes `limit` chunks without clearing it or
/// sends a chunk carrying more than [`INVENTORY_CHUNK`] entries in either list
/// — more than a compliant sender ever puts in one chunk, and what keeps the
/// chunk ceiling an actual memory bound.
///
/// `noun` names what was being read, so the refusal says which of the two
/// sequences ran away. The limit is a parameter so the ceiling can be exercised
/// on a handful of chunks rather than the real one.
fn recv_chunks<R, O>(
    reader: &mut dyn Read,
    limit: usize,
    noun: &str,
    accept: impl Fn(SyncMessage) -> Result<(Vec<R>, Vec<O>, bool)>,
) -> Result<(Vec<R>, Vec<O>)> {
    let (mut records, mut objects) = (Vec::new(), Vec::new());
    for _ in 0..limit {
        let (chunk_records, chunk_objects, more) = accept(recv(reader)?)?;
        if chunk_records.len() > INVENTORY_CHUNK || chunk_objects.len() > INVENTORY_CHUNK {
            return Err(Error::Validation(format!(
                "sync protocol error: a {noun} chunk carries {} records and {} objects, \
                 over the {INVENTORY_CHUNK}-entry chunk bound",
                chunk_records.len(),
                chunk_objects.len()
            )));
        }
        records.extend(chunk_records);
        objects.extend(chunk_objects);
        if !more {
            return Ok((records, objects));
        }
    }
    Err(Error::Validation(format!(
        "sync protocol error: the peer's {noun} passed {limit} chunks without clearing `more`"
    )))
}

/// Accepts a [`SyncMessage::Have`] chunk, returning its records, objects, and
/// whether another follows.
fn expect_have(message: SyncMessage) -> Result<HaveChunk> {
    match message {
        SyncMessage::Have {
            records,
            objects,
            more,
        } => Ok((records, objects, more)),
        other => Err(unexpected("have", &other)),
    }
}

/// Accepts a [`SyncMessage::Want`] chunk, returning its records, objects, and
/// whether another follows.
fn expect_want(message: SyncMessage) -> Result<WantChunk> {
    match message {
        SyncMessage::Want {
            records,
            objects,
            more,
        } => Ok((records, objects, more)),
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
    /// frame is precomputed, so the session searches synchronously against a byte
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
            SyncMessage::Have {
                records,
                objects,
                more: false,
            },
            SyncMessage::Want {
                records: Vec::new(),
                objects: Vec::new(),
                more: false,
            },
        ]
    }

    #[test]
    fn an_inventory_larger_than_one_chunk_travels_whole() {
        // The chunking is what makes an inventory of any size syncable: one
        // frame capped a search at about 1.3M tasks, past which sync was
        // impossible rather than slow. The bound is a parameter here so the
        // case searches on a handful of entries instead of a quarter of a gigabyte.
        let records: Vec<(TaskKey, Hash)> = (0..7u8)
            .map(|i| (TaskKey::from_hash(hash_bytes(&[i])), hash_bytes(&[i, 1])))
            .collect();
        let objects: Vec<Hash> = (0..5u8).map(|i| hash_bytes(&[i, 2])).collect();

        let mut chunks: Vec<(usize, usize, bool)> = Vec::new();
        chunked(&records, &objects, 3, |r, o, more| {
            chunks.push((r.len(), o.len(), more));
            Ok(())
        })
        .expect("the split succeeds");

        // Seven records at three per chunk is three chunks; the objects search out
        // first and their chunks empty rather than pacing the records.
        assert_eq!(chunks, vec![(3, 3, true), (3, 2, true), (1, 0, false)]);
    }

    #[test]
    fn an_empty_inventory_is_one_chunk_that_states_it_is_the_last() {
        // The end of the sequence is stated rather than inferred, so a side
        // holding nothing still says so and the peer stops reading.
        let mut chunks = 0;
        chunked::<TaskKey, Hash>(&[], &[], 4, |r, o, more| {
            chunks += 1;
            assert!(r.is_empty() && o.is_empty() && !more);
            Ok(())
        })
        .expect("the split succeeds");
        assert_eq!(chunks, 1);
    }

    #[test]
    fn a_chunk_carries_at_most_the_bound() {
        // What keeps a frame under the cap: no chunk exceeds the bound on
        // either list, whatever the two lengths are.
        let records: Vec<(TaskKey, Hash)> = (0..10u8)
            .map(|i| (TaskKey::from_hash(hash_bytes(&[i])), hash_bytes(&[i, 1])))
            .collect();
        let objects: Vec<Hash> = (0..25u8).map(|i| hash_bytes(&[i, 2])).collect();
        chunked(&records, &objects, 4, |r, o, _| {
            assert!(r.len() <= 4 && o.len() <= 4, "{} {}", r.len(), o.len());
            Ok(())
        })
        .expect("the split succeeds");
    }

    #[test]
    fn an_inventory_and_a_request_arriving_in_two_chunks_are_read_whole() {
        // The receive side of the chunking: `more` is what says another frame
        // follows, so a peer whose inventory or request spans two frames must
        // have both read before either is acted on. A reader that stopped at
        // the first chunk would serve half a request and silently drop the
        // rest, which is the failure this pins against.
        let (_dir, store) = temp_store();
        let first = store
            .put(b"the first object the peer asks for")
            .expect("put");
        let second = store
            .put(b"the second object the peer asks for")
            .expect("put");

        // The peer advertises two objects it holds across two `Have` chunks —
        // neither is in this store, so both must reach the responder's want —
        // and asks for two of this store's objects across two `Want` chunks.
        let held: [Vec<u8>; 2] = [b"the peer's first object".to_vec(), b"its second".to_vec()];
        let frames = [
            SyncMessage::Hello {
                protocol: SYNC_PROTOCOL_VERSION,
            },
            SyncMessage::Have {
                records: Vec::new(),
                objects: vec![hash_bytes(&held[0])],
                more: true,
            },
            SyncMessage::Have {
                records: Vec::new(),
                objects: vec![hash_bytes(&held[1])],
                more: false,
            },
            SyncMessage::Want {
                records: Vec::new(),
                objects: vec![first],
                more: true,
            },
            SyncMessage::Want {
                records: Vec::new(),
                objects: vec![second],
                more: false,
            },
            // The peer serves both objects it advertised. That the responder
            // accepts them at all is the proof the second `Have` chunk reached
            // its want: an object nobody asked for is refused.
            SyncMessage::Object {
                hash: hash_bytes(&held[0]),
                bytes: held[0].clone(),
            },
            SyncMessage::Object {
                hash: hash_bytes(&held[1]),
                bytes: held[1].clone(),
            },
            SyncMessage::Done,
        ];
        let report = responder_reads(&store, frames).expect("the session completes");
        assert_eq!(report.objects_sent, 2, "both requested chunks were served");
        assert_eq!(
            report.objects_received, 2,
            "both advertised chunks were asked for"
        );
    }

    #[test]
    fn a_peer_that_never_clears_more_is_refused_at_the_ceiling() {
        // `more` is the peer's to clear, so a peer that never does keeps this
        // side reading and accumulating without end. The ceiling is what makes
        // the loop terminate on it; the refusal names what ran away.
        let mut incoming = Vec::new();
        for _ in 0..4 {
            let chunk = SyncMessage::Have {
                records: Vec::new(),
                objects: Vec::new(),
                more: true,
            };
            write_frame(&mut incoming, &chunk.encode()).expect("frame");
        }
        let mut reader = incoming.as_slice();
        let refusal = recv_chunks(&mut reader, 3, "inventory", expect_have)
            .expect_err("a peer past the ceiling is refused");
        let Error::Validation(message) = refusal else {
            panic!("expected a protocol refusal");
        };
        assert!(
            message.contains("inventory") && message.contains('3'),
            "{message}"
        );
    }

    #[test]
    fn an_oversized_chunk_is_refused_naming_the_bound() {
        // A compliant sender never puts more than INVENTORY_CHUNK entries in
        // one chunk, so a chunk past that is a protocol violation — and
        // refusing it is what keeps the chunk ceiling a memory bound rather
        // than a termination bound alone.
        let mut incoming = Vec::new();
        let chunk = SyncMessage::Have {
            records: Vec::new(),
            objects: (0..=INVENTORY_CHUNK)
                .map(|i| hash_bytes(&(i as u64).to_le_bytes()))
                .collect(),
            more: false,
        };
        write_frame(&mut incoming, &chunk.encode()).expect("frame");
        let mut reader = incoming.as_slice();
        let refusal = recv_chunks(&mut reader, 3, "inventory", expect_have)
            .expect_err("an oversized chunk is refused");
        let Error::Validation(message) = refusal else {
            panic!("expected a protocol refusal");
        };
        assert!(
            message.contains("inventory") && message.contains(&INVENTORY_CHUNK.to_string()),
            "{message}"
        );
    }

    #[test]
    fn a_version_mismatch_is_refused_naming_both_versions() {
        // A hand-framed hello at a future version: the responder reads it
        // first and refuses before writing anything.
        let (_dir, store) = temp_store();
        match responder_reads(&store, [SyncMessage::Hello { protocol: 999 }]) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains("999") && msg.contains(&SYNC_PROTOCOL_VERSION.to_string()),
                    "{msg}"
                );
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
    fn an_object_that_was_not_requested_is_rejected_and_nothing_is_committed() {
        // The peer advertises one object and serves another. The served object
        // hashes to the digest its own frame carries; what disqualifies it is
        // that this side never asked for it, and accepting it would consume the
        // arrival the wanted object was to occupy.
        let (_dir, store) = temp_store();
        let requested = hash_bytes(b"the object this side wants");
        let served = b"an object nobody asked for".to_vec();
        let served_hash = hash_bytes(&served);
        assert_ne!(served_hash, requested);

        let frames = peer_offering(Vec::new(), vec![requested])
            .into_iter()
            .chain([
                SyncMessage::Object {
                    hash: served_hash,
                    bytes: served,
                },
                SyncMessage::Done,
            ]);
        match responder_reads(&store, frames) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains(&served_hash.to_string()) && msg.contains(&requested.to_string()),
                    "{msg}"
                );
            }
            other => panic!("expected an unrequested-object rejection, got {other:?}"),
        }
        for hash in [requested, served_hash] {
            assert!(!store.has(&hash).expect("object lookup"));
        }
    }

    #[test]
    fn what_this_side_holds_outside_its_key_set_is_never_asked_for() {
        // Advertising is bounded by the caller's key set; holding is not. A
        // store sent a record under a wider set in an earlier session must not
        // ask for it back, or every session would re-search every earlier one.
        // The key set here is empty, so this side advertises none of what it
        // holds, which is exactly the case.
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(7));
        store.commit(&record).expect("commit the record");
        let key = record.identity.key();
        let object = *record.artifacts()[0].object();

        let frames = peer_offering(vec![(key, hash_bytes(&record.to_bytes()))], vec![object])
            .into_iter()
            .chain([SyncMessage::Done]);
        assert_eq!(
            responder_reads(&store, frames).expect("the session completes"),
            SyncReport::default(),
            "nothing was asked for and nothing was served"
        );
    }

    #[test]
    fn divergence_is_caught_over_a_record_held_outside_the_key_set_too() {
        // The comparison follows what this side holds, so a determinism
        // violation is caught whether or not the key was advertised.
        let (_dir, store) = temp_store();
        store_identity_components(&store);
        let record = record_with_stored_artifact(&store, sample_identity(7));
        store.commit(&record).expect("commit the record");
        let key = record.identity.key();
        let elsewhere = hash_bytes(b"a record the peer holds under this key");

        let frames = peer_offering(vec![(key, elsewhere)], Vec::new())
            .into_iter()
            .chain([SyncMessage::Done]);
        match responder_reads(&store, frames) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains("divergence") && msg.contains(&key.to_string()),
                    "{msg}"
                );
            }
            other => panic!("expected a divergence rejection, got {other:?}"),
        }
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
