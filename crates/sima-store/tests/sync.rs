//! Integration tests for the store-to-store sync protocol: two real stores in
//! temp directories, joined by an in-memory duplex pipe, syncing over the
//! public [`Store::sync`] API. Adversarial cases that require injecting a
//! malformed frame (a tampered object) live beside the engine, where the wire
//! vocabulary is reachable; here every peer is a real `Store::sync`.

mod common;

use common::{commit_record, empty_store, run_sync, run_sync_scoped, sample_identity, store_with};
use sima_core::{Error, Result};
use sima_store::{ObjectScope, SyncReport};

/// A single sync carries records both ways: the disjoint halves converge to
/// the union in each store.
#[test]
fn disjoint_stores_converge_to_the_union() -> Result<()> {
    let (_da, a, a_keys) = store_with(&[1, 2]);
    let (_db, b, b_keys) = store_with(&[3]);
    let all: Vec<_> = a_keys.iter().chain(&b_keys).copied().collect();

    let (ra, rb) = run_sync(&a, &all, &b, &all);
    ra?;
    rb?;

    for store in [&a, &b] {
        for key in &all {
            assert!(store.record(key)?.is_some(), "record {key} missing");
        }
    }
    Ok(())
}

/// Overlapping stores transfer only the one record and object each lacks, and
/// the report counters are exact.
#[test]
fn overlapping_stores_transfer_only_the_difference() -> Result<()> {
    let (_da, a, a_keys) = store_with(&[1, 2, 3]);
    let (_db, b, b_keys) = store_with(&[2, 3, 4]);
    let all: Vec<_> = a_keys.iter().chain(&b_keys).copied().collect();

    let (ra, rb) = run_sync(&a, &all, &b, &all);
    // a lacks record 4 (and its artifact); b lacks record 1 (and its
    // artifact). The shared identity components and the overlap (2, 3) move
    // neither way.
    let one_each = SyncReport {
        records_sent: 1,
        records_received: 1,
        objects_sent: 1,
        objects_received: 1,
    };
    assert_eq!(ra?, one_each);
    assert_eq!(rb?, one_each);

    for store in [&a, &b] {
        for seed in [1u64, 2, 3, 4] {
            assert!(
                store.record(&sample_identity(seed).key())?.is_some(),
                "record for seed {seed} missing"
            );
        }
    }
    Ok(())
}

/// A packed source serves the objects it holds exactly as a loose one does:
/// the destination takes the same records and objects, and the counters
/// match the loose-store equivalent.
#[test]
fn a_packed_store_syncs_like_a_loose_one() -> Result<()> {
    let (_dl, loose, loose_keys) = store_with(&[1, 2]);
    let (_dd, loose_destination) = empty_store();
    let (rl, _) = run_sync(&loose, &loose_keys, &loose_destination, &loose_keys);
    let reference = rl?;

    let (_ds, source, keys) = store_with(&[1, 2]);
    source.pack()?;
    let (_dt, destination) = empty_store();
    let (rs, rd) = run_sync(&source, &keys, &destination, &keys);
    assert_eq!(rs?, reference);
    rd?;

    for key in &keys {
        let record = destination.record(key)?.expect("record");
        for artifact in record.artifacts() {
            assert!(destination.has(artifact.object())?, "artifact object taken");
        }
    }
    Ok(())
}

/// A second sync over the converged stores transfers nothing.
#[test]
fn a_second_sync_is_idempotent() -> Result<()> {
    let (_da, a, a_keys) = store_with(&[1, 2]);
    let (_db, b, b_keys) = store_with(&[3]);
    let all: Vec<_> = a_keys.iter().chain(&b_keys).copied().collect();

    run_sync(&a, &all, &b, &all).0?;
    let (ra, rb) = run_sync(&a, &all, &b, &all);
    assert_eq!(ra?, SyncReport::default(), "idempotent for the initiator");
    assert_eq!(rb?, SyncReport::default(), "idempotent for the responder");
    Ok(())
}

/// A record held on both sides under one key with differing bytes is a
/// determinism violation, surfaced loudly by name.
#[test]
fn a_diverged_record_fails_loudly_naming_the_key() -> Result<()> {
    let (_da, a, a_keys) = store_with(&[1]);
    // b holds a record for the same task with a different artifact.
    let (_db, b, _) = store_with(&[]);
    let diverged_key = commit_record(&b, 1, b"a different artifact");
    assert_eq!(diverged_key, a_keys[0]);

    let (ra, rb) = run_sync(&a, &a_keys, &b, &[diverged_key]);
    for report in [ra, rb] {
        match report {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains(&a_keys[0].to_string()), "{msg}")
            }
            other => panic!("expected a divergence validation error, got {other:?}"),
        }
    }
    Ok(())
}

/// An empty key set syncs nothing and reports zeros, even between stores that
/// hold records.
#[test]
fn an_empty_key_set_transfers_nothing() -> Result<()> {
    let (_da, a, _) = store_with(&[1]);
    let (_db, b, _) = store_with(&[2]);

    let (ra, rb) = run_sync(&a, &[], &b, &[]);
    assert_eq!(ra?, SyncReport::default());
    assert_eq!(rb?, SyncReport::default());
    assert!(a.record(&sample_identity(2).key())?.is_none());
    assert!(b.record(&sample_identity(1).key())?.is_none());
    Ok(())
}

/// A key neither store holds a record for is simply not transferred — a
/// half-evaluated search's future tasks have no records yet, and that is no
/// error.
#[test]
fn keys_absent_from_both_sides_are_not_transferred() -> Result<()> {
    let (_da, a, a_keys) = store_with(&[1]);
    let (_db, b, b_keys) = store_with(&[2]);
    let absent = sample_identity(3).key();
    let keys: Vec<_> = a_keys
        .iter()
        .chain(&b_keys)
        .copied()
        .chain([absent])
        .collect();

    let (ra, rb) = run_sync(&a, &keys, &b, &keys);
    ra?;
    rb?;

    for store in [&a, &b] {
        assert!(store.record(&sample_identity(1).key())?.is_some());
        assert!(store.record(&sample_identity(2).key())?.is_some());
        assert!(
            store.record(&absent)?.is_none(),
            "a key neither side holds must not be transferred"
        );
    }
    Ok(())
}

/// A push under a named scope: every record travels, and of the objects the
/// records reference only the named ones do.
#[test]
fn a_named_scope_sends_the_named_objects_and_every_record() -> Result<()> {
    let (_da, a, keys) = store_with(&[1, 2, 3]);
    let (_db, b) = empty_store();
    // Name one record's artifact: the object the far side will actually read.
    let named = [*a
        .record(&keys[2])?
        .expect("the record is committed")
        .artifacts()[0]
        .object()];

    let (ra, rb) = run_sync_scoped(&a, &keys, ObjectScope::Named(&named), &b, &keys);
    ra?;
    rb?;

    // Every record crossed: a chain is traversable forward only, so the far
    // side needs the prefix records to locate the frontier at all.
    for key in &keys {
        assert!(
            b.record(key)?.is_some(),
            "record {key} must travel whatever the object scope"
        );
    }
    // Only the named artifact's bytes crossed with them.
    assert!(b.has(&named[0])?, "the named object travelled");
    for key in &keys[..2] {
        let object = *a.record(key)?.expect("committed").artifacts()[0].object();
        assert!(
            !b.has(&object)?,
            "an unnamed artifact is bandwidth nobody opens"
        );
    }
    Ok(())
}

/// A store that took a named push advertises what it holds, never bytes it
/// cannot serve, so a pull from it completes and converges.
#[test]
fn a_pull_from_a_gapped_store_completes_and_a_third_sync_moves_nothing() -> Result<()> {
    let (_da, a, keys) = store_with(&[1, 2]);
    let (_db, b) = empty_store();
    let named = [*a.record(&keys[1])?.expect("committed").artifacts()[0].object()];

    let (ra, rb) = run_sync_scoped(&a, &keys, ObjectScope::Named(&named), &b, &keys);
    ra?;
    rb?;

    // The gapped store as the initiator of an ordinary sync: it advertises the
    // one object it holds, asks for the one it lacks, and converges.
    let (rb, ra) = run_sync(&b, &keys, &a, &keys);
    rb?;
    ra?;
    for key in &keys {
        let object = *a.record(key)?.expect("committed").artifacts()[0].object();
        assert!(b.has(&object)?, "the pull completed the store");
    }

    // A third session over the converged pair moves nothing.
    let (rb, ra) = run_sync(&b, &keys, &a, &keys);
    assert_eq!(rb?, SyncReport::default());
    assert_eq!(ra?, SyncReport::default());
    Ok(())
}

/// A named object no record references travels all the same: the scope is
/// exactly what the caller named, and not everything a search needs on the far
/// side is something a task record points at.
#[test]
fn a_named_object_outside_the_records_references_still_travels() -> Result<()> {
    let (_da, a, keys) = store_with(&[1]);
    let (_db, b) = empty_store();
    // Bytes nothing in the search references — the shape a program's payload
    // takes, which the far side needs before it can search anything at all.
    let unreferenced = a.put(b"the program this search is served by")?;
    let referenced = *a.record(&keys[0])?.expect("committed").artifacts()[0].object();

    let (ra, rb) = run_sync_scoped(&a, &keys, ObjectScope::Named(&[unreferenced]), &b, &keys);
    ra?;
    rb?;
    assert!(b.has(&unreferenced)?, "the named object travelled");
    assert!(
        !b.has(&referenced)?,
        "and an unnamed one did not, whatever references it"
    );
    Ok(())
}

/// A name for an object this side does not hold advertises nothing: a peer's
/// want is bounded by what was advertised, and a want that could not be
/// fulfilled would fail the session.
#[test]
fn a_named_object_this_side_lacks_is_never_advertised() -> Result<()> {
    let (_da, a, keys) = store_with(&[1]);
    let (_db, b) = empty_store();
    let absent = sima_core::hash_bytes(b"an object neither side holds");

    let (ra, rb) = run_sync_scoped(&a, &keys, ObjectScope::Named(&[absent]), &b, &keys);
    let ra = ra?;
    rb?;
    assert_eq!(ra.objects_sent, 0, "nothing was offered, so nothing moved");
    assert!(b.record(&keys[0])?.is_some(), "the record still travelled");
    assert!(!b.has(&absent)?);
    Ok(())
}
