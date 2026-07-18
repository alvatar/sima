//! Integration tests for the store-to-store sync protocol: two real stores in
//! temp directories, joined by an in-memory duplex pipe, syncing over the
//! public [`Store::sync`] API. Adversarial cases that require injecting a
//! malformed frame (a tampered object) live beside the engine, where the wire
//! vocabulary is reachable; here every peer is an honest `Store::sync`.

mod common;

use common::{commit_record, run_sync, sample_identity, store_with};
use sima_core::{Error, Result};
use sima_store::SyncReport;

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
/// half-evaluated run's future tasks have no records yet, and that is no
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
