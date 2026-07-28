//! `run_keys`: the task keys a run comprises, derived from `(config, store
//! state)` without driving anything.
//!
//! The derivation is the frontier's own, so it answers whatever the store
//! currently supports: over an empty store the keys a run starts from, over a
//! partly-committed chain the keys materialized so far, and over a finalized
//! run exactly what its manifest lists. A store sync needs that set on both
//! sides, and each side must derive it independently — no key list crosses the
//! wire — so this is the one derivation both halves call.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::{chained_config, config, environment, exec, run_controlled, run_into, temp_store};
use sima_core::Result;
use sima_domains::{StubBehavior, StubGenerator};
use sima_model::TaskKey;
use sima_scheduler::{Event, Record, RunControl, RunOutcome, run_keys};
use sima_store::Store;

/// The keys `cfg` comprises over `store`, as its current state materializes
/// them.
fn keys(store: &Store, cfg: &sima_model::RunConfig) -> Result<Vec<TaskKey>> {
    let generator = StubGenerator::new()?;
    run_keys(store, cfg, &environment(), &generator)
}

#[test]
fn over_an_empty_store_a_batch_yields_one_key_per_candidate() -> Result<()> {
    let (_dir, store) = temp_store();
    let cfg = config(
        7,
        vec![
            StubBehavior::Succeed,
            StubBehavior::Succeed,
            StubBehavior::Succeed,
        ],
    );
    assert_eq!(keys(&store, &cfg)?.len(), 3);
    Ok(())
}

#[test]
fn over_an_empty_store_a_chain_yields_one_key_per_candidate() -> Result<()> {
    // A chain is traversable forward only: segment k's key derives from segment
    // k−1's committed state, so an empty store materializes the first segment of
    // each candidate's chain and nothing beyond it.
    let (_dir, store) = temp_store();
    let cfg = chained_config(7, vec![StubBehavior::Accumulate(2); 2], 6);
    assert_eq!(keys(&store, &cfg)?.len(), 2);
    Ok(())
}

#[test]
fn a_partly_committed_chain_yields_the_keys_it_has_materialized() -> Result<()> {
    // One candidate over twenty segments, interrupted shortly after it starts
    // committing. A chain is traversable forward only, so what the derivation
    // can name is exactly the prefix the store answered plus the one successor
    // those answers made runnable — never the whole chain, and never a segment
    // beyond the frontier.
    //
    // How far the run got before the interrupt landed is not pinned: the
    // observer sees a commit that is already durable, so a worker may commit
    // once more before the driver reads the flag. The invariant is what this
    // asserts.
    let (_dir, store) = temp_store();
    let cfg = chained_config(7, vec![StubBehavior::Accumulate(2)], 20);
    let interrupt = AtomicBool::new(false);
    let committed = AtomicUsize::new(0);
    let control = RunControl {
        observer: &|record: &Record| {
            if matches!(record.event, Event::Committed { .. })
                && committed.fetch_add(1, Ordering::Relaxed) + 1 >= 2
            {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
        on_start: None,
    };
    let outcome = run_controlled(&store, &cfg, &exec(1, 1, 5_000), &control)?;
    assert!(matches!(outcome, RunOutcome::Interrupted { .. }));

    let derived = keys(&store, &cfg)?;
    let (frontier, answered) = derived.split_last().expect("at least one key");
    // Every key but the last is a segment the store already answered — the
    // records a far side needs to locate the frontier at all.
    let mut prefix = 0;
    for key in answered {
        assert!(
            store.record(key)?.is_some(),
            "the materialized prefix is what the store already answered"
        );
        prefix += 1;
    }
    assert!(prefix >= 2, "the run committed before it was interrupted");
    assert!(prefix < 20, "the interrupt landed before the chain ran out");
    assert!(
        store.record(frontier)?.is_none(),
        "the last key is the frontier, not yet answered"
    );
    Ok(())
}

#[test]
fn deriving_the_keys_twice_over_one_store_answers_the_same() -> Result<()> {
    // Constructing a source writes the run's spec objects, which is idempotent:
    // a spec's address is the hash of its own bytes. A second derivation over
    // the same store therefore answers identically and adds nothing.
    let (_dir, store) = temp_store();
    let cfg = chained_config(3, vec![StubBehavior::Accumulate(2); 2], 4);
    assert_eq!(keys(&store, &cfg)?, keys(&store, &cfg)?);
    Ok(())
}

#[test]
fn over_a_finalized_run_the_keys_are_exactly_what_the_manifest_lists() -> Result<()> {
    // The regression the sync rests on: after a run finalizes, the derivation
    // and the manifest name the same set, so a pull that completes it leaves
    // nothing the manifest would miss.
    let (_dir, store) = temp_store();
    let cfg = chained_config(11, vec![StubBehavior::Accumulate(2); 2], 3);
    assert!(matches!(
        run_into(&store, &cfg, &exec(2, 1, 5_000))?,
        RunOutcome::Finalized { .. }
    ));

    let manifest = store
        .manifest(&cfg.id())?
        .expect("a finalized run has a manifest");
    let mut listed: Vec<TaskKey> = manifest.entries.iter().map(|entry| entry.task).collect();
    let mut derived: Vec<TaskKey> = keys(&store, &cfg)?;
    listed.sort();
    derived.sort();
    assert_eq!(derived, listed);
    Ok(())
}
