//! End-to-end acceptance of segmented execution through the pipeline API:
//! a chain of segments equals one unsegmented task of equal length, the
//! manifest is deterministic across fresh stores, an interrupted chain
//! resumes to the reference manifest, a shared store reuses committed
//! chain prefixes, a segmented run over a stateless domain is rejected
//! naming the state artifact, and the segment count is identity-bearing.

mod common;

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{journal_events, loaded_text};
use sima_core::{Error, Result};
use sima_domains::StubState;
use sima_pipeline::{Event, LoadedConfig, Record, RunControl, RunOutcome, orchestrate};
use sima_store::Store;

/// A segmented `accumulate` config: `chains` candidates, `k` steps per
/// segment. `segments` renders the optional `[run]` key, `checkpoint` the
/// optional `[execution]` key.
fn accumulate_config(
    dir: &Path,
    name: &str,
    store: &str,
    k: u64,
    segments: Option<u64>,
    checkpoint_ms: Option<u64>,
) -> Result<LoadedConfig> {
    let segments = segments.map_or(String::new(), |n| format!("segments = {n}"));
    let checkpoint =
        checkpoint_ms.map_or(String::new(), |ms| format!("checkpoint_interval_ms = {ms}"));
    let text = format!(
        r#"
        [run]
        root_seed = 7
        format = "stub.v1"
        {segments}

        [run.generator]
        id = "stub.v1"
        behaviors = ["accumulate:{k}"]

        [execution]
        store = "{store}"
        workers = 2
        max_attempts = 3
        {checkpoint}
    "#
    );
    loaded_text(dir, name, &text)
}

/// The chain's final continuation state: the committed `state` artifact
/// with the highest absolute step across the run's manifest entries.
fn final_state(config: &LoadedConfig) -> Result<Vec<u8>> {
    let store = Store::open(&config.store)?;
    let manifest = store
        .manifest(&config.run.id())?
        .expect("a finalized manifest");
    let mut latest: Option<StubState> = None;
    for entry in &manifest.entries {
        let record = store
            .record(&entry.task)?
            .expect("a manifest entry's record");
        let artifact = record
            .artifacts()
            .iter()
            .find(|a| a.name() == "state")
            .expect("a segmented record commits the state artifact");
        let state = StubState::from_bytes(&store.get(artifact.object())?)?;
        if latest.is_none_or(|s| state.step > s.step) {
            latest = Some(state);
        }
    }
    Ok(latest.expect("at least one state artifact").to_bytes())
}

#[test]
fn a_segmented_run_equals_an_unsegmented_run_of_equal_length() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let segmented = accumulate_config(
        dir.path(),
        "segmented.toml",
        "./store-segmented",
        100,
        Some(10),
        None,
    )?;
    let whole = accumulate_config(dir.path(), "whole.toml", "./store-whole", 1000, None, None)?;
    for config in [&segmented, &whole] {
        assert!(matches!(
            orchestrate(config, &RunControl::detached())?,
            RunOutcome::Finalized { .. }
        ));
    }
    // Ten segments of 100 steps land on the same state bytes as one task
    // of 1000 steps: the trajectory is keyed by the absolute step index.
    assert_eq!(final_state(&segmented)?, final_state(&whole)?);
    Ok(())
}

#[test]
fn a_segmented_config_is_deterministic_across_fresh_stores() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let first = accumulate_config(dir.path(), "first.toml", "./store-first", 20, Some(5), None)?;
    let second = accumulate_config(
        dir.path(),
        "second.toml",
        "./store-second",
        20,
        Some(5),
        None,
    )?;
    assert_eq!(first.run.id(), second.run.id());
    for config in [&first, &second] {
        assert!(matches!(
            orchestrate(config, &RunControl::detached())?,
            RunOutcome::Finalized { .. }
        ));
    }
    let run = first.run.id();
    assert_eq!(
        Store::open(&first.store)?.manifest(&run)?.expect("first"),
        Store::open(&second.store)?.manifest(&run)?.expect("second"),
    );
    Ok(())
}

#[test]
fn an_interrupted_chain_resumes_to_the_reference_manifest() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = accumulate_config(
        dir.path(),
        "interrupted.toml",
        "./store-interrupted",
        50,
        Some(8),
        None,
    )?;
    let run = config.run.id();

    // Interrupt after the first commit: mid-chain, segments remain.
    let interrupt = AtomicBool::new(false);
    let control = RunControl {
        observer: &|record: &Record| {
            if matches!(record.event, Event::Committed { .. }) {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
        on_start: None,
    };
    assert!(matches!(
        orchestrate(&config, &control)?,
        RunOutcome::Interrupted { .. }
    ));
    assert!(
        Store::open(&config.store)?.manifest(&run)?.is_none(),
        "an interrupted run writes no manifest"
    );

    // Resume over the same store; reference in a fresh store.
    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let reference = accumulate_config(
        dir.path(),
        "reference.toml",
        "./store-reference",
        50,
        Some(8),
        None,
    )?;
    assert!(matches!(
        orchestrate(&reference, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(
        Store::open(&config.store)?
            .manifest(&run)?
            .expect("resumed"),
        Store::open(&reference.store)?
            .manifest(&run)?
            .expect("reference"),
    );
    Ok(())
}

#[test]
fn a_longer_chain_reuses_the_shared_prefix_of_a_shorter_run() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    // Five segments into the shared store.
    let five = accumulate_config(dir.path(), "five.toml", "./store-shared", 10, Some(5), None)?;
    assert!(matches!(
        orchestrate(&five, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    // Ten segments over the same store: the first five keys are already
    // answered, so only the second half runs.
    let ten = accumulate_config(dir.path(), "ten.toml", "./store-shared", 10, Some(10), None)?;
    assert_ne!(five.run.id(), ten.run.id(), "segments enters the run id");
    assert!(matches!(
        orchestrate(&ten, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let leases = journal_events(&ten)
        .iter()
        .filter(|e| matches!(e, Event::Leased { .. }))
        .count();
    assert_eq!(leases, 5, "exactly the unanswered segments run");

    // The shared-store manifest equals a fresh-store ten-segment manifest.
    let fresh = accumulate_config(
        dir.path(),
        "fresh.toml",
        "./store-fresh",
        10,
        Some(10),
        None,
    )?;
    assert!(matches!(
        orchestrate(&fresh, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let run = ten.run.id();
    assert_eq!(
        Store::open(&ten.store)?.manifest(&run)?.expect("shared"),
        Store::open(&fresh.store)?.manifest(&run)?.expect("fresh"),
    );
    Ok(())
}

#[test]
fn segments_over_a_stateless_behavior_fails_naming_the_state_artifact() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let text = r#"
        [run]
        root_seed = 7
        format = "stub.v1"
        segments = 3

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed"]

        [execution]
        store = "./store"
        workers = 1
        max_attempts = 1
    "#;
    let config = loaded_text(dir.path(), "stateless.toml", text)?;
    match orchestrate(&config, &RunControl::detached()) {
        Err(Error::Validation(msg)) => {
            assert!(msg.contains("state"), "the error names the artifact: {msg}");
        }
        other => panic!("expected Validation, got {other:?}"),
    }
    Ok(())
}

#[test]
fn one_segment_matches_the_static_batch_keys_under_a_distinct_run_id() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let one = accumulate_config(dir.path(), "one.toml", "./store-one", 25, Some(1), None)?;
    let batch = accumulate_config(dir.path(), "batch.toml", "./store-batch", 25, None, None)?;
    // The field is identity-bearing even at its degenerate value.
    assert_ne!(one.run.id(), batch.run.id());
    for config in [&one, &batch] {
        assert!(matches!(
            orchestrate(config, &RunControl::detached())?,
            RunOutcome::Finalized { .. }
        ));
    }
    // The task set is the same: segment 0 of a one-segment chain is the
    // static batch's stateless task.
    let one_manifest = Store::open(&one.store)?
        .manifest(&one.run.id())?
        .expect("one-segment manifest");
    let batch_manifest = Store::open(&batch.store)?
        .manifest(&batch.run.id())?
        .expect("batch manifest");
    let one_keys: Vec<_> = one_manifest.entries.iter().map(|e| e.task).collect();
    let batch_keys: Vec<_> = batch_manifest.entries.iter().map(|e| e.task).collect();
    assert_eq!(one_keys, batch_keys);
    Ok(())
}
