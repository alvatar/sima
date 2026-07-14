//! End-to-end acceptance of the `ca_evolution` domain's asynchronous Neural CA
//! model through the pipeline API: a `ca_evolution.nca` `sima.toml` runs generate
//! → execute → commit → inspect to a finalized manifest committing framed
//! continuation state over 8-channel grids, a segment boundary leaves the
//! committed trajectory byte-identical, and a malformed `[run.params]` or
//! `[run.generator]` section fails at load — before any store or GPU work.

mod common;

use std::path::Path;

use common::loaded_text;
use sima_core::{Error, Hash, Result};
use sima_domains::cellular::Grid;
use sima_domains::decode_continuation;
use sima_pipeline::{LoadedConfig, RunControl, RunOutcome, orchestrate};
use sima_store::Store;

/// The ca_evolution.nca config text: `count` candidates, each a network sampled
/// at `weight_scale = 0.5` (candidate `i` owns a distinct substream, so the
/// generator's duplicate-draw check passes), on a 32x32 grid, `steps` per
/// segment. `segments` renders the optional `[run]` key.
fn config_text(store: &str, count: u32, steps: u32, segments: Option<u64>) -> String {
    let segments = segments.map_or(String::new(), |n| format!("segments = {n}"));
    format!(
        r#"
        [run]
        root_seed = 42
        format = "ca_evolution.nca.v1"
        {segments}

        [run.generator]
        id = "ca_evolution.nca.v1"
        count = {count}
        weight_scale = 0.5

        [run.params]
        width = 32
        height = 32
        steps = {steps}
        dt = 1.0
        seed_value = 1.0
        side_divisor = 8
        noise_width = 0.0

        [execution]
        store = "{store}"
        workers = 2
        max_attempts = 3
    "#
    )
}

/// Writes and loads a ca_evolution.nca config named `name` under `dir`.
fn nca_config(
    dir: &Path,
    name: &str,
    store: &str,
    count: u32,
    steps: u32,
    segments: Option<u64>,
) -> Result<LoadedConfig> {
    loaded_text(dir, name, &config_text(store, count, steps, segments))
}

/// The `state` artifacts across the run's manifest entries: each entry's object
/// hash, the step its framed continuation state reached, and its decoded grid.
fn manifest_states(config: &LoadedConfig) -> Result<Vec<(Hash, u64, Grid)>> {
    let store = Store::open(&config.store)?;
    let manifest = store
        .manifest(&config.run.id())?
        .expect("a finalized manifest");
    manifest
        .entries
        .iter()
        .map(|entry| {
            let record = store
                .record(&entry.task)?
                .expect("a manifest entry's record");
            let artifact = record
                .artifacts()
                .iter()
                .find(|a| a.name() == "state")
                .expect("a ca_evolution record commits the state artifact");
            // The NCA is a stepped model, so its committed state frames the step
            // reached ahead of the grid's canonical bytes.
            let (step, grid) = decode_continuation(&store.get(artifact.object())?)?;
            Ok((*artifact.object(), step, grid))
        })
        .collect()
}

/// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
#[test]
#[ignore = "requires a Vulkan device"]
fn a_ca_evolution_nca_config_runs_the_full_spine() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = nca_config(dir.path(), "sima.toml", "./store", 4, 100, None)?;
    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    // Four candidates, one manifest entry each, every committed state framed at
    // step 100 over a 32x32 eight-channel grid.
    let states = manifest_states(&config)?;
    assert_eq!(states.len(), 4);
    for (_, step, grid) in &states {
        assert_eq!(*step, 100, "a single 100-step segment reaches step 100");
        assert_eq!((grid.width(), grid.height(), grid.channels()), (32, 32, 8));
    }
    Ok(())
}

/// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
#[test]
#[ignore = "requires a Vulkan device"]
fn a_segment_boundary_leaves_the_trajectory_byte_identical() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    // One candidate as a chain of two 50-step segments.
    let segmented = nca_config(
        dir.path(),
        "segmented.toml",
        "./store-segmented",
        1,
        50,
        Some(2),
    )?;
    assert!(matches!(
        orchestrate(&segmented, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let segment_states = manifest_states(&segmented)?;
    assert_eq!(segment_states.len(), 2);
    for (_, _, grid) in &segment_states {
        assert_eq!((grid.width(), grid.height(), grid.channels()), (32, 32, 8));
    }

    // The same trajectory as one unsegmented 100-step task, fresh store.
    let whole = nca_config(dir.path(), "whole.toml", "./store-whole", 1, 100, None)?;
    assert!(matches!(
        orchestrate(&whole, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let whole_states = manifest_states(&whole)?;
    assert_eq!(whole_states.len(), 1);
    let (whole_object, whole_step, whole_grid) = &whole_states[0];
    assert_eq!(*whole_step, 100, "the unsegmented run reaches step 100");
    assert_eq!(whole_grid.channels(), 8);

    // The framed step makes the committed state a complete continuation, so the
    // 100-step state is byte-identical whether or not a segment boundary cut the
    // trajectory. That turns into a content-addressed membership check: the
    // unsegmented state's object must already exist in the segmented run's store,
    // because the second segment committed the same framed bytes.
    let from_segmented = Store::open(&segmented.store)?.get(whole_object)?;
    let from_whole = Store::open(&whole.store)?.get(whole_object)?;
    assert_eq!(from_segmented, from_whole);
    Ok(())
}

#[test]
fn a_malformed_config_fails_at_load() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let valid = config_text("./store", 1, 10, None);
    // Each case malforms one aspect of the config; the load fails with Validation
    // naming the key, before any store or GPU work.
    let cases = [
        ("width = 32", "", "width"), // missing key
        ("width = 32", "width = 32\n        surprise = 1", "surprise"), // unknown key
        ("width = 32", "width = 0", "width"), // zero extent
        ("side_divisor = 8", "side_divisor = 0", "side_divisor"), // zero side_divisor
        ("weight_scale = 0.5", "weight_scale = 0.0", "weight_scale"), // non-positive scale
    ];
    for (original, bad, key) in cases {
        let text = valid.replace(original, bad);
        match loaded_text(dir.path(), "malformed.toml", &text) {
            Err(Error::Validation(message)) => {
                assert!(message.contains(key), "the error names {key}: {message}");
            }
            other => panic!("expected Validation for {bad:?}, got {other:?}"),
        }
    }
    Ok(())
}
