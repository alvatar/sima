//! End-to-end acceptance of the Gray-Scott CUDA program through the pipeline
//! API: a `ca_evolution.gray_scott_cuda` `sima.toml` runs generate → execute →
//! commit → inspect to a finalized manifest, a segment boundary leaves the
//! committed trajectory byte-identical, the same genome evaluated by the two
//! backends lands on the same grid within tolerance, and a malformed
//! `[run.params]` section fails at load — before any store or GPU work.
//!
//! The `on_device` tests drive a real device; the program's identity, its
//! params validation, and the shipped config are checked device-free.

mod common;

use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

use common::{
    comment_out, load_example_variant, loaded_text, shipped_example, uncomment, uncomment_block,
};
use sima_core::{Error, Hash, Result};
use sima_domains::substrates::cellular::Grid;
use sima_pipeline::{
    BinaryChange, Cost, DeviceSelector, Engagement, LoadedConfig, Pool, RunControl, RunOutcome,
    orchestrate,
};
use sima_store::Store;

/// The shipped example this suite guards.
const EXAMPLE: &str = "gray-scott-cuda-search.toml";

/// The commented blocks a fleet needs, in the order the example declares them.
const FLEET_BLOCKS: [&str; 7] = [
    "[host.gpubox]",
    "[host_class.lab]",
    "[host_class.oldlab]",
    "[host.cloudbox]",
    "[host_class.rtx4090]",
    "[fleet]",
    "[budget]",
];

/// The relative tolerance the two backends' grids are held to: the same
/// figure their engines' own cross-backend comparison uses. A fused
/// multiply-add one compiler emits and the other does not moves a cell within
/// this envelope; a transcription error moves it far outside.
const TOLERANCE: f64 = 1e-3;

/// A config over `format` for the Gray-Scott rule: `count` candidates in a
/// narrow band around the pattern point — the feed range is the one
/// non-degenerate axis, so every candidate is distinct — on a 32x32 grid,
/// `steps` per segment. `segments` renders the optional `[run]` key.
fn config_text(format: &str, store: &str, count: u32, steps: u32, segments: Option<u64>) -> String {
    let segments = segments.map_or(String::new(), |n| format!("segments = {n}"));
    format!(
        r#"
        [run]
        root_seed = 42
        format = "{format}"
        {segments}

        [run.generator]
        id = "{format}"
        count = {count}
        feed = [0.054, 0.056]
        kill = [0.062, 0.062]
        diffusion_u = [0.16, 0.16]
        diffusion_v = [0.08, 0.08]

        [run.params]
        width = 32
        height = 32
        steps = {steps}
        dt = 1.0
        base_u = 0.5
        base_v = 0.25
        side_divisor = 8
        noise_width = 0.02

        [config]
        store = "{store}"
        max_attempts = 3

        [orchestrator]
        workers = 2
    "#
    )
}

/// The CUDA program's format id, which is also its generator id.
const CUDA_FORMAT: &str = "ca_evolution.gray_scott_cuda.v1";

/// The WGSL program's format id, for the cross-program comparison.
const WGSL_FORMAT: &str = "ca_evolution.gray_scott.v1";

/// Writes and loads a config named `name` under `dir`.
fn config(
    dir: &Path,
    name: &str,
    format: &str,
    store: &str,
    count: u32,
    steps: u32,
    segments: Option<u64>,
) -> Result<LoadedConfig> {
    loaded_text(
        dir,
        name,
        &config_text(format, store, count, steps, segments),
    )
}

/// The `state` artifacts across the run's manifest entries: each entry's
/// object hash and its decoded grid.
fn manifest_states(config: &LoadedConfig) -> Result<Vec<(Hash, Grid)>> {
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
            let grid = Grid::from_bytes(&store.get(artifact.object())?)?;
            Ok((*artifact.object(), grid))
        })
        .collect()
}

#[test]
fn the_two_programs_are_separately_addressable() -> Result<()> {
    // Device-free: both configs load, and the runs they describe carry
    // different ids. The rule is shared and the identities are not, so a run
    // of one never resolves a task key to the other's stored result.
    let dir = tempfile::tempdir().expect("temp dir");
    let cuda = config(
        dir.path(),
        "cuda.toml",
        CUDA_FORMAT,
        "./store",
        4,
        100,
        None,
    )?;
    let wgsl = config(
        dir.path(),
        "wgsl.toml",
        WGSL_FORMAT,
        "./store",
        4,
        100,
        None,
    )?;
    assert_eq!(cuda.run.format.as_str(), CUDA_FORMAT);
    assert_ne!(cuda.run.id(), wgsl.run.id());
    Ok(())
}

#[test]
fn a_malformed_params_section_fails_at_load() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let valid = config_text(CUDA_FORMAT, "./store", 1, 10, None);
    // Each case malforms one aspect of [run.params]; the load fails with
    // Validation naming the key, before any store or GPU work.
    let cases = [
        ("width = 32", "", "width"), // missing key
        ("width = 32", "width = 32\n        surprise = 1", "surprise"), // unknown key
        ("dt = 1.0", "dt = 0.0", "dt"), // zero dt
        ("width = 32", "width = 0", "width"), // zero extent
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

#[test]
fn the_shipped_cuda_search_config_loads() -> Result<()> {
    // The committed `examples/gray-scott-cuda-search.toml` parses cleanly
    // through the full load path, device-free — a guard on the shipped file
    // itself, so an edit that breaks it fails here rather than only at run
    // time.
    let loaded = load_example_variant(EXAMPLE, &shipped_example(EXAMPLE))?;
    assert_eq!(loaded.run.format.as_str(), CUDA_FORMAT);
    assert_eq!(loaded.execution.workers, 2);
    Ok(())
}

// The variants below each enable one commented group of the example and load
// the result, so every knob the file ships is parsed by a test rather than only
// read by a human. The `[domain."<format>"]` block is in no variant: the binary
// it names is spawned when the config loads, so enabling it would run a program.

#[test]
fn the_shipped_search_config_loads_with_the_snapshot_predicate_enabled() -> Result<()> {
    // The example ships the `snapshot_when` line commented, so no test parses
    // the template's predicate syntax or its scalar vocabulary, and both can
    // drift unnoticed. Enabling the line and loading pins both: translation
    // validates the scalar against the reduction's names, so a load that
    // succeeds proves the shipped syntax parses and the scalar is one the
    // reduction emits.
    let text = uncomment(&shipped_example(EXAMPLE), &["snapshot_when"]);
    load_example_variant(EXAMPLE, &text)?;

    // Gray-Scott is a two-channel model (u, v); the example's scalar must be a
    // name the reduction emits for it.
    let names = sima_domains::substrates::cellular::scalar_names(2);
    assert!(
        names.iter().any(|name| name == "activity"),
        "the example's scalar is a reduction name: {names:?}"
    );
    Ok(())
}

#[test]
fn the_shipped_search_config_loads_with_its_deadlines_and_cadences() -> Result<()> {
    let text = uncomment(
        &shipped_example(EXAMPLE),
        &[
            "attempt_timeout_ms",
            "answer_timeout_ms",
            "checkpoint_interval_ms",
            "checkpoint_interval_steps",
        ],
    );
    let loaded = load_example_variant(EXAMPLE, &text)?;
    let execution = &loaded.execution;
    assert_eq!(execution.attempt_timeout, Duration::from_millis(300_000));
    assert_eq!(execution.answer_timeout, Duration::from_millis(120_000));
    assert_eq!(execution.checkpoint_interval, Duration::from_millis(30_000));
    assert_eq!(execution.checkpoint_interval_steps, NonZeroU64::new(500));
    Ok(())
}

#[test]
fn the_shipped_search_config_loads_segmented() -> Result<()> {
    // `segments` is exclusive with `snapshot_when`; the example ships both
    // commented, and this variant enables the one and leaves the other.
    let text = uncomment(&shipped_example(EXAMPLE), &["segments = 10"]);
    let loaded = load_example_variant(EXAMPLE, &text)?;
    assert_eq!(loaded.run.segments, NonZeroU64::new(10));
    Ok(())
}

#[test]
fn the_shipped_search_config_loads_with_an_orchestrator_device_table() -> Result<()> {
    // Device tables are exclusive with `workers`, so the plain count goes
    // behind a comment marker before the table comes out from behind one — in
    // that order, since the table declares a worker count of its own. A host's
    // device tables parse through the same code, so this covers that syntax
    // too.
    let text = comment_out(&shipped_example(EXAMPLE), &["workers = 2"]);
    let text = uncomment_block(&text, "[[orchestrator.device]]");
    let loaded = load_example_variant(EXAMPLE, &text)?;
    assert_eq!(
        loaded.orchestrator.pool,
        Some(Pool::Devices(vec![DeviceSelector {
            select: "nvidia".to_string(),
            workers: 2,
        }])),
        "the pool comes from the device table"
    );
    Ok(())
}

#[test]
fn the_shipped_search_config_loads_with_its_whole_fleet() -> Result<()> {
    let text = uncomment(&shipped_example(EXAMPLE), &["migrate"]);
    let text = FLEET_BLOCKS
        .iter()
        .fold(text, |text, header| uncomment_block(&text, header));
    let loaded = load_example_variant(EXAMPLE, &text)?;
    assert_eq!(loaded.orchestrator.migrate.as_deref(), Some("cloudbox"));
    assert_eq!(loaded.fleet.members, ["gpubox", "lab", "rtx4090"]);
    assert_eq!(loaded.budget.max_spend, Some(Cost(5_000_000)));
    // The example ships the ceiling at 0, which states none at all.
    assert_eq!(loaded.budget.max_wall_clock, None);
    // Every declared machine parsed, in both forms: two hosts and three
    // classes, owned and rented among them.
    assert_eq!(
        loaded.hosts.keys().collect::<Vec<_>>(),
        ["cloudbox", "gpubox"]
    );
    assert_eq!(
        loaded.host_classes.keys().collect::<Vec<_>>(),
        ["lab", "oldlab", "rtx4090"]
    );
    Ok(())
}

/// Running the CUDA program through the spine needs a CUDA device, and the cross-program comparison needs a Vulkan one beside it.
mod on_device {
    use super::*;

    #[test]
    fn a_gray_scott_cuda_config_runs_the_full_spine() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = config(
            dir.path(),
            "sima.toml",
            CUDA_FORMAT,
            "./store",
            4,
            100,
            None,
        )?;
        assert!(matches!(
            orchestrate(
                &config,
                &RunControl::detached(),
                Engagement::Orchestrator,
                BinaryChange::Refuse
            )?,
            RunOutcome::Finalized { .. }
        ));
        // Four candidates, one manifest entry each, every committed state a
        // 32x32 two-channel grid. The dimensions are the whole assertion: the
        // qualitative dynamics story lives with the engine's own tests.
        let states = manifest_states(&config)?;
        assert_eq!(states.len(), 4);
        for (_, grid) in &states {
            assert_eq!((grid.width(), grid.height(), grid.channels()), (32, 32, 2));
        }
        Ok(())
    }

    #[test]
    fn a_segment_boundary_leaves_the_trajectory_byte_identical() -> Result<()> {
        // Resume across a segment boundary is backend-independent machinery, and
        // this is what proves the CUDA engine's resident state survives it: the
        // second segment must continue from the first segment's committed grid and
        // land exactly where an unsegmented run of the same length lands.
        let dir = tempfile::tempdir().expect("temp dir");
        let segmented = config(
            dir.path(),
            "segmented.toml",
            CUDA_FORMAT,
            "./store-segmented",
            1,
            50,
            Some(2),
        )?;
        assert!(matches!(
            orchestrate(
                &segmented,
                &RunControl::detached(),
                Engagement::Orchestrator,
                BinaryChange::Refuse,
            )?,
            RunOutcome::Finalized { .. }
        ));
        assert_eq!(manifest_states(&segmented)?.len(), 2);

        let whole = config(
            dir.path(),
            "whole.toml",
            CUDA_FORMAT,
            "./store-whole",
            1,
            100,
            None,
        )?;
        assert!(matches!(
            orchestrate(
                &whole,
                &RunControl::detached(),
                Engagement::Orchestrator,
                BinaryChange::Refuse
            )?,
            RunOutcome::Finalized { .. }
        ));
        let whole_states = manifest_states(&whole)?;
        assert_eq!(whole_states.len(), 1);

        // Content addressing turns "the 100-step grid is byte-identical whether or
        // not a segment boundary cut the trajectory" into a hash membership check:
        // the unsegmented state's object must already exist in the segmented run's
        // store, because the second segment committed the same bytes.
        let (whole_object, whole_grid) = &whole_states[0];
        let bytes = Store::open(&segmented.store)?.get(whole_object)?;
        assert_eq!(bytes, whole_grid.to_bytes());
        Ok(())
    }

    /// Requires both a CUDA device and a Vulkan device.
    #[test]
    fn both_programs_evolve_the_same_rule_to_the_same_grid() -> Result<()> {
        // The port's acceptance at program level: one genome, two backends, two
        // full runs through the spine, and grids that agree cell for cell within
        // tolerance. The step count is deliberately short — a rounding difference
        // one backend makes and the other does not compounds with every step, so
        // a long run measures divergence growth rather than transcription
        // fidelity, which is what this test is for.
        let dir = tempfile::tempdir().expect("temp dir");
        let cuda = config(
            dir.path(),
            "cuda.toml",
            CUDA_FORMAT,
            "./store-cuda",
            1,
            20,
            None,
        )?;
        let wgsl = config(
            dir.path(),
            "wgsl.toml",
            WGSL_FORMAT,
            "./store-wgsl",
            1,
            20,
            None,
        )?;
        for config in [&cuda, &wgsl] {
            assert!(matches!(
                orchestrate(
                    config,
                    &RunControl::detached(),
                    Engagement::Orchestrator,
                    BinaryChange::Refuse
                )?,
                RunOutcome::Finalized { .. }
            ));
        }
        let cuda_states = manifest_states(&cuda)?;
        let wgsl_states = manifest_states(&wgsl)?;
        assert_eq!(cuda_states.len(), 1);
        assert_eq!(wgsl_states.len(), 1);
        let cuda_grid = &cuda_states[0].1;
        let wgsl_grid = &wgsl_states[0].1;
        assert_eq!(cuda_grid.data().len(), wgsl_grid.data().len());
        for (index, (&c, &w)) in cuda_grid.data().iter().zip(wgsl_grid.data()).enumerate() {
            let (c, w) = (f64::from(c), f64::from(w));
            let scale = w.abs().max(1.0);
            assert!(
                (c - w).abs() <= TOLERANCE * scale,
                "cell {index}: CUDA {c} against WGSL {w}"
            );
        }
        Ok(())
    }
}
