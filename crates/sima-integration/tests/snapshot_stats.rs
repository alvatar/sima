//! End-to-end acceptance of the structured stats and the snapshot predicate
//! through the pipeline API: a stub search journals named scalars for completed
//! and failed tasks and `report` renders them, and a Gray-Scott search with a
//! `snapshot_when` predicate drops the snapshots of candidates that fail it,
//! keeps those that pass, journals scalars for every task, and finalizes a
//! deterministic manifest. Each predicate case searches on both Gray-Scott
//! programs, so the verdict is checked on the WGSL and the CUDA backend alike.

mod common;

use std::path::Path;

use common::{journal_events, loaded_text};
use sima_core::Result;
use sima_pipeline::{
    BinaryChange, Engagement, Event, LoadedConfig, SearchControl, SearchOutcome, orchestrate,
    report,
};
use sima_store::Store;

/// A stub `sima.toml` running `behaviors`, on a two-worker pool.
fn stub_config(dir: &Path, name: &str, store: &str, behaviors: &str) -> Result<LoadedConfig> {
    let text = format!(
        r#"
        [search]
        root_seed = 11
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [config]
        store = "{store}"
        max_attempts = 3

        [orchestrator]
        workers = 2
    "#
    );
    loaded_text(dir, name, &text)
}

/// The two Gray-Scott programs the predicate is exercised on, each with the
/// short label its per-case config and store are named after. The rule is the
/// same on both; the backend is not. Deciding a snapshot is backend-independent
/// machinery, but a dropped snapshot means `grid()` is never called, so the
/// CUDA engine releases its resident device buffers without a readback — a path
/// only a program on that backend reaches.
const FORMATS: [(&str, &str); 2] = [
    ("wgsl", "ca_evolution.gray_scott.v1"),
    ("cuda", "ca_evolution.gray_scott_cuda.v1"),
];

/// A Gray-Scott `sima.toml` for one `case` of one program: `count` candidates
/// on a 32x32 grid, `steps` per segment, with an optional `snapshot_when`
/// predicate line. The format id is also the generator id, since both name the
/// program. `case` and the program's `label` name the config file and its
/// store together, so each case of each program searches into a store of its own.
fn gray_scott_config(
    dir: &Path,
    case: &str,
    label: &str,
    format: &str,
    count: u32,
    steps: u32,
    predicate: Option<&str>,
) -> Result<LoadedConfig> {
    let predicate = predicate.map_or(String::new(), |p| format!("snapshot_when = {p}"));
    let store = format!("./store-{case}-{label}");
    let text = format!(
        r#"
        [search]
        root_seed = 42
        format = "{format}"

        [search.generator]
        id = "{format}"
        count = {count}
        feed = [0.054, 0.056]
        kill = [0.062, 0.062]
        diffusion_u = [0.16, 0.16]
        diffusion_v = [0.08, 0.08]

        [search.params]
        width = 32
        height = 32
        steps = {steps}
        dt = 1.0
        base_u = 0.5
        base_v = 0.25
        side_divisor = 8
        noise_width = 0.02
        {predicate}

        [config]
        store = "{store}"
        max_attempts = 3

        [orchestrator]
        workers = 2
    "#
    );
    loaded_text(dir, &format!("{case}-{label}.toml"), &text)
}

/// Whether each committed task's record carries a `state` artifact, one entry
/// per manifest entry.
fn state_artifact_present(config: &LoadedConfig) -> Result<Vec<bool>> {
    let store = Store::open(&config.store)?;
    let manifest = store
        .manifest(&config.search.id())?
        .expect("a finalized manifest");
    manifest
        .entries
        .iter()
        .map(|entry| {
            let record = store
                .record(&entry.task)?
                .expect("a manifest entry's record");
            Ok(record.artifacts().iter().any(|a| a.name() == "state"))
        })
        .collect()
}

/// Whether every committed task journaled a non-empty scalar list.
fn every_committed_task_has_scalars(events: &[Event]) -> bool {
    let committed: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Committed { stats, .. } => Some(stats),
            _ => None,
        })
        .collect();
    !committed.is_empty() && committed.iter().all(|stats| !stats.is_empty())
}

#[test]
fn a_stub_run_journals_scalars_and_report_renders_them() -> Result<()> {
    // A clean success and a candidate that fails once before committing: the
    // journal carries structured scalars for both the Committed and the Failed
    // events, and `report` renders them generically.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = stub_config(
        dir.path(),
        "sima.toml",
        "./store",
        r#""succeed", "flaky:1""#,
    )?;
    assert!(matches!(
        orchestrate(
            &config,
            &SearchControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
    ));

    let events = journal_events(&config);
    // Both tasks commit; each Committed event carries the attempt scalar and the
    // stub's four-byte family blob.
    let committed: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Committed {
                stats,
                stats_blob_hex,
                ..
            } => Some((stats, stats_blob_hex)),
            _ => None,
        })
        .collect();
    assert_eq!(committed.len(), 2, "both tasks commit");
    for (stats, blob_hex) in &committed {
        assert!(stats.iter().any(|s| s.name == "attempt"));
        assert_eq!(
            blob_hex.len(),
            8,
            "the stub's four-byte blob is eight hex chars"
        );
    }

    // The flaky candidate's first attempt failed transiently; that Failed event
    // carries scalars too — stats cover the failed-evaluation case.
    let failed: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Failed { stats, .. } => Some(stats),
            _ => None,
        })
        .collect();
    assert!(
        !failed.is_empty(),
        "the flaky candidate failed at least once"
    );
    for stats in &failed {
        assert!(stats.iter().any(|s| s.name == "attempt"));
    }

    // report renders the scalars: each row names the attempt and the blob size.
    let rows = report(&config)?;
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert!(
            row.stats.contains("attempt=") && row.stats.contains("blob=4B"),
            "the rendered stats line: {}",
            row.stats
        );
    }
    Ok(())
}

/// Each predicate case searches both Gray-Scott programs, so these need a real GPU and a CUDA device.
mod on_device {
    use super::*;

    #[test]
    fn a_predicate_run_finalizes_a_deterministic_manifest() -> Result<()> {
        // The same predicate config search twice into fresh stores finalizes
        // byte-identical manifests: the predicate verdict is a pure function of the
        // deterministic final grid, so it decides identically both times.
        let dir = tempfile::tempdir().expect("temp dir");
        let predicate = Some(r#"{ scalar = "activity", min = 1e-4 }"#);
        let manifest = |config: &LoadedConfig| -> Result<_> {
            Ok(Store::open(&config.store)?
                .manifest(&config.search.id())?
                .expect("a finalized manifest"))
        };
        for (label, format) in FORMATS {
            let first = gray_scott_config(dir.path(), "first", label, format, 2, 60, predicate)?;
            let second = gray_scott_config(dir.path(), "second", label, format, 2, 60, predicate)?;
            for config in [&first, &second] {
                assert!(matches!(
                    orchestrate(
                        config,
                        &SearchControl::detached(),
                        Engagement::Orchestrator,
                        BinaryChange::Refuse
                    )?,
                    SearchOutcome::Finalized { .. }
                ));
            }
            assert_eq!(manifest(&first)?, manifest(&second)?, "{format}");
        }
        Ok(())
    }

    #[test]
    fn a_no_predicate_run_keeps_every_snapshot() -> Result<()> {
        // The pre-milestone behavior: without a predicate every candidate commits
        // its state artifact, unchanged.
        let dir = tempfile::tempdir().expect("temp dir");
        for (label, format) in FORMATS {
            let config = gray_scott_config(dir.path(), "plain", label, format, 3, 60, None)?;
            assert!(matches!(
                orchestrate(
                    &config,
                    &SearchControl::detached(),
                    Engagement::Orchestrator,
                    BinaryChange::Refuse
                )?,
                SearchOutcome::Finalized { .. }
            ));
            let present = state_artifact_present(&config)?;
            assert_eq!(present.len(), 3, "{format}");
            assert!(present.iter().all(|&p| p), "{format}");
        }
        Ok(())
    }

    #[test]
    fn a_passing_predicate_keeps_every_snapshot() -> Result<()> {
        // A minimum of 0.0 is met by every candidate, so each commits its snapshot,
        // and every task journals scalars.
        let dir = tempfile::tempdir().expect("temp dir");
        for (label, format) in FORMATS {
            let config = gray_scott_config(
                dir.path(),
                "keep",
                label,
                format,
                3,
                60,
                Some(r#"{ scalar = "population", min = 0.0 }"#),
            )?;
            assert!(matches!(
                orchestrate(
                    &config,
                    &SearchControl::detached(),
                    Engagement::Orchestrator,
                    BinaryChange::Refuse
                )?,
                SearchOutcome::Finalized { .. }
            ));
            let present = state_artifact_present(&config)?;
            assert_eq!(present.len(), 3, "{format}");
            assert!(
                present.iter().all(|&p| p),
                "{format}: every passing candidate keeps its snapshot"
            );
            assert!(
                every_committed_task_has_scalars(&journal_events(&config)),
                "{format}: every task journals its scalars"
            );
        }
        Ok(())
    }

    #[test]
    fn a_failing_predicate_drops_every_snapshot_but_journals_scalars() -> Result<()> {
        // population is a fraction, so a minimum of 2.0 can never be met: every
        // candidate commits a record with no state artifact, yet every task still
        // journals its scalars.
        let dir = tempfile::tempdir().expect("temp dir");
        for (label, format) in FORMATS {
            let config = gray_scott_config(
                dir.path(),
                "drop",
                label,
                format,
                3,
                60,
                Some(r#"{ scalar = "population", min = 2.0 }"#),
            )?;
            assert!(matches!(
                orchestrate(
                    &config,
                    &SearchControl::detached(),
                    Engagement::Orchestrator,
                    BinaryChange::Refuse
                )?,
                SearchOutcome::Finalized { .. }
            ));
            let present = state_artifact_present(&config)?;
            assert_eq!(present.len(), 3, "{format}");
            assert!(
                present.iter().all(|&p| !p),
                "{format}: every failing candidate drops its snapshot"
            );
            assert!(
                every_committed_task_has_scalars(&journal_events(&config)),
                "{format}: a dropped snapshot still journals its scalars"
            );
        }
        Ok(())
    }
}
