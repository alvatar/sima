//! Crash-injection harness — acceptance criterion (b): a run SIGKILLed at
//! any crashpoint and resumed yields a manifest identical to a
//! never-interrupted run's.
//!
//! Each case spawns the real binary with `SIMA_CRASHPOINT` arming one
//! point, asserts the child died by SIGKILL — an unmaskable death, no
//! destructors, no unwinding — then re-runs unarmed over the same store
//! and compares the manifest against an uninterrupted reference run's.
//! The kernel-released orchestrator lock is asserted along the way.

mod common;

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use common::{manifest_of, sima_command};
use sima_pipeline::load;
use sima_store::Store;

/// Enough tasks that mid-run hit counts land while work is still ahead:
/// commits and leases number six, object writes a multiple of that.
const BEHAVIORS: &str = r#""succeed", "succeed", "succeed", "sleep:20", "sleep:20", "succeed""#;

/// Writes a `sima.toml` named `name` under `dir`, its store at `store`.
fn write_config(dir: &Path, name: &str, store: &str) -> PathBuf {
    common::write_config(dir, name, BEHAVIORS, store)
}

/// Runs `sima run <config>`, armed with `crashpoint` when given, and
/// returns the exit status. Output is discarded — the store carries the
/// assertions.
fn sima_run(config: &Path, crashpoint: Option<&str>) -> ExitStatus {
    let mut command = sima_command();
    command
        .args(["run", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(arming) = crashpoint {
        command.env("SIMA_CRASHPOINT", arming);
    }
    command.status().expect("spawn sima")
}

/// Harness soundness: the SIGKILL assertion is falsifiable. An unarmed
/// child sails past every planted point and exits 0, so a passing armed
/// case below genuinely proves the injected death.
#[test]
fn an_unarmed_run_is_not_sigkilled() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), "unarmed.toml", "./store");
    let status = sima_run(&config, None);
    assert_ne!(status.signal(), Some(9), "unarmed child died by SIGKILL");
    assert_eq!(status.code(), Some(0), "unarmed child finalizes");
}

/// A segmented, checkpointing config over one `accumulate` chain: `k`
/// steps per segment, `segments` tasks, a zero checkpoint interval so
/// every step boundary saves.
fn write_segmented_config(dir: &Path, name: &str, store: &str) -> PathBuf {
    let text = r#"
        [run]
        root_seed = 11
        format = "stub.v1"
        segments = 2

        [run.generator]
        id = "stub.v1"
        behaviors = ["accumulate:100"]

        [execution]
        store = "STORE"
        workers = 2
        max_attempts = 3
        checkpoint_interval_ms = 0
    "#
    .replace("STORE", store);
    common::write_config_text(dir, name, &text)
}

/// The steps each committed attempt executed after the journal's last
/// `run_started` line — the resume segment — from the stub's stats
/// encoding `(u32 attempt, u64 steps)`.
fn resumed_steps(config_path: &Path) -> Vec<u64> {
    let config = load(config_path).expect("load config");
    let store = Store::open(&config.store).expect("open store");
    let lines = store.journal(&config.run.id()).expect("read journal");
    let events: Vec<sima_pipeline::LifecycleEvent> = lines
        .iter()
        .map(|line| sima_pipeline::LifecycleEvent::from_line(line).expect("parse journal line"))
        .collect();
    let resume_start = events
        .iter()
        .rposition(|e| matches!(e, sima_pipeline::LifecycleEvent::RunStarted { .. }))
        .expect("a run_started line");
    events[resume_start..]
        .iter()
        .filter_map(|e| match e {
            sima_pipeline::LifecycleEvent::Committed { stats_hex, .. } => {
                let bytes: Vec<u8> = (0..stats_hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&stats_hex[i..i + 2], 16).expect("hex"))
                    .collect();
                let mut dec = sima_core::Dec::new(&bytes);
                dec.u32().expect("attempt");
                Some(dec.u64().expect("steps"))
            }
            _ => None,
        })
        .collect()
}

/// A death mid-segment under a segmented, checkpointing run: the resumed
/// run converges on the reference manifest, and its journal proves the
/// checkpoint shortened re-execution — the resumed segment ran fewer
/// steps than a full segment.
#[test]
fn a_death_mid_segment_resumes_from_the_checkpoint() {
    let dir = tempfile::tempdir().expect("temp dir");

    let reference_config =
        write_segmented_config(dir.path(), "reference.toml", "./store-seg-reference");
    assert_eq!(sima_run(&reference_config, None).code(), Some(0));
    let reference = manifest_of(&reference_config).expect("reference manifest");

    // Hit 150 of the per-step point lands 50 steps into the second
    // segment; the zero interval saved a checkpoint at every prior step.
    let config = write_segmented_config(dir.path(), "armed.toml", "./store-seg-armed");
    let status = sima_run(&config, Some("stub.accumulate.step:150"));
    assert_eq!(
        status.signal(),
        Some(9),
        "the armed child dies by SIGKILL, got {status:?}"
    );
    assert!(manifest_of(&config).is_none(), "no manifest survives");

    let resumed = sima_run(&config, None);
    assert_eq!(resumed.code(), Some(0), "the resumed run finalizes");
    assert_eq!(
        manifest_of(&config).as_ref(),
        Some(&reference),
        "the resumed manifest equals the reference"
    );

    // The second segment resumed from its checkpoint: strictly fewer than
    // its full 100 steps ran on the resumed attempt.
    let steps = resumed_steps(&config);
    assert_eq!(steps.len(), 1, "one task ran on resume, got {steps:?}");
    assert!(
        steps[0] < 100,
        "the checkpoint must shorten re-execution, got {} steps",
        steps[0]
    );
}

/// The pre-existing crashpoints re-run under a segmented, checkpointing
/// config: every death resumes to the segmented reference manifest.
#[test]
fn existing_crashpoints_hold_under_a_segmented_config() {
    let dir = tempfile::tempdir().expect("temp dir");

    let reference_config =
        write_segmented_config(dir.path(), "seg-reference.toml", "./store-seg-ref");
    assert_eq!(sima_run(&reference_config, None).code(), Some(0));
    let reference = manifest_of(&reference_config).expect("reference manifest");

    for arming in [
        "object.mid-write:1",
        "commit.after-object:1",
        "lease.held:1",
        "finalize.pre-write:1",
    ] {
        let slug = format!("seg-{}", arming.replace([':', '.'], "-"));
        let config = write_segmented_config(
            dir.path(),
            &format!("{slug}.toml"),
            &format!("./store-{slug}"),
        );

        let status = sima_run(&config, Some(arming));
        assert_eq!(
            status.signal(),
            Some(9),
            "{arming}: the armed child dies by SIGKILL, got {status:?}"
        );
        let resumed = sima_run(&config, None);
        assert_eq!(
            resumed.code(),
            Some(0),
            "{arming}: the resumed run finalizes"
        );
        assert_eq!(
            manifest_of(&config).as_ref(),
            Some(&reference),
            "{arming}: the resumed manifest equals the reference"
        );
    }
}

/// The matrix over the four planted points, each at the first hit and —
/// where a run hits the point more than once — at a mid-run count.
/// `finalize.pre-write` fires once per run, so only `:1` can land.
#[test]
fn every_crashpoint_death_resumes_to_the_reference_manifest() {
    let dir = tempfile::tempdir().expect("temp dir");

    // The reference: the same behaviors run uninterrupted.
    let reference_config = write_config(dir.path(), "reference.toml", "./store-reference");
    assert_eq!(sima_run(&reference_config, None).code(), Some(0));
    let reference = manifest_of(&reference_config).expect("reference manifest");

    for arming in [
        "object.mid-write:1",
        "object.mid-write:5",
        "commit.after-object:1",
        "commit.after-object:3",
        "lease.held:1",
        "lease.held:3",
        "finalize.pre-write:1",
    ] {
        let slug = arming.replace([':', '.'], "-");
        let config = write_config(
            dir.path(),
            &format!("{slug}.toml"),
            &format!("./store-{slug}"),
        );

        let status = sima_run(&config, Some(arming));
        assert_eq!(
            status.signal(),
            Some(9),
            "{arming}: the armed child dies by SIGKILL, got {status:?}"
        );
        assert!(
            manifest_of(&config).is_none(),
            "{arming}: no manifest survives the death"
        );

        // The kernel released the orchestrator lock with the process:
        // it is acquirable immediately, no staleness protocol.
        let loaded = load(&config).expect("load config");
        let store = Store::open(&loaded.store).expect("open store");
        drop(
            store
                .acquire_run_lock(&loaded.run.id())
                .unwrap_or_else(|e| panic!("{arming}: the lock must be free after death: {e}")),
        );

        // Resume unarmed: the frontier re-derives and the run finalizes
        // to the byte-identical manifest.
        let resumed = sima_run(&config, None);
        assert_eq!(
            resumed.code(),
            Some(0),
            "{arming}: the resumed run finalizes"
        );
        assert_eq!(
            manifest_of(&config).as_ref(),
            Some(&reference),
            "{arming}: the resumed manifest equals the reference"
        );
    }
}
