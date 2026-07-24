//! Crash-injection harness: a death at any planted crashpoint leaves a
//! store that converges to the reference manifest.
//!
//! Parent-side points SIGKILL the orchestrator — each case asserts the
//! unmaskable death, then re-runs unarmed over the same store and compares
//! manifests, with the kernel-released orchestrator lock asserted along
//! the way. The executor-side point fires inside a `sima-worker` child,
//! so the orchestrator survives: those cases assert convergence through
//! retry within the same invocation.

mod common;

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use common::{journal_events, manifest_of, sima_command};
use sima_pipeline::load;
use sima_store::{Manifest, Store};

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

/// Runs `sima run <config>` unarmed and captures its output, for assertions
/// on the progress lines a resumed run prints.
fn sima_run_output(config: &Path) -> std::process::Output {
    let mut command = sima_command();
    command
        .args(["run", config.to_str().expect("utf-8 path")])
        .output()
        .expect("spawn sima")
}

/// Runs `sima rm <config>`, armed with `crashpoint` when given, and returns
/// the exit status. Output is discarded — the store carries the assertions.
fn sima_rm(config: &Path, crashpoint: Option<&str>) -> ExitStatus {
    let mut command = sima_command();
    command
        .args(["rm", config.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(arming) = crashpoint {
        command.env("SIMA_CRASHPOINT", arming);
    }
    command.status().expect("spawn sima")
}

/// The number of object files under a store's `objects/`, recursively.
fn object_file_count(store: &Path) -> usize {
    std::fs::read_dir(store.join("objects"))
        .expect("read objects dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_dir())
        .map(|entry| {
            std::fs::read_dir(entry.path())
                .expect("read fan-out")
                .count()
        })
        .sum()
}

/// A run removal SIGKILLed at each of its crashpoints resumes to the same
/// empty store an uninterrupted removal produces: the intent written before
/// any deletion makes the removal replayable.
#[test]
fn a_removal_death_resumes_to_an_empty_store() {
    for arming in ["remove.after-intent", "remove.mid-objects"] {
        let dir = tempfile::tempdir().expect("temp dir");
        let slug = arming.replace('.', "-");
        let store_rel = format!("./store-{slug}");
        let config = write_config(dir.path(), &format!("rm-{slug}.toml"), &store_rel);

        // A finalized run, then an armed removal that dies mid-way.
        assert_eq!(sima_run(&config, None).code(), Some(0));
        let status = sima_rm(&config, Some(arming));
        assert_eq!(
            status.signal(),
            Some(9),
            "{arming}: the armed removal dies by SIGKILL, got {status:?}"
        );

        // Resume unarmed: the intent replays and the store empties.
        assert_eq!(
            sima_rm(&config, None).code(),
            Some(0),
            "{arming}: the resumed removal finalizes"
        );
        let store = dir.path().join(format!("store-{slug}"));
        assert_eq!(
            object_file_count(&store),
            0,
            "{arming}: object files survived the removal"
        );
        assert_eq!(
            std::fs::read_dir(store.join("runs"))
                .expect("read runs dir")
                .count(),
            0,
            "{arming}: a run directory survived the removal"
        );
    }
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
/// every step boundary saves. One worker, so the chain's two segments run
/// in the same long-lived worker process and a per-step crashpoint's hit
/// count lands at a deterministic point of the second segment.
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
        workers = 1
        max_attempts = 3
        checkpoint_interval_ms = 0
    "#
    .replace("STORE", store);
    common::write_config_text(dir, name, &text)
}

/// A segmented, checkpointing config over one `accumulate` chain driven by the
/// step-count cadence alone: `checkpoint_interval_steps` saves every tenth step
/// with no wall-clock interval set. One worker, for the same deterministic
/// hit-count reason as [`write_segmented_config`].
fn write_step_segmented_config(dir: &Path, name: &str, store: &str) -> PathBuf {
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
        workers = 1
        max_attempts = 3
        checkpoint_interval_steps = 10
    "#
    .replace("STORE", store);
    common::write_config_text(dir, name, &text)
}

/// The steps each committed attempt executed after the journal's last
/// `run_started` line — the resume segment — read from the stub's `steps`
/// scalar in each `Committed` event.
fn resumed_steps(config_path: &Path) -> Vec<u64> {
    let events = journal_events(config_path);
    let resume_start = events
        .iter()
        .rposition(|e| matches!(e, sima_pipeline::Event::RunStarted { .. }))
        .expect("a run_started line");
    events[resume_start..]
        .iter()
        .filter_map(|e| match e {
            sima_pipeline::Event::Committed { stats, .. } => Some(
                stats
                    .iter()
                    .find(|s| s.name == "steps")
                    .map(|s| s.value as u64)
                    .expect("a steps scalar"),
            ),
            _ => None,
        })
        .collect()
}

/// Asserts the worker-crash convergence contract on `config` armed with
/// `arming`: the executor-side crashpoint SIGKILLs the worker process
/// mid-segment, and — in the same invocation — the run journals a transient
/// failure and a retry, the retry resumes from the last checkpoint (its
/// step count is strictly below a full segment), and the run finalizes to
/// `reference`.
fn assert_worker_death_converges(config: &Path, arming: &str, reference: &Manifest) {
    let status = sima_run(config, Some(arming));
    assert_eq!(
        status.code(),
        Some(0),
        "the run survives its worker's death and finalizes, got {status:?}"
    );
    assert_eq!(
        manifest_of(config).as_ref(),
        Some(reference),
        "the converged manifest equals the reference"
    );

    // The worker death is journaled as a transient failure and a retry.
    let events = journal_events(config);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, sima_pipeline::Event::Failed { .. })),
        "the worker death journals a transient failure"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, sima_pipeline::Event::Retried { .. })),
        "the failed attempt is retried"
    );

    // Both segments committed in this one invocation; the first ran fully,
    // the killed second resumed from its checkpoint and ran strictly fewer
    // than its full 100 steps.
    let steps = resumed_steps(config);
    assert_eq!(steps.len(), 2, "both segments committed, got {steps:?}");
    assert_eq!(steps[0], 100, "the first segment runs fully");
    assert!(
        steps[1] < 100,
        "the checkpoint must shorten the retry, got {} steps",
        steps[1]
    );
}

/// A worker SIGKILLed mid-segment under the wall-clock checkpoint cadence:
/// the per-step crashpoint fires inside the worker process — hit 150 lands
/// 50 steps into the second segment of the single worker's hit count — and
/// the run converges through retry in one invocation.
#[test]
fn a_worker_death_mid_segment_converges_through_retry() {
    let dir = tempfile::tempdir().expect("temp dir");

    let reference_config =
        write_segmented_config(dir.path(), "reference.toml", "./store-seg-reference");
    assert_eq!(sima_run(&reference_config, None).code(), Some(0));
    let reference = manifest_of(&reference_config).expect("reference manifest");

    let config = write_segmented_config(dir.path(), "armed.toml", "./store-seg-armed");
    assert_worker_death_converges(&config, "stub.accumulate.step:150", &reference);
}

/// A resumed run's progress accounts for the commits already in the store:
/// the started line names them and the commit counter continues from them
/// instead of restarting at 1.
#[test]
fn a_resumed_run_reports_prior_commits_in_its_progress() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_segmented_config(
        dir.path(),
        "resume-progress.toml",
        "./store-resume-progress",
    );

    // Death at the second lease: the first segment's task is committed and
    // durable, the second is not.
    let status = sima_run(&config, Some("lease.held:2"));
    assert_eq!(
        status.signal(),
        Some(9),
        "the armed child dies by SIGKILL, got {status:?}"
    );

    let output = sima_run_output(&config);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(
        text.contains("started: 2 tasks, 1 already committed"),
        "the started line names the prior commit: {text}"
    );
    assert!(
        text.contains("committed 2/2"),
        "the counter continues from the store state: {text}"
    );
    assert!(
        !text.contains("committed 1/2"),
        "no line recounts the already-committed task: {text}"
    );
}

/// A worker SIGKILLed mid-segment under the step-count checkpoint cadence
/// alone: the cadence saved every tenth step before the death, and the run
/// converges through retry in one invocation.
#[test]
fn a_worker_death_converges_under_the_step_cadence() {
    let dir = tempfile::tempdir().expect("temp dir");

    let reference_config =
        write_step_segmented_config(dir.path(), "step-reference.toml", "./store-step-ref");
    assert_eq!(sima_run(&reference_config, None).code(), Some(0));
    let reference = manifest_of(&reference_config).expect("reference manifest");

    let config = write_step_segmented_config(dir.path(), "step-armed.toml", "./store-step-armed");
    assert_worker_death_converges(&config, "stub.accumulate.step:150", &reference);
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
