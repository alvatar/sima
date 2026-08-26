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

use std::collections::BTreeSet;
use std::fs;
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

/// Runs `sima run <config>` with `extra` appended, armed with `crashpoint` when
/// given, and returns the exit status. Output is discarded — the store carries
/// the assertions.
fn sima_run_with(config: &Path, crashpoint: Option<&str>, extra: &[&str]) -> ExitStatus {
    let mut command = sima_command();
    command
        .args(["run", config.to_str().expect("utf-8 path")])
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(arming) = crashpoint {
        command.env("SIMA_CRASHPOINT", arming);
    }
    command.status().expect("spawn sima")
}

/// Runs `sima run <config>` on this machine alone.
fn sima_run(config: &Path, crashpoint: Option<&str>) -> ExitStatus {
    sima_run_with(config, crashpoint, &[])
}

/// Runs `sima run <config> --fleet`, so the config's rented machine is engaged.
fn sima_run_fleet(config: &Path, crashpoint: Option<&str>) -> ExitStatus {
    sima_run_with(config, crashpoint, &["--fleet"])
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

/// The crashpoints this suite arms, one entry per plant in the workspace.
///
/// The guard below scans the sources for `crashpoint(` calls and asserts the
/// two sets are equal, so a new plant that nobody armed fails the build rather
/// than sitting untested — and a name removed from the code without being
/// removed here fails too.
const ARMED: [&str; 13] = [
    "commit.after-object",
    "finalize.pre-write",
    "lease.held",
    "object.mid-write",
    "pack.after-pack-write",
    "pack.mid-loose-delete",
    "provider.destroyed",
    "provider.entry-written",
    "provider.intent-written",
    "provider.provisioned",
    "remove.after-intent",
    "remove.mid-objects",
    "stub.accumulate.step",
];

/// Every crashpoint planted in the workspace's sources, by name.
///
/// Read from the text rather than from a registry, because a registry is the
/// thing that would go stale: what this suite must cover is what the code
/// calls, and the call is the only statement of that.
fn planted() -> BTreeSet<String> {
    /// The workspace root: two levels above this crate's manifest.
    fn workspace() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("the workspace root above crates/sima")
            .to_path_buf()
    }

    /// Every `.rs` file under `dir`, skipping build output.
    fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(listing) = fs::read_dir(dir) else {
            return;
        };
        for entry in listing.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "target") {
                    continue;
                }
                sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs")
                // The module that defines the mechanism calls it in its own
                // tests; those are the mechanism under test, not a plant in a
                // code path this suite must cover.
                && !path.ends_with("sima-core/src/crashpoint.rs")
            {
                out.push(path);
            }
        }
    }

    let mut files = Vec::new();
    sources(&workspace().join("crates"), &mut files);
    let mut names = BTreeSet::new();
    for file in files {
        let text = fs::read_to_string(&file).expect("read a source file");
        let mut rest = text.as_str();
        while let Some(at) = rest.find("crashpoint(\"") {
            let after = &rest[at + "crashpoint(\"".len()..];
            let end = after.find('"').expect("a closed crashpoint name");
            names.insert(after[..end].to_string());
            rest = &after[end..];
        }
    }
    names
}

#[test]
fn every_planted_crashpoint_is_armed_by_this_suite() {
    // The matrix is only a crash-safety guarantee over the points it arms. A
    // plant nobody armed is a code path with no test behind it, and nothing
    // else would say so.
    let planted = planted();
    let armed: BTreeSet<String> = ARMED.iter().map(|name| (*name).to_string()).collect();
    assert!(
        !planted.is_empty(),
        "the scan found no crashpoints at all, so it is not reading the sources"
    );
    let unarmed: Vec<&String> = planted.difference(&armed).collect();
    assert!(unarmed.is_empty(), "planted but never armed: {unarmed:?}");
    let stale: Vec<&String> = armed.difference(&planted).collect();
    assert!(stale.is_empty(), "armed but no longer planted: {stale:?}");
}

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

/// Runs `sima pack <store>` over `store`, armed with `crashpoint` when given,
/// and returns the exit status. Output is discarded — the store carries the
/// assertions.
fn sima_pack(store: &Path, crashpoint: Option<&str>) -> ExitStatus {
    let mut command = sima_command();
    command
        .args(["pack", store.to_str().expect("utf-8 path")])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(arming) = crashpoint {
        command.env("SIMA_CRASHPOINT", arming);
    }
    command.status().expect("spawn sima")
}

/// A packing run SIGKILLed at each of its crashpoints converges by re-running:
/// no object loses its last copy at any point, so the re-run finishes what the
/// dead one started and the run the store holds still enumerates whole.
///
/// The run's objects fit one pack, so the shape at every point is exact: the
/// pack is durable before a single loose file goes, the armed point fires at
/// its first hit, and a fixed object set packs to one fixed file name.
#[test]
fn a_pack_death_converges_on_re_run() {
    let mut converged = Vec::new();
    for arming in ["pack.after-pack-write", "pack.mid-loose-delete"] {
        let dir = tempfile::tempdir().expect("temp dir");
        let slug = arming.replace('.', "-");
        let store_rel = format!("./store-{slug}");
        let config = write_config(dir.path(), &format!("pack-{slug}.toml"), &store_rel);
        assert_eq!(sima_run(&config, None).code(), Some(0));
        let reference = manifest_of(&config).expect("the run finalized");
        let store = dir.path().join(format!("store-{slug}"));
        let loose_before = object_file_count(&store);

        let status = sima_pack(&store, Some(arming));
        assert_eq!(
            status.signal(),
            Some(9),
            "{arming}: the armed packing dies by SIGKILL, got {status:?}"
        );
        // The kill lands between the phases it names: the one pack is
        // already durable at either point, deletion has not begun at the
        // first, and exactly one unlink — the first hit — happened at the
        // second.
        let packs_at_death = common::pack_files(&store);
        assert_eq!(
            packs_at_death.len(),
            1,
            "{arming}: the pack is durable at the kill"
        );
        let expected_loose = match arming {
            "pack.after-pack-write" => loose_before,
            _ => loose_before - 1,
        };
        assert_eq!(
            object_file_count(&store),
            expected_loose,
            "{arming}: the deletion phase is exactly where the point sits"
        );
        // Mid-flight the store holds both representations, and every object
        // is readable throughout.
        let opened = Store::open(&store).expect("open store");
        let run = load(&config).expect("load config").run.id();
        opened.run_closure(&run).expect("the closure enumerates");

        assert_eq!(
            sima_pack(&store, None).code(),
            Some(0),
            "{arming}: the resumed packing finalizes"
        );
        assert_eq!(
            object_file_count(&store),
            0,
            "{arming}: loose files survived the packing"
        );
        // The re-run recognizes the completed pack: the same single file,
        // nothing written twice.
        assert_eq!(
            common::pack_files(&store),
            packs_at_death,
            "{arming}: the re-run lands on the pack the dead run wrote"
        );
        assert_eq!(manifest_of(&config), Some(reference), "{arming}");
        Store::open(&store)
            .expect("reopen store")
            .run_closure(&run)
            .expect("the closure still enumerates whole");
        converged.push(common::pack_files(&store));
    }
    // The two configs differ only in store path, which is outside run
    // identity: identical object sets pack to the identically named file.
    assert_eq!(
        converged[0], converged[1],
        "both armings converge to the same pack"
    );
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

        [config]
        store = "STORE"
        max_attempts = 3
        checkpoint_interval_ms = 0

        [orchestrator]
        workers = 1
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

        [config]
        store = "STORE"
        max_attempts = 3
        checkpoint_interval_steps = 10

        [orchestrator]
        workers = 1
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
        text.contains("resuming: 1/2 committed, 1 outstanding"),
        "the start line states the ledger it resumes: {text}"
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

/// A config whose fleet is one rented stub machine beside the orchestrator's
/// own worker, so a run exercises the provider's acquisition and teardown
/// close-out windows.
fn write_fleet_config(dir: &Path, name: &str, store: &str) -> PathBuf {
    let text = format!(
        r#"
        [run]
        root_seed = 11
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{BEHAVIORS}]

        [config]
        store = "{store}"
        max_attempts = 3

        [orchestrator]
        workers = 1

        [host.rented]
        provider = "stub"

        [fleet]
        members = ["rented"]
    "#
    );
    common::write_config_text(dir, name, &text)
}

/// A death at each provider close-out window leaves a store that recovers: the
/// unarmed re-run finalizes over the same store, and no rental is charged
/// twice.
///
/// The stub provider is in-process, so a crash takes its market with it: the
/// crashed attempt's ledger record survives, but reconcile keeps a run's own
/// records while it holds the lock, so the re-run legitimately does not clear
/// it — a separate lock-free reconcile does, which an in-process stub has no
/// cross-process backend for. What this asserts end-to-end is that each window
/// fires, the run recovers, and the ledger never double-charges a rental; the
/// reconcile pass that clears the survivor is covered in the provider crate.
#[test]
fn a_provider_crash_recovers_without_double_charging() {
    for arming in [
        "provider.intent-written",
        "provider.provisioned",
        "provider.destroyed",
        "provider.entry-written",
    ] {
        let dir = tempfile::tempdir().expect("temp dir");
        let slug = arming.replace('.', "-");
        let config = write_fleet_config(
            dir.path(),
            &format!("{slug}.toml"),
            &format!("./store-{slug}"),
        );

        // Armed: the orchestrator dies by SIGKILL at the window.
        let status = sima_run_fleet(&config, Some(arming));
        assert_eq!(
            status.signal(),
            Some(9),
            "{arming}: the armed run dies by SIGKILL, got {status:?}"
        );

        // Unarmed re-run: the run recovers and finalizes despite the crashed
        // attempt's record still standing.
        assert_eq!(
            sima_run_fleet(&config, None).code(),
            Some(0),
            "{arming}: the re-run finalizes"
        );
        assert!(
            manifest_of(&config).is_some(),
            "{arming}: a manifest is written"
        );

        // No rental is charged twice — the spend ledger keys each rental by its
        // tag and stamp, and a re-close overwrites under that key rather than
        // adding a second charge.
        let loaded = load(&config).expect("load config");
        let store = Store::open(&loaded.store).expect("open store");
        let entries = store
            .spend_entries(&loaded.run.id().to_string())
            .expect("spend entries");
        let mut keys: Vec<(String, u64)> = entries
            .iter()
            .map(|entry| (entry.tag.clone(), entry.started_ms))
            .collect();
        let total = keys.len();
        keys.sort();
        keys.dedup();
        assert_eq!(
            keys.len(),
            total,
            "{arming}: a rental appears twice in the ledger"
        );
    }
}
