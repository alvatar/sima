//! The far half of a store sync, over the real binary: `sima sync-serve` on one
//! store, a `Store::sync` initiator on another, joined by the child's stdio.
//!
//! This is the boundary a migration's push and pull both cross. The verb
//! addresses a store and a run rather than a config, because loading a config
//! resolves its `[domain.*]` entries — which installs and spawns the program
//! that the session may be there to deliver. The far side therefore derives
//! its key set from the run's journal, while the initiator derives its own
//! from (config, store state): no key list travels and the protocol is
//! unchanged.
//!
//! Every test here runs in the ordinary gate: the far half is a subprocess on
//! this machine, with no ssh hop and no network.

mod common;

use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::process::{Command, Stdio};

use common::{sima_command, write_config_text};
use sima_core::{Error, Result};
use sima_model::TaskKey;
use sima_pipeline::{load, task_keys};
use sima_store::{ObjectScope, Store, SyncReport, SyncRole};

/// A stub config over `store`, dividing each candidate into `segments`
/// accumulating tasks so a partly-run store has a real chain in it.
fn config_text(store: &str, segments: u64) -> String {
    format!(
        r#"
        [run]
        root_seed = 21
        format = "stub.v1"
        segments = {segments}

        [run.generator]
        id = "stub.v1"
        behaviors = ["accumulate:2", "accumulate:2"]

        [config]
        store = "{store}"
        max_attempts = 1

        [orchestrator]
        workers = 2
    "#
    )
}

/// Runs `sima run` over `config` to completion, asserting it finalized.
fn run_to_completion(config: &Path) {
    let output = sima_command()
        .args(["run", config.to_str().expect("utf-8 path")])
        .output()
        .expect("spawn sima run");
    assert_eq!(output.status.code(), Some(0), "{output:?}");
}

/// Runs one sync session as the initiator against `sima sync-serve <far>`,
/// advertising under `scope`, and returns this side's report.
fn sync_against(near: &Path, far: &Path, scope: ObjectScope<'_>) -> Result<SyncReport> {
    let loaded = load(near)?;
    let store = Store::open(&loaded.store)?;
    let keys = task_keys(&loaded, &store)?;
    // The initiator addresses the far side by store and run, both of which it
    // knows: the run id is derived from the config it holds, and the store
    // sits where the far config names it.
    let far_loaded = load(far)?;
    let mut child = sima_command()
        .args([
            "sync-serve",
            far_loaded.store.to_str().expect("utf-8 path"),
            "--run",
            &far_loaded.run.id().to_string(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn sima sync-serve");
    let (stdin, stdout) = (
        child.stdin.take().expect("piped stdin"),
        child.stdout.take().expect("piped stdout"),
    );
    let mut writer = BufWriter::new(stdin);
    let mut reader = BufReader::new(stdout);
    let report = store.sync(&keys, scope, &mut reader, &mut writer, SyncRole::Initiator);
    drop(writer);
    let status = child.wait().expect("reap sima sync-serve");
    match (report, status.success()) {
        (Ok(report), true) => Ok(report),
        (_, false) => Err(Error::Validation(format!(
            "sima sync-serve exited with {status}"
        ))),
        (Err(error), true) => Err(error),
    }
}

/// The task keys `config`'s run comprises over its own store.
fn keys_of(config: &Path) -> Result<Vec<TaskKey>> {
    let loaded = load(config)?;
    let store = Store::open(&loaded.store)?;
    task_keys(&loaded, &store)
}

#[test]
fn a_push_lands_the_run_and_the_far_side_derives_the_same_frontier() -> Result<()> {
    // A finished run pushed into an empty store: every record travels, and the
    // far side ends deriving from its own config exactly the set this side
    // does — which is what makes the transfer complete rather than merely
    // large.
    let dir = tempfile::tempdir().expect("temp dir");
    let near = write_config_text(dir.path(), "near.toml", &config_text("./near-store", 3));
    let far = write_config_text(dir.path(), "far.toml", &config_text("./far-store", 3));
    run_to_completion(&near);

    let report = sync_against(&near, &far, ObjectScope::Referenced)?;
    assert!(report.records_sent > 0, "the run travelled");
    assert_eq!(keys_of(&far)?, keys_of(&near)?);
    Ok(())
}

#[test]
fn a_pull_brings_home_a_run_only_the_far_side_drove() -> Result<()> {
    // The direction the verb exists for, and the one that rests on the far
    // side's own key set: it derives from its journal what it holds, and a
    // near side that holds none of it takes the whole run in one session.
    let dir = tempfile::tempdir().expect("temp dir");
    let near = write_config_text(dir.path(), "near.toml", &config_text("./near-store", 3));
    let far = write_config_text(dir.path(), "far.toml", &config_text("./far-store", 3));
    // Only the far side ran, which is what a migrated run looks like when the
    // orchestrator comes back to collect.
    run_to_completion(&far);

    let report = sync_against(&near, &far, ObjectScope::Referenced)?;
    assert!(report.records_received > 0, "the run came home");
    assert_eq!(report.records_sent, 0, "and nothing went the other way");
    // Every key of the run is now committed here, which is what finalizing
    // locally rests on.
    let loaded = load(&near)?;
    let store = Store::open(&loaded.store)?;
    let keys = task_keys(&loaded, &store)?;
    assert_eq!(keys.len(), 6, "two candidates over three segments");
    for key in &keys {
        assert!(store.has_record(key)?, "{key} came home");
    }
    Ok(())
}

#[test]
fn a_second_session_over_converged_stores_transfers_nothing() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let near = write_config_text(dir.path(), "near.toml", &config_text("./near-store", 2));
    let far = write_config_text(dir.path(), "far.toml", &config_text("./far-store", 2));
    run_to_completion(&near);

    sync_against(&near, &far, ObjectScope::Referenced)?;
    assert_eq!(
        sync_against(&near, &far, ObjectScope::Referenced)?,
        SyncReport::default(),
        "a converged pair has nothing to say to each other"
    );
    Ok(())
}

#[test]
fn a_far_side_whose_run_lock_is_held_fails_cleanly_rather_than_hanging() -> Result<()> {
    // `sync-serve` takes the run lock for the session, so a run driving that
    // store on that machine makes the sync fail on the lock. The failure is the
    // safe one: nothing is written underneath a live orchestrator.
    let dir = tempfile::tempdir().expect("temp dir");
    let near = write_config_text(dir.path(), "near.toml", &config_text("./near-store", 2));
    let far = write_config_text(dir.path(), "far.toml", &config_text("./far-store", 2));
    run_to_completion(&near);

    // Hold the far store's run lock from this process, as a live run would.
    let far_loaded = load(&far)?;
    let far_store = Store::open(&far_loaded.store)?;
    let _held = far_store.acquire_run_lock(&far_loaded.run.id())?;

    assert!(
        sync_against(&near, &far, ObjectScope::Referenced).is_err(),
        "a held lock is a clean failure, not a hang"
    );
    Ok(())
}

#[test]
fn a_run_id_that_is_not_one_surfaces_as_a_failure() {
    // The verb takes a content address, so an argument that is not one fails
    // before a store is touched.
    let dir = tempfile::tempdir().expect("temp dir");
    let output = sima_command()
        .args([
            "sync-serve",
            dir.path().join("store").to_str().expect("utf-8 path"),
            "--run",
            "not-a-run-id",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("spawn sima sync-serve");
    assert_ne!(output.status.code(), Some(0));
    assert!(!dir.path().join("store").exists(), "no store was opened");
}

#[test]
fn a_store_serving_a_run_it_never_held_advertises_nothing() -> Result<()> {
    // What a fresh push finds: no journal on the far side is an empty key set,
    // so the far half advertises nothing and takes everything offered.
    let dir = tempfile::tempdir().expect("temp dir");
    let near = write_config_text(dir.path(), "near.toml", &config_text("./near-store", 2));
    let far = write_config_text(dir.path(), "far.toml", &config_text("./far-store", 2));
    run_to_completion(&near);

    let report = sync_against(&near, &far, ObjectScope::Referenced)?;
    assert_eq!(report.records_received, 0, "the far side offered nothing");
    assert!(report.records_sent > 0, "and took the whole run");
    Ok(())
}

#[test]
fn sync_serve_writes_frames_to_stdout_and_diagnostics_to_stderr() {
    // The stream carries protocol frames and nothing else: a failure must not
    // put a word on stdout, or the frame decoder on the other end would read
    // prose.
    let dir = tempfile::tempdir().expect("temp dir");
    let output = Command::new(env!("CARGO_BIN_EXE_sima"))
        .args([
            "sync-serve",
            dir.path().join("store").to_str().expect("utf-8 path"),
            "--run",
            "not-a-run-id",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("spawn sima sync-serve");
    assert_ne!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty(), "stdout carries frames alone");
    assert!(!output.stderr.is_empty(), "the cause reaches stderr");
}

#[test]
fn sync_serve_stays_out_of_the_usage_text() {
    let output = sima_command().output().expect("spawn sima");
    let usage = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        !usage.contains("sync-serve"),
        "it is a transport half, not a verb: {usage}"
    );
}
