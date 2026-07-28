//! End-to-end acceptance of a migrated run: a run interrupted partway here,
//! moved onto another machine, finished there, and brought home — with the
//! manifest byte-identical to a run that was never interrupted.
//!
//! The far side is the real `sima` binary, reached through the stub provider,
//! whose machines are local subprocesses. Nothing here needs a network, a GPU,
//! an ssh hop, or a container, so it runs in the ordinary gate.
//!
//! The local halves are driven in-process, so the interrupt is raised from the
//! run observer rather than by signalling a subprocess: a fixed number of
//! commits, not a wall-clock guess.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::{manifest_bytes, worker_binary};
use sima_core::Result;
use sima_model::{TaskKey, TaskRecord};
use sima_pipeline::{
    Engagement, Event, MigrateOutcome, Record, RunControl, RunOutcome, load, migrate, orchestrate,
    task_keys,
};
use sima_store::Store;

/// Two candidates over `segments` accumulating segments, so a chain is left
/// partway by an early interrupt and has a frontier to hand over.
///
/// `[run]` is the only hashed section, so two configs written from the same
/// `segments` describe the same run whatever machine drives them.
fn run_section(segments: u64) -> String {
    format!(
        r#"
        [run]
        root_seed = 21
        segments = {segments}
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["accumulate:2", "accumulate:2"]

        [config]
        store = "./store"
        max_attempts = 3
    "#
    )
}

/// Writes a config under `dir` naming a store beside it, plus `machines`.
fn config(dir: &Path, name: &str, segments: u64, machines: &str) -> PathBuf {
    let text = format!(
        "{}\n[orchestrator]\nworkers = 2\n{machines}\n",
        run_section(segments)
    );
    common::write_config_text(dir, name, &text)
}

/// A config whose orchestrator migrates onto a rented stub machine, rooted at
/// `root` and driving the `sima` binary this build produced.
fn migrating(dir: &Path, root: &Path, segments: u64) -> PathBuf {
    // The stub provider's machines are reached on this machine, so the far side
    // is a local subprocess with its own store; the bounds are the readiness
    // bounds a wind-down also waits on, kept short so no test sleeps.
    config(
        dir,
        "migrating.toml",
        segments,
        &format!(
            r#"
            migrate = "far"

            [host.far]
            provider = "stub"
            root = {root:?}
            binary = {binary:?}
            ready_timeout_ms = 30000
            ready_poll_ms = 20
            "#,
            root = root.to_string_lossy(),
            binary = env!("CARGO_BIN_EXE_sima"),
        ),
    )
}

/// Drives the run `config` describes, interrupting once `stop_after` tasks have
/// committed; `None` runs it to its end.
fn drive(config: &Path, stop_after: Option<usize>) -> Result<RunOutcome> {
    let loaded = load(config)?;
    let interrupt = AtomicBool::new(false);
    let committed = AtomicUsize::new(0);
    let control = RunControl {
        observer: &|record: &Record| {
            if let Some(stop_after) = stop_after
                && matches!(record.event, Event::Committed { .. })
                && committed.fetch_add(1, Ordering::Relaxed) + 1 >= stop_after
            {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
        on_start: None,
    };
    orchestrate(&loaded, &control, Engagement::Orchestrator)
}

/// Moves the run `config` describes onto its destination, discarding the
/// records it forwards.
fn move_run(config: &Path) -> Result<MigrateOutcome> {
    migrate(config, &|_: &Record| {}, &AtomicBool::new(false))
}

/// Every record the store of the run `config` describes currently holds, keyed
/// by task. The frontier key of an unfinished chain has no record and is
/// absent.
fn committed_records(config: &Path) -> Result<BTreeMap<TaskKey, TaskRecord>> {
    let loaded = load(config)?;
    let store = Store::open(&loaded.store)?;
    let mut records = BTreeMap::new();
    for key in task_keys(&loaded, &store)? {
        if let Some(record) = store.record(&key)? {
            records.insert(key, record);
        }
    }
    Ok(records)
}

/// The far side's own store, under the run's directory beneath `root`.
fn far_store(config: &Path, root: &Path) -> Result<Store> {
    let run = load(config)?.run.id();
    Store::open(root.join(run.to_string()).join("store"))
}

/// Every record the far side's store holds for the run `config` describes,
/// keyed by task.
fn far_committed(config: &Path, far: &Store) -> Result<BTreeMap<TaskKey, TaskRecord>> {
    let loaded = load(config)?;
    let mut records = BTreeMap::new();
    for key in task_keys(&loaded, far)? {
        if let Some(record) = far.record(&key)? {
            records.insert(key, record);
        }
    }
    Ok(records)
}

/// The tasks the run `config` describes has journaled as committed.
fn journaled_commits(config: &Path) -> Vec<String> {
    common::journal_events(config)
        .into_iter()
        .filter_map(|event| match event {
            Event::Committed { task, .. } => Some(task),
            _ => None,
        })
        .collect()
}

/// The segment count a run finishes in: short enough that a whole run is a
/// fraction of a second.
const SEGMENTS: u64 = 6;

/// The segment count a run cannot finish while a migration is watching its
/// first record arrive. It makes the interrupt test decide on the ordering of
/// events rather than on how fast this machine is.
const UNFINISHABLE: u64 = 400;

/// Builds the worker binary once, so both the enumeration probe here and the
/// far side's own `sima run` find it beside the test's executable.
fn workers_built() {
    let _ = worker_binary();
}

#[test]
fn a_migrated_run_finalizes_to_the_manifest_an_uninterrupted_run_writes() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");

    // The reference: the same run, never interrupted, driven here throughout.
    let reference_dir = dir.path().join("reference");
    std::fs::create_dir_all(&reference_dir).expect("reference dir");
    let reference = config(&reference_dir, "reference.toml", SEGMENTS, "");
    assert!(matches!(
        drive(&reference, None)?,
        RunOutcome::Finalized { .. }
    ));

    // The migrated run: interrupted here after two commits, so its chains are
    // partway and the rest is the far side's to finish.
    let migrated_dir = dir.path().join("migrated");
    std::fs::create_dir_all(&migrated_dir).expect("migrated dir");
    let migrated = migrating(&migrated_dir, &far_root, SEGMENTS);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    let before = committed_records(&migrated)?;
    assert!(!before.is_empty(), "the local run committed something");
    assert!(
        manifest_bytes(&migrated).is_none(),
        "an interrupted run writes no manifest"
    );
    let total = load(&reference)?
        .run
        .segments
        .expect("a segmented run")
        .get() as usize
        * 2;
    assert!(
        before.len() < total,
        "the local run stopped short of the {total} tasks: {} committed",
        before.len()
    );

    let outcome = move_run(&migrated)?;
    assert!(
        matches!(outcome, MigrateOutcome::Finalized { .. }),
        "the migration came home complete: {outcome:?}"
    );

    // The criterion the milestone carries: byte equality with a run that was
    // never interrupted and never moved.
    assert_eq!(
        manifest_bytes(&migrated),
        manifest_bytes(&reference),
        "the migrated run's manifest is the uninterrupted run's manifest"
    );

    // Nothing committed here was recomputed there: every record that existed
    // before the move is the record that is there after it.
    let after = committed_records(&migrated)?;
    for (key, record) in &before {
        assert_eq!(
            after.get(key),
            Some(record),
            "task {key} was recomputed rather than carried"
        );
    }
    assert!(
        after.len() > before.len(),
        "the migration brought new records home"
    );

    // The rest ran on the far side: its own store holds them, and the local
    // journal gained them only because the follow forwarded them.
    let far = far_store(&migrated, &far_root)?;
    let commits = journaled_commits(&migrated);
    for key in after.keys().filter(|key| !before.contains_key(key)) {
        assert!(
            far.record(key)?.is_some(),
            "task {key} was committed on the far side"
        );
        assert!(
            commits.contains(&key.to_string()),
            "task {key}'s commit reached the local journal"
        );
    }

    // The rental is gone: the ledger holds nothing to reconcile.
    let store = Store::open(&load(&migrated)?.store)?;
    assert!(
        store.instances()?.is_empty(),
        "the machine that hosted the run was torn down"
    );
    Ok(())
}

#[test]
fn a_second_migration_over_a_finished_run_finalizes_to_the_same_manifest() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, SEGMENTS);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    assert!(matches!(
        move_run(&migrated)?,
        MigrateOutcome::Finalized { .. }
    ));
    let manifest = manifest_bytes(&migrated).expect("a finalized manifest");

    // Re-running is the resume path: the frontier re-derives empty, the far
    // side has nothing to do, and the run re-finalizes to the same bytes.
    assert!(matches!(
        move_run(&migrated)?,
        MigrateOutcome::Finalized { .. }
    ));
    assert_eq!(manifest_bytes(&migrated), Some(manifest));
    let store = Store::open(&load(&migrated)?.store)?;
    assert!(store.instances()?.is_empty(), "nothing was left rented");
    Ok(())
}

#[test]
fn a_migration_interrupted_during_the_follow_still_pulls_and_tears_down() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    // A chain the far side cannot reach the end of while this migration is
    // still reading its first record, so the wind-down decides the outcome
    // rather than a race with how fast this machine runs.
    let migrated = migrating(dir.path(), &far_root, UNFINISHABLE);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    let before = committed_records(&migrated)?;

    // Wound down as soon as the far run's first record arrives: the far side is
    // signalled, whatever it committed is pulled, and the rental is destroyed.
    let interrupt = AtomicBool::new(false);
    let outcome = migrate(
        &migrated,
        &|_: &Record| interrupt.store(true, Ordering::Relaxed),
        &interrupt,
    )?;
    assert!(
        matches!(outcome, MigrateOutcome::Interrupted { .. }),
        "a wound-down migration is resumable, not finalized: {outcome:?}"
    );
    assert!(
        manifest_bytes(&migrated).is_none(),
        "an interrupted migration seals nothing"
    );

    // The results that existed still do.
    let after = committed_records(&migrated)?;
    for (key, record) in &before {
        assert_eq!(after.get(key), Some(record), "task {key} came home intact");
    }
    // And the pull ran to completion: nothing the far side committed was left
    // behind, however far it got before the signal.
    let far = far_store(&migrated, &far_root)?;
    let far_keys = far_committed(&migrated, &far)?;
    assert!(
        !far_keys.is_empty(),
        "the far side held the chain it was sent"
    );
    for (key, record) in &far_keys {
        assert_eq!(
            Store::open(&load(&migrated)?.store)?.record(key)?.as_ref(),
            Some(record),
            "task {key} was left on the far side"
        );
    }

    let store = Store::open(&load(&migrated)?.store)?;
    assert!(
        store.instances()?.is_empty(),
        "the machine was torn down on the interrupt path"
    );
    Ok(())
}
