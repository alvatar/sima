//! End-to-end acceptance of a migrated run: a run interrupted partway here,
//! moved onto another machine, finished there, and brought home — with the
//! manifest byte-identical to a run that was never interrupted.
//!
//! The far side is the real `sima` binary. A rented destination reaches it
//! through the stub provider, whose machines are local subprocesses; a machine
//! of yours reaches it through the `ssh` and container-runtime stand-ins
//! `common::machine_stubs` writes, which strip their own wrapping and run the
//! command here. Every argv the pipeline builds is therefore the real one, and
//! nothing needs a network, a GPU, or a namespace, so it runs in the ordinary
//! gate.
//!
//! The local halves are driven in-process, so the interrupt is raised from the
//! run observer rather than by signalling a subprocess: a fixed number of
//! commits, not a wall-clock guess.
//!
//! The last test moves a run over a real ssh hop, against a throwaway server
//! the test stands up and tears down. It needs no root, changes nothing outside
//! its temporary directory, and runs in the ordinary gate, because an ssh path
//! nobody exercises is an ssh path nobody knows works.

mod common;

use std::collections::BTreeMap;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::{manifest_bytes, sima_command, worker_binary};
use sima_core::Result;
use sima_model::{TaskKey, TaskRecord};
use sima_pipeline::{
    BinaryChange, Engagement, Event, MigrateOutcome, Record, RunControl, RunOutcome, load, migrate,
    orchestrate, task_keys,
};
use sima_store::Store;

/// Two candidates over `segments` accumulating segments, so a chain is left
/// partway by an early interrupt and has a frontier to hand over.
///
/// `[run]` is the only hashed section, so two configs written from the same
/// `segments` describe the same run whatever machine drives them.
fn run_section(segments: u64, behaviors: &str) -> String {
    format!(
        r#"
        [run]
        root_seed = 21
        segments = {segments}
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [config]
        store = "./store"
        max_attempts = 3
    "#
    )
}

/// Writes a config under `dir` naming a store beside it, plus `machines`.
fn config(dir: &Path, name: &str, segments: u64, behaviors: &str, machines: &str) -> PathBuf {
    let text = format!(
        "{}\n[orchestrator]\nworkers = 2\n{machines}\n",
        run_section(segments, behaviors)
    );
    common::write_config_text(dir, name, &text)
}

/// A config whose orchestrator migrates onto a rented stub machine, rooted at
/// `root` and driving the `sima` binary this build produced.
fn migrating(dir: &Path, root: &Path, segments: u64, behaviors: &str) -> PathBuf {
    // The stub provider's machines are reached on this machine, so the far side
    // is a local subprocess with its own store; the bounds are the readiness
    // bounds a wind-down also waits on, kept short so no test sleeps.
    config(
        dir,
        "migrating.toml",
        segments,
        behaviors,
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
            binary = far_binary(),
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
    orchestrate(
        &loaded,
        &control,
        Engagement::Orchestrator,
        BinaryChange::Refuse,
    )
}

/// Moves the run `config` describes onto its destination, discarding the
/// records it forwards.
fn move_run(config: &Path) -> Result<MigrateOutcome> {
    let loaded = sima_pipeline::load(config)?;
    migrate(
        config,
        &loaded,
        &|_: &Record| {},
        &AtomicBool::new(false),
        BinaryChange::Refuse,
    )
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

/// The candidate behaviors of a run meant to complete: fast accumulating
/// chains.
const CHAINS: &str = r#""accumulate:2", "accumulate:2""#;

/// The segment count of a run that cannot finish while a migration is
/// watching its first record arrive. It pairs with [`PACED`]: the count alone
/// is a bet on how fast a loaded machine runs, the sleep per segment is what
/// bounds the chain in time.
const UNFINISHABLE: u64 = 400;

/// The candidate behaviors of a run that cannot finish: paced accumulation,
/// each step sleeping, so the [`UNFINISHABLE`] chain has a hundred seconds
/// of work left whenever the wind-down lands — the interrupt test decides on
/// the ordering of events, never on how fast this machine is.
const PACED: &str = r#""accumulate:2:250", "accumulate:2:250""#;

/// Builds the worker binary once, so both the enumeration probe here and the
/// far side's own `sima run` find it beside the test's executable.
fn workers_built() {
    let _ = worker_binary();
}

/// Where the far side's `sima` is, for a config that names it.
fn far_binary() -> &'static str {
    env!("CARGO_BIN_EXE_sima")
}

#[test]
fn a_migrated_run_finalizes_to_the_manifest_an_uninterrupted_run_writes() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");

    // The reference: the same run, never interrupted, driven here throughout.
    let reference_dir = dir.path().join("reference");
    std::fs::create_dir_all(&reference_dir).expect("reference dir");
    let reference = config(&reference_dir, "reference.toml", SEGMENTS, CHAINS, "");
    assert!(matches!(
        drive(&reference, None)?,
        RunOutcome::Finalized { .. }
    ));

    // The migrated run: interrupted here after two commits, so its chains are
    // partway and the rest is the far side's to finish.
    let migrated_dir = dir.path().join("migrated");
    std::fs::create_dir_all(&migrated_dir).expect("migrated dir");
    let migrated = migrating(&migrated_dir, &far_root, SEGMENTS, CHAINS);
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
        store.instance_records()?.is_empty(),
        "the machine that hosted the run was torn down"
    );
    Ok(())
}

/// Asserts `lines` appear in `text` in the order given, naming the first one
/// that does not.
fn in_order(text: &str, lines: &[&str]) {
    let mut rest = text;
    for line in lines {
        let at = rest
            .find(line)
            .unwrap_or_else(|| panic!("{line:?} follows what precedes it, in:\n{text}"));
        rest = &rest[at + line.len()..];
    }
}

#[test]
fn a_migration_narrates_the_phases_of_placing_the_run() -> Result<()> {
    // Between the run id and the far run's first record a migration rents a
    // machine, waits for it to come up, and puts the run on it — minutes on a
    // real destination. Each phase says so as it begins, so an operator can
    // tell a placement in progress from a hang.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, SEGMENTS, CHAINS);

    let output = sima_command()
        .args(["migrate", migrated.to_str().expect("utf-8 path")])
        .output()
        .expect("spawn sima migrate");
    assert_eq!(
        output.status.code(),
        Some(0),
        "the migration finalized: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    in_order(
        &stdout,
        &[
            "run ",
            "renting",
            "waiting for the machine to come up",
            "sending the run",
            "starting the run",
            "started:",
            "committed",
            "migrated:",
        ],
    );
    // The wait is one phase however many times it polls, so a machine slow to
    // answer says so once rather than once a second.
    assert_eq!(
        stdout.matches("waiting for the machine to come up").count(),
        1,
        "{stdout}"
    );
    Ok(())
}

#[test]
fn a_migration_onto_a_machine_of_yours_narrates_the_phases_it_has() -> Result<()> {
    // A machine of yours is standing and is not paid for, so the phases that
    // acquire one do not exist; the ones that place the run on it do.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let bin = common::machine_stubs(dir.path(), false);
    let migrated = owned(dir.path(), &far_root, SEGMENTS, CHAINS);

    let output = sima_with(&bin, &["migrate", migrated.to_str().expect("utf-8 path")]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "the migration finalized: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    in_order(
        &stdout,
        &["run ", "sending the run", "starting the run", "migrated:"],
    );
    assert!(!stdout.contains("renting"), "nothing was rented: {stdout}");
    assert!(
        !stdout.contains("waiting for the machine"),
        "nothing was waited for: {stdout}"
    );
    Ok(())
}

#[test]
fn a_second_migration_over_a_finished_run_finalizes_to_the_same_manifest() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, SEGMENTS, CHAINS);
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
    assert!(
        store.instance_records()?.is_empty(),
        "nothing was left rented"
    );
    Ok(())
}

/// An observer that lets go the moment the far run produces a record of its
/// own, which is what an operator does once they can see the run is going.
///
/// The phases of placing the run are records too, and reaching for the
/// interrupt during those means something else entirely: an offer walk is
/// abandoned rather than a far run left computing.
fn letting_go(interrupt: &AtomicBool) -> impl Fn(&Record) + '_ {
    move |record: &Record| {
        if matches!(
            record.event,
            Event::RunStarted { .. } | Event::Committed { .. }
        ) {
            interrupt.store(true, Ordering::Relaxed);
        }
    }
}

/// The far-side `sima run` process id, read from the run directory the
/// migration placed, and `None` once nothing answers to it.
fn far_pid(config: &Path, root: &Path) -> Option<u32> {
    let run = load(config).expect("the config loads").run.id();
    let pid: u32 = std::fs::read_to_string(root.join(run.to_string()).join("run.pid"))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    // The signal's own complaint over a pid nothing answers to is the answer,
    // not something to print.
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .expect("run kill -0")
        .status
        .success()
        .then_some(pid)
}

/// Ends a far run a test detached from, so no paced chain outlives the suite.
/// A run that has already gone is the outcome, not a fault, so the signal's
/// own complaint is captured rather than printed.
fn end_far_run(pid: u32) {
    let _ = Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .output();
}

#[test]
fn a_migration_interrupted_during_the_follow_detaches_and_a_second_one_reattaches() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    // A chain the far side cannot reach the end of while this migration is
    // still reading its first record: every segment sleeps, so a hundred
    // seconds of far-side work remain when the interrupt lands, and the
    // outcome is decided by the interrupt rather than by a race with how
    // fast this machine runs.
    let migrated = migrating(dir.path(), &far_root, UNFINISHABLE, PACED);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));

    // Interrupted as soon as the far run's first record arrives: the operator
    // let go, and everything on the far side stays as it was.
    let interrupt = AtomicBool::new(false);
    let loaded = sima_pipeline::load(&migrated)?;
    let outcome = migrate(
        &migrated,
        &loaded,
        &letting_go(&interrupt),
        &interrupt,
        BinaryChange::Refuse,
    )?;
    assert_eq!(
        outcome,
        MigrateOutcome::Detached {
            run: loaded.run.id(),
            machine: "far".to_string(),
        },
        "an interrupted migration detaches"
    );
    assert!(
        manifest_bytes(&migrated).is_none(),
        "a detached migration seals nothing"
    );

    let pid = far_pid(&migrated, &far_root).expect("the far run keeps computing");
    let store = Store::open(&load(&migrated)?.store)?;
    assert!(
        !store.instance_records()?.is_empty(),
        "the machine it computes on was not torn down"
    );

    // The way back: a second migration finds the same far run and attaches to
    // it rather than starting another.
    let second = AtomicBool::new(false);
    let outcome = migrate(
        &migrated,
        &loaded,
        &letting_go(&second),
        &second,
        BinaryChange::Refuse,
    )?;
    assert!(
        matches!(outcome, MigrateOutcome::Detached { .. }),
        "the second migration detached too: {outcome:?}"
    );
    assert_eq!(
        far_pid(&migrated, &far_root),
        Some(pid),
        "it attached to the run already there rather than starting another"
    );

    end_far_run(pid);
    Ok(())
}

/// A config in `dir` whose orchestrator migrates onto a machine of yours,
/// rooted at `root` and reached at the ssh destination `host`.
///
/// It names the same run as [`migrating`], because a run's directory on a
/// machine derives from the run id under the host's root: the same far run is
/// therefore reachable through either form of entry, which is what lets a
/// migration onto a rented machine be recalled from a machine of yours whose
/// hop the test stands in for.
fn recalling(dir: &Path, root: &Path, host: &str, segments: u64, behaviors: &str) -> PathBuf {
    config(
        dir,
        "recalling.toml",
        segments,
        behaviors,
        &format!(
            r#"
            migrate = "far"

            [host.far]
            ssh = {host:?}
            workers = 1
            root = {root:?}
            binary = {binary:?}
            "#,
            root = root.to_string_lossy(),
            binary = far_binary(),
        ),
    )
}

/// Runs `sima <args…>` with `bin` ahead of the PATH, so the stand-in `ssh` is
/// the one it finds.
fn sima_with(bin: &Path, args: &[&str]) -> Output {
    let path = std::env::var("PATH").expect("a PATH");
    sima_command()
        .args(args)
        .env("PATH", format!("{}:{path}", bin.display()))
        .output()
        .expect("spawn sima")
}

/// A migrating config whose run may spend `cap` dollars in total.
fn migrating_under_budget(dir: &Path, root: &Path, cap: f64) -> PathBuf {
    let text = format!(
        "{}\n[budget]\nmax_spend_usd = {cap}\n",
        std::fs::read_to_string(migrating(dir, root, UNFINISHABLE, PACED))
            .expect("read the migrating config")
    );
    common::write_config_text(dir, "migrating.toml", &text)
}

#[test]
fn an_exhausted_budget_winds_the_far_run_down_pulls_and_takes_the_machine_away() -> Result<()> {
    // The one thing that still ends a far run from this side while a migration
    // watches it: money cannot wait for an operator to come back.
    //
    // The ceiling is one micro-dollar, which the stub's machine accrues in
    // some tens of milliseconds: nothing is owed when the rental is asked for,
    // so it is granted, and the ceiling is past by the time the follow first
    // assesses it — which is after the far run has started and journaled.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating_under_budget(dir.path(), &far_root, 0.000_001);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));

    let outcome = move_run(&migrated)?;
    assert!(
        matches!(outcome, MigrateOutcome::Interrupted { .. }),
        "the ceiling wound the run down: {outcome:?}"
    );
    assert_eq!(
        far_pid(&migrated, &far_root),
        None,
        "the far run was ended rather than left computing"
    );
    assert!(
        manifest_bytes(&migrated).is_none(),
        "a wound-down migration seals nothing"
    );

    // The pull ran, and the machine it ran against is gone.
    let far = far_store(&migrated, &far_root)?;
    let store = Store::open(&load(&migrated)?.store)?;
    for (key, record) in &far_committed(&migrated, &far)? {
        assert_eq!(
            store.record(key)?.as_ref(),
            Some(record),
            "task {key} was left on the far side"
        );
    }
    assert!(
        store.instance_records()?.is_empty(),
        "the machine was torn down on the wind-down path"
    );
    Ok(())
}

/// A migrating config whose run may compute for `ms` milliseconds per launch,
/// on the rented stub machine [`migrating`] names.
fn migrating_under_ceiling(dir: &Path, root: &Path, ms: u64) -> PathBuf {
    let text = format!(
        "{}\n[budget]\nmax_wall_clock_ms = {ms}\n",
        std::fs::read_to_string(migrating(dir, root, UNFINISHABLE, PACED))
            .expect("read the migrating config")
    );
    common::write_config_text(dir, "migrating.toml", &text)
}

/// A config in `dir` whose orchestrator migrates onto a machine of yours,
/// rooted at `root`.
///
/// The machine is reached through the stand-ins [`common::machine_stubs`]
/// writes, so its workers run in a container the same way a real one's do and
/// the far side is still this machine.
fn owned(dir: &Path, root: &Path, segments: u64, behaviors: &str) -> PathBuf {
    config(
        dir,
        "owned.toml",
        segments,
        behaviors,
        &format!(
            r#"
            migrate = "far"

            [host.far]
            ssh = "farbox"
            image = "{image}"
            runtime = "docker"
            workers = 1
            root = {root:?}
            binary = {binary:?}
            "#,
            image = common::IMAGE,
            root = root.to_string_lossy(),
            binary = far_binary(),
        ),
    )
}

/// The same machine of yours, whose run may compute for `ms` milliseconds per
/// launch.
fn owned_under_ceiling(dir: &Path, root: &Path, ms: u64) -> PathBuf {
    let text = format!(
        "{}\n[budget]\nmax_wall_clock_ms = {ms}\n",
        std::fs::read_to_string(owned(dir, root, UNFINISHABLE, PACED))
            .expect("read the owned config")
    );
    common::write_config_text(dir, "owned.toml", &text)
}

#[test]
fn a_migrated_run_under_a_wall_clock_ceiling_winds_itself_down_on_the_far_side() -> Result<()> {
    // The ceiling travels to a machine of yours, so the far run keeps it: the
    // chain has a hundred seconds of work left and the run ends anyway, on its
    // own.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let bin = common::machine_stubs(dir.path(), false);
    let migrated = owned_under_ceiling(dir.path(), &far_root, 1_500);

    let output = sima_with(&bin, &["migrate", migrated.to_str().expect("utf-8 path")]);
    assert_eq!(
        output.status.code(),
        Some(130),
        "the far run interrupted itself: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        far_pid(&migrated, &far_root),
        None,
        "and nothing is left computing there"
    );
    assert!(
        manifest_bytes(&migrated).is_none(),
        "an interrupted run seals nothing"
    );
    Ok(())
}

#[test]
fn a_detached_run_ends_on_its_own_ceiling_and_the_next_attach_brings_it_home() -> Result<()> {
    // What bounds a run nobody is watching on a machine of yours: this side
    // lets go, and the far run's own ceiling is what ends it.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let bin = common::machine_stubs(dir.path(), false);
    // Long enough that the interrupt below lands while the far run is still
    // computing, so what ends it is the ceiling rather than a race with the
    // detach.
    let migrated = owned_under_ceiling(dir.path(), &far_root, 8_000);

    let output = detached_from(&bin, &migrated, &far_root, Signalled::Migration);
    assert_eq!(
        output.status.code(),
        Some(0),
        "letting go is its own outcome: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.contains("detached"), "{stdout}");

    // Nothing is attached to it now, and it ends all the same.
    assert!(
        poll_for(Duration::from_secs(60), || far_pid(&migrated, &far_root)
            .is_none()
            .then_some(()))
        .is_some(),
        "the far run wound itself down unattended"
    );

    // What it committed before the ceiling comes home on the next attach.
    let far = far_store(&migrated, &far_root)?;
    let far_keys = far_committed(&migrated, &far)?;
    assert!(!far_keys.is_empty(), "the far run committed something");
    sima_with(&bin, &["migrate", migrated.to_str().expect("utf-8 path")]);
    let store = Store::open(&load(&migrated)?.store)?;
    for (key, record) in &far_keys {
        assert_eq!(
            store.record(key)?.as_ref(),
            Some(record),
            "task {key} was left on the far side"
        );
    }
    Ok(())
}

#[test]
fn a_detached_run_on_a_rented_machine_carries_no_ceiling_and_keeps_computing() -> Result<()> {
    // A rental bills by the hour rather than by use, so a run that stops early
    // there saves nothing and leaves the worst state of all: a machine still
    // billing and no longer computing. The ceiling stays home — the far config
    // states none — and the far run computes past it until a recall ends it.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating_under_ceiling(dir.path(), &far_root, 1_000);

    let interrupt = AtomicBool::new(false);
    let loaded = sima_pipeline::load(&migrated)?;
    let outcome = migrate(
        &migrated,
        &loaded,
        &letting_go(&interrupt),
        &interrupt,
        BinaryChange::Refuse,
    )?;
    assert!(
        matches!(outcome, MigrateOutcome::Detached { .. }),
        "{outcome:?}"
    );

    let far_text =
        std::fs::read_to_string(far_root.join(loaded.run.id().to_string()).join("sima.toml"))
            .expect("the far config");
    assert!(
        !far_text.contains("max_wall_clock_ms"),
        "the ceiling stayed home: {far_text}"
    );

    // Three times the ceiling this side states, and the far run is still going.
    assert!(
        poll_for(Duration::from_secs(3), || far_pid(&migrated, &far_root)
            .is_none()
            .then_some(()))
        .is_none(),
        "nothing on the far side ended a run under no ceiling"
    );
    end_far_run(far_pid(&migrated, &far_root).expect("the far run is still computing"));
    Ok(())
}

/// Who the interrupt that ends a migration is delivered to.
#[derive(Clone, Copy)]
enum Signalled {
    /// The `sima` process alone, as a `kill` naming it does.
    Migration,
    /// Every process in the migration's group, as a terminal's Ctrl-C does:
    /// the children it spawned are signalled directly, before sima handles
    /// anything.
    Terminal,
}

/// Runs `sima migrate <config>` with `bin` ahead of the PATH and interrupts it
/// once the far run is computing, which is what letting go of one looks like.
fn detached_from(bin: &Path, config: &Path, root: &Path, signalled: Signalled) -> Output {
    let path = std::env::var("PATH").expect("a PATH");
    let child = sima_command()
        .args(["migrate", config.to_str().expect("utf-8 path")])
        .env("PATH", format!("{}:{path}", bin.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // A group of its own, so signalling the whole of it reaches the
        // migration and its children and nothing of the suite's.
        .process_group(0)
        .spawn()
        .expect("spawn sima");
    // The far run writes its pid when it starts, so that file appearing is what
    // says the migration has something to let go of.
    assert!(
        poll_for(Duration::from_secs(60), || far_pid(config, root)).is_some(),
        "the far run started"
    );
    let target = match signalled {
        Signalled::Migration => child.id() as libc::pid_t,
        // A negative pid names the group.
        Signalled::Terminal => -(child.id() as libc::pid_t),
    };
    assert_eq!(
        unsafe { libc::kill(target, libc::SIGINT) },
        0,
        "the migration was signalled"
    );
    child.wait_with_output().expect("the migration ends")
}

#[test]
fn a_terminal_interrupt_detaches_the_migration_it_is_meant_to() -> Result<()> {
    // The bug a real terminal shows and a raised flag cannot: Ctrl-C reaches
    // every process in the foreground group, so the transport carrying the
    // follow dies before sima handles its own signal. Letting go is what was
    // asked for, and a stream that ended with the operator does not turn it
    // into a fault.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let bin = common::machine_stubs(dir.path(), false);
    let migrated = migrating(dir.path(), &far_root, UNFINISHABLE, PACED);

    let output = detached_from(&bin, &migrated, &far_root, Signalled::Terminal);
    assert_eq!(
        output.status.code(),
        Some(0),
        "letting go is its own outcome: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.contains("detached"), "{stdout}");
    assert!(
        stdout.contains("sima migrate") && stdout.contains("sima recall"),
        "both ways back are printed: {stdout}"
    );
    assert!(
        manifest_bytes(&migrated).is_none(),
        "a detached migration seals nothing"
    );

    // And the far run is where it was left: still computing, still rented.
    let pid = far_pid(&migrated, &far_root).expect("the far run keeps computing");
    let store = Store::open(&load(&migrated)?.store)?;
    assert!(
        !store.instance_records()?.is_empty(),
        "the machine it computes on was not torn down"
    );
    end_far_run(pid);
    Ok(())
}

#[test]
fn a_recall_ends_a_detached_run_and_brings_its_results_home() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, UNFINISHABLE, PACED);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    let before = committed_records(&migrated)?;

    // Detached: the far run is left computing, which is the state a recall
    // exists to end.
    let interrupt = AtomicBool::new(false);
    let loaded = sima_pipeline::load(&migrated)?;
    let outcome = migrate(
        &migrated,
        &loaded,
        &letting_go(&interrupt),
        &interrupt,
        BinaryChange::Refuse,
    )?;
    assert!(
        matches!(outcome, MigrateOutcome::Detached { .. }),
        "{outcome:?}"
    );
    let pid = far_pid(&migrated, &far_root).expect("the far run is computing before the recall");

    // Everything the far side committed while it ran: it is on that machine
    // and nowhere else until the recall pulls it.
    let far = far_store(&migrated, &far_root)?;
    assert!(
        !far_committed(&migrated, &far)?.is_empty(),
        "the far side held the chain it was sent"
    );

    let bin = common::machine_stubs(dir.path(), false);
    let recalling = recalling(dir.path(), &far_root, "farbox", UNFINISHABLE, PACED);
    let output = sima_with(&bin, &["recall", recalling.to_str().expect("utf-8 path")]);
    assert_eq!(
        output.status.code(),
        Some(130),
        "a recalled run is resumable, not finalized: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!process_alive(pid), "the far run was ended");
    assert!(
        manifest_bytes(&migrated).is_none(),
        "a recalled run seals nothing"
    );

    // The results that existed still do, and the pull left nothing behind.
    let store = Store::open(&load(&migrated)?.store)?;
    let after = committed_records(&migrated)?;
    for (key, record) in &before {
        assert_eq!(after.get(key), Some(record), "task {key} came home intact");
    }
    for (key, record) in &far_committed(&migrated, &far)? {
        assert_eq!(
            store.record(key)?.as_ref(),
            Some(record),
            "task {key} was left on the far side"
        );
    }
    Ok(())
}

#[test]
fn a_recall_of_a_machine_never_migrated_to_names_what_is_missing() -> Result<()> {
    // Nothing was ever put there, so there is nothing to end and nothing to
    // pull — and a recall says so rather than creating a far directory.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let bin = common::machine_stubs(dir.path(), false);
    let recalling = recalling(dir.path(), &far_root, "farbox", SEGMENTS, CHAINS);

    let output = sima_with(&bin, &["recall", recalling.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("nothing to recall"), "{stderr}");
    let run = load(&recalling)?.run.id();
    assert!(
        !far_root.join(run.to_string()).exists(),
        "and nothing was created there"
    );
    Ok(())
}

/// The candidate behaviors of a run that cannot complete: one accumulating
/// chain, and one candidate the domain rejects outright — a definitive failure
/// no retry revisits, which is what a far run ending in `RunFailed` is.
const REJECTED: &str = r#""accumulate:2", "reject""#;

#[test]
fn a_recall_of_a_far_run_that_failed_brings_the_failure_home() -> Result<()> {
    // A definitive failure is written in the far run's journal, which does not
    // travel: a recall follows nothing, so reading that journal is the only way
    // the failure reaches this side. Without it the run would come home
    // resumable, counting the tasks the failure made unreachable as work left.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");

    // The far run really fails: it is the migration that puts it there and the
    // far `sima run` that writes the failure into its own journal.
    let migrated = migrating(dir.path(), &far_root, SEGMENTS, REJECTED);
    let outcome = move_run(&migrated)?;
    assert!(
        matches!(outcome, MigrateOutcome::Failed { .. }),
        "the far run failed definitively: {outcome:?}"
    );
    assert!(
        far_pid(&migrated, &far_root).is_none(),
        "a run that failed exited"
    );

    // The recall reaches that same far directory as a machine of yours, over a
    // far side that ended before it ever arrived.
    let bin = common::machine_stubs(dir.path(), false);
    let recalling = recalling(dir.path(), &far_root, "farbox", SEGMENTS, REJECTED);
    let output = sima_with(&bin, &["recall", recalling.to_str().expect("utf-8 path")]);
    assert_eq!(
        output.status.code(),
        Some(2),
        "a run that failed comes home failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(
        stdout.contains("definitive failure"),
        "the failure is what it reports: {stdout}"
    );
    assert!(
        manifest_bytes(&recalling).is_none(),
        "a failed run seals nothing"
    );
    Ok(())
}

// ---- The same acceptance, over a real ssh hop ----

/// A throwaway sshd for the duration of a test: its own host key, its own
/// authorized-keys file, a free high port, and a log of what it accepted.
///
/// It needs no root and writes nothing outside `dir`, so it changes no system
/// state and leaves nothing behind. The `Drop` kills it on every path, including
/// a panicking assertion.
struct Sshd {
    port: u16,
    /// The private key a client authenticates with.
    key: PathBuf,
    /// What the server itself recorded, which is the only evidence a hop
    /// happened that a local spawn cannot produce.
    log: PathBuf,
    pid: u32,
    /// The agent holding the key the server authorizes, and its process. The
    /// migration builds its own ssh invocations and names no identity —
    /// correctly, since a rented machine is reached with the operator's own ssh
    /// configuration — so an agent is how a test supplies one.
    agent_sock: PathBuf,
    agent_pid: u32,
}

impl Sshd {
    /// Stands a server up under `dir`, with `path_prefix` prepended to the PATH
    /// of every session it serves — which is how the far side's `sima-worker`
    /// is found without touching anything outside the test.
    fn start(dir: &Path, path_prefix: &Path) -> Sshd {
        let host_key = dir.join("hostkey");
        let key = dir.join("clientkey");
        for path in [&host_key, &key] {
            let generated = Command::new("ssh-keygen")
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(path)
                .status()
                .expect("run ssh-keygen");
            assert!(generated.success(), "ssh-keygen failed for {path:?}");
        }
        let authorized = dir.join("authorized_keys");
        std::fs::copy(dir.join("clientkey.pub"), &authorized).expect("authorize the client key");

        // Bound and released, so sshd takes a port nothing else is on.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind a free port")
            .local_addr()
            .expect("the bound address")
            .port();
        let log = dir.join("sshd.log");
        let pid_file = dir.join("sshd.pid");
        // `ForceCommand` runs through the login shell, whose word splitting is
        // its own; routing the requested command through `/bin/sh -c` makes the
        // split POSIX whatever that shell is.
        let started = Command::new("/usr/sbin/sshd")
            .args(["-f", "/dev/null", "-h"])
            .arg(&host_key)
            .arg("-p")
            .arg(port.to_string())
            .arg("-E")
            .arg(&log)
            .arg("-o")
            .arg(format!("AuthorizedKeysFile={}", authorized.display()))
            .args([
                "-o",
                "StrictModes=no",
                "-o",
                "UsePAM=no",
                "-o",
                "PasswordAuthentication=no",
                "-o",
            ])
            .arg(format!("PidFile={}", pid_file.display()))
            .arg("-o")
            .arg(format!(
                "ForceCommand=PATH={}:$PATH exec /bin/sh -c \"$SSH_ORIGINAL_COMMAND\"",
                path_prefix.display()
            ))
            .status()
            .expect("run sshd");
        assert!(started.success(), "sshd refused to start");

        let pid = poll_for(Duration::from_secs(10), || {
            std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|text| text.trim().parse::<u32>().ok())
        })
        .expect("sshd wrote its pid");

        // An agent of this test's own, on a socket inside `dir`, holding the one
        // key the server authorizes.
        let agent_sock = dir.join("agent.sock");
        let agent = Command::new("ssh-agent")
            .arg("-a")
            .arg(&agent_sock)
            .output()
            .expect("run ssh-agent");
        assert!(agent.status.success(), "ssh-agent refused to start");
        let agent_pid = String::from_utf8_lossy(&agent.stdout)
            .split("SSH_AGENT_PID=")
            .nth(1)
            .and_then(|rest| rest.split(';').next())
            .and_then(|pid| pid.trim().parse::<u32>().ok())
            .expect("ssh-agent reported its pid");
        let added = Command::new("ssh-add")
            .arg(&key)
            .env("SSH_AUTH_SOCK", &agent_sock)
            .output()
            .expect("run ssh-add");
        assert!(added.status.success(), "the agent refused the key");

        let server = Sshd {
            port,
            key,
            log,
            pid,
            agent_sock,
            agent_pid,
        };
        // The server is up when it answers, not when it forked.
        assert!(
            poll_for(Duration::from_secs(10), || server.answers().then_some(())).is_some(),
            "the server never accepted a session"
        );
        server
    }

    /// Whether a client can reach the server and run a command. The options
    /// are the harness's own, naming the key explicitly and remembering no host
    /// key, so the probe touches nothing outside the test either.
    fn answers(&self) -> bool {
        Command::new("ssh")
            .args(["-p", &self.port.to_string(), "-i"])
            .arg(&self.key)
            .args([
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
            ])
            .arg(format!("{}@127.0.0.1", whoami()))
            .args(["--", "true"])
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    /// The agent socket a client authenticates through.
    fn agent(&self) -> &Path {
        &self.agent_sock
    }

    /// The endpoint the stub backend is pointed at.
    fn endpoint(&self) -> String {
        format!("{}@127.0.0.1:{}", whoami(), self.port)
    }

    /// How many sessions the server itself recorded accepting.
    fn accepted(&self) -> usize {
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains("Accepted publickey"))
            .count()
    }

    /// Whether the server's process is still there.
    fn alive(&self) -> bool {
        process_alive(self.pid)
    }
}

/// Whether a process is still there. Signal zero is the existence probe: it
/// delivers nothing and reports whether it could have.
fn process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

impl Drop for Sshd {
    fn drop(&mut self) {
        // Both processes, on every path out — including a panicking assertion.
        for pid in [self.pid, self.agent_pid] {
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }
    }
}

/// The user the test runs as, which is the user its own server authenticates.
/// The environment names it on an interactive machine and `id` names it
/// everywhere else.
fn whoami() -> String {
    if let Ok(user) = std::env::var("USER") {
        return user;
    }
    let named = Command::new("id").arg("-un").output().expect("run id");
    assert!(
        named.status.success(),
        "id could not name the invoking user"
    );
    String::from_utf8(named.stdout)
        .expect("the user name is UTF-8")
        .trim()
        .to_string()
}

/// Polls `probe` every 20 ms until it yields a value or `deadline` elapses.
fn poll_for<T>(deadline: Duration, probe: impl Fn() -> Option<T>) -> Option<T> {
    let end = Instant::now() + deadline;
    loop {
        if let Some(value) = probe() {
            return Some(value);
        }
        if Instant::now() >= end {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Runs `sima migrate <config>` with the stub backend pointed at `endpoint` and
/// authenticating through `agent`, so every far-side operation crosses a real
/// ssh hop and nothing outside the test's directory is read or written.
fn migrate_over(config: &Path, endpoint: &str, agent: &Path) -> Output {
    sima_command()
        .args(["migrate", config.to_str().expect("utf-8 path")])
        .env("SIMA_STUB_SSH", endpoint)
        .env("SSH_AUTH_SOCK", agent)
        .output()
        .expect("spawn sima migrate")
}

#[test]
fn a_run_migrated_over_a_real_ssh_hop_finalizes_and_the_server_saw_it() -> Result<()> {
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    // The far side's `sima-worker` is found through the PATH the server sets
    // for its sessions; the binary sits beside the `sima` the config names.
    let binaries = Path::new(far_binary())
        .parent()
        .expect("a binary directory");
    let sshd = Sshd::start(dir.path(), binaries);

    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, SEGMENTS, CHAINS);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    let before = committed_records(&migrated)?;
    assert!(!before.is_empty(), "the local run committed something");

    let output = migrate_over(&migrated, &sshd.endpoint(), sshd.agent());
    assert_eq!(
        output.status.code(),
        Some(0),
        "the migration finalized: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // It really crossed the hop: the server recorded every session, and a
    // migration reached in process would have produced none.
    assert!(
        sshd.accepted() > 1,
        "the server accepted the far-side sessions: {} in {:?}",
        sshd.accepted(),
        sshd.log
    );

    // And it is the same run, finished: every record carried, the rest
    // committed on the far side, nothing left rented.
    assert!(
        manifest_bytes(&migrated).is_some(),
        "the manifest is sealed"
    );
    let after = committed_records(&migrated)?;
    for (key, record) in &before {
        assert_eq!(after.get(key), Some(record), "task {key} was recomputed");
    }
    assert!(after.len() > before.len(), "the far side did the rest");
    let store = Store::open(&load(&migrated)?.store)?;
    assert!(
        store.instance_records()?.is_empty(),
        "nothing was left rented"
    );
    Ok(())
}

#[test]
fn a_destination_that_cannot_be_reached_fails_rather_than_hanging() -> Result<()> {
    // `BatchMode=yes` is what makes this prompt rather than block: a server that
    // is not there refuses at once instead of waiting on a password.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated = migrating(dir.path(), &far_root, SEGMENTS, CHAINS);
    assert!(matches!(
        drive(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));

    // A port nothing listens on, taken and released.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
        .port();
    let started = Instant::now();
    let output = migrate_over(
        &migrated,
        &format!("{}@127.0.0.1:{port}", whoami()),
        &dir.path().join("no-such-agent"),
    );
    assert_ne!(
        output.status.code(),
        Some(0),
        "an unreachable far side fails"
    );
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "it failed rather than hanging: {:?}",
        started.elapsed()
    );
    Ok(())
}

#[test]
fn a_malformed_stub_endpoint_is_refused_by_name() -> Result<()> {
    // Set but unparseable means the caller meant to cross a hop, so it fails
    // instead of quietly falling back to the in-process path.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let migrated = migrating(dir.path(), &dir.path().join("far"), SEGMENTS, CHAINS);
    let output = migrate_over(
        &migrated,
        "not-an-endpoint",
        &dir.path().join("no-such-agent"),
    );
    assert_ne!(output.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("SIMA_STUB_SSH"),
        "names the variable: {stderr}"
    );
    Ok(())
}

#[test]
fn the_harness_leaves_no_server_behind() {
    // The guard is what makes the tier safe to run anywhere: a failing
    // assertion must not leave a listening server on the machine.
    workers_built();
    let dir = tempfile::tempdir().expect("temp dir");
    let binaries = Path::new(far_binary())
        .parent()
        .expect("a binary directory");
    let pid = {
        let sshd = Sshd::start(dir.path(), binaries);
        assert!(sshd.alive(), "the server runs while the test holds it");
        sshd.pid
    };
    assert!(
        poll_for(Duration::from_secs(10), || (!process_alive(pid))
            .then_some(()))
        .is_some(),
        "the server outlived its guard"
    );
}
