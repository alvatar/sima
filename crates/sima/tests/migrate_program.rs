//! End-to-end acceptance of a migrated run whose format is served by a program
//! of its own: the program travels to the destination as objects, installs
//! itself there, runs the search, and the results come home.
//!
//! The far side is the real `sima` binary, reached through the stub provider,
//! whose machines are local subprocesses. Nothing here needs a network, a GPU,
//! an ssh hop, or a container, so it runs in the ordinary gate. The program is
//! a wrapper around the built `sima-worker`, which answers for the in-tree
//! formats over exactly the protocol a program outside this workspace speaks.
//!
//! What each test fixes:
//!
//! - the run executes on the destination and finalizes to the manifest an
//!   uninterrupted local run writes, over the same run id and the same keys;
//! - both payload shapes travel — a single file that is its own entry point,
//!   and a directory whose install script builds one;
//! - the destination installs once and the stamp answers every later load;
//! - a changed payload reaches the destination, and the far run's own binding
//!   guard stops it until the invocation accepts the change;
//! - an interrupt winds the far run down and brings home what its program
//!   computed;
//! - a program written against the SDK finds it on the destination, vended
//!   there by that machine's own binary;
//! - an install that fails states its own last words to the operator who asked
//!   for the migration;
//! - a run whose format this workspace carries no code for migrates onto a
//!   rented destination and finalizes there, its workers derived from the
//!   program\'s own enumeration.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::{example_binary, manifest_bytes, worker_binary};
use sima_core::Result;
use sima_model::{TaskKey, TaskRecord};
use sima_pipeline::{
    BinaryChange, Engagement, MigrateOutcome, Record, RunControl, RunOutcome, load, migrate,
    orchestrate, task_keys,
};
use sima_store::Store;

/// The segment count a run finishes in: short enough that a whole run is a
/// fraction of a second.
const SEGMENTS: u64 = 4;

/// The run every test here drives: two candidates over accumulating chains,
/// answered by a program rather than by this build.
///
/// `[run]` is the only hashed section, so every config written from it
/// describes one run whatever machine drives it and whatever serves its format.
fn run_section() -> String {
    format!(
        r#"
        [run]
        root_seed = 21
        segments = {SEGMENTS}
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

/// Where the far side's `sima` is, for a config that names it.
fn far_binary() -> &'static str {
    env!("CARGO_BIN_EXE_sima")
}

/// Writes a program that imports the vended SDK before it answers, appending to
/// `resolved` the directory each import resolved from, and answers the
/// `[domain.*]` entry that declares it.
///
/// It is the ordinary wrapper with one line in front: what the entry declares
/// is what has to be there, so a program that cannot import fails the spawn,
/// and the report says which copy every spawn that succeeded read — on this
/// machine and on the destination alike, since both sides spawn it.
fn importing_program(dir: &Path, resolved: &Path) -> String {
    executable(
        &dir.join("program.sh"),
        &format!(
            "#!/bin/sh\n\
             python3 -c 'import sima, os; print(os.path.dirname(sima.__file__))' \
             >> {resolved:?} || exit 9\n\
             exec {} \"$@\"\n",
            worker_binary().display(),
            resolved = resolved.display(),
        ),
    );
    "[domain.\"stub.v1\"]\nbinary = \"./program.sh\"\npayload = \"./program.sh\"\nsdk = \"python\"\n"
        .to_string()
}

/// Every directory the importing program reported resolving `sima` from.
fn imported_from(resolved: &Path) -> Vec<String> {
    std::fs::read_to_string(resolved)
        .expect("the program reported where its import resolved")
        .lines()
        .map(str::to_string)
        .collect()
}

/// Asserts `python3` runs.
///
/// The one test here that imports the vended SDK requires it, and a machine
/// without it fails naming what is missing rather than reporting a green suite
/// that tested nothing.
fn require_python3() {
    let version = std::process::Command::new("python3")
        .arg("--version")
        .output();
    match version {
        Ok(output) if output.status.success() => {}
        other => panic!(
            "this test drives a Python program, so python3 is required and must run: {other:?}"
        ),
    }
}

/// Writes an executable file at `path`, creating its parents.
fn executable(path: &Path, text: &str) -> PathBuf {
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("create the parent");
    std::fs::write(path, text).expect("write the file");
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("make it executable");
    path.to_path_buf()
}

/// A program that answers for `stub.v1`: a wrapper around the built worker,
/// small enough to ingest and to travel. `marker` distinguishes two builds of
/// one program, which is what a payload edit looks like.
fn wrapper(path: &Path, marker: &str) -> PathBuf {
    executable(
        path,
        &format!(
            "#!/bin/sh\n# {marker}\nexec {} \"$@\"\n",
            worker_binary().display()
        ),
    )
}

/// The shape a payload takes in a config.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// One file, which is its own entry point: no install script is needed.
    File,
    /// A directory, whose install script decides which of its files runs.
    Directory,
}

/// Writes the program `shape` describes under `dir` and answers the
/// `[domain.*]` entry that declares it.
///
/// `marker` goes into the wrapper, so two calls differing only in it produce
/// two payloads and two digests. `installs` is a path the directory form's
/// script appends to on every run, which is how the stamp is observed.
fn program(dir: &Path, shape: Shape, marker: &str, installs: &Path) -> String {
    match shape {
        Shape::File => {
            wrapper(&dir.join("program.sh"), marker);
            "[domain.\"stub.v1\"]\nbinary = \"./program.sh\"\npayload = \"./program.sh\"\n"
                .to_string()
        }
        Shape::Directory => {
            wrapper(&dir.join("src/wrapper.sh"), marker);
            executable(
                &dir.join("install.sh"),
                &format!(
                    "#!/bin/sh\n\
                     set -e\n\
                     echo ran >> {installs:?}\n\
                     cp \"$SIMA_PAYLOAD_DIR/wrapper.sh\" \"$SIMA_INSTALL_DIR/program\"\n\
                     chmod 755 \"$SIMA_INSTALL_DIR/program\"\n",
                    installs = installs.display(),
                ),
            );
            "[domain.\"stub.v1\"]\nbinary = \"./src/wrapper.sh\"\n\
             payload = \"./src\"\ninstall = \"./install.sh\"\n"
                .to_string()
        }
    }
}

/// A config under `dir` whose orchestrator migrates onto a rented stub machine
/// rooted at `root`, its format served by the program `entry` declares.
fn migrating(dir: &Path, root: &Path, entry: &str) -> PathBuf {
    write(
        dir,
        "migrating.toml",
        &format!(
            "{}\n[orchestrator]\nworkers = 2\nmigrate = \"far\"\n\
             \n[host.far]\nprovider = \"stub\"\nroot = {root:?}\nbinary = {binary:?}\n\
             ready_timeout_ms = 30000\nready_poll_ms = 20\n\n{entry}",
            run_section(),
            root = root.to_string_lossy(),
            binary = far_binary(),
        ),
    )
}

/// A config under `dir` that drives the same run here, its format served by
/// the program `entry` declares. It names no destination, so nothing moves.
fn local(dir: &Path, entry: &str) -> PathBuf {
    write(
        dir,
        "local.toml",
        &format!("{}\n[orchestrator]\nworkers = 2\n\n{entry}", run_section()),
    )
}

/// Writes `text` as a config named `name` under `dir`, creating the directory.
fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
    std::fs::create_dir_all(dir).expect("create the config directory");
    common::write_config_text(dir, name, text)
}

/// Drives the run `config` describes to its end, here.
fn drive(config: &Path) -> Result<RunOutcome> {
    drive_stopping(config, None)
}

/// Drives the run `config` describes, interrupting once `stop_after` tasks
/// have committed; `None` runs it to its end.
fn drive_stopping(config: &Path, stop_after: Option<usize>) -> Result<RunOutcome> {
    let loaded = load(config)?;
    let interrupt = AtomicBool::new(false);
    let committed = std::sync::atomic::AtomicUsize::new(0);
    let control = RunControl {
        observer: &|record: &Record| {
            if let Some(stop_after) = stop_after
                && matches!(record.event, sima_pipeline::Event::Committed { .. })
                && committed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1 >= stop_after
            {
                interrupt.store(true, std::sync::atomic::Ordering::Relaxed);
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

/// Moves the run `config` describes onto its destination.
fn move_run(config: &Path, accept: BinaryChange) -> Result<MigrateOutcome> {
    let loaded = load(config)?;
    migrate(
        config,
        &loaded,
        &|_: &Record| {},
        &AtomicBool::new(false),
        accept,
    )
}

/// Every record the store of the run `config` describes currently holds.
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

/// The run directory the far side keeps for the run `config` describes.
fn far_dir(config: &Path, root: &Path) -> PathBuf {
    root.join(load(config).expect("the config loads").run.id().to_string())
}

/// The program digests the far side's journal records, in the order its
/// sessions bound them: one per session that got past the binding guard.
fn far_bindings(config: &Path, root: &Path) -> Vec<String> {
    let loaded = load(config).expect("the config loads");
    let store = Store::open(far_dir(config, root).join("store")).expect("open the far store");
    store
        .journal(&loaded.run.id())
        .expect("read the far journal")
        .iter()
        .filter_map(|line| Record::from_line(line).ok())
        .filter_map(|record| match record.event {
            sima_pipeline::Event::ProgramBound { digest, .. } => Some(digest),
            _ => None,
        })
        .collect()
}

/// The program digest each of the far side's workers answered at its
/// handshake, one per spawn and respawn.
fn far_worker_programs(config: &Path, root: &Path) -> Vec<Option<String>> {
    let loaded = load(config).expect("the config loads");
    let store = Store::open(far_dir(config, root).join("store")).expect("open the far store");
    store
        .journal(&loaded.run.id())
        .expect("read the far journal")
        .iter()
        .filter_map(|line| Record::from_line(line).ok())
        .filter_map(|record| match record.event {
            sima_pipeline::Event::WorkerBound { program, .. } => Some(program),
            _ => None,
        })
        .collect()
}

/// The payload digest the far config states, which is the manifest object the
/// local side ingested into the store that travelled.
fn far_payload_digest(config: &Path, root: &Path) -> String {
    let text =
        std::fs::read_to_string(far_dir(config, root).join("sima.toml")).expect("the far config");
    text.lines()
        .find_map(|line| line.strip_prefix("payload_digest = "))
        .map(|value| value.trim_matches('"').to_string())
        .unwrap_or_else(|| panic!("the far config states a payload digest: {text}"))
}

/// How many times the directory form's install script ran.
fn installs(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|text| text.lines().count())
        .unwrap_or(0)
}

#[test]
fn a_program_served_run_executes_on_its_destination_and_comes_home_complete() -> Result<()> {
    // The milestone in one test: the program travels as objects, installs
    // itself on the destination, runs the search there, and the store that
    // comes home is byte-identical to one this machine produced alone.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let installs_at = dir.path().join("installs");

    // The reference, driven here throughout, through the program as this
    // machine holds it.
    let reference_dir = dir.path().join("reference");
    let reference_entry = program(&reference_dir, Shape::Directory, "one", &installs_at);
    let reference = local(&reference_dir, &reference_entry);
    assert!(matches!(drive(&reference)?, RunOutcome::Finalized { .. }));

    // The migrated run: the same run, moved before it starts.
    let migrated_dir = dir.path().join("migrated");
    let migrated_entry = program(&migrated_dir, Shape::Directory, "one", &installs_at);
    let migrated = migrating(&migrated_dir, &far_root, &migrated_entry);
    assert_eq!(
        load(&migrated)?.run.id(),
        load(&reference)?.run.id(),
        "where a format is answered from is operational, so it is one run"
    );

    let outcome = move_run(&migrated, BinaryChange::Refuse)?;
    assert!(
        matches!(outcome, MigrateOutcome::Finalized { .. }),
        "the migration came home complete: {outcome:?}"
    );

    // The far side really did the work: its own store holds the run.
    let far = far_dir(&migrated, &far_root);
    assert!(
        far.join("store/runs").is_dir(),
        "the destination drove the run in its own store"
    );
    assert!(
        far.join("program/stub.v1/installed/program").is_file(),
        "and installed the program to do it with"
    );

    // The criterion the milestone carries: byte equality with a run this
    // machine drove alone.
    assert_eq!(
        manifest_bytes(&migrated).expect("the migrated run finalized"),
        manifest_bytes(&reference).expect("the reference finalized"),
        "one run, one manifest, whichever machine computed it"
    );
    assert_eq!(
        committed_records(&migrated)?,
        committed_records(&reference)?
    );
    Ok(())
}

#[test]
fn a_single_file_payload_needs_no_install_script_to_travel() -> Result<()> {
    // The other shape: the file is the program, so the convention puts it
    // where the far config's binary looks and no script is involved.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let installs_at = dir.path().join("installs");
    let migrated_dir = dir.path().join("migrated");
    let entry = program(&migrated_dir, Shape::File, "one", &installs_at);
    let migrated = migrating(&migrated_dir, &far_root, &entry);

    assert!(matches!(
        move_run(&migrated, BinaryChange::Refuse)?,
        MigrateOutcome::Finalized { .. }
    ));
    let far = far_dir(&migrated, &far_root);
    assert!(far.join("program/stub.v1/installed/program").is_file());
    assert!(
        !far.join("program/stub.v1/install.sh").exists(),
        "no script travelled, because none was declared"
    );
    Ok(())
}

#[test]
fn the_run_a_migration_drives_is_the_run_this_machine_would_have_driven() -> Result<()> {
    // Identity, stated over both halves: the run id and every task key are the
    // same whether the program answers here or on the destination.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let installs_at = dir.path().join("installs");

    let local_dir = dir.path().join("local");
    let local_entry = program(&local_dir, Shape::Directory, "one", &installs_at);
    let here = local(&local_dir, &local_entry);
    assert!(matches!(drive(&here)?, RunOutcome::Finalized { .. }));

    let migrated_dir = dir.path().join("migrated");
    let migrated_entry = program(&migrated_dir, Shape::Directory, "one", &installs_at);
    let migrated = migrating(&migrated_dir, &far_root, &migrated_entry);
    assert!(matches!(
        move_run(&migrated, BinaryChange::Refuse)?,
        MigrateOutcome::Finalized { .. }
    ));

    let (here_loaded, migrated_loaded) = (load(&here)?, load(&migrated)?);
    assert_eq!(here_loaded.run.id(), migrated_loaded.run.id());
    assert_eq!(
        task_keys(&here_loaded, &Store::open(&here_loaded.store)?)?,
        task_keys(&migrated_loaded, &Store::open(&migrated_loaded.store)?)?,
    );
    Ok(())
}

#[test]
fn a_second_migration_of_an_unchanged_payload_installs_nothing() -> Result<()> {
    // The stamp, end to end: the destination built the program once and every
    // later load reads one file and spawns what is already there.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let installs_at = dir.path().join("installs");
    let migrated_dir = dir.path().join("migrated");
    let entry = program(&migrated_dir, Shape::Directory, "one", &installs_at);
    let migrated = migrating(&migrated_dir, &far_root, &entry);

    assert!(matches!(
        move_run(&migrated, BinaryChange::Refuse)?,
        MigrateOutcome::Finalized { .. }
    ));
    assert_eq!(installs(&installs_at), 1, "the destination built it once");

    // The run is complete, so the second migration starts a far run that has
    // nothing to compute — and nothing to install either.
    assert!(matches!(
        move_run(&migrated, BinaryChange::Refuse)?,
        MigrateOutcome::Finalized { .. }
    ));
    assert_eq!(installs(&installs_at), 1, "and did not build it again");
    Ok(())
}

#[test]
fn a_changed_payload_reaches_the_destination_and_stops_at_its_binding_guard() -> Result<()> {
    // A payload edit reaches the destination by re-running the config: the new
    // manifest travels, the destination installs it, and the far run's own
    // binding guard is what stops there — its stored results and checkpoints
    // came from the previous build, and only the invocation can say that is
    // acceptable.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let installs_at = dir.path().join("installs");
    let migrated_dir = dir.path().join("migrated");
    let entry = program(&migrated_dir, Shape::Directory, "one", &installs_at);
    let migrated = migrating(&migrated_dir, &far_root, &entry);
    assert!(matches!(
        move_run(&migrated, BinaryChange::Refuse)?,
        MigrateOutcome::Finalized { .. }
    ));
    let first = far_bindings(&migrated, &far_root);
    assert_eq!(first.len(), 1, "the destination bound what it installed");

    // The program is edited here. Its declarations are unchanged, so the run
    // id and every task key stand; what changed is the build that computes.
    program(&migrated_dir, Shape::Directory, "two", &installs_at);
    move_run(&migrated, BinaryChange::Refuse)?;

    // The edit travelled and was installed, and the guard refused it.
    assert_eq!(
        installs(&installs_at),
        2,
        "the changed payload was built on the destination"
    );
    let refused = std::fs::read_to_string(far_dir(&migrated, &far_root).join("run.log"))
        .expect("the far run wrote a log");
    assert!(
        refused.contains("--accept-binary"),
        "the far binding guard refused the changed program: {refused}"
    );
    assert_eq!(
        far_bindings(&migrated, &far_root),
        first,
        "a refused session binds nothing, so the run still names the build that drove it"
    );

    // With the acceptance, the far run binds the new build and drives on.
    assert!(matches!(
        move_run(&migrated, BinaryChange::Accept)?,
        MigrateOutcome::Finalized { .. }
    ));
    let accepted = far_bindings(&migrated, &far_root);
    assert_eq!(accepted.len(), 2, "the accepted build was bound");
    assert_ne!(accepted[1], accepted[0], "and it is the changed one");
    assert_eq!(
        installs(&installs_at),
        2,
        "the stamp answered the accepted run, which installs nothing new"
    );
    Ok(())
}

#[test]
fn a_program_served_migration_winds_down_on_an_interrupt_and_brings_home_what_ran() -> Result<()> {
    // The wind-down over a run the destination had to install a program for:
    // the far run is signalled, whatever its program computed is pulled, and
    // the run comes home resumable rather than sealed.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let installs_at = dir.path().join("installs");
    let migrated_dir = dir.path().join("migrated");
    // A chain the far side cannot reach the end of while this migration is
    // still reading its first record: every segment sleeps, so the outcome is
    // decided by the wind-down rather than by how fast this machine runs.
    let entry = program(&migrated_dir, Shape::Directory, "one", &installs_at);
    let migrated = write(
        &migrated_dir,
        "paced.toml",
        &format!(
            r#"
            [run]
            root_seed = 21
            segments = 400
            format = "stub.v1"

            [run.generator]
            id = "stub.v1"
            behaviors = ["accumulate:2:250", "accumulate:2:250"]

            [config]
            store = "./store"
            max_attempts = 3

            [orchestrator]
            workers = 2
            migrate = "far"

            [host.far]
            provider = "stub"
            root = {root:?}
            binary = {binary:?}
            ready_timeout_ms = 30000
            ready_poll_ms = 20

            {entry}
            "#,
            root = far_root.to_string_lossy(),
            binary = far_binary(),
        ),
    );

    // Driven partway here first, so the far side is sent a chain with a
    // frontier and the pull has something of its own to bring home.
    assert!(matches!(
        drive_stopping(&migrated, Some(2))?,
        RunOutcome::Interrupted { .. }
    ));
    let before = committed_records(&migrated)?;
    assert!(!before.is_empty(), "the local run committed something");

    let interrupt = AtomicBool::new(false);
    let loaded = load(&migrated)?;
    let outcome = migrate(
        &migrated,
        &loaded,
        &|_: &Record| interrupt.store(true, std::sync::atomic::Ordering::Relaxed),
        &interrupt,
        BinaryChange::Refuse,
    )?;
    assert!(
        matches!(outcome, MigrateOutcome::Interrupted { .. }),
        "a wound-down migration is resumable, not finalized: {outcome:?}"
    );
    assert!(
        manifest_bytes(&migrated).is_none(),
        "an interrupted migration seals nothing"
    );
    assert_eq!(
        installs(&installs_at),
        1,
        "the destination installed the program it needed to run at all"
    );

    // The results that existed still do.
    let after = committed_records(&migrated)?;
    for (key, record) in &before {
        assert_eq!(after.get(key), Some(record), "task {key} came home intact");
    }

    // And the pull ran to completion: nothing the far side holds was left
    // behind, however far its program got before the signal.
    let far = Store::open(far_dir(&migrated, &far_root).join("store"))?;
    let here = Store::open(&loaded.store)?;
    let mut held = 0;
    for key in task_keys(&loaded, &far)? {
        if let Some(record) = far.record(&key)? {
            assert_eq!(
                here.record(&key)?.as_ref(),
                Some(&record),
                "task {key} was left on the far side"
            );
            held += 1;
        }
    }
    assert!(held > 0, "the far side held the chain it was sent");
    Ok(())
}

#[test]
fn a_program_written_against_the_sdk_finds_it_on_the_destination() -> Result<()> {
    // The SDK travels inside the binary rather than with the payload: the
    // program's own file is all that crosses, the destination's `sima` vends
    // the package its protocol matches, and `import sima` resolves there with
    // no interpreter path declared anywhere. The program reports the directory
    // its import resolved from, so what is asserted is the vended copy rather
    // than some package the machine happened to hold.
    require_python3();
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated_dir = dir.path().join("migrated");
    let resolved_at = dir.path().join("resolved");
    let entry = importing_program(&migrated_dir, &resolved_at);
    let migrated = migrating(&migrated_dir, &far_root, &entry);

    assert!(matches!(
        move_run(&migrated, BinaryChange::Refuse)?,
        MigrateOutcome::Finalized { .. }
    ));

    // The destination vended the package beside the program it installed, and
    // that is the copy the program imported.
    let far = far_dir(&migrated, &far_root);
    let vended = far.join("sdk/python/installed/sima");
    assert!(
        vended.join("__init__.py").is_file(),
        "the destination's own binary wrote the package"
    );
    let imported = imported_from(&resolved_at);
    assert!(
        imported.contains(&vended.to_string_lossy().into_owned()),
        "the program on the destination read the package vended there: {imported:?}"
    );
    // Every spawn on either side read a package sima wrote: what a machine
    // happens to hold under that name is shadowed, here as well as there.
    for directory in &imported {
        assert!(
            directory.ends_with("sdk/python/installed/sima"),
            "{directory} is a vended package"
        );
    }

    // And it asked for it by name alone: nothing about an interpreter path
    // crossed, because a path is the destination's own business.
    let far_config = std::fs::read_to_string(far.join("sima.toml")).expect("the far config");
    assert!(
        far_config.contains(r#"sdk = "python""#),
        "the declaration travelled: {far_config}"
    );
    assert!(
        !far_config.contains("PYTHONPATH"),
        "and no interpreter path did: {far_config}"
    );
    Ok(())
}

#[test]
fn every_far_worker_answers_the_program_the_migration_sent() -> Result<()> {
    // The agreement observed as a recorded fact where it matters: the far side
    // installed the tree the digest names, spawned its workers from it, and
    // each one answered that same digest back — journaled beside the device it
    // ran on. The spawn is what enforces it, so a run that finalized is a run
    // whose every worker agreed.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let installs_at = dir.path().join("installs");
    let migrated_dir = dir.path().join("migrated");
    let entry = program(&migrated_dir, Shape::Directory, "one", &installs_at);
    let migrated = migrating(&migrated_dir, &far_root, &entry);

    assert!(matches!(
        move_run(&migrated, BinaryChange::Refuse)?,
        MigrateOutcome::Finalized { .. }
    ));

    let sent = far_payload_digest(&migrated, &far_root);
    let answered = far_worker_programs(&migrated, &far_root);
    assert!(!answered.is_empty(), "the far side bound workers");
    for program in &answered {
        assert_eq!(
            program.as_deref(),
            Some(sent.as_str()),
            "every far worker ran the program the migration sent"
        );
    }
    Ok(())
}

#[test]
fn a_local_run_of_a_program_this_machine_holds_binds_no_program_digest() -> Result<()> {
    // The other presence direction, end to end: nothing travelled, so nothing
    // is stated to the workers, the wire field crosses empty, and the journal
    // records no digest.
    let dir = tempfile::tempdir().expect("temp dir");
    let installs_at = dir.path().join("installs");
    let local_dir = dir.path().join("local");
    let entry = program(&local_dir, Shape::Directory, "one", &installs_at);
    let config = local(&local_dir, &entry);
    assert!(matches!(drive(&config)?, RunOutcome::Finalized { .. }));

    let loaded = load(&config)?;
    let store = Store::open(local_dir.join("store"))?;
    let bound: Vec<Option<String>> = store
        .journal(&loaded.run.id())?
        .iter()
        .filter_map(|line| Record::from_line(line).ok())
        .filter_map(|record| match record.event {
            sima_pipeline::Event::WorkerBound { program, .. } => Some(program),
            _ => None,
        })
        .collect();
    assert!(!bound.is_empty(), "the run bound workers");
    assert!(
        bound.iter().all(Option::is_none),
        "a program that travelled nowhere names no digest: {bound:?}"
    );
    Ok(())
}

/// A digest naming a program no run of these tests ever sent, for the drift
/// case below.
const DRIFTED: &str = "3333333333333333333333333333333333333333333333333333333333333333";

#[test]
fn a_destination_running_another_program_fails_the_spawn_naming_both_digests() -> Result<()> {
    // A machine whose installed tree drifted from what the run sent. Overriding
    // the variable inside the program is this test's stand-in for that: sima
    // states the digest it installed and the child answers another, which is
    // exactly what a machine holding a different program produces. The run must
    // stop at the first worker spawn, naming the two programs.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated_dir = dir.path().join("migrated");
    executable(
        &migrated_dir.join("program.sh"),
        &format!(
            "#!/bin/sh\nSIMA_PROGRAM_DIGEST={DRIFTED} exec {} \"$@\"\n",
            worker_binary().display()
        ),
    );
    let migrated = migrating(
        &migrated_dir,
        &far_root,
        "[domain.\"stub.v1\"]\nbinary = \"./program.sh\"\npayload = \"./program.sh\"\n",
    );

    let outcome = move_run(&migrated, BinaryChange::Refuse)?;
    assert!(
        matches!(outcome, MigrateOutcome::Outstanding { .. }),
        "no worker took a task, so the run came home with all of them: {outcome:?}"
    );
    assert!(manifest_bytes(&migrated).is_none(), "nothing was finalized");
    assert!(
        far_worker_programs(&migrated, &far_root).is_empty(),
        "a refused spawn binds no worker, so the journal records none"
    );

    // The config the migration synthesized is a config in its own right, and
    // the tree it names is installed and drifted: driving it here is the same
    // machine state met a second time, with the refusal in hand rather than on
    // the destination's own stderr.
    let far_config = far_dir(&migrated, &far_root).join("sima.toml");
    let error = drive(&far_config).expect_err("a program disagreement fails the run");
    let text = error.to_string();
    assert!(text.contains("program digest mismatch"), "{text}");
    assert!(
        text.contains(DRIFTED),
        "names what the machine answered: {text}"
    );
    assert!(
        text.contains(&far_payload_digest(&migrated, &far_root)),
        "and what the run sent: {text}"
    );
    Ok(())
}

#[test]
fn an_install_that_fails_on_the_destination_states_its_own_last_words() -> Result<()> {
    // The failure happens inside the far `sima run`, before it journals, so
    // the follow finds a run that never started. What the operator gets back
    // is the machine's name and the script's own output.
    let dir = tempfile::tempdir().expect("temp dir");
    let far_root = dir.path().join("far");
    let migrated_dir = dir.path().join("migrated");
    wrapper(&migrated_dir.join("src/wrapper.sh"), "one");
    executable(
        &migrated_dir.join("install.sh"),
        "#!/bin/sh\necho 'no compiler on this machine' >&2\nexit 3\n",
    );
    let migrated = migrating(
        &migrated_dir,
        &far_root,
        "[domain.\"stub.v1\"]\nbinary = \"./src/wrapper.sh\"\n\
         payload = \"./src\"\ninstall = \"./install.sh\"\n",
    );

    let error = move_run(&migrated, BinaryChange::Refuse)
        .expect_err("an install that fails fails the migration");
    let text = error.to_string();
    assert!(text.contains("\"far\""), "names the machine: {text}");
    assert!(
        text.contains("no compiler on this machine"),
        "carries the script's own output: {text}"
    );
    assert!(manifest_bytes(&migrated).is_none(), "nothing was finalized");
    Ok(())
}

/// A config under `dir` migrating a run of `example.doubler.v1` — a format this
/// workspace carries no code for — onto a rented stub machine rooted at `root`,
/// served by the example program itself as a single-file payload.
fn migrating_unknown_format(dir: &Path, root: &Path) -> PathBuf {
    let payload = dir.join("program");
    std::fs::create_dir_all(dir).expect("create the config directory");
    std::fs::copy(example_binary(), &payload).expect("copy the program into the payload");
    std::fs::set_permissions(
        &payload,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .expect("make the payload executable");
    write(
        dir,
        "unknown.toml",
        &format!(
            r#"
        [run]
        root_seed = 7
        format = "example.doubler.v1"

        [run.generator]
        id = "example.doubler.v1"
        count = 4

        [config]
        store = "./store"
        max_attempts = 2

        [orchestrator]
        workers = 2
        migrate = "far"

        [host.far]
        provider = "stub"
        root = {root:?}
        binary = {binary:?}
        ready_timeout_ms = 30000
        ready_poll_ms = 20

        [domain."example.doubler.v1"]
        binary = "./program"
        payload = "./program"
    "#,
            root = root.to_string_lossy(),
            binary = far_binary(),
        ),
    )
}

#[test]
fn a_format_this_build_carries_no_code_for_migrates_onto_a_rented_machine() -> Result<()> {
    // The destination could answer nothing about this run before it had the
    // program: its readiness probe names no format, and the config synthesized
    // for it states no worker layout. The far run installs the program, asks it
    // which devices its work can go on, and derives its workers from that.
    let dir = tempfile::tempdir().expect("temp dir");
    let root = tempfile::tempdir().expect("temp dir");
    let config = migrating_unknown_format(&dir.path().join("near"), root.path());

    let outcome = move_run(&config, BinaryChange::Refuse)?;
    assert!(
        matches!(outcome, MigrateOutcome::Finalized { .. }),
        "{outcome:?}"
    );
    // The far run's workers ran the program the migration delivered, each
    // answering the digest the destination's own stamp carries.
    let programs = far_worker_programs(&config, root.path());
    assert!(!programs.is_empty(), "the far side bound workers");
    let digest = far_payload_digest(&config, root.path());
    for program in &programs {
        assert_eq!(
            program.as_deref(),
            Some(digest.as_str()),
            "every far worker answers the payload the migration sent: {programs:?}"
        );
    }
    // And the layout it derived is one worker per usable device, which for a
    // program that opens none is a single deviceless worker.
    let far = std::fs::read_to_string(far_dir(&config, root.path()).join("sima.toml"))
        .expect("the far config");
    assert!(!far.contains("workers ="), "no layout was written: {far}");
    assert!(!far.contains("[[orchestrator.device]]"), "{far}");
    Ok(())
}
