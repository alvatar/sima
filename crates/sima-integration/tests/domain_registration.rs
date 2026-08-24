//! End-to-end acceptance of a format served by its own program: the protocol
//! carries everything a run's identity is made of, and a program outside this
//! workspace runs a whole search through the same spine.
//!
//! The equivalence here is what proves the protocol sufficient. A run driven
//! through a program produces the run id and the task keys the same run
//! produces by direct call, so anything the protocol failed to carry would
//! change a hash and fail these tests rather than pass silently.
//!
//! The spawn surface is acceptance-tested the same way, from the program's own
//! point of view: a wrapper script reports what it was handed, and a whole run
//! through it is the proof.
//!
//! Two boundaries around the program's identity are here too: the build that
//! served a session is journaled and compared at the next resume, and a
//! migration of a run whose entry names no payload is refused, since a
//! migration carries the program to the destination as objects and an entry
//! that names none states the program is this machine's alone.

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::{built_binary, journal_events, loaded_text};
use sima_contracts::Generator;
use sima_core::{Error, Result, hash_bytes};
use sima_example_executor::DoublerGenerator;
use sima_pipeline::{
    BinaryChange, Engagement, Event, LoadedConfig, Record, RunControl, RunOutcome, migrate,
    orchestrate, task_keys,
};
use sima_store::Store;

/// A stub run, optionally routing its format to `program` and declaring the
/// variable names that program receives. Everything else is identical, so what
/// the two configs differ in is where the format is answered from.
fn stub_config(store: &str, program: Option<&Path>, env: &[&str]) -> String {
    let entry = program.map_or(String::new(), |binary| {
        let names: Vec<String> = env.iter().map(|name| format!("{name:?}")).collect();
        format!(
            "[domain.\"stub.v1\"]\nbinary = \"{}\"\nenv = [{}]\n",
            binary.display(),
            names.join(", ")
        )
    });
    format!(
        r#"
        [run]
        root_seed = 42
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed", "succeed", "flaky:1", "succeed"]

        [run.params]
        hex = "00ff"

        [config]
        store = "{store}"
        max_attempts = 2

        [orchestrator]
        workers = 2

        {entry}
    "#
    )
}

/// A search over the example program: `count` candidates, each one byte
/// doubled by the executor that program hosts.
fn example_config(store: &str, count: u32, program: &Path) -> String {
    format!(
        r#"
        [run]
        root_seed = 7
        format = "example.doubler.v1"

        [run.generator]
        id = "example.doubler.v1"
        count = {count}

        [config]
        store = "{store}"
        max_attempts = 2

        [orchestrator]
        workers = 2

        [domain."example.doubler.v1"]
        binary = "{}"
    "#,
        program.display()
    )
}

/// The candidate count a run cannot finish in the window between the commit
/// that raises the interrupt and the driver observing it. The observer runs on
/// the collector's thread, so a loaded machine can delay the flag past a short
/// run's last commit; this count makes the interrupting tests decide on the
/// ordering of events rather than on how fast this machine is.
const UNFINISHABLE: u32 = 200;

/// The task keys `config`'s run comprises, over a store of its own.
fn keys(config: &LoadedConfig) -> Result<Vec<String>> {
    let store = Store::open(&config.store)?;
    Ok(task_keys(config, &store)?
        .iter()
        .map(ToString::to_string)
        .collect())
}

/// The doubled bytes the run committed, in manifest order.
fn doubled(config: &LoadedConfig) -> Result<Vec<u8>> {
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
                .find(|artifact| artifact.name() == "doubled")
                .expect("the example commits the doubled artifact");
            Ok(store.get(artifact.object())?[0])
        })
        .collect()
}

/// The `sima-worker` binary, which serves the in-tree formats over the same
/// protocol a program outside the workspace does.
fn worker() -> PathBuf {
    built_binary("sima-worker")
}

/// The variable the wrapper script refuses to run with: a stand-in for a
/// credential the orchestrator holds in its own environment and no program
/// has a claim on.
const CANARY: &str = "SIMA_TEST_CANARY";

/// The variable the wrapper script requires: a stand-in for a setting a
/// program needs, which its config entry declares by name.
const DECLARED: &str = "ACME_ASSETS";

/// Writes an executable wrapper around the worker under `dir` and returns it.
///
/// The wrapper reports the spawn surface it was handed before becoming the
/// worker: it refuses the canary variable, requires the declared one, and
/// writes a file by relative path, which lands wherever its working directory
/// is.
fn spawn_surface_wrapper(dir: &Path) -> PathBuf {
    let path = dir.join("wrapper.sh");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\n\
             [ -z \"${CANARY}\" ] || exit 7\n\
             [ -n \"${DECLARED}\" ] || exit 8\n\
             touch ./canary\n\
             exec {} \"$@\"\n",
            worker().display()
        ),
    )
    .expect("write the wrapper");
    make_executable(&path);
    path
}

/// Writes an executable wrapper around `program` at `path`, carrying `comment`
/// in its own text: two wrappers around one program differ in their bytes, and
/// so in the digest sima records for them.
///
/// The file is replaced rather than rewritten, so the wrapper a winding-down
/// process still holds open is a different file from the one written here.
fn program_wrapper(path: &Path, program: &Path, comment: &str) {
    let _ = std::fs::remove_file(path);
    std::fs::write(
        path,
        format!(
            "#!/bin/sh\n# {comment}\nexec {} \"$@\"\n",
            program.display()
        ),
    )
    .expect("write the wrapper");
    make_executable(path);
}

/// Gives `path` the permissions a spawned program needs.
fn make_executable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .expect("make the wrapper executable");
    }
}

/// The digests the run's journal records its programs under, in append order.
fn bound_digests(config: &LoadedConfig) -> Vec<String> {
    journal_events(config)
        .into_iter()
        .filter_map(|event| match event {
            Event::ProgramBound { digest, .. } => Some(digest),
            _ => None,
        })
        .collect()
}

/// The blake3 digest of `path`'s bytes, as the journal renders it.
fn digest_of(path: &Path) -> String {
    hash_bytes(&std::fs::read(path).expect("read the program")).to_string()
}

#[test]
fn a_program_receives_the_spawn_surface_its_entry_declares() {
    // The spawn surface, observed by the program itself through a whole run:
    // the canary dropped, the declared name forwarded, the working directory
    // its own. The orchestrator runs as its own process here because that is
    // the only way its environment can hold what the child must and must not
    // receive.
    let dir = tempfile::tempdir().expect("temp dir");
    let wrapper = spawn_surface_wrapper(dir.path());
    std::fs::write(
        dir.path().join("sima.toml"),
        stub_config("./store", Some(&wrapper), &[DECLARED]),
    )
    .expect("write the config");
    let output = std::process::Command::new(built_binary("sima"))
        .args(["run", "sima.toml"])
        .current_dir(dir.path())
        .env(CANARY, "a credential the orchestrator holds")
        .env(DECLARED, "/opt/acme/assets")
        .output()
        .expect("run sima");
    // The run finalized, so both roles of the program ran past their two
    // checks: a spawn that carried the canary would have exited 7, and one
    // missing the declared name would have exited 8.
    assert!(
        output.status.success(),
        "the run finalized: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    // Both roles wrote `./canary` and neither wrote it here: the relative path
    // resolved inside a scratch directory that is gone with the process.
    assert!(
        !dir.path().join("canary").exists(),
        "a relative write by the program landed in the run's directory"
    );
}

#[test]
fn a_run_through_a_program_keeps_the_identity_it_has_by_direct_call() -> Result<()> {
    // The proof the protocol carries everything the parent needs: the params,
    // the generator's settings, the environment, and every spec the generator
    // produced all enter these hashes, so a field the protocol dropped would
    // change one of them.
    let dir = tempfile::tempdir().expect("temp dir");
    let direct = loaded_text(
        dir.path(),
        "direct.toml",
        &stub_config("./direct", None, &[]),
    )?;
    let served = loaded_text(
        dir.path(),
        "served.toml",
        &stub_config("./served", Some(&worker()), &[]),
    )?;
    assert_eq!(
        direct.run.id(),
        served.run.id(),
        "the run id is the same whichever side answered"
    );
    assert_eq!(
        keys(&direct)?,
        keys(&served)?,
        "every task key is the same whichever side answered"
    );
    Ok(())
}

#[test]
fn a_run_through_a_program_commits_what_it_would_have_committed() -> Result<()> {
    // The identity holds through execution too: the same run driven both ways
    // finalizes over the same manifest entries.
    let dir = tempfile::tempdir().expect("temp dir");
    let direct = loaded_text(
        dir.path(),
        "direct.toml",
        &stub_config("./direct", None, &[]),
    )?;
    let served = loaded_text(
        dir.path(),
        "served.toml",
        &stub_config("./served", Some(&worker()), &[]),
    )?;
    for config in [&direct, &served] {
        let outcome = orchestrate(
            config,
            &RunControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse,
        )?;
        assert!(
            matches!(outcome, RunOutcome::Finalized { .. }),
            "{outcome:?}"
        );
    }
    let entries = |config: &LoadedConfig| -> Result<Vec<String>> {
        let store = Store::open(&config.store)?;
        Ok(store
            .manifest(&config.run.id())?
            .expect("a finalized manifest")
            .entries
            .iter()
            .map(|entry| entry.task.to_string())
            .collect())
    };
    assert_eq!(entries(&direct)?, entries(&served)?);
    Ok(())
}

#[test]
fn a_search_runs_end_to_end_through_a_program_of_its_own() -> Result<()> {
    // The whole spine over a format this workspace carries no code for:
    // generation, execution, and commitment all reach the example program, and
    // what the store holds is what its executor returned.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = built_binary("sima-example-executor");
    let config = loaded_text(
        dir.path(),
        "sima.toml",
        &example_config("./store", 4, &program),
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
    let store = Store::open(&config.store)?;
    let manifest = store
        .manifest(&config.run.id())?
        .expect("a finalized manifest");
    assert_eq!(manifest.entries.len(), 4, "one entry per candidate");
    // What the store holds is what the program's own executor returns: every
    // candidate the program's generator drew, doubled.
    let expected: Vec<u8> = DoublerGenerator::new()?
        .generate(7, &[4])?
        .iter()
        .map(|spec| spec.bytes[0].wrapping_mul(2))
        .collect();
    let mut committed = doubled(&config)?;
    let mut expected = expected;
    committed.sort_unstable();
    expected.sort_unstable();
    assert_eq!(committed, expected);
    Ok(())
}

#[test]
fn a_run_through_a_program_resumes_after_an_interruption() -> Result<()> {
    // The store is the only durable state, so an interrupted run continues
    // where it stopped — the program is spawned afresh and the tasks already
    // committed are not run again.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = built_binary("sima-example-executor");
    let text = example_config("./store", UNFINISHABLE, &program);
    let config = loaded_text(dir.path(), "sima.toml", &text)?;
    let interrupt = AtomicBool::new(false);
    let committed = AtomicUsize::new(0);
    let observer = |record: &Record| {
        if matches!(record.event, Event::Committed { .. })
            && committed.fetch_add(1, Ordering::Relaxed) + 1 >= 2
        {
            interrupt.store(true, Ordering::Relaxed);
        }
    };
    let outcome = orchestrate(
        &config,
        &RunControl {
            observer: &observer,
            interrupt: &interrupt,
            on_start: None,
        },
        Engagement::Orchestrator,
        BinaryChange::Refuse,
    )?;
    assert!(
        matches!(outcome, RunOutcome::Interrupted { .. }),
        "the run stopped partway: {outcome:?}"
    );
    // The same config again: the run resumes over the store it left.
    let resumed = loaded_text(dir.path(), "sima.toml", &text)?;
    assert!(matches!(
        orchestrate(
            &resumed,
            &RunControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(
        doubled(&resumed)?.len(),
        UNFINISHABLE as usize,
        "every candidate committed"
    );
    Ok(())
}

#[test]
fn a_run_through_a_program_journals_the_build_that_served_it() -> Result<()> {
    // Provenance the environment hash never sees: the run's identity is what
    // the program declares, and the journal says which build declared it.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded_text(
        dir.path(),
        "sima.toml",
        &stub_config("./store", Some(&worker()), &[]),
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
    assert_eq!(bound_digests(&config), [digest_of(&worker())]);
    // The run's identity is untouched by the record: the same config answered
    // in process produces the same run id.
    let direct = loaded_text(
        dir.path(),
        "direct.toml",
        &stub_config("./direct", None, &[]),
    )?;
    assert_eq!(config.run.id(), direct.run.id());
    Ok(())
}

/// Drives the example run through `wrapper` until two candidates commit, then
/// interrupts it — the state a resume gate is asked about.
fn interrupted_through(dir: &Path, text: &str) -> Result<()> {
    let config = loaded_text(dir, "sima.toml", text)?;
    let interrupt = AtomicBool::new(false);
    let committed = AtomicUsize::new(0);
    let observer = |record: &Record| {
        if matches!(record.event, Event::Committed { .. })
            && committed.fetch_add(1, Ordering::Relaxed) + 1 >= 2
        {
            interrupt.store(true, Ordering::Relaxed);
        }
    };
    let outcome = orchestrate(
        &config,
        &RunControl {
            observer: &observer,
            interrupt: &interrupt,
            on_start: None,
        },
        Engagement::Orchestrator,
        BinaryChange::Refuse,
    )?;
    assert!(
        matches!(outcome, RunOutcome::Interrupted { .. }),
        "the run stopped partway: {outcome:?}"
    );
    Ok(())
}

#[test]
fn a_resume_after_the_program_changed_refuses_and_names_both_builds() -> Result<()> {
    // The gate the milestone exists for: stored results and checkpoints came
    // from a build that is no longer on disk, and only the user can say
    // whether that matters.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = built_binary("sima-example-executor");
    let wrapper = dir.path().join("wrapper.sh");
    program_wrapper(&wrapper, &program, "the build that ran");
    let text = example_config("./store", UNFINISHABLE, &wrapper);
    let before = digest_of(&wrapper);
    interrupted_through(dir.path(), &text)?;

    program_wrapper(&wrapper, &program, "the build on disk now");
    let after = digest_of(&wrapper);
    let resumed = loaded_text(dir.path(), "sima.toml", &text)?;
    let Err(error) = orchestrate(
        &resumed,
        &RunControl::detached(),
        Engagement::Orchestrator,
        BinaryChange::Refuse,
    ) else {
        panic!("expected a changed program to stop the resume");
    };
    assert!(matches!(error, Error::Validation(_)), "{error:?}");
    let text = error.to_string();
    for named in [
        "example.doubler.v1",
        &wrapper.display().to_string(),
        &before,
        &after,
        "--accept-binary",
    ] {
        assert!(text.contains(named), "{named} is missing from {text}");
    }
    // The refused session drove nothing: the run still names the build that
    // did, and the store holds no manifest.
    assert_eq!(bound_digests(&resumed), [before]);
    let store = Store::open(&resumed.store)?;
    assert!(store.manifest(&resumed.run.id())?.is_none());
    Ok(())
}

#[test]
fn a_resume_that_accepts_the_change_runs_and_binds_the_new_build() -> Result<()> {
    // Accepting is a decision about this run: the changed build finishes it,
    // and becomes what the next resume compares against.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = built_binary("sima-example-executor");
    let wrapper = dir.path().join("wrapper.sh");
    program_wrapper(&wrapper, &program, "the build that ran");
    let text = example_config("./store", UNFINISHABLE, &wrapper);
    let before = digest_of(&wrapper);
    interrupted_through(dir.path(), &text)?;

    program_wrapper(&wrapper, &program, "the build on disk now");
    let after = digest_of(&wrapper);
    let accepted = loaded_text(dir.path(), "sima.toml", &text)?;
    assert!(matches!(
        orchestrate(
            &accepted,
            &RunControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Accept
        )?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(
        doubled(&accepted)?.len(),
        UNFINISHABLE as usize,
        "every candidate committed"
    );
    assert_eq!(bound_digests(&accepted), [before.clone(), after.clone()]);

    // A further session over the accepted build passes the gate on its own:
    // the comparison is against the build that actually ran.
    let again = loaded_text(dir.path(), "sima.toml", &text)?;
    assert!(matches!(
        orchestrate(
            &again,
            &RunControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(bound_digests(&again), [before, after.clone(), after]);
    Ok(())
}

#[test]
fn a_resume_over_an_unchanged_program_passes_the_gate() -> Result<()> {
    // The refusing default is the every-run case, so an unchanged program
    // resumes with the flag absent.
    let dir = tempfile::tempdir().expect("temp dir");
    let program = built_binary("sima-example-executor");
    let wrapper = dir.path().join("wrapper.sh");
    program_wrapper(&wrapper, &program, "the only build");
    let text = example_config("./store", UNFINISHABLE, &wrapper);
    let digest = digest_of(&wrapper);
    interrupted_through(dir.path(), &text)?;

    let resumed = loaded_text(dir.path(), "sima.toml", &text)?;
    assert!(matches!(
        orchestrate(
            &resumed,
            &RunControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(
        doubled(&resumed)?.len(),
        UNFINISHABLE as usize,
        "every candidate committed"
    );
    assert_eq!(bound_digests(&resumed), [digest.clone(), digest]);
    Ok(())
}

#[test]
fn a_migration_of_a_program_that_states_no_payload_is_refused_where_it_is_asked_for() -> Result<()>
{
    // A migration carries the program to the destination as objects, so the
    // entry has to name what travels. An entry that names none is a program
    // this machine holds and no other, and the refusal states that before
    // anything moves.
    //
    // This config names no destination either, and the error names the program
    // rather than the missing host: the guard runs ahead of the destination,
    // the store, the lock, and any provider.
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("sima.toml");
    std::fs::write(&path, stub_config("./store", Some(&worker()), &[])).expect("write the config");
    let interrupt = AtomicBool::new(false);
    // Progress reporting is a side effect like any other, so the observer
    // counts: the guard runs ahead of the collector that would feed it.
    let observed = AtomicUsize::new(0);
    let observer = |_: &Record| {
        observed.fetch_add(1, Ordering::Relaxed);
    };
    let loaded = sima_pipeline::load(&path).expect("the config loads");
    let Err(error) = migrate(&path, &loaded, &observer, &interrupt, BinaryChange::Refuse) else {
        panic!("expected a program that states no payload to be refused a migration");
    };
    assert!(matches!(error, Error::Validation(_)), "{error:?}");
    let text = error.to_string();
    for named in ["stub.v1", &worker().display().to_string(), "payload"] {
        assert!(text.contains(named), "{named} is missing from {text}");
    }
    assert!(
        !dir.path().join("store").exists(),
        "the refused migration opened a store"
    );
    assert_eq!(
        observed.load(Ordering::Relaxed),
        0,
        "the refused migration reported progress"
    );
    Ok(())
}
