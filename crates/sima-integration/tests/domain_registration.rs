//! End-to-end acceptance of a format served by its own program: the protocol
//! carries everything a run's identity is made of, and a program outside this
//! workspace runs a whole search through the same spine.
//!
//! The equivalence here is what proves the seam sufficient. A run driven
//! through a program produces the run id and the task keys the same run
//! produces by direct call, so anything the protocol failed to carry would
//! change a hash and fail these tests rather than pass silently.

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::{built_binary, loaded_text};
use sima_contracts::Generator;
use sima_core::Result;
use sima_example_executor::{FORMAT, Sampler};
use sima_pipeline::{
    Engagement, Event, LoadedConfig, Record, RunControl, RunOutcome, orchestrate, task_keys,
};
use sima_store::Store;

/// A stub run, optionally routing its format to `program`. Everything else is
/// identical, so what the two configs differ in is where the format is
/// answered from.
fn stub_config(store: &str, program: Option<&Path>) -> String {
    let entry = program.map_or(String::new(), |binary| {
        format!("[domain.\"stub.v1\"]\nbinary = \"{}\"\n", binary.display())
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

#[test]
fn a_run_through_a_program_keeps_the_identity_it_has_by_direct_call() -> Result<()> {
    // The proof the protocol carries everything the parent needs: the params,
    // the generator's settings, the environment, and every spec the generator
    // produced all enter these hashes, so a field the protocol dropped would
    // change one of them.
    let dir = tempfile::tempdir().expect("temp dir");
    let direct = loaded_text(dir.path(), "direct.toml", &stub_config("./direct", None))?;
    let served = loaded_text(
        dir.path(),
        "served.toml",
        &stub_config("./served", Some(&worker())),
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
    let direct = loaded_text(dir.path(), "direct.toml", &stub_config("./direct", None))?;
    let served = loaded_text(
        dir.path(),
        "served.toml",
        &stub_config("./served", Some(&worker())),
    )?;
    for config in [&direct, &served] {
        let outcome = orchestrate(config, &RunControl::detached(), Engagement::Orchestrator)?;
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
        orchestrate(&config, &RunControl::detached(), Engagement::Orchestrator)?,
        RunOutcome::Finalized { .. }
    ));
    let store = Store::open(&config.store)?;
    let manifest = store
        .manifest(&config.run.id())?
        .expect("a finalized manifest");
    assert_eq!(manifest.entries.len(), 4, "one entry per candidate");
    // What the store holds is what the program's own executor returns: every
    // candidate the program's generator drew, doubled.
    let expected: Vec<u8> = Sampler::new()?
        .generate(7, &[4], &sima_model::FormatId::new(FORMAT)?)?
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
    let text = example_config("./store", 4, &program);
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
    )?;
    assert!(
        matches!(outcome, RunOutcome::Interrupted { .. }),
        "the run stopped partway: {outcome:?}"
    );
    // The same config again: the run resumes over the store it left.
    let resumed = loaded_text(dir.path(), "sima.toml", &text)?;
    assert!(matches!(
        orchestrate(&resumed, &RunControl::detached(), Engagement::Orchestrator)?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(doubled(&resumed)?.len(), 4, "every candidate committed");
    Ok(())
}
