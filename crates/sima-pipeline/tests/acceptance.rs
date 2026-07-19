//! Acceptance criteria at the pipeline API level, one test per criterion:
//!
//! - (a) determinism — one config into two fresh stores yields identical
//!   manifests;
//! - (c) re-evaluation — orchestrating a finalized run touches no executor;
//! - (d) portability — a copied store resumes elsewhere to the identical
//!   manifest.
//!
//! Criterion (b) — SIGKILL at any crashpoint, then resume — lives in the
//! `sima` crate's crash harness, which spawns the real binary.

mod common;

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{journal_events, loaded, loaded_with};
use sima_core::Result;
use sima_pipeline::{Event, Record, RunControl, RunOutcome, RunState, orchestrate, status};
use sima_store::Store;

/// Journal lines copied from a real stub run, in the format written before
/// `ts_ms` existed: what every journal already on disk holds. One task
/// retried once; all three committed; the run finalized.
const OLD_FORMAT_LINES: &[&str] = &[
    r#"{"event":"run_started","run":"df27656c67e534f3d6de64173da73efae9e41809734a5c0b647fffa452da920b","tasks":3,"committed":0}"#,
    r#"{"event":"queued","task":"c543cde6cbedd1edb2d3b323fd31b269682e8c75a206eb0ff2557bcae7f31ea8"}"#,
    r#"{"event":"queued","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a"}"#,
    r#"{"event":"queued","task":"b10a30f53cf23913eb37f79c71851587719df963e803f5967765070f3981d625"}"#,
    r#"{"event":"worker_bound","worker":0,"device":"","driver":"","host":""}"#,
    r#"{"event":"leased","task":"c543cde6cbedd1edb2d3b323fd31b269682e8c75a206eb0ff2557bcae7f31ea8","worker":0,"attempt":0}"#,
    r#"{"event":"worker_bound","worker":1,"device":"","driver":"","host":""}"#,
    r#"{"event":"leased","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","worker":1,"attempt":0}"#,
    r#"{"event":"failed","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","attempt":0,"reason":"programmed failure: attempt 0 of 1","stats_hex":"00000000"}"#,
    r#"{"event":"retried","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","next_attempt":1}"#,
    r#"{"event":"leased","task":"b10a30f53cf23913eb37f79c71851587719df963e803f5967765070f3981d625","worker":1,"attempt":0}"#,
    r#"{"event":"committed","task":"c543cde6cbedd1edb2d3b323fd31b269682e8c75a206eb0ff2557bcae7f31ea8","record":"62e29c69cbeb106a03499e64158fa6a83115eb0aacec5d69eb5617a4468956a7","stats_hex":"00000000"}"#,
    r#"{"event":"leased","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","worker":0,"attempt":1}"#,
    r#"{"event":"committed","task":"b10a30f53cf23913eb37f79c71851587719df963e803f5967765070f3981d625","record":"15a083e519d05e2dab09bd9a4e347b664bd9d8f0e0396ed94c98a1cd32acb9ac","stats_hex":"00000000"}"#,
    r#"{"event":"committed","task":"98099e55fa22dc94c02d9bd3ec732e7b27cb17503bb4da0613691f6c1480fc3a","record":"5087167e14e7f401b5724edeb5a7368b98cf2c972eca980bcf884857f9a55471","stats_hex":"01000000"}"#,
    r#"{"event":"run_finalized","run":"df27656c67e534f3d6de64173da73efae9e41809734a5c0b647fffa452da920b","committed":3}"#,
];

#[test]
fn a_pre_existing_format_journal_replays_through_status() -> Result<()> {
    // The compatibility criterion at the public surface: a journal whose
    // lines predate `ts_ms` replays through `status` to the state the run
    // reached when it wrote them.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed", "succeed", "succeed""#, 2)?;
    let store = Store::open(&config.store)?;
    store.create_run(&config.run)?;
    let mut writer = store.journal_writer(&config.run.id())?;
    for line in OLD_FORMAT_LINES {
        writer.append(line)?;
    }
    let replayed = status(&config)?;
    assert_eq!(replayed.tasks, 3);
    assert_eq!(replayed.committed, 3);
    assert_eq!(replayed.retried, 1);
    assert_eq!(replayed.rejected, 0);
    assert_eq!(replayed.faulted, 0);
    assert_eq!(replayed.state, RunState::Finalized);
    assert!(replayed.devices.is_empty());
    Ok(())
}

/// A behavior mix covering retry and timing variance alongside plain
/// successes.
const BEHAVIORS: &str = r#""succeed", "flaky:2", "succeed", "sleep:10""#;

#[test]
fn a_determinism_one_config_two_fresh_stores_identical_manifests() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let first = loaded_with(dir.path(), "first.toml", BEHAVIORS, 2, "./store-first")?;
    let second = loaded_with(dir.path(), "second.toml", BEHAVIORS, 2, "./store-second")?;
    assert_eq!(first.run.id(), second.run.id(), "one config, one run id");

    for config in [&first, &second] {
        assert!(matches!(
            orchestrate(config, &RunControl::detached())?,
            RunOutcome::Finalized { .. }
        ));
    }

    let run = first.run.id();
    assert_eq!(
        Store::open(&first.store)?.manifest(&run)?.expect("first"),
        Store::open(&second.store)?.manifest(&run)?.expect("second"),
    );
    Ok(())
}

#[test]
fn c_re_evaluation_of_a_finalized_run_touches_no_executor() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), BEHAVIORS, 2)?;
    let run = config.run.id();

    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let store = Store::open(&config.store)?;
    let manifest = store.manifest(&run)?.expect("finalized manifest");
    let journal_len = journal_events(&config).len();

    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(
        store.manifest(&run)?.expect("re-finalized manifest"),
        manifest,
        "the manifest is unchanged"
    );
    for event in &journal_events(&config)[journal_len..] {
        assert!(
            !matches!(
                event,
                Event::Queued { .. } | Event::Leased { .. } | Event::Committed { .. }
            ),
            "re-evaluation queued or executed work: {event:?}"
        );
    }
    Ok(())
}

/// Copies a directory tree, preserving the relative layout.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create copy target");
    for entry in std::fs::read_dir(from).expect("read source dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

#[test]
fn d_a_copied_store_resumes_elsewhere_to_the_identical_manifest() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    // Sleeps hold work in flight so the interrupt lands mid-run and the
    // copy carries a genuinely partial store.
    let behaviors = r#""succeed", "sleep:200", "sleep:200", "sleep:200""#;
    let config = loaded_with(dir.path(), "origin.toml", behaviors, 2, "./store-origin")?;
    let run = config.run.id();

    let interrupt = AtomicBool::new(false);
    let control = RunControl {
        observer: &|record: &Record| {
            if matches!(record.event, Event::Committed { .. }) {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
    };
    assert!(matches!(
        orchestrate(&config, &control)?,
        RunOutcome::Interrupted { .. }
    ));

    // Copy the partial store elsewhere and resume there, with a different
    // worker count — execution settings stay outside run identity.
    copy_tree(&config.store, &dir.path().join("store-copied"));
    let moved = loaded_with(dir.path(), "moved.toml", behaviors, 4, "./store-copied")?;
    assert_eq!(moved.run.id(), run, "the copy answers the same run");
    assert!(matches!(
        orchestrate(&moved, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));

    // The reference: the same config run uninterrupted in a fresh store.
    let reference = loaded_with(
        dir.path(),
        "reference.toml",
        behaviors,
        2,
        "./store-reference",
    )?;
    assert!(matches!(
        orchestrate(&reference, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(
        Store::open(&moved.store)?.manifest(&run)?.expect("moved"),
        Store::open(&reference.store)?
            .manifest(&run)?
            .expect("reference"),
    );
    Ok(())
}
