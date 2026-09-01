//! Acceptance criteria at the pipeline API level, one test per criterion:
//!
//! - (a) determinism — one config into two fresh stores yields identical
//!   manifests;
//! - (c) re-evaluation — orchestrating a finalized search touches no executor;
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
use sima_pipeline::{
    BinaryChange, Engagement, Event, Record, SearchControl, SearchOutcome, orchestrate,
};
use sima_store::Store;

/// A behavior mix covering retry and timing variance alongside plain
/// successes.
const BEHAVIORS: &str = r#""succeed", "flaky:2", "succeed", "sleep:10""#;

#[test]
fn a_determinism_one_config_two_fresh_stores_identical_manifests() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let first = loaded_with(dir.path(), "first.toml", BEHAVIORS, 2, "./store-first")?;
    let second = loaded_with(dir.path(), "second.toml", BEHAVIORS, 2, "./store-second")?;
    assert_eq!(
        first.search.id(),
        second.search.id(),
        "one config, one search id"
    );

    for config in [&first, &second] {
        assert!(matches!(
            orchestrate(
                config,
                &SearchControl::detached(),
                Engagement::Orchestrator,
                BinaryChange::Refuse
            )?,
            SearchOutcome::Finalized { .. }
        ));
    }

    let search = first.search.id();
    assert_eq!(
        Store::open(&first.store)?
            .manifest(&search)?
            .expect("first"),
        Store::open(&second.store)?
            .manifest(&search)?
            .expect("second"),
    );
    Ok(())
}

#[test]
fn c_re_evaluation_of_a_finalized_search_touches_no_executor() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), BEHAVIORS, 2)?;
    let search = config.search.id();

    assert!(matches!(
        orchestrate(
            &config,
            &SearchControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
    ));
    let store = Store::open(&config.store)?;
    let manifest = store.manifest(&search)?.expect("finalized manifest");
    let journal_len = journal_events(&config).len();

    assert!(matches!(
        orchestrate(
            &config,
            &SearchControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
    ));
    assert_eq!(
        store.manifest(&search)?.expect("re-finalized manifest"),
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
    // Sleeps hold work in flight so the interrupt lands mid-search and the
    // copy carries a genuinely partial store.
    let behaviors = r#""succeed", "sleep:200", "sleep:200", "sleep:200""#;
    let config = loaded_with(dir.path(), "origin.toml", behaviors, 2, "./store-origin")?;
    let search = config.search.id();

    let interrupt = AtomicBool::new(false);
    let control = SearchControl {
        observer: &|record: &Record| {
            if matches!(record.event, Event::Committed { .. }) {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
        on_start: None,
    };
    assert!(matches!(
        orchestrate(
            &config,
            &control,
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Interrupted { .. }
    ));

    // Copy the partial store elsewhere and resume there, with a different
    // worker count — execution settings stay outside search identity.
    copy_tree(&config.store, &dir.path().join("store-copied"));
    let moved = loaded_with(dir.path(), "moved.toml", behaviors, 4, "./store-copied")?;
    assert_eq!(
        moved.search.id(),
        search,
        "the copy answers the same search"
    );
    assert!(matches!(
        orchestrate(
            &moved,
            &SearchControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
    ));

    // The reference: the same config search uninterrupted in a fresh store.
    let reference = loaded_with(
        dir.path(),
        "reference.toml",
        behaviors,
        2,
        "./store-reference",
    )?;
    assert!(matches!(
        orchestrate(
            &reference,
            &SearchControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse,
        )?,
        SearchOutcome::Finalized { .. }
    ));
    assert_eq!(
        Store::open(&moved.store)?
            .manifest(&search)?
            .expect("moved"),
        Store::open(&reference.store)?
            .manifest(&search)?
            .expect("reference"),
    );
    Ok(())
}
