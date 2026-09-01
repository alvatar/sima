//! CLI acceptance for the retention policy's floor: what a finalized search
//! needs to be re-evaluated.
//!
//! Snapshots are the bulk of a store and the part an operator may want gone;
//! the record spine — config, task index, records, journal — is what
//! re-evaluation reads. This suite pins that division by taking a finalized
//! search's snapshot objects away and asking the binary for the search again.

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Output;

use common::{journal_events, manifest_bytes, sima_command};
use sima_core::Hash;
use sima_pipeline::{Event, load};
use sima_store::Store;

/// Runs the sima binary with `args`, capturing output.
fn sima(args: &[&str]) -> Output {
    sima_command().args(args).output().expect("spawn sima")
}

/// How many tasks the journal reports committed.
fn committed(config: &std::path::Path) -> usize {
    journal_events(config)
        .iter()
        .filter(|event| matches!(event, Event::Committed { .. }))
        .count()
}

/// Every artifact object the search's records reference, deduplicated — the
/// snapshots, since an executor's result state is committed as an artifact.
fn snapshots(config: &std::path::Path) -> BTreeSet<Hash> {
    let loaded = load(config).expect("load config");
    let store = Store::open(&loaded.store).expect("open store");
    let manifest = store
        .manifest(&loaded.search.id())
        .expect("read manifest")
        .expect("the search finalized");
    let mut objects = BTreeSet::new();
    for entry in &manifest.entries {
        let record = store
            .record(&entry.task)
            .expect("read record")
            .expect("a finalized search's tasks are committed");
        objects.extend(record.artifacts().iter().map(|artifact| *artifact.object()));
    }
    objects
}

#[test]
fn a_finalized_search_re_evaluates_after_its_snapshots_are_deleted() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = common::write_config(
        dir.path(),
        "sima.toml",
        r#""succeed", "succeed", "succeed""#,
        "./store",
    );
    let path = config.to_str().expect("utf-8 path");

    let output = sima(&["search", path]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let manifest = manifest_bytes(&config).expect("the search finalized");
    let commits = committed(&config);
    assert!(commits > 0, "the search committed its tasks");

    let objects = snapshots(&config);
    assert!(
        !objects.is_empty(),
        "the stub commits a state artifact per task"
    );

    // The snapshot objects are removed from the store's files directly. The
    // store offers no single-object delete — deletion is search-grained and
    // reference-guarded by design — so this models the floor the policy
    // claims, a store whose snapshots are gone, rather than inventing an API
    // for it. The search was never packed, so every object is loose at
    // `objects/<aa>/<hash>`.
    let store = load(&config).expect("load config").store;
    for object in &objects {
        let hex = object.to_string();
        let file: PathBuf = store.join("objects").join(&hex[..2]).join(&hex);
        assert!(file.is_file(), "the snapshot object is loose: {hex}");
        std::fs::remove_file(&file).expect("remove the snapshot object");
    }

    // The search is asked for again. The frontier re-derives from the task
    // index and the records, both untouched, so the search re-finalizes without
    // executing anything.
    let again = sima(&["search", path]);
    assert_eq!(again.status.code(), Some(0), "{again:?}");
    assert_eq!(
        committed(&config),
        commits,
        "re-evaluation commits nothing: no task ran again"
    );
    assert_eq!(
        manifest_bytes(&config).expect("the search stays finalized"),
        manifest,
        "the manifest is byte-identical"
    );

    // The query commands answer for the search too: neither reads a snapshot.
    let status = sima(&["status", path]);
    assert_eq!(status.status.code(), Some(0), "{status:?}");
    let text = String::from_utf8(status.stdout.clone()).expect("stdout is UTF-8");
    assert!(text.contains("finalized"), "{text}");
    let report = sima(&["report", path]);
    assert_eq!(report.status.code(), Some(0), "{report:?}");
}
