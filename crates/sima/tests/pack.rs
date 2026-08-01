//! CLI acceptance for `sima pack`: consolidating a store built by a real
//! run, and sweeping it with `--gc`, against the built binary.
//!
//! The verb takes a store directory and nothing else, so these tests point
//! it straight at the store a run wrote and then read that run back through
//! the ordinary query commands — which is the claim: how the store holds an
//! object is invisible to everything above it.

mod common;

use std::path::{Path, PathBuf};
use std::process::Output;

use common::{manifest_of, pack_files, sima_command};
use sima_pipeline::load;

/// Writes a `sima.toml` under `dir` whose store lives beside it.
fn write_config(dir: &Path, behaviors: &str) -> PathBuf {
    common::write_config(dir, "sima.toml", behaviors, "./store")
}

/// Runs the sima binary with `args`, capturing output.
fn sima(args: &[&str]) -> Output {
    sima_command().args(args).output().expect("spawn sima")
}

/// The stdout of `output`, as UTF-8.
fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

/// The stderr of `output`, as UTF-8.
fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr is UTF-8")
}

/// The loose object files under a store, recursively.
fn loose_object_count(store: &Path) -> usize {
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

/// A finalized run over `behaviors`, returning the temp dir, the config
/// path, and the store directory.
fn finalized_run(behaviors: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), behaviors);
    let output = sima(&["run", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let store = dir.path().join("store");
    (dir, config, store)
}

#[test]
fn pack_consolidates_a_store_a_run_wrote_and_the_run_still_reads() {
    let (_dir, config, store) = finalized_run(r#""succeed", "succeed", "succeed""#);
    let before = loose_object_count(&store);
    assert!(before > 0, "the run wrote loose objects");
    let manifest = manifest_of(&config).expect("the run finalized");

    let output = sima(&["pack", store.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(
        text.contains("packed"),
        "the report names what it did: {text}"
    );
    assert!(
        text.contains(&before.to_string()),
        "the report counts the objects: {text}"
    );
    assert_eq!(loose_object_count(&store), 0, "no loose object survives");
    assert_eq!(pack_files(&store).len(), 1, "one pack holds them");

    // The run reads back through the ordinary query commands: the store's
    // shape is invisible above it.
    let path = config.to_str().expect("utf-8 path");
    let report = sima(&["report", path]);
    assert_eq!(report.status.code(), Some(0), "{report:?}");
    let status = sima(&["status", path]);
    assert_eq!(status.status.code(), Some(0), "{status:?}");
    assert!(stdout(&status).contains("finalized"), "{status:?}");
    assert_eq!(manifest_of(&config).expect("manifest"), manifest);
}

#[test]
fn a_second_pack_reports_nothing_to_do() {
    let (_dir, _config, store) = finalized_run(r#""succeed", "succeed""#);
    let path = store.to_str().expect("utf-8 path");
    sima(&["pack", path]);
    let before = pack_files(&store);

    let output = sima(&["pack", path]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(
        stdout(&output).contains("packed 0 objects"),
        "the second run says it did nothing: {}",
        stdout(&output)
    );
    assert_eq!(pack_files(&store), before);
}

#[test]
fn pack_with_gc_keeps_a_finalized_run_whole() {
    let (_dir, config, store) = finalized_run(r#""succeed", "succeed""#);
    let manifest = manifest_of(&config).expect("the run finalized");
    let path = store.to_str().expect("utf-8 path");

    let output = sima(&["pack", path, "--gc"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(
        text.contains("gc:"),
        "the sweep reports its own line: {text}"
    );

    assert_eq!(manifest_of(&config).expect("manifest"), manifest);
    let report = sima(&["report", config.to_str().expect("utf-8 path")]);
    assert_eq!(report.status.code(), Some(0), "{report:?}");
    // The run's closure survived, so nothing it references was swept.
    let loaded = load(&config).expect("load config");
    let opened = sima_store::Store::open(&loaded.store).expect("open store");
    opened
        .run_closure(&loaded.run.id())
        .expect("the closure enumerates whole");
}

#[test]
fn pack_on_a_directory_that_is_not_a_store_fails() {
    let dir = tempfile::tempdir().expect("temp dir");
    let blocker = dir.path().join("a-file");
    std::fs::write(&blocker, b"not a store").expect("write blocker");
    let output = sima(&["pack", blocker.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(stderr(&output).contains("sima:"), "{output:?}");
}

#[test]
fn pack_refuses_a_remote_target_and_an_unknown_flag() {
    let (_dir, _config, store) = finalized_run(r#""succeed""#);
    let path = store.to_str().expect("utf-8 path");
    // The verb reshapes a store, so it never observes one over ssh; and it
    // takes no flag but --gc.
    for args in [
        vec!["pack", path, "--on", "somewhere"],
        vec!["pack", path, "--all"],
        vec!["pack"],
    ] {
        let output = sima(&args);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        assert!(stderr(&output).contains("usage"), "{args:?}: {output:?}");
    }
}

#[test]
fn the_usage_text_names_both_forms_of_the_verb() {
    let output = sima(&["nonsense"]);
    let text = stderr(&output);
    assert!(text.contains("sima pack <store-dir>"), "{text}");
    assert!(text.contains("--gc"), "{text}");
}

#[test]
fn a_store_past_the_threshold_is_told_to_pack() {
    let (_dir, config, store) = finalized_run(r#""succeed""#);
    // The estimate scales one fan-out subdirectory by 256, so filling `00`
    // with 391 entries puts the store past the six-figure threshold without
    // writing six figures of files. The names are object-shaped, so every
    // walk over `objects/` still reads them as objects.
    let fanout = store.join("objects").join("00");
    std::fs::create_dir_all(&fanout).expect("create fan-out dir");
    for i in 0..391u32 {
        std::fs::write(fanout.join(format!("00{i:062x}")), b"").expect("write object");
    }
    // Every verb of main.rs's STORE_OPENING_VERBS, `rm` last: it deletes
    // the run, so the others must observe the store first. The warning
    // prints before the verb dispatches, so only stderr is asserted on.
    for verb in ["status", "report", "run", "migrate", "rm"] {
        let output = sima(&[verb, config.to_str().expect("utf-8 path")]);
        let text = stderr(&output);
        assert!(
            text.contains("loose objects") && text.contains("sima pack"),
            "{verb} recommends packing: {text}"
        );
    }
}

#[test]
fn a_small_store_prints_no_loose_object_warning() {
    let (_dir, config, _store) = finalized_run(r#""succeed""#);
    // The warning fires only past the threshold; a store this size is far
    // under it, and every store-opening verb stays silent — `rm` last,
    // since it deletes the run the others observe.
    for verb in ["status", "report", "run", "migrate", "rm"] {
        let output = sima(&[verb, config.to_str().expect("utf-8 path")]);
        assert!(
            !stderr(&output).contains("loose objects"),
            "{verb} warned about a small store: {}",
            stderr(&output)
        );
    }
}
