//! CLI acceptance: `sima run` and `sima status` end to end, spawning the
//! built binary against configs written into temp directories.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use sima_pipeline::load;
use sima_store::{Manifest, Store};

/// Writes a `sima.toml` under `dir` whose store lives beside it.
fn write_config(dir: &Path, behaviors: &str) -> PathBuf {
    let text = format!(
        r#"
        [run]
        root_seed = 11
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [execution]
        store = "./store"
        workers = 2
        max_attempts = 3
    "#
    );
    let path = dir.join("sima.toml");
    std::fs::write(&path, text).expect("write config");
    path
}

/// Runs the sima binary with `args`, capturing output.
fn sima(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_sima"))
        .args(args)
        .output()
        .expect("spawn sima")
}

/// The stdout of `output`, as UTF-8.
fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

/// The manifest of the run `config_path` describes, from its store.
fn manifest_of(config_path: &Path) -> Option<Manifest> {
    let config = load(config_path).expect("load config");
    let store = Store::open(&config.store).expect("open store");
    store.manifest(&config.run.id()).expect("read manifest")
}

#[test]
fn run_finalizes_a_succeeding_config_and_writes_the_manifest() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);

    let output = sima(&["run", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");

    let run_id = load(&config).expect("load config").run.id().to_string();
    let text = stdout(&output);
    assert!(text.contains(&run_id[..12]), "stdout names the run: {text}");
    assert!(text.contains("finalized"), "stdout reports the end: {text}");
    assert!(manifest_of(&config).is_some(), "the manifest exists");
}

#[test]
fn run_exits_2_on_a_definitive_failure_and_prints_the_reason() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "reject""#);

    let output = sima(&["run", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let text = stdout(&output);
    assert!(
        text.contains("programmed rejection"),
        "the reason is printed: {text}"
    );
    assert!(manifest_of(&config).is_none(), "no manifest on failure");
}

#[test]
fn a_second_run_over_the_same_store_re_evaluates_to_success() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    let output = sima(&["run", path]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(stdout(&output).contains("finalized"));
}

#[test]
fn status_before_any_run_exits_1_and_after_reports_the_counts() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed", "flaky:1""#);
    let path = config.to_str().expect("utf-8 path");

    let before = sima(&["status", path]);
    assert_eq!(before.status.code(), Some(1), "{before:?}");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    let after = sima(&["status", path]);
    assert_eq!(after.status.code(), Some(0), "{after:?}");
    let text = stdout(&after);
    assert!(text.contains("finalized"), "the state is reported: {text}");
    assert!(
        text.contains("committed") && text.contains('3'),
        "the committed count is reported: {text}"
    );
}

#[test]
fn status_on_a_missing_store_exits_1_and_creates_nothing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let output = sima(&["status", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    // A status query is read-only: no store may appear on disk.
    assert!(
        !dir.path().join("store").exists(),
        "sima status created the store"
    );
}

#[test]
fn a_malformed_config_exits_1() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("broken.toml");
    std::fs::write(&path, "run = [not toml").expect("write config");
    let output = sima(&["run", path.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
}

#[test]
fn an_unknown_subcommand_exits_1_with_usage_on_stderr() {
    for args in [vec!["frobnicate"], vec![], vec!["run"], vec!["status"]] {
        let output = sima(&args);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
        assert!(stderr.contains("usage"), "{args:?}: {stderr}");
    }
}

#[test]
fn sigint_interrupts_gracefully_and_a_rerun_matches_an_uninterrupted_store() {
    let dir = tempfile::tempdir().expect("temp dir");
    let behaviors = r#""sleep:1500", "sleep:1500", "sleep:1500", "sleep:1500""#;
    let config = write_config(dir.path(), behaviors);
    let path = config.to_str().expect("utf-8 path");

    let mut child = Command::new(env!("CARGO_BIN_EXE_sima"))
        .args(["run", path])
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn sima run");
    // Let the run get in flight, then interrupt it; the drain outlasts the
    // in-flight sleeps, so a prompt exit proves graceful wind-down.
    std::thread::sleep(std::time::Duration::from_millis(500));
    let kill = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("send SIGINT");
    assert!(kill.success());
    let status = child.wait().expect("wait for sima");
    assert_eq!(status.code(), Some(130), "graceful interrupt exits 130");
    assert!(manifest_of(&config).is_none(), "no manifest yet");

    // A clean re-run completes the abandoned work.
    let rerun = sima(&["run", path]);
    assert_eq!(rerun.status.code(), Some(0), "{rerun:?}");

    // The resumed store's manifest equals an uninterrupted reference run's.
    let reference_dir = tempfile::tempdir().expect("reference temp dir");
    let reference = write_config(reference_dir.path(), behaviors);
    assert_eq!(
        sima(&["run", reference.to_str().expect("utf-8 path")])
            .status
            .code(),
        Some(0)
    );
    assert_eq!(
        manifest_of(&config).expect("resumed manifest"),
        manifest_of(&reference).expect("reference manifest"),
    );
}
