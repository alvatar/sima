//! CLI acceptance: `sima run` and `sima status` end to end, spawning the
//! built binary against configs written into temp directories.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use common::{manifest_of, sima_command};
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
fn report_after_a_run_prints_one_line_per_committed_task() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    let output = sima(&["report", path]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    // One line per committed task, each a stub `succeed` whose stats render the
    // attempt.
    assert_eq!(
        text.lines().count(),
        2,
        "one line per committed task: {text}"
    );
    assert!(
        text.lines().all(|line| line.contains("attempt 0")),
        "each line renders the stats: {text}"
    );
}

#[test]
fn a_rerun_of_a_finalized_run_reports_prior_commits() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    // The rerun re-derives an empty frontier: no task executes, and the
    // progress must say so instead of reading as a restart from zero.
    let rerun = sima(&["run", path]);
    assert_eq!(rerun.status.code(), Some(0), "{rerun:?}");
    let text = stdout(&rerun);
    assert!(
        text.contains("started: 2 tasks, 2 already committed"),
        "the started line names the prior commits: {text}"
    );
    assert!(
        !text.contains("committed 1/2"),
        "no line recounts an already-committed task: {text}"
    );
}

#[test]
fn report_before_any_run_exits_1() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let output = sima(&["report", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
}

#[test]
fn rm_removes_the_only_run_and_a_second_rm_fails_cleanly() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    let rm = sima(&["rm", path]);
    assert_eq!(rm.status.code(), Some(0), "{rm:?}");
    assert!(
        stdout(&rm).contains("removed run"),
        "prints the report: {}",
        stdout(&rm)
    );

    // The run is gone: status fails, and the objects directory holds no files.
    assert_eq!(sima(&["status", path]).status.code(), Some(1));
    let objects = dir.path().join("store").join("objects");
    let object_files: usize = std::fs::read_dir(&objects)
        .expect("read objects dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| std::fs::read_dir(e.path()).expect("read fan-out").count())
        .sum();
    assert_eq!(object_files, 0, "no object files survive the removal");

    // A second rm fails cleanly rather than panicking.
    assert_eq!(sima(&["rm", path]).status.code(), Some(1));
}

#[test]
fn rm_before_any_run_exits_1() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let output = sima(&["rm", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
}

#[test]
fn a_second_rm_reports_run_not_found_and_leaves_no_run_dir() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    assert_eq!(sima(&["rm", path]).status.code(), Some(0));

    let second = sima(&["rm", path]);
    assert_eq!(second.status.code(), Some(1), "{second:?}");
    let stderr = String::from_utf8(second.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("run not found"),
        "the second rm names the absent run: {stderr}"
    );
    // The failed rm mutated nothing: no ghost run directory survives.
    let runs = dir.path().join("store").join("runs");
    let run_dirs = std::fs::read_dir(&runs).expect("read runs dir").count();
    assert_eq!(run_dirs, 0, "a failed rm left a ghost run directory");
}

#[test]
fn rm_on_a_missing_store_exits_1_and_creates_nothing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let output = sima(&["rm", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    // An rm against a store that does not exist is a query that fails: no
    // store may appear on disk, mirroring the status contract.
    assert!(
        !dir.path().join("store").exists(),
        "sima rm created the store"
    );
}

#[test]
fn a_failed_second_rm_does_not_block_removing_another_run() {
    let dir = tempfile::tempdir().expect("temp dir");
    // Two runs sharing one store: different behaviors give distinct run ids.
    let config_a = common::write_config(dir.path(), "a.toml", r#""succeed""#, "./store");
    let config_b = common::write_config(dir.path(), "b.toml", r#""succeed", "succeed""#, "./store");
    let a = config_a.to_str().expect("utf-8 path");
    let b = config_b.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", a]).status.code(), Some(0));
    assert_eq!(sima(&["run", b]).status.code(), Some(0));

    // Remove A, then attempt A again: the second attempt must fail without
    // leaving an unfinalized ghost that would make B unremovable.
    assert_eq!(sima(&["rm", a]).status.code(), Some(0));
    assert_eq!(sima(&["rm", a]).status.code(), Some(1));

    let rm_b = sima(&["rm", b]);
    assert_eq!(rm_b.status.code(), Some(0), "{rm_b:?}");
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
fn an_extensionless_config_argument_resolves_to_the_toml_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_config(dir.path(), r#""succeed""#);
    let bare = dir.path().join("sima");
    let path = bare.to_str().expect("utf-8 path");

    let run = sima(&["run", path]);
    assert_eq!(run.status.code(), Some(0), "{run:?}");
    let status = sima(&["status", path]);
    assert_eq!(status.status.code(), Some(0), "{status:?}");
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
    for args in [
        vec!["frobnicate"],
        vec![],
        vec!["run"],
        vec!["status"],
        vec!["report"],
        vec!["rm"],
    ] {
        let output = sima(&args);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
        assert!(stderr.contains("usage"), "{args:?}: {stderr}");
    }
}

#[test]
fn the_usage_text_names_the_tui_subcommand() {
    let output = sima(&[]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("sima tui"), "usage names tui: {stderr}");
    assert!(
        stderr.contains("sima report"),
        "usage names report: {stderr}"
    );
    assert!(stderr.contains("sima rm"), "usage names rm: {stderr}");
}

#[test]
fn tui_without_a_terminal_exits_1_and_names_the_requirement() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    // The test harness captures stdout, so it is not a TTY: the tui
    // subcommand must refuse rather than drive a terminal it has not got.
    let output = sima(&["tui", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("requires a terminal"),
        "names the terminal requirement: {stderr}"
    );
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
