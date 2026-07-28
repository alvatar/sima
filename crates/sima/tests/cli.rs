//! CLI acceptance: `sima run` and `sima status` end to end, spawning the
//! built binary against configs written into temp directories.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use common::{manifest_of, sima_command, worker_processes};
use sima_pipeline::{Event, RunObserver, load};
use sima_store::{IncidentKind, InstanceRecord, InstanceRecordState, MachineIncident, Store};

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

/// The run path executes tasks in worker subprocesses: while sleep tasks
/// run, `sima-worker` children of the run process are visible in the
/// process table.
#[test]
fn run_executes_tasks_in_worker_subprocesses() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""sleep:3000", "sleep:3000""#);
    let mut child = sima_command()
        .args(["run", config.to_str().expect("utf-8 path")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn sima");
    // Poll the process table to a deadline; the sleeps keep the workers
    // alive far longer than the poll needs.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = false;
    while Instant::now() < deadline {
        if !worker_processes(child.id()).is_empty() {
            seen = true;
            break;
        }
        if child.try_wait().expect("probe the run").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        seen,
        "no sima-worker child of the run process appeared in the process table"
    );
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
fn report_timeline_reports_the_throughput_utilization_and_temporal_shape_of_a_run() {
    let dir = tempfile::tempdir().expect("temp dir");
    // Tasks that occupy their worker for a measurable span, so the utilization
    // figures and the occupancy bars have something to draw.
    let config = write_config(
        dir.path(),
        r#""sleep:200", "sleep:200", "sleep:200", "sleep:200""#,
    );
    let path = config.to_str().expect("utf-8 path");

    let before = sima(&["report", path, "--timeline"]);
    assert_eq!(before.status.code(), Some(1), "{before:?}");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    let output = sima(&["report", path, "--timeline"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    for field in [
        "run",
        "wall-clock",
        "committed",
        "throughput",
        "task/s",
        "retries / tasks",
        "tasks retried / tasks",
        "failed attempts / attempts",
        "each column spans",
        "commits",
    ] {
        assert!(text.contains(field), "the report states {field}: {text}");
    }
    // The per-worker table, and an occupancy bar for every worker in it.
    let squeezed = text.split_whitespace().collect::<Vec<&str>>().join(" ");
    assert!(
        squeezed.contains("worker device host spawn respawns util commits attempts"),
        "{text}"
    );
    for worker in ["w0", "w1"] {
        assert!(
            text.lines().filter(|line| line.contains(worker)).count() >= 2,
            "{worker} has a table row and a bar: {text}"
        );
    }
}

#[test]
fn report_timeline_over_a_local_run_names_no_host_and_no_device() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // Local workers bind as the pool launches and the stub domain names no
    // device, so both columns state their placeholder, the spawn latency is a
    // fraction of a second, and no worker respawned.
    let text = stdout(&sima(&["report", path, "--timeline"]));
    for worker in ["w0", "w1"] {
        let row = worker_row(&text, worker);
        assert_eq!(row[1], "(none)", "the stub domain names no device: {text}");
        assert_eq!(row[2], "—", "a local worker names no host: {text}");
        let spawn: f64 = row[3]
            .trim_end_matches('s')
            .parse()
            .unwrap_or_else(|_| panic!("{worker} states a spawn latency in seconds: {text}"));
        assert!(spawn < 1.0, "{worker} bound as the pool launched: {text}");
        assert_eq!(row[4], "0", "{worker} did not respawn: {text}");
    }
}

/// The cells of `worker`'s row in a rendered timeline's per-worker table. The
/// table's rows are the lines carrying one cell per column, which tells them
/// from the worker's occupancy bar further down.
fn worker_row(text: &str, worker: &str) -> Vec<String> {
    text.lines()
        .map(|line| {
            line.split_whitespace()
                .map(str::to_string)
                .collect::<Vec<String>>()
        })
        .find(|cells| cells.len() == 8 && cells[0] == worker)
        .unwrap_or_else(|| panic!("a table row for {worker}: {text}"))
}

#[test]
fn report_timeline_over_a_failed_run_answers_and_exits_0() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "reject""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(2));

    // The query reports what the run did, whatever the run's own outcome was.
    let output = sima(&["report", path, "--timeline"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains("throughput"), "{text}");
    // The rejection is the attempt the run wasted. How many attempts it took
    // in total is a race with the definitive failure that ends the run, so the
    // assertion names the numerator alone.
    let squeezed = text.split_whitespace().collect::<Vec<&str>>().join(" ");
    assert!(
        squeezed.contains("failed attempts / attempts 1 /"),
        "{text}"
    );
}

/// The key of the run's task that ended on `outcome`, from its journal.
fn task_ending_in(config: &Path, outcome: fn(&Event) -> Option<&String>) -> String {
    common::journal_events(config)
        .iter()
        .find_map(outcome)
        .expect("a task with that outcome")
        .clone()
}

/// The key of the first task the run committed. Commit order across workers is
/// a race, so callers use a config in which exactly one task commits.
fn committed_task(config: &Path) -> String {
    task_ending_in(config, |event| match event {
        Event::Committed { task, .. } => Some(task),
        _ => None,
    })
}

/// The key of a task the run rejected.
fn rejected_task(config: &Path) -> String {
    task_ending_in(config, |event| match event {
        Event::Rejected { task, .. } => Some(task),
        _ => None,
    })
}

#[test]
fn status_task_prints_every_attempt_of_a_retried_task() {
    let dir = tempfile::tempdir().expect("temp dir");
    // The flaky task fails once and commits on its second attempt: its
    // timeline names both attempts and the outcome each reached.
    let config = write_config(dir.path(), r#""succeed", "flaky:1""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    let flaky = task_ending_in(&config, |event| match event {
        Event::Failed { task, .. } => Some(task),
        _ => None,
    });
    let output = sima(&["status", path, "--task", &flaky[..8]]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains(&flaky[..12]), "names the task: {text}");
    assert!(text.contains("committed"), "the terminal outcome: {text}");
    assert!(text.contains("failed"), "the first attempt failed: {text}");
    assert!(text.contains("elapsed"), "the elapsed column: {text}");
    // One row per attempt: the rows are the lines an attempt number opens.
    let rows = text
        .lines()
        .filter(|line| {
            line.split_whitespace()
                .next()
                .is_some_and(|first| first.parse::<u32>().is_ok())
        })
        .count();
    assert_eq!(rows, 2, "one row per attempt: {text}");
}

#[test]
fn status_task_reports_a_rejected_task_and_its_reason() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "reject""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(2));

    let rejected = rejected_task(&config);
    let output = sima(&["status", path, "--task", &rejected[..8]]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains("rejected"), "the terminal outcome: {text}");
    assert!(text.contains("programmed rejection"), "the reason: {text}");
}

#[test]
fn status_task_rejects_an_ambiguous_or_unmatched_prefix() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // The empty prefix matches every task the run journaled.
    let ambiguous = sima(&["status", path, "--task", ""]);
    assert_eq!(ambiguous.status.code(), Some(1), "{ambiguous:?}");
    let stderr = String::from_utf8(ambiguous.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("ambiguous"), "{stderr}");

    let unmatched = sima(&["status", path, "--task", "ffffffffff"]);
    assert_eq!(unmatched.status.code(), Some(1), "{unmatched:?}");
    let stderr = String::from_utf8(unmatched.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("no task"), "{stderr}");
}

#[test]
fn status_failed_names_the_tasks_a_failed_run_did_not_commit() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "reject""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(2));

    // The run failed; the query over its journal answers, so it exits 0.
    let output = sima(&["status", path, "--failed"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains("1 task did not commit"), "{text}");
    let rejected = rejected_task(&config);
    assert!(text.contains(&rejected[..12]), "names the task: {text}");
    assert!(text.contains("rejected"), "names the outcome: {text}");
    assert!(text.contains("programmed rejection"), "the reason: {text}");
}

#[test]
fn status_failed_over_an_all_committed_run_names_no_task() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "flaky:1""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    let output = sima(&["status", path, "--failed"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains("0 tasks did not commit"), "{text}");
    // A retried task committed in the end, so no row names it.
    assert_eq!(text.lines().count(), 1, "the header alone: {text}");
}

#[test]
fn report_defaults_to_the_compact_summary() {
    let dir = tempfile::tempdir().expect("temp dir");
    // Two clean successes and one task that succeeds on its second attempt:
    // two distinct rendered stats values, with a count each.
    let config = write_config(dir.path(), r#""succeed", "succeed", "flaky:1""#);
    let path = config.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    let output = sima(&["report", path]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    // A total header, then one line per distinct stats value with its count,
    // ordered by count descending.
    assert_eq!(
        stdout(&output),
        "3 committed tasks\n2  attempt=0 blob=4B\n1  attempt=1 blob=4B\n"
    );
}

#[test]
fn report_all_prints_one_line_per_committed_task() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", path]).status.code(), Some(0));
    let output = sima(&["report", path, "--all"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    // One line per committed task — `<short task key>  <rendered stats>`, each
    // a stub `succeed` whose stats render the attempt.
    assert_eq!(
        text.lines().count(),
        2,
        "one line per committed task: {text}"
    );
    for line in text.lines() {
        let (key, stats) = line.split_once("  ").expect("key and stats");
        assert_eq!(key.len(), 12, "the short task key: {line}");
        assert!(
            key.chars().all(|c| c.is_ascii_hexdigit()),
            "the key is hex: {line}"
        );
        assert_eq!(stats, "attempt=0 blob=4B", "the rendered stats: {line}");
    }
}

#[test]
fn report_task_prints_one_committed_task_s_stats() {
    let dir = tempfile::tempdir().expect("temp dir");
    // Exactly one task commits, so the task the report addresses and the
    // attempt it committed on are both fixed regardless of worker ordering.
    let config = write_config(dir.path(), r#""succeed", "reject""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(2));

    let task = committed_task(&config);
    let output = sima(&["report", path, "--task", &task[..8]]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        stdout(&output),
        format!("{}  attempt=0 blob=4B\n", &task[..12])
    );
}

#[test]
fn report_task_over_a_task_that_never_committed_exits_1() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "reject""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(2));

    let rejected = rejected_task(&config);
    let output = sima(&["report", path, "--task", &rejected[..8]]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("has no committed result"), "{stderr}");
}

#[test]
fn report_full_is_no_longer_a_command() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    for args in [
        vec!["report", "--full", path],
        vec!["report", path, "--full"],
    ] {
        let output = sima(&args);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
        assert!(stderr.contains("usage"), "{args:?}: {stderr}");
    }
}

#[test]
fn report_spend_reports_the_rental_ledger() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // A local run rents no hardware, so the ledger is empty; the view still
    // renders its three sections, unchanged from the removed `spend` command.
    let output = sima(&["report", path, "--spend"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(
        text.contains("closed rentals"),
        "the ledger sections: {text}"
    );
    assert!(text.contains("open rentals"), "the ledger sections: {text}");
    assert!(text.contains("total"), "the ledger total: {text}");
}

#[test]
fn the_removed_top_level_timeline_and_spend_commands_report_usage() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // The reporting views moved under `report`; the top-level verbs are gone.
    for args in [vec!["timeline", path], vec!["spend", path]] {
        let output = sima(&args);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
        assert!(stderr.contains("usage"), "{args:?}: {stderr}");
    }
}

#[test]
fn report_machines_over_a_clean_store_reports_no_incidents() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // A local run rents no hardware and records no incident; the view still
    // answers, with its explicit no-incidents line.
    let output = sima(&["report", path, "--machines"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(
        stdout(&output).contains("no machine incidents recorded"),
        "the empty-ledger line: {}",
        stdout(&output)
    );
}

#[test]
fn report_machines_names_the_machine_its_count_and_its_blacklist_status() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // Plant two incidents against one machine directly in the store, as the
    // recording sites would; two strikes blacklist it.
    let loaded = load(&config).expect("load config");
    let store = Store::open(&loaded.store).expect("open the store");
    for tag in ["sima-inc-0", "sima-inc-1"] {
        store
            .put_machine_incident(&MachineIncident {
                provider: "vastai".to_string(),
                machine: "81234".to_string(),
                kind: IncidentKind::Lost,
                tag: tag.to_string(),
                occurred_ms: 1,
            })
            .expect("record an incident");
    }

    let output = sima(&["report", path, "--machines"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains("vastai-81234"), "names the machine: {text}");
    assert!(text.contains("2 incidents"), "names the count: {text}");
    assert!(text.contains("blacklisted"), "names the status: {text}");
}

#[test]
fn report_machines_refuses_a_remote_host() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // The reputation ledger is local store state the follow feed does not
    // carry, so the view stays local-only, exactly as `--spend`.
    let output = sima(&["report", path, "--machines", "--on", "gpubox"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("usage"), "{stderr}");
}

#[test]
fn report_view_flags_are_mutually_exclusive() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // No arm matches two view flags together, so a combination falls to the
    // usage error.
    for args in [
        vec!["report", path, "--timeline", "--spend"],
        vec!["report", path, "--all", "--timeline"],
        vec!["report", path, "--all", "--spend"],
        vec!["report", path, "--machines", "--spend"],
        vec!["report", path, "--machines", "--timeline"],
    ] {
        let output = sima(&args);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
        assert!(stderr.contains("usage"), "{args:?}: {stderr}");
    }
}

#[test]
fn report_spend_refuses_a_remote_host() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // The ledger is local store state that the follow feed does not carry, so
    // the spend view stays local-only, exactly as the `spend` command was.
    let output = sima(&["report", path, "--spend", "--on", "gpubox"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("usage"), "{stderr}");
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
fn rm_removes_an_unfinalized_run() {
    let dir = tempfile::tempdir().expect("temp dir");
    // A rejected candidate leaves the run unfinalized: no manifest, committed
    // work for the succeeding task only. An abandoned run must be removable.
    let config = write_config(dir.path(), r#""succeed", "reject""#);
    let path = config.to_str().expect("utf-8 path");

    assert_eq!(sima(&["run", path]).status.code(), Some(2));
    let rm = sima(&["rm", path]);
    assert_eq!(rm.status.code(), Some(0), "{rm:?}");
    assert_eq!(sima(&["status", path]).status.code(), Some(1));
    let objects = dir.path().join("store").join("objects");
    let object_files: usize = std::fs::read_dir(&objects)
        .expect("read objects dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| std::fs::read_dir(e.path()).expect("read fan-out").count())
        .sum();
    assert_eq!(object_files, 0, "no object files survive the removal");
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
fn the_usage_text_names_every_command_form() {
    let output = sima(&[]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    for form in [
        "sima run",
        "sima status",
        "--failed",
        "--task",
        "sima report",
        "--all",
        "--timeline",
        "--spend",
        "--machines",
        "sima rm",
        "sima tui",
        "sima follow",
        "--on",
        "--fleet",
    ] {
        assert!(stderr.contains(form), "usage names {form}: {stderr}");
    }
    // The reporting views live under `report`, so no top-level `timeline` or
    // `spend` verb remains to name.
    assert!(
        !stderr.contains("sima timeline"),
        "no top-level timeline verb: {stderr}"
    );
    assert!(
        !stderr.contains("sima spend"),
        "no top-level spend verb: {stderr}"
    );
    // Every read view takes a host, and the note that says so must name them
    // all: a verb missing from it reads as local-only.
    let on = stderr
        .lines()
        .skip_while(|line| !line.contains("--on <host>"))
        .collect::<Vec<&str>>()
        .join(" ");
    for view in ["status", "report", "tui", "follow"] {
        assert!(on.contains(view), "the host note names {view}: {stderr}");
    }
    // The far half of the follow transport is internal, not a verb a user
    // invokes, so the usage text does not offer it.
    assert!(
        !stderr.contains("follow-serve"),
        "the internal verb stays unlisted: {stderr}"
    );
    assert!(
        !stderr.contains("--full"),
        "the renamed flag is gone: {stderr}"
    );
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
fn an_observer_follows_a_live_run_to_its_end() {
    let dir = tempfile::tempdir().expect("temp dir");
    // The sleeps keep the child alive long enough that the observer sees the
    // lock held mid-run: four tasks over two workers span about a second.
    let config = write_config(
        dir.path(),
        r#""sleep:400", "sleep:400", "sleep:400", "sleep:400""#,
    );
    let mut child = sima_command()
        .args(["run", config.to_str().expect("utf-8 path")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn sima run");

    let loaded = load(&config).expect("load config");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let pause = std::time::Duration::from_millis(10);
    // The child bootstraps the store; the observer opens once it exists.
    let mut observer = loop {
        match RunObserver::new(&loaded) {
            Ok(observer) => break observer,
            Err(_) if std::time::Instant::now() < deadline => std::thread::sleep(pause),
            Err(e) => panic!("the store never appeared: {e}"),
        }
    };

    // Follow the run to its terminal event, noting the holder while the
    // child lives. Every wait polls up to the deadline; no fixed sleep
    // carries a correctness assumption.
    let mut events = Vec::new();
    let mut held = None;
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "the run did not end in time; events so far: {events:?}"
        );
        if held.is_none() {
            held = observer.holder().expect("probe the lock");
        }
        events.extend(observer.poll().expect("poll the journal"));
        if events
            .iter()
            .any(|record| matches!(record.event, Event::RunFinalized { .. }))
        {
            break;
        }
        std::thread::sleep(pause);
    }

    // The lock named the child while it drove the run.
    let holder = held.expect("the run was held while in flight");
    assert_eq!(
        holder.split_whitespace().next(),
        Some(child.id().to_string().as_str()),
        "the holder line names the driving process: {holder}"
    );
    // The tail delivered the full lifecycle: the start and every commit,
    // each exactly once.
    assert!(
        events
            .iter()
            .any(|record| matches!(record.event, Event::RunStarted { .. })),
        "the seed replays the run start: {events:?}"
    );
    let committed = events
        .iter()
        .filter(|record| matches!(record.event, Event::Committed { .. }))
        .count();
    assert_eq!(committed, 4, "every commit arrives once: {events:?}");

    // The child exits after finalizing; the lock frees with it.
    let status = child.wait().expect("wait for sima run");
    assert_eq!(status.code(), Some(0), "the child finalized");
    assert_eq!(observer.holder().expect("probe after exit"), None);
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

#[test]
fn the_write_commands_refuse_a_remote_host() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    // Driving a run happens where the hardware is, and removing a run mutates
    // a store; neither observes, so neither takes `--on`.
    for args in [
        vec!["run", path, "--on", "gpubox"],
        vec!["rm", path, "--on", "gpubox"],
    ] {
        let output = sima(&args);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
        let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
        assert!(stderr.contains("usage"), "{args:?}: {stderr}");
    }
    // The refusal is the flag, not the command: the run itself still drives.
    assert_eq!(sima(&["run", path]).status.code(), Some(0));
}

#[test]
fn a_host_flag_without_a_host_is_a_usage_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    let output = sima(&["status", path, "--on"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("usage"), "{stderr}");
}

#[test]
fn follow_serve_writes_a_frame_stream_opening_with_a_handshake() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    let output = sima(&["follow-serve", path, "--once"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let mut stream = output.stdout.as_slice();
    let payload = sima_core::read_frame(&mut stream)
        .expect("a readable stream")
        .expect("an opening frame");
    let loaded = load(&config).expect("load config");
    assert_eq!(
        sima_pipeline::FollowFrame::decode(&payload).expect("the opening frame decodes"),
        sima_pipeline::FollowFrame::Hello {
            protocol: sima_pipeline::FOLLOW_PROTOCOL_VERSION,
            run: loaded.run.id(),
            format: loaded.run.format.clone(),
            workers: loaded.execution.workers as u32,
            holder: None,
        }
    );
}

#[test]
fn follow_prints_a_finished_run_s_events_and_exits_on_its_outcome() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "succeed""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(0));

    // The run is over and nothing holds it: follow replays what it recorded
    // and leaves with the run's own outcome code.
    let output = sima(&["follow", path]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains("started: 2 tasks"), "{text}");
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("committed"))
            .count(),
        2,
        "one line per commit: {text}"
    );
    assert!(text.contains("finalized"), "{text}");
}

#[test]
fn follow_over_an_abandoned_run_prints_its_history_and_exits_0() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""sleep:4000", "sleep:4000""#);
    let path = config.to_str().expect("utf-8 path");
    common::abandon_run(&config);

    // The journal stops mid-run and nothing holds the lock: such a run is
    // resumable, so the follow renders what was recorded and leaves
    // successfully rather than reporting a failure that did not happen.
    let output = sima(&["follow", path]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let text = stdout(&output);
    assert!(text.contains("started: 2 tasks"), "{text}");
    assert!(!text.contains("finalized"), "{text}");
}

#[test]
fn follow_over_a_failed_run_exits_2() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed", "reject""#);
    let path = config.to_str().expect("utf-8 path");
    assert_eq!(sima(&["run", path]).status.code(), Some(2));

    let output = sima(&["follow", path]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(stdout(&output).contains("rejected"), "{output:?}");
}

#[test]
fn follow_over_an_interrupted_run_exits_130() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""sleep:1500", "sleep:1500", "sleep:1500""#);
    let path = config.to_str().expect("utf-8 path");
    let mut child = Command::new(env!("CARGO_BIN_EXE_sima"))
        .args(["run", path])
        .env("SIMA_WORKER", common::worker_binary())
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn sima run");
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .expect("send SIGINT")
            .success()
    );
    assert_eq!(child.wait().expect("wait for sima").code(), Some(130));

    let output = sima(&["follow", path]);
    assert_eq!(output.status.code(), Some(130), "{output:?}");
}

#[test]
fn follow_before_any_run_reports_what_status_reports() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let path = config.to_str().expect("utf-8 path");

    let followed = sima(&["follow", path]);
    let reported = sima(&["status", path]);
    assert_eq!(followed.status.code(), Some(1), "{followed:?}");
    assert_eq!(reported.status.code(), Some(1), "{reported:?}");
    assert_eq!(
        String::from_utf8(followed.stderr).expect("stderr is UTF-8"),
        String::from_utf8(reported.stderr).expect("stderr is UTF-8"),
    );
}

#[test]
fn the_start_gate_surfaces_a_broken_config_instead_of_waiting_it_out() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = common::write_config_text(dir.path(), "sima.toml", "this is not a config");
    // The gate absorbs one transient — a store root the orchestrator has yet
    // to create. A config that will never load is a fault of the test setup,
    // and reporting it at the first tick beats a deadline that names the
    // wrong cause.
    let opened = Instant::now();
    let panic = std::panic::catch_unwind(|| common::poll_until_started(&config))
        .expect_err("a config that does not load cannot gate a run");
    assert!(opened.elapsed() < Duration::from_secs(5), "the gate waited");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .unwrap_or_default();
    assert!(message.contains("load the config"), "{message}");
}

#[test]
fn follow_ends_successfully_when_its_reader_closes_the_pipe() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""sleep:400", "sleep:400", "sleep:400""#);
    let path = config.to_str().expect("utf-8 path");
    let mut run = common::driving(&config);

    let mut followed = sima_command()
        .args(["follow", path])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn sima follow");
    // `sima follow <config> | head -1`: one line read, then the reader is
    // gone. The next write finds a closed pipe, which ends the follow on the
    // state the run had reached — in progress, so successfully.
    let mut out = std::io::BufReader::new(followed.stdout.take().expect("a piped stdout"));
    let mut line = String::new();
    std::io::BufRead::read_line(&mut out, &mut line).expect("read the first line");
    assert!(line.starts_with("started:"), "{line}");
    drop(out);

    let ended = common::wait_within(followed, Duration::from_secs(30));
    assert_eq!(ended.status.code(), Some(0), "{ended:?}");
    assert_eq!(run.wait().expect("wait for sima run").code(), Some(0));
}

#[test]
fn follow_streams_a_live_run_to_its_end() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""sleep:800", "sleep:800", "sleep:800""#);
    let path = config.to_str().expect("utf-8 path");
    let mut run = common::driving(&config);

    let output = sima(&["follow", path]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(stdout(&output).contains("finalized"), "{output:?}");
    assert_eq!(run.wait().expect("wait for sima run").code(), Some(0));
}

/// The stderr of `output`, as UTF-8.
fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr is UTF-8")
}

#[test]
fn reconcile_over_a_store_holding_no_rental_reports_nothing_to_do() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let output = sima(&["reconcile", config.to_str().expect("utf-8 path")]);
    // No record names a provider, so the pass needs no credentials and
    // reaches no API.
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(
        stdout(&output).contains("nothing to reconcile"),
        "{output:?}"
    );
}

#[test]
fn reconcile_over_a_record_naming_an_unknown_provider_fails_naming_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), r#""succeed""#);
    let loaded = load(&config).expect("load config");
    let store = Store::open(&loaded.store).expect("open the store");
    store
        .put_instance(&InstanceRecord {
            tag: "sima-tag-0".to_string(),
            provider: "nowhere".to_string(),
            machine: "m-0".to_string(),
            owner: loaded.run.id().to_string(),
            state: InstanceRecordState::Live {
                instance: "i-1".to_string(),
            },
            price_micro_usd_hour: 100_000,
            created_ms: 1_700_000_000_000,
        })
        .expect("seed the instance ledger");

    let output = sima(&["reconcile", config.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(stderr(&output).contains("nowhere"), "{output:?}");
    // Nothing judged the machine the record names, so the record stands.
    assert_eq!(store.instances().expect("read the ledger").len(), 1);
}
