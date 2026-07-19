//! End-to-end acceptance of the trace facade over real subprocess workers:
//! stamped journal records, captured stderr diagnostics, correlated panic
//! backtraces, and manifest identity untouched by diagnostics.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use common::{journal_events, journal_records, manifest_bytes, worker_binary, write_config};
use sima_pipeline::{Event, Level};

/// Runs `sima run` over `config`, with the worker pinned to `worker`.
fn run_with_worker(config: &Path, worker: &Path) -> std::process::Output {
    let mut command = common::sima_command();
    command.env("SIMA_WORKER", worker);
    command
        .args(["run", config.to_str().expect("utf-8 path")])
        .output()
        .expect("run sima")
}

/// Runs `sima run` over `config` with the plain worker binary.
fn run(config: &Path) -> std::process::Output {
    run_with_worker(config, &worker_binary())
}

/// Writes an executable worker wrapper under `dir` that runs `prelude` in
/// `sh` and then execs the real worker — the shape a container client
/// takes, used here to make a worker print to stderr before serving.
fn wrapper_worker(dir: &Path, prelude: &str) -> PathBuf {
    let real = worker_binary();
    let path = dir.join("worker.sh");
    std::fs::write(
        &path,
        format!("#!/bin/sh\n{prelude}\nexec {}\n", real.display()),
    )
    .expect("write the wrapper");
    let mut permissions = std::fs::metadata(&path)
        .expect("stat the wrapper")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).expect("chmod the wrapper");
    path
}

/// One journaled diagnostic, destructured for assertions.
#[derive(Debug)]
struct Diagnostic {
    level: Level,
    source: String,
    message: String,
    worker: Option<u64>,
    host: Option<String>,
    task: Option<String>,
}

/// The diagnostics among `events`.
fn diagnostics(events: &[Event]) -> Vec<Diagnostic> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Diagnostic {
                level,
                source,
                message,
                worker,
                host,
                task,
            } => Some(Diagnostic {
                level: *level,
                source: source.clone(),
                message: message.clone(),
                worker: *worker,
                host: host.clone(),
                task: task.clone(),
            }),
            _ => None,
        })
        .collect()
}

#[test]
fn every_journal_line_of_a_subprocess_run_is_a_stamped_record() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(
        dir.path(),
        "sima.toml",
        r#""succeed", "succeed""#,
        "./store",
    );
    let output = run(&config);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let records = journal_records(&config);
    assert!(!records.is_empty(), "the run journaled its lifecycle");
    for record in &records {
        assert!(record.ts_ms.is_some(), "unstamped record: {record:?}");
    }
}

#[test]
fn a_worker_stderr_line_lands_as_a_correlated_diagnostic() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(
        dir.path(),
        "sima.toml",
        r#""succeed", "succeed""#,
        "./store",
    );
    let worker = wrapper_worker(dir.path(), "echo 'noise from the worker' >&2");
    let output = run_with_worker(&config, &worker);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let events = journal_events(&config);
    let captured: Vec<Diagnostic> = diagnostics(&events)
        .into_iter()
        .filter(|d| d.source == "worker stderr" && d.message == "noise from the worker")
        .collect();
    assert!(!captured.is_empty(), "the stderr line was journaled");
    for diagnostic in captured {
        assert_eq!(diagnostic.level, Level::Info);
        assert!(
            diagnostic.worker.is_some(),
            "the diagnostic names its worker"
        );
        assert_eq!(diagnostic.host, None, "a local pool carries no host key");
    }
}

#[test]
fn an_executor_panic_lands_a_correlated_diagnostic_and_rejects_as_before() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config(dir.path(), "sima.toml", r#""succeed", "panic""#, "./store");
    let output = run(&config);
    // The panic is a definitive candidate failure, exactly as before.
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let events = journal_events(&config);
    let rejected: Vec<(&String, &String)> = events
        .iter()
        .filter_map(|event| match event {
            Event::Rejected { task, reason, .. } => Some((task, reason)),
            _ => None,
        })
        .collect();
    assert_eq!(rejected.len(), 1, "{events:?}");
    let (rejected_task, reason) = rejected[0];
    assert!(reason.starts_with("panic:"), "{reason}");
    // The correlated diagnostic names the same task, its worker, and the
    // panic, with the backtrace the worker's hook captured.
    let panics: Vec<Diagnostic> = diagnostics(&events)
        .into_iter()
        .filter(|d| d.source == "panic")
        .collect();
    assert_eq!(panics.len(), 1, "{events:?}");
    let diagnostic = &panics[0];
    assert_eq!(diagnostic.level, Level::Error);
    assert!(
        diagnostic.message.contains("programmed panic"),
        "{}",
        diagnostic.message
    );
    assert!(
        diagnostic.worker.is_some(),
        "the diagnostic names its worker"
    );
    assert_eq!(diagnostic.task.as_ref(), Some(rejected_task));
}

#[test]
fn diagnostics_leave_the_manifest_identical() {
    // The same config into two fresh stores: one run's workers print to
    // stderr, the other's stay silent. The journals differ; the manifests
    // are byte-identical — journals are excluded from every equality
    // criterion.
    let quiet = tempfile::tempdir().expect("temp dir");
    let quiet_config = write_config(
        quiet.path(),
        "sima.toml",
        r#""succeed", "succeed""#,
        "./store",
    );
    assert_eq!(run(&quiet_config).status.code(), Some(0));

    let noisy = tempfile::tempdir().expect("temp dir");
    let noisy_config = write_config(
        noisy.path(),
        "sima.toml",
        r#""succeed", "succeed""#,
        "./store",
    );
    let worker = wrapper_worker(noisy.path(), "echo 'diagnostic noise' >&2");
    assert_eq!(
        run_with_worker(&noisy_config, &worker).status.code(),
        Some(0)
    );

    // The noisy run's journal holds diagnostics the quiet run's does not.
    let noisy_diagnostics = diagnostics(&journal_events(&noisy_config));
    assert!(
        noisy_diagnostics
            .iter()
            .any(|d| d.source == "worker stderr" && d.message == "diagnostic noise"),
        "{noisy_diagnostics:?}"
    );
    // The manifests are byte-identical regardless.
    assert_eq!(
        manifest_bytes(&quiet_config).expect("the quiet run finalized"),
        manifest_bytes(&noisy_config).expect("the noisy run finalized"),
    );
}
