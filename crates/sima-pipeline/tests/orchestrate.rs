//! Orchestration acceptance: a loaded config drives to its outcome, the
//! journal answers status, re-evaluation touches no executor, and the
//! orchestrator lock admits one driver at a time.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use sima_core::{Error, Result};
use sima_pipeline::{
    LifecycleEvent, LoadedConfig, RunControl, RunOutcome, RunState, load, orchestrate, status,
};
use sima_store::Store;

/// Writes a `sima.toml` into `dir` whose store lives beside it, and loads
/// it. `behaviors` is the TOML list literal's inner content.
fn loaded(dir: &Path, behaviors: &str, workers: u32) -> Result<LoadedConfig> {
    let text = format!(
        r#"
        [run]
        root_seed = 7
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [execution]
        store = "./store"
        workers = {workers}
        max_attempts = 3
        attempt_timeout_ms = 5000
    "#
    );
    let path: PathBuf = dir.join("sima.toml");
    std::fs::write(&path, text).expect("write config");
    load(&path)
}

/// The typed journal of `config`'s run in its store.
fn journal_events(config: &LoadedConfig) -> Vec<LifecycleEvent> {
    let store = Store::open(&config.store).expect("open store");
    store
        .journal(&config.run.id())
        .expect("read journal")
        .iter()
        .map(|line| LifecycleEvent::from_line(line).expect("parse journal line"))
        .collect()
}

#[test]
fn a_config_orchestrates_to_finalized_and_status_reports_it() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed", "succeed", "flaky:2""#, 2)?;

    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));

    let store = Store::open(&config.store)?;
    let run = config.run.id();
    assert!(store.manifest(&run)?.is_some(), "the manifest exists");

    let report = status(&store, &run)?;
    assert_eq!(report.state, RunState::Finalized);
    assert_eq!(report.tasks, 3);
    assert_eq!(report.committed, 3);
    assert_eq!(report.retried, 2);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.faulted, 0);
    Ok(())
}

#[test]
fn re_evaluation_finalizes_again_without_touching_an_executor() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed", "succeed""#, 2)?;

    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let first = journal_events(&config).len();

    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let events = journal_events(&config);
    // Only run-level events append: the frontier was empty, so nothing was
    // queued, leased, or committed — no executor ran.
    assert!(events.len() > first, "the second segment appends events");
    for event in &events[first..] {
        assert!(
            matches!(
                event,
                LifecycleEvent::RunStarted { .. } | LifecycleEvent::RunFinalized { .. }
            ),
            "unexpected event in the re-evaluation segment: {event:?}"
        );
    }
    Ok(())
}

#[test]
fn a_rejected_candidate_fails_the_run_and_status_carries_the_reason() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed", "reject""#, 1)?;

    let outcome = orchestrate(&config, &RunControl::detached())?;
    let reason = match outcome {
        RunOutcome::Failed { reason, .. } => reason,
        other => panic!("expected Failed, got {other:?}"),
    };

    let store = Store::open(&config.store)?;
    let run = config.run.id();
    assert!(store.manifest(&run)?.is_none(), "no manifest on failure");
    match status(&store, &run)?.state {
        RunState::Failed {
            reason: reported, ..
        } => assert_eq!(reported, reason),
        other => panic!("expected Failed state, got {other:?}"),
    }
    Ok(())
}

#[test]
fn a_held_lock_keeps_a_second_orchestrator_out() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed""#, 1)?;

    let store = Store::open(&config.store)?;
    let _lock = store.acquire_run_lock(&config.run.id())?;
    assert!(matches!(
        orchestrate(&config, &RunControl::detached()),
        Err(Error::Validation(_))
    ));
    Ok(())
}

#[test]
fn an_interrupt_through_the_pipeline_stays_resumable() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(
        dir.path(),
        r#""succeed", "sleep:200", "sleep:200", "sleep:200""#,
        2,
    )?;

    let interrupt = AtomicBool::new(false);
    let control = RunControl {
        observer: &|event: &LifecycleEvent| {
            if matches!(event, LifecycleEvent::Committed { .. }) {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
    };
    assert!(matches!(
        orchestrate(&config, &control)?,
        RunOutcome::Interrupted { .. }
    ));

    let store = Store::open(&config.store)?;
    let run = config.run.id();
    assert!(store.manifest(&run)?.is_none());
    assert_eq!(status(&store, &run)?.state, RunState::Interrupted);

    // The lock released with the interrupted call; the following
    // orchestration completes the abandoned work.
    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    assert!(store.manifest(&run)?.is_some());
    assert_eq!(status(&store, &run)?.state, RunState::Finalized);
    Ok(())
}

#[test]
fn status_on_a_never_started_run_is_validation() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed""#, 1)?;
    let store = Store::open(&config.store)?;
    assert!(matches!(
        status(&store, &config.run.id()),
        Err(Error::Validation(_))
    ));
    Ok(())
}
