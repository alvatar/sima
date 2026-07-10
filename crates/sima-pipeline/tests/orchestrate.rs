//! Orchestration acceptance: a loaded config drives to its outcome, the
//! journal answers status, re-evaluation touches no executor, and the
//! orchestrator lock admits one driver at a time.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};

use common::{journal_events, loaded};
use sima_core::{Error, Result};
use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig};
use sima_pipeline::{
    LifecycleEvent, LoadedConfig, RunControl, RunOutcome, RunState, orchestrate, status,
};
use sima_scheduler::ExecutionConfig;
use sima_store::Store;

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

    let report = status(&config)?;
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
    match status(&config)?.state {
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
    assert_eq!(status(&config)?.state, RunState::Interrupted);

    // The lock released with the interrupted call; the following
    // orchestration completes the abandoned work.
    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    assert!(store.manifest(&run)?.is_some());
    assert_eq!(status(&config)?.state, RunState::Finalized);
    Ok(())
}

#[test]
fn an_undispatchable_config_orchestrates_to_validation_without_touching_the_store() -> Result<()> {
    // load() already rejects unknown ids through translation, so the
    // config is built directly: the reorder is defense in depth, pinned
    // where it is observable.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = LoadedConfig {
        run: RunConfig {
            root_seed: 1,
            segments: None,
            format: FormatId::new("no-such-domain.v1")?,
            generator: GeneratorConfig {
                id: GeneratorId::new("stub.v1")?,
                params: Vec::new(),
            },
            params: Params { bytes: Vec::new() },
        },
        execution: ExecutionConfig::new(1, 1, std::time::Duration::MAX, std::time::Duration::MAX)?,
        store: dir.path().join("store"),
    };
    assert!(matches!(
        orchestrate(&config, &RunControl::detached()),
        Err(Error::Validation(_))
    ));
    // Dispatch precedes every store mutation: no store, no run directory,
    // no lock file may appear for a run that can never execute.
    assert!(
        !config.store.exists(),
        "orchestrate created {}",
        config.store.display()
    );
    Ok(())
}

#[test]
fn status_on_a_never_started_run_is_validation() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed""#, 1)?;
    // The store exists — created here — but the run was never driven.
    Store::open(&config.store)?;
    assert!(matches!(status(&config), Err(Error::Validation(_))));
    Ok(())
}

#[test]
fn status_on_a_missing_store_is_validation_and_creates_nothing() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed""#, 1)?;
    assert!(matches!(status(&config), Err(Error::Validation(_))));
    // A status query is read-only: it must not leave a store behind.
    assert!(
        !config.store.exists(),
        "status created {}",
        config.store.display()
    );
    Ok(())
}
