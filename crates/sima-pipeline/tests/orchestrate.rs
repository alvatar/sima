//! Orchestration acceptance: a loaded config drives to its outcome, the
//! journal answers status, re-evaluation touches no executor, and the
//! orchestrator lock admits one driver at a time.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{journal_events, loaded};
use sima_core::{Error, Result};
use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig};
use sima_pipeline::{
    Engagement, Event, Fleet, LoadedConfig, Orchestrator, Pool, Record, RunControl, RunOutcome,
    RunState, orchestrate, status,
};
use sima_provider::Budget;
use sima_scheduler::ExecutionConfig;
use sima_store::Store;

#[test]
fn a_config_orchestrates_to_finalized_and_status_reports_it() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = loaded(dir.path(), r#""succeed", "succeed", "flaky:2""#, 2)?;

    assert!(matches!(
        orchestrate(&config, &RunControl::detached(), Engagement::Orchestrator)?,
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
        orchestrate(&config, &RunControl::detached(), Engagement::Orchestrator)?,
        RunOutcome::Finalized { .. }
    ));
    let first = journal_events(&config).len();

    assert!(matches!(
        orchestrate(&config, &RunControl::detached(), Engagement::Orchestrator)?,
        RunOutcome::Finalized { .. }
    ));
    let events = journal_events(&config);
    // Only run-level events and the pool's own arrival append: the frontier was
    // empty, so nothing was queued, leased, or committed — no executor ran.
    // The workers still start and report the device they would compute on,
    // which is what `WorkerBound` records.
    assert!(events.len() > first, "the second segment appends events");
    for event in &events[first..] {
        assert!(
            matches!(
                event,
                Event::RunStarted { .. } | Event::WorkerBound { .. } | Event::RunFinalized { .. }
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

    let outcome = orchestrate(&config, &RunControl::detached(), Engagement::Orchestrator)?;
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
        orchestrate(&config, &RunControl::detached(), Engagement::Orchestrator),
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
        observer: &|record: &Record| {
            if matches!(record.event, Event::Committed { .. }) {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
        on_start: None,
    };
    assert!(matches!(
        orchestrate(&config, &control, Engagement::Orchestrator)?,
        RunOutcome::Interrupted { .. }
    ));

    let store = Store::open(&config.store)?;
    let run = config.run.id();
    assert!(store.manifest(&run)?.is_none());
    assert_eq!(status(&config)?.state, RunState::Interrupted);

    // The lock released with the interrupted call; the following
    // orchestration completes the abandoned work.
    assert!(matches!(
        orchestrate(&config, &RunControl::detached(), Engagement::Orchestrator)?,
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
        orchestrator: Orchestrator {
            migrate: None,
            container: None,
            pool: Some(Pool::Workers(1)),
        },
        hosts: BTreeMap::new(),
        host_classes: BTreeMap::new(),
        fleet: Fleet::default(),
        budget: Budget::default(),
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
        execution: ExecutionConfig::new(
            1,
            1,
            std::time::Duration::MAX,
            std::time::Duration::MAX,
            None,
        )?,
        store: dir.path().join("store"),
        domains: sima_pipeline::DomainRegistry::builtin(),
    };
    assert!(matches!(
        orchestrate(&config, &RunControl::detached(), Engagement::Orchestrator),
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

#[test]
fn a_run_with_nowhere_to_execute_is_a_validation_error_before_the_store() -> Result<()> {
    // An orchestrator that declares no worker layout has nothing to execute on
    // by itself. Without the flag the error names it, since engaging the fleet
    // is what would give the run somewhere to go; with the flag, and a fleet
    // that names no machine, it names that instead. Either way the failure
    // precedes Store::open.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = common::loaded_text(
        dir.path(),
        "empty.toml",
        r#"
        [run]
        root_seed = 1
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed"]

        [config]
        store = "./store"
        max_attempts = 1
    "#,
    )?;
    for (engagement, expected) in [
        (Engagement::Orchestrator, "--fleet"),
        (Engagement::Fleet, "[fleet] names no machine"),
    ] {
        match orchestrate(&config, &RunControl::detached(), engagement) {
            Err(Error::Validation(message)) => assert!(
                message.contains(expected),
                "{engagement:?}: the error names {expected:?}: {message}"
            ),
            other => panic!("{engagement:?}: expected a validation error, got {other:?}"),
        }
    }
    assert!(
        !config.store.exists(),
        "a run with nowhere to execute creates no store"
    );
    Ok(())
}
