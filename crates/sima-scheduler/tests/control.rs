//! RunControl acceptance: the observer mirrors the journal, and the
//! interrupt flag winds a run down gracefully, leaving the store
//! resumable.

mod common;

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{config, exec, journal_events, run_controlled, run_id, run_into, temp_store};
use sima_core::Result;
use sima_domains::StubBehavior;
use sima_scheduler::{LifecycleEvent, RunControl, RunOutcome};

/// The observer receives every event, typed, in exactly the order the
/// journal records: the sink thread appends each line and then invokes
/// the observer, so the two sequences are the same by construction.
#[test]
fn the_observer_mirrors_the_journal() -> Result<()> {
    let cfg = config(
        20,
        vec![
            StubBehavior::Succeed,
            StubBehavior::Flaky(1),
            StubBehavior::Succeed,
        ],
    );
    let seen: Mutex<Vec<LifecycleEvent>> = Mutex::new(Vec::new());
    let interrupt = AtomicBool::new(false);
    let control = RunControl {
        observer: &|event: &LifecycleEvent| {
            seen.lock().expect("observer mutex").push(event.clone());
        },
        interrupt: &interrupt,
    };

    let (_dir, store) = temp_store();
    assert!(matches!(
        run_controlled(&store, &cfg, &exec(4, 3, 1_000), &control)?,
        RunOutcome::Finalized { .. }
    ));

    let journal = journal_events(&store, &run_id(&cfg));
    assert_eq!(*seen.lock().expect("observer mutex"), journal);
    Ok(())
}

/// An interrupt landing mid-run drains gracefully: in-flight attempts
/// finish and commit, queued tasks are abandoned, no manifest is written,
/// and the journal closes with `run_interrupted`. A following clean run
/// finalizes to a manifest identical to an uninterrupted reference run's.
#[test]
fn an_interrupt_mid_run_drains_and_stays_resumable() -> Result<()> {
    // The sleeps keep several tasks in flight when the first commit lands,
    // and outlast the driver's interrupt-poll interval so the flag is
    // observed while work is still leased.
    let cfg = config(
        21,
        vec![
            StubBehavior::Succeed,
            StubBehavior::Sleep(200),
            StubBehavior::Sleep(200),
            StubBehavior::Sleep(200),
        ],
    );
    let interrupt = AtomicBool::new(false);
    let control = RunControl {
        observer: &|event: &LifecycleEvent| {
            if matches!(event, LifecycleEvent::Committed { .. }) {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
    };

    let (_dir, store) = temp_store();
    let exec_cfg = exec(2, 3, 10_000);
    assert!(matches!(
        run_controlled(&store, &cfg, &exec_cfg, &control)?,
        RunOutcome::Interrupted { .. }
    ));

    let run = run_id(&cfg);
    // No manifest: the interrupted run did not finalize.
    assert!(store.manifest(&run)?.is_none());
    let events = journal_events(&store, &run);
    assert!(
        matches!(events.last(), Some(LifecycleEvent::RunInterrupted { .. })),
        "the journal closes with run_interrupted"
    );
    // In-flight attempts finished and committed; their records survive.
    let committed = events
        .iter()
        .filter(|e| matches!(e, LifecycleEvent::Committed { .. }))
        .count();
    assert!(committed >= 1, "at least the first commit survives");

    // A clean second run completes the abandoned work; its manifest equals
    // an uninterrupted reference run's.
    assert!(matches!(
        run_into(&store, &cfg, &exec_cfg)?,
        RunOutcome::Finalized { .. }
    ));
    let (_reference_dir, reference) = temp_store();
    assert!(matches!(
        run_into(&reference, &cfg, &exec_cfg)?,
        RunOutcome::Finalized { .. }
    ));
    assert_eq!(
        store.manifest(&run)?.expect("resumed manifest"),
        reference.manifest(&run)?.expect("reference manifest"),
    );
    Ok(())
}

/// An interrupt set before the run starts commits nothing: the driver
/// observes the flag on its first iteration, never polls the source, and
/// the store stays clean for a later clean run.
#[test]
fn an_interrupt_before_any_task_starts_commits_nothing() -> Result<()> {
    let cfg = config(22, vec![StubBehavior::Succeed, StubBehavior::Succeed]);
    let interrupt = AtomicBool::new(true);
    let control = RunControl {
        observer: &|_: &LifecycleEvent| {},
        interrupt: &interrupt,
    };

    let (_dir, store) = temp_store();
    let exec_cfg = exec(2, 3, 1_000);
    assert!(matches!(
        run_controlled(&store, &cfg, &exec_cfg, &control)?,
        RunOutcome::Interrupted { .. }
    ));

    let run = run_id(&cfg);
    assert!(store.manifest(&run)?.is_none());
    let events = journal_events(&store, &run);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, LifecycleEvent::Committed { .. }))
            .count(),
        0,
        "nothing ran, so nothing committed"
    );

    // Resumable: a clean run over the same store finalizes.
    assert!(matches!(
        run_into(&store, &cfg, &exec_cfg)?,
        RunOutcome::Finalized { .. }
    ));
    Ok(())
}
