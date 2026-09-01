//! SearchControl acceptance: the observer mirrors the journal, and the
//! interrupt flag winds a search down gracefully, leaving the store
//! resumable.

mod common;

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{config, exec, journal_events, run_into, search_controlled, search_id, temp_store};
use sima_core::Result;
use sima_domains::StubBehavior;
use sima_scheduler::{Event, Record, SearchControl, SearchOutcome};

/// The observer receives every record, typed, in exactly the order the
/// journal records: the collector thread appends each line and then invokes
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
    let seen: Mutex<Vec<Event>> = Mutex::new(Vec::new());
    let interrupt = AtomicBool::new(false);
    let control = SearchControl {
        observer: &|record: &Record| {
            seen.lock()
                .expect("observer mutex")
                .push(record.event.clone());
        },
        interrupt: &interrupt,
        on_start: None,
    };

    let (_dir, store) = temp_store();
    assert!(matches!(
        search_controlled(&store, &cfg, &exec(4, 3, 1_000), &control)?,
        SearchOutcome::Finalized { .. }
    ));

    let journal = journal_events(&store, &search_id(&cfg));
    assert_eq!(*seen.lock().expect("observer mutex"), journal);
    Ok(())
}

/// An interrupt landing mid-search drains gracefully: in-flight attempts
/// finish and commit, queued tasks are abandoned, no manifest is written,
/// and the journal closes with `search_interrupted`. A following clean search
/// finalizes to a manifest identical to an uninterrupted reference search's.
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
    let control = SearchControl {
        observer: &|record: &Record| {
            if matches!(record.event, Event::Committed { .. }) {
                interrupt.store(true, Ordering::Relaxed);
            }
        },
        interrupt: &interrupt,
        on_start: None,
    };

    let (_dir, store) = temp_store();
    let exec_cfg = exec(2, 3, 10_000);
    assert!(matches!(
        search_controlled(&store, &cfg, &exec_cfg, &control)?,
        SearchOutcome::Interrupted { .. }
    ));

    let search = search_id(&cfg);
    // No manifest: the interrupted search did not finalize.
    assert!(store.manifest(&search)?.is_none());
    let events = journal_events(&store, &search);
    assert!(
        matches!(events.last(), Some(Event::SearchInterrupted { .. })),
        "the journal closes with search_interrupted"
    );
    // In-flight attempts finished and committed; their records survive.
    let committed = events
        .iter()
        .filter(|e| matches!(e, Event::Committed { .. }))
        .count();
    assert!(committed >= 1, "at least the first commit survives");

    // A clean second search completes the abandoned work; its manifest equals
    // an uninterrupted reference search's.
    assert!(matches!(
        run_into(&store, &cfg, &exec_cfg)?,
        SearchOutcome::Finalized { .. }
    ));
    let (_reference_dir, reference) = temp_store();
    assert!(matches!(
        run_into(&reference, &cfg, &exec_cfg)?,
        SearchOutcome::Finalized { .. }
    ));
    assert_eq!(
        store.manifest(&search)?.expect("resumed manifest"),
        reference.manifest(&search)?.expect("reference manifest"),
    );
    Ok(())
}

/// An interrupt set before the search starts commits nothing: the driver
/// observes the flag on its first iteration, never polls the source, and
/// the store stays clean for a later clean search.
#[test]
fn an_interrupt_before_any_task_starts_commits_nothing() -> Result<()> {
    let cfg = config(22, vec![StubBehavior::Succeed, StubBehavior::Succeed]);
    let interrupt = AtomicBool::new(true);
    let control = SearchControl {
        observer: &|_: &Record| {},
        interrupt: &interrupt,
        on_start: None,
    };

    let (_dir, store) = temp_store();
    let exec_cfg = exec(2, 3, 1_000);
    assert!(matches!(
        search_controlled(&store, &cfg, &exec_cfg, &control)?,
        SearchOutcome::Interrupted { .. }
    ));

    let search = search_id(&cfg);
    assert!(store.manifest(&search)?.is_none());
    let events = journal_events(&store, &search);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::Committed { .. }))
            .count(),
        0,
        "nothing ran, so nothing committed"
    );

    // Resumable: a clean search over the same store finalizes.
    assert!(matches!(
        run_into(&store, &cfg, &exec_cfg)?,
        SearchOutcome::Finalized { .. }
    ));
    Ok(())
}
