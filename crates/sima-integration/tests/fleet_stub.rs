//! End-to-end acceptance of the distributed run over the stub provider: a
//! `[fleet] provider = "stub"` config drives the full spine — acquire, probe,
//! run, teardown, ledger — with no GPU and no network. The stub domain carries
//! the work, and the stub provider's instances are reached through the
//! transport's local mode, spawning `sima-worker` directly.
//!
//! This file is the milestone's regression net: it exercises the fleet path
//! every layer above the transport shares with a real provider.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};

use common::{journal_events, loaded_text};
use sima_core::Result;
use sima_pipeline::{Event, LoadedConfig, Record, RunControl, RunOutcome, orchestrate, spend};
use sima_store::Store;

/// A stub-fleet config: no local pool, so `count` rented instances carry the
/// whole run; `behaviors` programs the stub candidates.
fn fleet_config(
    dir: &std::path::Path,
    name: &str,
    store: &str,
    behaviors: &str,
    count: u32,
) -> Result<LoadedConfig> {
    let text = format!(
        r#"
        [run]
        root_seed = 3
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = [{behaviors}]

        [execution]
        store = "{store}"
        max_attempts = 3

        [fleet]
        provider = "stub"
        count = {count}
    "#
    );
    loaded_text(dir, name, &text)
}

#[test]
fn a_stub_fleet_run_finalizes_with_records_from_fleet_workers() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = fleet_config(
        dir.path(),
        "fleet.toml",
        "./store",
        r#""succeed", "succeed", "succeed", "succeed""#,
        2,
    )?;
    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));

    // Every candidate committed a record, and the workers that produced them
    // ran on the fleet instances — the journal attributes each `WorkerBound`
    // to a stub host, since no local pool carried the run.
    let store = Store::open(&config.store)?;
    let manifest = store
        .manifest(&config.run.id())?
        .expect("a finalized manifest");
    assert_eq!(manifest.entries.len(), 4);
    let events = journal_events(&config);
    let hosts: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            Event::WorkerBound { host, .. } => Some(host.as_str()),
            _ => None,
        })
        .collect();
    assert!(!hosts.is_empty(), "the fleet's workers bound");
    assert!(
        hosts.iter().all(|host| host.starts_with("stub-")),
        "every worker ran on a fleet instance: {hosts:?}"
    );
    Ok(())
}

#[test]
fn the_ledger_holds_one_closed_entry_per_instance() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = fleet_config(
        dir.path(),
        "ledger.toml",
        "./store",
        r#""succeed", "succeed""#,
        2,
    )?;
    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));

    // Two instances acquired and torn down: two closed ledger entries, none
    // still open, and a positive total.
    let report = spend(&config)?;
    assert_eq!(report.entries.len(), 2, "one closed entry per instance");
    assert!(report.open.is_empty(), "no rental is left open");
    assert!(report.total.0 > 0, "the rentals cost something");
    Ok(())
}

#[test]
fn a_re_run_resumes_and_finalizes() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = fleet_config(
        dir.path(),
        "resume.toml",
        "./store",
        r#""succeed", "succeed", "succeed""#,
        1,
    )?;
    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    // The same store, re-run: the frontier is empty, so the run re-finalizes
    // without re-evaluating a candidate, and the fleet is acquired and torn
    // down again cleanly.
    assert!(matches!(
        orchestrate(&config, &RunControl::detached())?,
        RunOutcome::Finalized { .. }
    ));
    let store = Store::open(&config.store)?;
    assert!(store.manifest(&config.run.id())?.is_some());
    Ok(())
}

#[test]
fn an_interrupt_tears_the_fleet_down_and_leaves_the_ledger_closed() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    // Sleeping candidates so the interrupt lands mid-run, after the first
    // commit.
    let config = fleet_config(
        dir.path(),
        "interrupt.toml",
        "./store",
        r#""succeed", "sleep:200", "sleep:200", "sleep:200""#,
        1,
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
        orchestrate(&config, &control)?,
        RunOutcome::Interrupted { .. }
    ));

    // The fleet was torn down on the interrupt: the ledger is closed, with no
    // rental left open, and no manifest was written — the store is resumable.
    let store = Store::open(&config.store)?;
    assert!(store.manifest(&config.run.id())?.is_none());
    let report = spend(&config)?;
    assert!(report.open.is_empty(), "the interrupt tore the fleet down");
    assert!(
        !report.entries.is_empty(),
        "the torn-down rental left a closed entry"
    );
    Ok(())
}
