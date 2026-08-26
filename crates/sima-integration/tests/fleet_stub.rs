//! End-to-end acceptance of the distributed run over the stub provider: a
//! rented host class the fleet names drives the full spine — acquire, probe,
//! run, teardown, ledger — with no GPU and no network. The stub domain carries
//! the work, and the stub provider's machines are reached through the
//! transport's local mode, spawning `sima-worker` directly.
//!
//! It exercises the renting path every layer above the transport shares with a
//! real provider, so every run here is engaged with [`Engagement::Fleet`] — the
//! answer `sima run --fleet` gives.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};

use common::{journal_events, loaded_text};
use sima_core::Result;
use sima_pipeline::{
    BinaryChange, Engagement, Event, LoadedConfig, Record, RunControl, RunOutcome, orchestrate,
    spend,
};
use sima_store::Store;

/// A config whose fleet is one rented class of `count` machines and whose
/// orchestrator declares no workers, so the rentals carry the whole run;
/// `behaviors` programs the stub candidates.
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

        [config]
        store = "{store}"
        max_attempts = 3

        [host_class.rented]
        provider = "stub"
        count = {count}

        [fleet]
        members = ["rented"]
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
        orchestrate(
            &config,
            &RunControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
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
fn a_fleet_names_each_member_it_rents_before_that_machine_comes_up() -> Result<()> {
    // Acquisition is minutes of spending with nothing to show, so each member
    // says what it took and at what rate the moment the offer is taken —
    // before the machine it names is up and before its worker binds.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = fleet_config(
        dir.path(),
        "fleet.toml",
        "./store",
        r#""succeed", "succeed""#,
        2,
    )?;
    orchestrate(
        &config,
        &RunControl::detached(),
        Engagement::Fleet,
        BinaryChange::Refuse,
    )?;

    let events = journal_events(&config);
    let members: Vec<&str> = events
        .iter()
        .filter_map(|event| match event {
            Event::Renting { member, .. } => Some(member.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        members,
        ["rented[0]", "rented[1]"],
        "each member names its class and index: {events:?}"
    );
    // A rate travels with it: what is being paid for is the whole point of
    // saying so at all.
    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::Renting { rate_microusd_hour, .. } if *rate_microusd_hour > 0
        )),
        "the line carries what it costs: {events:?}"
    );
    // And it precedes the machine serving anything, which is the silence it
    // exists to fill.
    let renting = events
        .iter()
        .position(|event| matches!(event, Event::Renting { .. }))
        .expect("a member was rented");
    let bound = events
        .iter()
        .position(|event| matches!(event, Event::WorkerBound { .. }))
        .expect("a worker bound on it");
    assert!(renting < bound, "{events:?}");
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
        orchestrate(
            &config,
            &RunControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
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
        orchestrate(
            &config,
            &RunControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
        RunOutcome::Finalized { .. }
    ));
    // The same store, re-run: the frontier is empty, so the run re-finalizes
    // without re-evaluating a candidate, and the fleet is acquired and torn
    // down again cleanly.
    assert!(matches!(
        orchestrate(
            &config,
            &RunControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
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
        orchestrate(&config, &control, Engagement::Fleet, BinaryChange::Refuse)?,
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

/// A config declaring a rented class beside an orchestrator that can carry the
/// run itself, so the invocation decides which machines are used.
fn opt_in_config(dir: &std::path::Path, name: &str, store: &str) -> Result<LoadedConfig> {
    let text = format!(
        r#"
        [run]
        root_seed = 5
        format = "stub.v1"

        [run.generator]
        id = "stub.v1"
        behaviors = ["succeed", "succeed", "succeed", "succeed"]

        [config]
        store = "{store}"
        max_attempts = 3

        [orchestrator]
        workers = 1

        [host_class.rented]
        provider = "stub"
        count = 2

        [fleet]
        members = ["rented"]
    "#
    );
    loaded_text(dir, name, &text)
}

/// The hosts the journal's `WorkerBound` events name; the orchestrator's own
/// workers report the empty label.
fn bound_hosts(events: &[Event]) -> std::collections::HashSet<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::WorkerBound { host, .. } => Some(host.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn without_the_flag_the_orchestrator_carries_the_run_and_nothing_is_rented() -> Result<()> {
    // Declaring a rented class says a run *may* use it. This invocation does
    // not ask for it, so no provider is constructed, nothing is acquired, and
    // the orchestrator's own worker finalizes the run alone.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = opt_in_config(dir.path(), "local.toml", "./store")?;
    assert!(matches!(
        orchestrate(
            &config,
            &RunControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        RunOutcome::Finalized { .. }
    ));

    let report = spend(&config)?;
    assert!(report.entries.is_empty(), "nothing was rented");
    assert!(report.open.is_empty(), "no rental is left open");
    assert_eq!(report.total.0, 0, "an unasked-for rental costs nothing");
    assert_eq!(
        bound_hosts(&journal_events(&config)),
        std::collections::HashSet::from(["".to_string()]),
        "every worker bound on this machine"
    );
    Ok(())
}

#[test]
fn with_the_flag_the_declared_machines_join_the_orchestrator() -> Result<()> {
    // The same config, asked for the fleet: the rented machines are acquired
    // beside the orchestrator's own worker, and the ledger records them.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = opt_in_config(dir.path(), "fleet.toml", "./store")?;
    assert!(matches!(
        orchestrate(
            &config,
            &RunControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
        RunOutcome::Finalized { .. }
    ));

    let report = spend(&config)?;
    assert_eq!(
        report.entries.len(),
        2,
        "one closed entry per rented machine"
    );
    assert!(report.open.is_empty(), "no rental is left open");
    let hosts = bound_hosts(&journal_events(&config));
    assert!(
        hosts.contains(""),
        "the orchestrator's own worker still bound: {hosts:?}"
    );
    assert!(
        hosts.len() > 1,
        "the rented machines bound workers of their own: {hosts:?}"
    );
    Ok(())
}
