//! End-to-end acceptance of the distributed search over the stub provider: a
//! rented host class the fleet names drives the full spine — acquire, probe,
//! search, teardown, ledger — with no GPU and no network. The stub domain carries
//! the work, and the stub provider's machines are reached through the
//! transport's local mode, spawning `sima-worker` directly.
//!
//! It exercises the renting path every layer above the transport shares with a
//! real provider, so every search here is engaged with [`Engagement::Fleet`] — the
//! answer `sima search --fleet` gives.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};

use common::{journal_events, loaded_text};
use sima_core::Result;
use sima_pipeline::{
    BinaryChange, Engagement, Event, LoadedConfig, Record, SearchControl, SearchOutcome,
    orchestrate, spend,
};
use sima_store::Store;

/// A config whose fleet is one rented class of `count` machines and whose
/// orchestrator declares no workers, so the rentals carry the whole search;
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
        [search]
        root_seed = 3
        format = "stub.v1"

        [search.generator]
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
fn a_stub_fleet_search_finalizes_with_records_from_fleet_workers() -> Result<()> {
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
            &SearchControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
    ));

    // Every candidate committed a record, and the workers that produced them
    // ran on the fleet instances — the journal attributes each `WorkerBound`
    // to a stub host, since no local pool carried the search.
    let store = Store::open(&config.store)?;
    let manifest = store
        .manifest(&config.search.id())?
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
        &SearchControl::detached(),
        Engagement::Fleet,
        BinaryChange::Refuse,
    )?;

    // The members are asked for at once, so which reaches the market first is
    // not fixed. What is fixed is that each names its class and index, and
    // that the wait it then sits in says the same name — the two lines
    // interleave with the other member's, and an operator has to be able to
    // tell whose is whose.
    let events = journal_events(&config);
    let named = |whose: fn(&Event) -> Option<&str>| {
        let mut members: Vec<&str> = events.iter().filter_map(whose).collect();
        members.sort_unstable();
        members
    };
    let members = named(|event| match event {
        Event::Renting { member, .. } => Some(member.as_str()),
        _ => None,
    });
    assert_eq!(
        members,
        ["rented[0]", "rented[1]"],
        "each member names its class and index: {events:?}"
    );
    assert_eq!(
        named(|event| match event {
            Event::AwaitingMachine { member, .. } => Some(member.as_str()),
            _ => None,
        }),
        members,
        "and says the same name while it waits: {events:?}"
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
fn a_search_that_dies_acquiring_is_in_the_store_with_what_it_said() -> Result<()> {
    // The search is registered before any machine is asked for, so what putting it
    // on its machines takes is journaled where the work will be. A fleet that
    // cannot be brought up therefore leaves a search in the store: no task ran, so
    // it stands at nothing committed, and its journal holds what it said about
    // the acquisition that failed.
    let dir = tempfile::tempdir().expect("temp dir");
    let text = r#"
        [search]
        root_seed = 3
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["succeed", "succeed"]

        [config]
        store = "./store"
        max_attempts = 3

        [host_class.rented]
        provider = "stub"
        count = 1

        [host_class.rented.constraints]
        max_price_usd_hour = 0.01

        [fleet]
        members = ["rented"]
    "#;
    let config = loaded_text(dir.path(), "fleet.toml", text)?;
    let error = orchestrate(
        &config,
        &SearchControl::detached(),
        Engagement::Fleet,
        BinaryChange::Refuse,
    )
    .expect_err("no offer sits under a cent an hour");
    assert!(
        error.to_string().contains("no offer satisfies"),
        "the market's own answer reaches the caller: {error}"
    );

    // What the store holds: the search, listed, in progress with an empty ledger.
    let summaries = sima_pipeline::searches(&config.store)?;
    let [summary] = summaries.as_slice() else {
        panic!("one search was registered: {summaries:?}");
    };
    assert_eq!(summary.search, config.search.id());
    assert_eq!((summary.tasks, summary.committed), (0, 0));
    // And its journal holds the shortfall, so why there is nothing there is
    // readable after the fact rather than only on the terminal that watched.
    let events = journal_events(&config);
    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::Diagnostic { source, message, .. }
                if source == "rental" && message.contains("rented[0] could not be brought up")
        )),
        "{events:?}"
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
        orchestrate(
            &config,
            &SearchControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
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
fn running_again_resumes_and_finalizes() -> Result<()> {
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
            &SearchControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
    ));
    // The same store, repeated search: the frontier is empty, so the search re-finalizes
    // without re-evaluating a candidate, and the fleet is acquired and torn
    // down again cleanly.
    assert!(matches!(
        orchestrate(
            &config,
            &SearchControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
    ));
    let store = Store::open(&config.store)?;
    assert!(store.manifest(&config.search.id())?.is_some());
    Ok(())
}

#[test]
fn an_interrupt_tears_the_fleet_down_and_leaves_the_ledger_closed() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    // Sleeping candidates so the interrupt lands mid-search, after the first
    // commit.
    let config = fleet_config(
        dir.path(),
        "interrupt.toml",
        "./store",
        r#""succeed", "sleep:200", "sleep:200", "sleep:200""#,
        1,
    )?;
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
    assert!(matches!(
        orchestrate(&config, &control, Engagement::Fleet, BinaryChange::Refuse)?,
        SearchOutcome::Interrupted { .. }
    ));

    // The fleet was torn down on the interrupt: the ledger is closed, with no
    // rental left open, and no manifest was written — the store is resumable.
    let store = Store::open(&config.store)?;
    assert!(store.manifest(&config.search.id())?.is_none());
    let report = spend(&config)?;
    assert!(report.open.is_empty(), "the interrupt tore the fleet down");
    assert!(
        !report.entries.is_empty(),
        "the torn-down rental left a closed entry"
    );
    Ok(())
}

#[test]
fn an_interrupt_while_the_fleet_is_being_acquired_abandons_the_search() -> Result<()> {
    // Acquisition is minutes of paid-for waiting before a single task runs,
    // and it is where an operator most often changes their mind. The flag is
    // already up when the search starts, so the first member's walk is called off
    // before any offer is taken: the search comes back interrupted rather than
    // failed, nothing was executed, and the store stands as it did.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = fleet_config(
        dir.path(),
        "abandon.toml",
        "./store",
        r#""succeed", "succeed""#,
        2,
    )?;
    let interrupt = AtomicBool::new(true);
    let control = SearchControl {
        observer: &|_: &Record| {},
        interrupt: &interrupt,
        on_start: None,
    };
    assert!(matches!(
        orchestrate(&config, &control, Engagement::Fleet, BinaryChange::Refuse)?,
        SearchOutcome::Interrupted { .. }
    ));

    // The search is registered and resumable: no manifest, and a ledger holding
    // nothing, because no offer was ever taken.
    let store = Store::open(&config.store)?;
    assert!(store.manifest(&config.search.id())?.is_none());
    let report = spend(&config)?;
    assert!(report.open.is_empty(), "no rental is left open");
    assert!(report.entries.is_empty(), "no rental was ever made");
    // And the journal says why there is nothing there, which is the only place
    // a search abandoned before it drove leaves an account of itself.
    let events = journal_events(&config);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Event::AcquisitionAbandoned { released: 0 })),
        "{events:?}"
    );
    Ok(())
}

/// A config declaring a rented class beside an orchestrator that can carry the
/// search itself, so the invocation decides which machines are used.
fn opt_in_config(dir: &std::path::Path, name: &str, store: &str) -> Result<LoadedConfig> {
    let text = format!(
        r#"
        [search]
        root_seed = 5
        format = "stub.v1"

        [search.generator]
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
fn without_the_flag_the_orchestrator_carries_the_search_and_nothing_is_rented() -> Result<()> {
    // Declaring a rented class says a search *may* use it. This invocation does
    // not ask for it, so no provider is constructed, nothing is acquired, and
    // the orchestrator's own worker finalizes the search alone.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = opt_in_config(dir.path(), "local.toml", "./store")?;
    assert!(matches!(
        orchestrate(
            &config,
            &SearchControl::detached(),
            Engagement::Orchestrator,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
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
            &SearchControl::detached(),
            Engagement::Fleet,
            BinaryChange::Refuse
        )?,
        SearchOutcome::Finalized { .. }
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
