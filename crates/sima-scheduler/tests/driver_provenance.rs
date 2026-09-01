//! Driver provenance across sessions: each spawn journals its child's driver
//! in `WorkerBound`, and a session emits `DriverChanged` when a worker's
//! reported driver differs from the one the run's journal last recorded for
//! the same host and device. The driver never enters identity — the warning
//! is observational, and the run proceeds either way.

mod common;

use std::sync::Arc;

use common::{config, exec, journal_events, run_with, search_id, temp_store};
use sima_contracts::Executor;
use sima_core::Result;
use sima_domains::{StubBehavior, StubExecutor};
use sima_model::SearchConfig;
use sima_store::Store;
use sima_trace::{Event, Record};
use sima_transport::loopback::SharedResolver;

/// A stub-executor resolver whose workers all report the given device name
/// and driver version, whatever binding they are handed.
fn reporting_resolver(device: &str, driver: &str) -> SharedResolver {
    let device = device.to_string();
    let driver = driver.to_string();
    Arc::new(move |_, _| {
        let executor: Box<dyn Executor> = Box::new(StubExecutor::new()?);
        Ok((executor, device.clone(), driver.clone()))
    })
}

/// Appends a `WorkerBound` record to `cfg`'s journal, modelling a prior
/// session whose child on (`host`, `device`) reported `driver`.
fn seed_bound(
    store: &Store,
    cfg: &SearchConfig,
    host: &str,
    device: &str,
    driver: &str,
) -> Result<()> {
    store.create_search(cfg)?;
    let mut writer = store.journal_writer(&search_id(cfg))?;
    writer.append(
        &Record::stamped(Event::WorkerBound {
            worker: 0,
            device: device.to_string(),
            driver: driver.to_string(),
            host: host.to_string(),
            program: None,
        })
        .to_line()?,
    )
}

/// The `DriverChanged` events in `cfg`'s journal, as (host, device, from, to).
fn changes(store: &Store, cfg: &SearchConfig) -> Vec<(String, String, String, String)> {
    journal_events(store, &search_id(cfg))
        .into_iter()
        .filter_map(|event| match event {
            Event::DriverChanged {
                host,
                device,
                from,
                to,
            } => Some((host, device, from, to)),
            _ => None,
        })
        .collect()
}

/// A session whose worker reports a driver other than the journaled one
/// emits one `DriverChanged` naming both versions, and the run still
/// finalizes: the driver is provenance, never an admission gate.
#[test]
fn a_changed_driver_is_journaled_and_the_run_proceeds() -> Result<()> {
    let (_dir, store) = temp_store();
    let cfg = config(7, vec![StubBehavior::Succeed]);
    seed_bound(&store, &cfg, "", "stub gpu", "570.86.15")?;

    run_with(
        &store,
        &cfg,
        &exec(1, 3, 60_000),
        reporting_resolver("stub gpu", "580.65.6"),
    )?;

    assert_eq!(
        changes(&store, &cfg),
        vec![(
            String::new(),
            "stub gpu".to_string(),
            "570.86.15".to_string(),
            "580.65.6".to_string(),
        )]
    );
    let events = journal_events(&store, &search_id(&cfg));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::RunFinalized { .. })),
        "the warning never blocks the run: {events:?}"
    );
    Ok(())
}

/// A session whose worker reports the journaled driver emits nothing.
#[test]
fn the_journaled_driver_stays_silent() -> Result<()> {
    let (_dir, store) = temp_store();
    let cfg = config(7, vec![StubBehavior::Succeed]);
    seed_bound(&store, &cfg, "", "stub gpu", "580.65.6")?;

    run_with(
        &store,
        &cfg,
        &exec(1, 3, 60_000),
        reporting_resolver("stub gpu", "580.65.6"),
    )?;

    assert_eq!(changes(&store, &cfg), Vec::new());
    Ok(())
}

/// A fresh run has no journaled driver to differ from, so its first session
/// emits nothing whatever its workers report.
#[test]
fn a_fresh_run_emits_no_change() -> Result<()> {
    let (_dir, store) = temp_store();
    let cfg = config(7, vec![StubBehavior::Succeed]);

    run_with(
        &store,
        &cfg,
        &exec(1, 3, 60_000),
        reporting_resolver("stub gpu", "580.65.6"),
    )?;

    assert_eq!(changes(&store, &cfg), Vec::new());
    Ok(())
}

/// One change on one (host, device) is journaled once, however many slots
/// spawn on it: the comparison state advances with the first spawn, so the
/// siblings and every respawn see the current driver as the recorded one.
#[test]
fn one_change_is_journaled_once_across_slots() -> Result<()> {
    let (_dir, store) = temp_store();
    let cfg = config(
        7,
        vec![
            StubBehavior::Succeed,
            StubBehavior::Succeed,
            StubBehavior::Succeed,
            StubBehavior::Succeed,
        ],
    );
    seed_bound(&store, &cfg, "", "stub gpu", "570.86.15")?;

    run_with(
        &store,
        &cfg,
        &exec(4, 3, 60_000),
        reporting_resolver("stub gpu", "580.65.6"),
    )?;

    assert_eq!(changes(&store, &cfg).len(), 1, "one warning per transition");
    Ok(())
}
