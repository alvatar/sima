//! The instance lifecycle over the public surface: rent a machine, hold
//! it, give it back — clean up after a process that never gave it back, and
//! account for what every machine cost.

use std::time::Duration;

use sima_core::{Error, Result};
use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig, RunId};
use sima_provider::stub::StubProvider;
use sima_provider::{
    AcquireLimits, Budget, Constraints, Cost, InstanceId, Objective, Offer, OfferId, Price,
    Provider, Verdict, acquire, assess, reconcile, spend_report,
};
use sima_store::{RunLock, Store};

/// An offer at `price` micro-USD per hour, ample enough for constraints
/// that disqualify nothing.
fn offer(id: &str, price: u64) -> Offer {
    Offer {
        id: OfferId(id.to_string()),
        gpu_model: "RTX 4090".to_string(),
        gpu_count: 1,
        vram_mb: 24_576,
        price: Price(price),
        reliability: 0.99,
        verified: true,
        disk_gb: 100,
        bandwidth_mbps: 1_000,
        location: "eu-west".to_string(),
    }
}

/// A run to own acquisitions with, varying by `root_seed`.
fn owner(root_seed: u64) -> RunId {
    RunConfig {
        root_seed,
        segments: None,
        format: FormatId::new("stub.v1").expect("format id"),
        generator: GeneratorConfig {
            id: GeneratorId::new("gen.v1").expect("generator id"),
            params: vec![0xDE, 0xAD],
        },
        params: Params {
            bytes: vec![1, 2, 3],
        },
    }
    .id()
}

/// Limits that poll without waiting.
fn limits() -> AcquireLimits {
    AcquireLimits {
        ready_timeout: Duration::from_millis(500),
        ready_poll: Duration::ZERO,
    }
}

/// A marketplace of two offers, the cheaper of which another renter takes
/// first.
fn contested_market() -> StubProvider {
    StubProvider::new(vec![offer("cheap", 100_000), offer("dearer", 200_000)])
        .gone_at_provision(OfferId("cheap".to_string()))
}

/// Rents one machine over `provider`, ranked by price, returning the run
/// lock the rental was made under alongside the guard: a caller standing in
/// for a killed process drops the lock to make the owner look gone.
fn rent<'a, P: Provider>(
    provider: &'a P,
    store: &'a Store,
    run: &RunId,
) -> Result<(sima_provider::InstanceGuard<'a, P>, RunLock)> {
    rent_within(provider, store, run, &Budget::default())
}

/// Rents one machine as [`rent`] does, under `budget`.
fn rent_within<'a, P: Provider>(
    provider: &'a P,
    store: &'a Store,
    run: &RunId,
    budget: &Budget,
) -> Result<(sima_provider::InstanceGuard<'a, P>, RunLock)> {
    let lock = store.acquire_run_lock(run)?;
    let guard = acquire(
        provider,
        store,
        &lock,
        &Constraints::default(),
        Objective::CheapestPerHour,
        &limits(),
        budget,
    )?;
    Ok((guard, lock))
}

#[test]
fn a_rental_falls_through_a_lost_offer_and_is_given_back_on_release() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path())?;
    let provider = contested_market();

    let (guard, _lock) = rent(&provider, &store, &owner(11))?;
    assert_eq!(guard.endpoint().user, "root");
    let records = store.instances()?;
    assert_eq!(records.len(), 1, "one record for the machine held");
    assert_eq!(records[0].price_micro_usd_hour, 200_000, "the lost offer");
    assert_eq!(records[0].owner, owner(11).to_string());
    let id = guard.id().clone();

    guard.release()?;
    assert_eq!(provider.destroyed(), vec![id]);
    assert!(provider.live().is_empty());
    assert!(store.instances()?.is_empty());
    Ok(())
}

#[test]
fn a_machine_a_dead_process_left_running_is_destroyed_by_reconciliation() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let provider = StubProvider::new(vec![offer("only", 100_000)]);
    let leaked: InstanceId = {
        let store = Store::open(dir.path())?;
        let (guard, lock) = rent(&provider, &store, &owner(11))?;
        let id = guard.id().clone();
        // Standing in for a process killed outright: no destructor runs, so
        // the machine stays up and its ledger record stays behind, while the
        // kernel frees the run lock the dead process held.
        std::mem::forget(guard);
        drop(lock);
        id
    };

    // A later invocation, over the same store root.
    let store = Store::open(dir.path())?;
    assert_eq!(store.instances()?.len(), 1, "the orphan's record survived");
    let report = reconcile(&provider, &store)?;
    assert_eq!(report.destroyed, vec![leaked.clone()]);
    assert_eq!(provider.destroyed(), vec![leaked]);
    assert!(provider.live().is_empty());
    assert!(store.instances()?.is_empty());
    Ok(())
}

#[test]
fn acquiring_cleans_a_dead_runs_orphan_before_renting_a_new_machine() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let provider = StubProvider::new(vec![offer("first", 100_000), offer("second", 200_000)]);
    let leaked: InstanceId = {
        let store = Store::open(dir.path())?;
        let (guard, lock) = rent(&provider, &store, &owner(11))?;
        let id = guard.id().clone();
        std::mem::forget(guard);
        drop(lock);
        id
    };

    let store = Store::open(dir.path())?;
    let (guard, _lock) = rent(&provider, &store, &owner(12))?;
    // Acquisition reconciles first, so the orphan of the run whose lock is
    // free was down before the new machine came up.
    assert_eq!(provider.destroyed(), vec![leaked]);
    assert_eq!(provider.live(), vec![guard.id().clone()]);
    let records = store.instances()?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tag, guard.tag());
    Ok(())
}

#[test]
fn a_runs_own_leftover_survives_its_next_acquisition_while_it_holds_the_lock() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let provider = StubProvider::new(vec![offer("first", 100_000), offer("second", 200_000)]);
    let leaked: InstanceId = {
        let store = Store::open(dir.path())?;
        let (guard, lock) = rent(&provider, &store, &owner(11))?;
        let id = guard.id().clone();
        std::mem::forget(guard);
        drop(lock);
        id
    };

    let store = Store::open(dir.path())?;
    let (guard, lock) = rent(&provider, &store, &owner(11))?;
    // A held run lock is the owner still running, so the leftover an earlier
    // process of this same run wrote is kept: reconciliation cannot tell it
    // from a machine the live process is using.
    assert!(provider.destroyed().is_empty());
    assert_eq!(provider.live(), vec![leaked.clone(), guard.id().clone()]);
    assert_eq!(store.instances()?.len(), 2);

    // Once the run's lock is free, the leftover is reaped like any orphan.
    std::mem::forget(guard);
    drop(lock);
    let report = reconcile(&provider, &store)?;
    assert_eq!(report.destroyed.len(), 2);
    assert!(report.destroyed.contains(&leaked));
    assert!(store.instances()?.is_empty());
    Ok(())
}

#[test]
fn a_rental_given_back_leaves_what_it_cost_behind() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path())?;
    let provider = StubProvider::new(vec![offer("only", 100_000)]);

    let (guard, _lock) = rent(&provider, &store, &owner(11))?;
    let tag = guard.tag().to_string();
    // A window long enough that the rental is charged for something.
    std::thread::sleep(Duration::from_millis(2));
    guard.release()?;

    // The record is gone with the machine, and what the machine cost stays.
    assert!(store.instances()?.is_empty());
    let entries = store.spend_entries(&owner(11).to_string())?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].tag, tag);
    assert_eq!(entries[0].price_micro_usd_hour, 100_000);
    assert!(entries[0].cost_micro_usd > 0);

    let now_ms = entries[0].ended_ms;
    let report = spend_report(&store, &owner(11), now_ms)?;
    assert!(report.open.is_empty());
    assert_eq!(report.total, Cost(entries[0].cost_micro_usd));
    assert_eq!(
        assess(&store, &owner(11), &Budget::default(), now_ms)?,
        Verdict::Within {
            accrued: report.total,
            deadline_ms: None,
        }
    );
    Ok(())
}

#[test]
fn a_machine_a_dead_process_left_running_is_charged_by_reconciliation() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let provider = StubProvider::new(vec![offer("only", 100_000)]);
    let tag: String = {
        let store = Store::open(dir.path())?;
        let (guard, lock) = rent(&provider, &store, &owner(11))?;
        let tag = guard.tag().to_string();
        // Standing in for a process killed outright: the machine stays up,
        // its record stays behind, and nothing closed the rental out.
        std::mem::forget(guard);
        drop(lock);
        tag
    };

    let store = Store::open(dir.path())?;
    reconcile(&provider, &store)?;
    assert!(store.instances()?.is_empty());
    let entries = store.spend_entries(&owner(11).to_string())?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].tag, tag);
    // The rate the machine was rented at is what it is charged: a rental
    // reaped by reconciliation is booked no differently from one released.
    assert_eq!(entries[0].price_micro_usd_hour, 100_000);
    // The cost of a rental nobody gave back is still the run's to account
    // for.
    let report = spend_report(&store, &owner(11), entries[0].ended_ms)?;
    assert_eq!(report.total, Cost(entries[0].cost_micro_usd));
    Ok(())
}

#[test]
fn a_budget_an_earlier_rental_consumed_refuses_the_next_one() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path())?;
    let provider = StubProvider::new(vec![offer("first", 100_000), offer("second", 200_000)]);

    let (guard, lock) = rent(&provider, &store, &owner(11))?;
    std::thread::sleep(Duration::from_millis(2));
    guard.release()?;
    drop(lock);

    let budget = Budget {
        max_spend: Some(Cost(1)),
        ..Budget::default()
    };
    let outcome = rent_within(&provider, &store, &owner(11), &budget);
    assert!(matches!(
        outcome,
        Err(Error::Provider(message))
            if message.starts_with("the run's rental budget is exhausted: spent ")
    ));
    // Nothing was rented under the refusal: no second machine, no record.
    assert_eq!(provider.live().len(), 0);
    assert!(store.instances()?.is_empty());
    Ok(())
}
