//! The instance lifecycle over the public surface: rent a machine, hold
//! it, give it back — and clean up after a process that never gave it back.

use std::time::Duration;

use sima_core::Result;
use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig, RunId};
use sima_provider::stub::StubProvider;
use sima_provider::{
    AcquireLimits, Constraints, InstanceId, Objective, Offer, OfferId, Price, Provider, acquire,
    reconcile,
};
use sima_store::Store;

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

/// The run owning these acquisitions.
fn owner() -> RunId {
    RunConfig {
        root_seed: 11,
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

/// Rents one machine over `provider`, ranked by price.
fn rent<'a, P: Provider>(
    provider: &'a P,
    store: &'a Store,
) -> Result<sima_provider::InstanceGuard<'a, P>> {
    acquire(
        provider,
        store,
        &owner(),
        &Constraints::default(),
        Objective::CheapestPerHour,
        &limits(),
    )
}

#[test]
fn a_rental_falls_through_a_lost_offer_and_is_given_back_on_release() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path())?;
    let provider = contested_market();

    let guard = rent(&provider, &store)?;
    assert_eq!(guard.endpoint().user, "root");
    let records = store.instances()?;
    assert_eq!(records.len(), 1, "one record for the machine held");
    assert_eq!(records[0].price_micro_usd_hour, 200_000, "the lost offer");
    assert_eq!(records[0].owner, owner().to_string());
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
        let guard = rent(&provider, &store)?;
        let id = guard.id().clone();
        // Standing in for a process killed outright: no destructor runs, so
        // the machine stays up and its ledger record stays behind.
        std::mem::forget(guard);
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
fn acquiring_again_cleans_the_orphan_before_renting_a_new_machine() -> Result<()> {
    let dir = tempfile::tempdir().expect("create temp dir");
    let provider = StubProvider::new(vec![offer("first", 100_000), offer("second", 200_000)]);
    let leaked: InstanceId = {
        let store = Store::open(dir.path())?;
        let guard = rent(&provider, &store)?;
        let id = guard.id().clone();
        std::mem::forget(guard);
        id
    };

    let store = Store::open(dir.path())?;
    let guard = rent(&provider, &store)?;
    // Acquisition reconciles first, so the orphan was down before the new
    // machine came up.
    assert_eq!(provider.destroyed(), vec![leaked]);
    assert_eq!(provider.live(), vec![guard.id().clone()]);
    let records = store.instances()?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tag, guard.tag());
    Ok(())
}
