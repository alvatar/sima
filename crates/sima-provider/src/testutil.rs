//! Fixtures shared by the crate's test modules.

use std::time::Duration;

use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig, RunId};
use sima_store::{InstanceRecord, InstanceRecordState, Store};
use tempfile::TempDir;

use sima_core::Result;

use crate::acquire::{AcquireLimits, acquire};
use crate::guard::InstanceGuard;
use crate::offer::{Constraints, Objective, Offer, OfferId, Price};
use crate::provider::Provider;

/// An offer at `price` micro-USD per hour, with hardware ample enough that
/// default constraints admit it.
pub(crate) fn stub_offer(id: &str, price: u64) -> Offer {
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

/// Opens a store over a fresh temporary directory, keeping the directory
/// guard alive for the test's duration.
pub(crate) fn temp_store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path()).expect("open temp store");
    (dir, store)
}

/// A ledger record for `tag` in `state`, owned by `owner`.
pub(crate) fn instance_record(
    tag: &str,
    state: InstanceRecordState,
    owner: RunId,
) -> InstanceRecord {
    InstanceRecord {
        tag: tag.to_string(),
        provider: "stub".to_string(),
        owner: owner.to_string(),
        state,
        price_micro_usd_hour: 100_000,
        created_ms: 1_700_000_000_000,
    }
}

/// The live record state naming `instance`.
pub(crate) fn live_state(instance: &str) -> InstanceRecordState {
    InstanceRecordState::Live {
        instance: instance.to_string(),
    }
}

/// A run id to own acquisitions with, varying by `root_seed`.
pub(crate) fn sample_run(root_seed: u64) -> RunId {
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

/// Limits that poll without waiting, so no test sleeps.
pub(crate) fn prompt_limits() -> AcquireLimits {
    AcquireLimits {
        ready_timeout: Duration::from_millis(500),
        ready_poll: Duration::ZERO,
    }
}

/// Rents one machine over `provider` under constraints that disqualify
/// nothing, ranked by the only objective.
///
/// The run lock lives for the call alone, which is what a test needing only
/// a guard requires; a test whose assertions depend on the lock still being
/// held takes it itself and calls [`acquire`] directly.
pub(crate) fn acquire_any<'a, P: Provider>(
    provider: &'a P,
    store: &'a Store,
) -> Result<InstanceGuard<'a, P>> {
    let lock = store.acquire_run_lock(&sample_run(7))?;
    acquire(
        provider,
        store,
        &lock,
        &Constraints::default(),
        Objective::CheapestPerHour,
        &prompt_limits(),
    )
}
