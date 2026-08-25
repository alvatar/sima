//! The values a rental test starts from: a rented specification, the rental
//! that draws on it, a store with a run to own acquisitions, and the offers a
//! stub marketplace lists.
//!
//! Both halves of this namespace build the same handful of things to exercise
//! anything, so they are built once here rather than twice. Compiled only for
//! the tests, which are the whole of what reaches them.

use std::path::PathBuf;
use std::time::Duration;

use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, RunConfig, RunId};
use sima_provider::{Constraints, Offer, OfferId, Price, Provider};
use sima_scheduler::ExecutionConfig;
use sima_store::Store;
use sima_transport::SpawnMode;
use tempfile::TempDir;

use crate::config::{FillPolicy, ProviderId, Rented};
use crate::fleet::Rental;
use crate::rental::acquire::{RentalGroup, RentedHost};

/// A rented specification reaching the stub control plane, polling without
/// waiting so a probe retry never sleeps in tests.
pub(super) fn spec() -> Rented {
    Rented {
        provider: ProviderId::Stub,
        image: "ghcr.io/alvatar/sima:latest".to_string(),
        disk_gb: 32,
        ready_timeout: Duration::from_millis(500),
        ready_poll: Duration::ZERO,
        constraints: Constraints::default(),
    }
}

/// A rental of `count` machines under `fill`, over `spec`.
pub(super) fn rental(spec: &Rented, count: usize, fill: FillPolicy) -> Rental<'_> {
    Rental {
        name: "rented",
        spec,
        count,
        fill,
        // The machines these fixtures rent are given no program, so neither
        // path a delivery would take is exercised through them.
        root: "~/sima-runs",
        binary: "sima",
    }
}

/// The one group a single-rental test supervises.
pub(super) fn one_group<'a>(
    provider: &'a (dyn Provider + Sync),
    spec: &'a Rented,
    fill: FillPolicy,
    hosts: Vec<RentedHost<'a>>,
) -> Vec<RentalGroup<'a>> {
    vec![RentalGroup {
        provider,
        spec,
        fill,
        hosts,
    }]
}

/// A generous stub offer at `price` micro-USD/hour, distinct by `id`.
pub(super) fn offer(id: &str, price: u64) -> Offer {
    Offer {
        id: OfferId(id.to_string()),
        machine: format!("machine-{id}"),
        gpu_model: "stub-gpu".to_string(),
        gpu_count: 1,
        vram_mb: 24_000,
        price: Price(price),
        reliability: 1.0,
        verified: true,
        disk_gb: 1_000,
        bandwidth_mbps: 10_000,
        location: String::new(),
    }
}

/// A store over a fresh temp directory and a run id to own acquisitions.
pub(super) fn acquisition_env() -> (TempDir, Store, RunId) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open store");
    let run = RunConfig {
        root_seed: 1,
        segments: None,
        format: FormatId::new("stub.v1").expect("format id"),
        generator: GeneratorConfig {
            id: GeneratorId::new("stub.v1").expect("generator id"),
            params: Vec::new(),
        },
        params: Params { bytes: vec![1] },
    }
    .id();
    (dir, store, run)
}

/// The execution settings the transport carries; no checkpoint cadence.
pub(super) fn exec() -> ExecutionConfig {
    ExecutionConfig::new(1, 3, Duration::MAX, Duration::MAX, Duration::MAX, None)
        .expect("execution config")
}

/// A local probe that enumerates no device, so every acquired machine derives a
/// single deviceless slot without a real worker binary or GPU.
pub(super) fn deviceless_probe() -> SpawnMode {
    SpawnMode::Local(PathBuf::from("/bin/true"))
}
