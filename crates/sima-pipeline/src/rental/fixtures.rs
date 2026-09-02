//! The values a rental test starts from: a rented specification, the rental
//! that draws on it, a store with a search to own acquisitions, and the offers a
//! stub marketplace lists.
//!
//! Both halves of this namespace build the same handful of things to exercise
//! anything, so they are built once here rather than twice. Compiled only for
//! the tests, which are the whole of what reaches them.

use std::path::PathBuf;
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;

use sima_model::{FormatId, GeneratorConfig, GeneratorId, Params, SearchConfig, SearchId};
use sima_provider::{Constraints, Offer, OfferId, Price, Provider};
use sima_scheduler::{Event, ExecutionConfig};
use sima_store::Store;
use sima_trace::Emitter;
use sima_transport::SpawnMode;
use tempfile::TempDir;

use crate::config::{FillPolicy, ProviderId, Rented};
use crate::fleet::Rental;
use crate::rental::acquire::{RentalGroup, RentedHost};

/// An emitter and everything emitted through it, for the tests that read what
/// an acquisition said.
pub(super) fn heard() -> (Emitter, Receiver<Event>) {
    let (sender, heard) = channel();
    (Emitter::from(sender), heard)
}

/// An emitter nothing reads, for the tests whose subject is not the narration.
pub(super) fn unheard() -> Emitter {
    // The receiver is dropped at the end of this expression, so every send
    // through the emitter is a no-op.
    Emitter::from(channel().0)
}

/// A rented specification reaching the stub control plane, polling without
/// waiting so a probe retry never sleeps in tests.
pub(super) fn spec() -> Rented {
    Rented {
        provider: ProviderId::Stub,
        image: "ghcr.io/alvatar/sima:latest".to_string(),
        env: Default::default(),
        bootstrap_sima: false,
        disk_gb: 32,
        ready_timeout: Duration::from_millis(500),
        ready_poll: Duration::ZERO,
        constraints: Constraints::default(),
    }
}

/// A rented specification whose readiness wait is long enough that sitting it
/// out shows plainly in a test's wall clock, and polled often enough that
/// nothing waits on the poll itself. The tests about ending a wait early
/// measure against it.
pub(super) fn waiting_spec() -> Rented {
    Rented {
        ready_timeout: Duration::from_secs(3),
        ready_poll: Duration::from_millis(5),
        ..spec()
    }
}

/// How long one poll of a machine coming up under [`booting_spec`] waits, and
/// how many polls it answers before it is ready. Their product is one member's
/// boot, which is what a rental acquiring its members at once should cost
/// however many it has.
///
/// The count is the stub's own setting, so a test using this spec scripts its
/// provider with [`BOOT_POLLS`] for the two to describe the same machine.
pub(super) const BOOT_POLL: Duration = Duration::from_millis(150);
pub(super) const BOOT_POLLS: u32 = 4;

/// A rented specification whose machines take a boot to come up, under a
/// timeout far longer than one, so what a test measures is the boot.
pub(super) fn booting_spec() -> Rented {
    Rented {
        ready_timeout: Duration::from_secs(10),
        ready_poll: BOOT_POLL,
        ..spec()
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
        root: "~/sima",
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
pub(crate) fn offer(id: &str, price: u64) -> Offer {
    Offer {
        id: OfferId(id.to_string()),
        machine: format!("machine-{id}"),
        gpu_model: "stub-gpu".to_string(),
        gpu_count: 1,
        vram_mb: 24_000,
        cuda: 99.0,
        price: Price(price),
        reliability: 1.0,
        verified: true,
        disk_gb: 1_000,
        bandwidth_mbps: 10_000,
        location: String::new(),
    }
}

/// A store over a fresh temp directory and a search id to own acquisitions.
pub(crate) fn acquisition_env() -> (TempDir, Store, SearchId) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(dir.path()).expect("open store");
    let search = SearchConfig {
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
    (dir, store, search)
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
