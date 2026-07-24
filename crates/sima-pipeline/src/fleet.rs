//! Fleet dispatch: the config's `provider` id resolved to a control-plane
//! backend and the transport mode its instances are reached through.
//!
//! The pipeline is where provider choice becomes concrete, so this is the one
//! edge from configuration to a boxed [`Provider`]. A run that names no
//! `[fleet]` never reaches here, so it constructs no provider and reads no
//! `VAST_API_KEY`.

use std::thread;
use std::time::Duration;

use sima_contracts::DeviceBinding;
use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_model::FormatId;
use sima_provider::stub::StubProvider;
use sima_provider::{
    AcquireLimits, InstanceGuard, Objective, Offer, OfferId, Price, Provider, SshEndpoint, acquire,
};
use sima_provider_vast::{VastConfig, VastProvider};
use sima_scheduler::ExecutionConfig;
use sima_store::{RunLock, Store};
use sima_transport::{FleetMode, FleetTransport, SshTarget};

use crate::config::{FillPolicy, FleetConfig, FleetProvider};
use crate::devices::parse_enumeration;
use crate::orchestrate::{command_stdout, worker_binary};

/// Builds the control-plane backend the fleet acquires instances through.
///
/// The `vast` backend reads its key from `VAST_API_KEY`; an absent key is an
/// [`Error::Provider`](sima_core::Error::Provider) naming the variable, raised
/// here before any store mutation. The `stub` backend is in-process, listing a
/// generous always-available marketplace so a stub fleet fills its declared
/// count. An unknown id never reaches here — the config load rejects it.
pub(crate) fn provider_for(fleet: &FleetConfig) -> Result<Box<dyn Provider>> {
    match fleet.provider {
        FleetProvider::Vast => {
            let config = VastConfig::from_env(&fleet.image, fleet.disk_gb)?;
            Ok(Box::new(VastProvider::new(config)))
        }
        FleetProvider::Stub => Ok(Box::new(StubProvider::new(stub_offers(fleet.count)))),
    }
}

/// The transport mode the fleet's instances are reached through: ssh to a real
/// rented instance, or a local `sima-worker` spawn for the stub, so the stub
/// exercises every layer above the transport with no network.
pub(crate) fn transport_mode(fleet: &FleetConfig) -> Result<FleetMode> {
    match fleet.provider {
        FleetProvider::Vast => Ok(FleetMode::Ssh),
        FleetProvider::Stub => Ok(FleetMode::Local(worker_binary()?)),
    }
}

/// Maps a provider's ssh endpoint into the transport's target, the seam that
/// keeps the transport free of any dependency on the provider crate.
pub(crate) fn endpoint_target(endpoint: SshEndpoint) -> SshTarget {
    SshTarget {
        host: endpoint.host,
        port: endpoint.port,
        user: endpoint.user,
    }
}

/// How many times an instance's enumeration probe is retried before its
/// acquisition is abandoned: sshd can lag the provider's `Ready`, so the first
/// probe against a fresh host may be refused.
const PROBE_ATTEMPTS: u32 = 6;

/// One acquired fleet instance: the guard that owns and tears it down, the
/// transport its pool spawns workers through, and the worker slots its probe
/// derived (one per enumerated GPU, or one deviceless slot when it reports
/// none).
pub(crate) struct FleetInstance<'a> {
    /// Ownership of the rented instance; its teardown runs on release or drop.
    pub(crate) guard: InstanceGuard<'a, dyn Provider + 'a>,
    /// The transport spawning this instance's workers.
    pub(crate) transport: FleetTransport,
    /// The instance's host label, for the journal.
    pub(crate) host: String,
    /// One slot per enumerated GPU, or a single deviceless slot.
    pub(crate) slots: Vec<Option<DeviceBinding>>,
}

/// Acquires the fleet's instances, each behind a teardown guard, and builds a
/// transport and worker slots for each.
///
/// Every acquisition is budget-admitted and intent-recorded by
/// [`acquire`](sima_provider::acquire), and an instance that fails to acquire
/// or probe is torn down individually. On a shortfall the fill policy decides:
/// strict tears down everything acquired so far and fails the run; best-effort
/// proceeds with what came up, so long as one instance did.
pub(crate) fn acquire_fleet<'a>(
    fleet: &FleetConfig,
    provider: &'a dyn Provider,
    store: &'a Store,
    lock: &RunLock,
    mode: &FleetMode,
    format: &FormatId,
    exec: &ExecutionConfig,
) -> Result<Vec<FleetInstance<'a>>> {
    let limits = AcquireLimits {
        ready_timeout: fleet.ready_timeout,
        ready_poll: fleet.ready_poll,
    };
    let mut instances: Vec<FleetInstance<'a>> = Vec::with_capacity(fleet.count);
    for _ in 0..fleet.count {
        // An instance that fails to acquire or probe is torn down inside
        // `acquire_one` before its error returns here.
        match acquire_one(provider, store, lock, fleet, &limits, mode, format, exec) {
            Ok(instance) => instances.push(instance),
            Err(error) => match fleet.fill {
                // Strict: the declared count or nothing. Dropping `instances`
                // here tears down every instance already acquired.
                FillPolicy::Strict => return Err(error),
                // Best-effort: run with what came up. Stop asking on the first
                // shortfall — the market is not filling the count.
                FillPolicy::BestEffort => break,
            },
        }
    }
    if instances.is_empty() {
        return Err(Error::Provider(
            "the fleet acquired no instances".to_string(),
        ));
    }
    Ok(instances)
}

/// Acquires one instance, probes it, and builds its transport and slots. On a
/// probe failure the guard drops here, tearing the instance down, so no
/// half-acquired instance leaks.
#[allow(clippy::too_many_arguments)]
fn acquire_one<'a>(
    provider: &'a dyn Provider,
    store: &'a Store,
    lock: &RunLock,
    fleet: &FleetConfig,
    limits: &AcquireLimits,
    mode: &FleetMode,
    format: &FormatId,
    exec: &ExecutionConfig,
) -> Result<FleetInstance<'a>> {
    let guard = acquire(
        provider,
        store,
        lock,
        &fleet.constraints,
        Objective::CheapestPerHour,
        limits,
        &fleet.budget,
    )?;
    let target = endpoint_target(guard.endpoint().clone());
    let host = target.host.clone();
    // The probe drives the instance's device enumeration; a failure drops the
    // guard, tearing the instance down.
    let slots = probe_slots(mode, &target, fleet.ready_poll)?;
    let transport = FleetTransport::new(
        mode.clone(),
        target,
        format.clone(),
        exec.checkpoint_interval,
        exec.checkpoint_interval_steps,
    );
    Ok(FleetInstance {
        guard,
        transport,
        host,
        slots,
    })
}

/// Probes an instance for its devices and derives its worker slots, retrying
/// briefly because sshd can lag the provider's `Ready`.
fn probe_slots(
    mode: &FleetMode,
    target: &SshTarget,
    poll: Duration,
) -> Result<Vec<Option<DeviceBinding>>> {
    let argv = sima_transport::fleet::probe_argv(mode, target);
    let mut last: Option<Error> = None;
    for attempt in 0..PROBE_ATTEMPTS {
        match command_stdout(&argv).and_then(|stdout| parse_enumeration(&stdout)) {
            Ok(devices) => return Ok(fleet_slots(&devices)),
            Err(error) => {
                last = Some(error);
                // No sleep after the final attempt.
                if attempt + 1 < PROBE_ATTEMPTS {
                    thread::sleep(poll.min(Duration::from_secs(5)));
                }
            }
        }
    }
    Err(last.unwrap_or_else(|| Error::Provider("the instance probe never ran".to_string())))
}

/// One worker slot per enumerated GPU, each bound to its own device; a probe
/// reporting no GPU yields a single deviceless worker — the stub testing path,
/// and any device-free instance.
fn fleet_slots(devices: &[DeviceInfo]) -> Vec<Option<DeviceBinding>> {
    if devices.is_empty() {
        return vec![None];
    }
    devices
        .iter()
        .map(|device| {
            Some(DeviceBinding {
                vendor_id: device.vendor_id,
                device_id: device.device_id,
                member: device.member,
            })
        })
        .collect()
}

/// Releases every fleet instance's guard on the way out, returning the first
/// teardown failure. Every guard is released whatever the others do, so one
/// failure never strands the rest; a guard whose release is not reached is torn
/// down by its drop, and the ledger record a failed teardown leaves is what the
/// next reconciliation pass acts on.
pub(crate) fn release_all(instances: Vec<FleetInstance<'_>>) -> Result<()> {
    let mut first: Option<Error> = None;
    for instance in instances {
        // The transport drops with the instance; only the guard's teardown can
        // fail and is worth reporting.
        if let Err(error) = instance.guard.release()
            && first.is_none()
        {
            first = Some(error);
        }
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The stub marketplace: `count` always-available offers, each generous enough
/// to pass typical constraints, priced distinctly so selection's ranking is
/// deterministic.
fn stub_offers(count: usize) -> Vec<Offer> {
    (0..count.max(1))
        .map(|n| Offer {
            id: OfferId(format!("stub-offer-{n}")),
            gpu_model: "stub-gpu".to_string(),
            gpu_count: 1,
            vram_mb: 24_000,
            // Distinct rates keep the cheapest-per-hour ranking a total order;
            // $0.10/hr and up, low enough to sit under an ordinary price cap.
            price: Price(100_000 + n as u64),
            reliability: 1.0,
            verified: true,
            disk_gb: 1_000,
            bandwidth_mbps: 10_000,
            location: String::new(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use sima_model::{GeneratorConfig, GeneratorId, Params, RunConfig, RunId};
    use sima_provider::stub::StubProvider;
    use sima_provider::{InstanceStatus, Provision};
    use tempfile::TempDir;

    use super::*;
    use crate::config::{FillPolicy, FleetConfig};

    /// A stub fleet requesting `count` instances, permissive constraints.
    fn stub_fleet(count: usize) -> FleetConfig {
        fleet_config(count, FillPolicy::Strict)
    }

    /// A stub fleet requesting `count` instances under `fill`.
    fn fleet_config(count: usize, fill: FillPolicy) -> FleetConfig {
        FleetConfig {
            provider: FleetProvider::Stub,
            count,
            fill,
            image: "ghcr.io/alvatar/sima-worker:latest".to_string(),
            disk_gb: 32,
            // Poll without waiting so a probe retry never sleeps in tests.
            ready_timeout: Duration::from_millis(500),
            ready_poll: Duration::ZERO,
            constraints: sima_provider::Constraints::default(),
            budget: sima_provider::Budget::default(),
        }
    }

    /// A generous stub offer at `price` micro-USD/hour, distinct by `id`.
    fn offer(id: &str, price: u64) -> Offer {
        Offer {
            id: OfferId(id.to_string()),
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
    fn acquisition_env() -> (TempDir, Store, RunId) {
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
    fn exec() -> ExecutionConfig {
        ExecutionConfig::new(1, 3, Duration::MAX, Duration::MAX, None).expect("execution config")
    }

    /// A local probe that enumerates no device, so every acquired instance
    /// derives a single deviceless slot without a real worker binary or GPU.
    fn deviceless_probe() -> FleetMode {
        FleetMode::Local(PathBuf::from("/bin/true"))
    }

    #[test]
    fn a_strict_shortfall_tears_down_what_was_acquired_and_fails() -> Result<()> {
        // One offer for two requested instances under strict fill: the run
        // fails, and the one instance that came up is torn down.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let result = acquire_fleet(
            &fleet_config(2, FillPolicy::Strict),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        );
        assert!(matches!(result, Err(Error::Provider(_))));
        assert_eq!(
            provider.destroyed().len(),
            1,
            "the acquired instance is torn down"
        );
        assert!(provider.live().is_empty(), "no instance is left running");
        Ok(())
    }

    #[test]
    fn a_best_effort_shortfall_proceeds_with_what_came_up() -> Result<()> {
        // One offer for two requested instances under best-effort: the run
        // proceeds with the one instance, torn down on release.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let instances = acquire_fleet(
            &fleet_config(2, FillPolicy::BestEffort),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        assert_eq!(instances.len(), 1, "best-effort runs on what came up");
        assert!(
            provider.destroyed().is_empty(),
            "still running before release"
        );
        release_all(instances)?;
        assert_eq!(
            provider.destroyed().len(),
            1,
            "release tears the instance down"
        );
        assert!(provider.live().is_empty());
        Ok(())
    }

    #[test]
    fn the_fleet_acquires_and_probes_every_instance() -> Result<()> {
        // Two offers for two instances: both acquire, each probed into a single
        // deviceless slot, all torn down on release.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000), offer("b", 200_000)]);
        let format = FormatId::new("stub.v1")?;
        let instances = acquire_fleet(
            &fleet_config(2, FillPolicy::Strict),
            &provider,
            &store,
            &lock,
            &deviceless_probe(),
            &format,
            &exec(),
        )?;
        assert_eq!(instances.len(), 2);
        for instance in &instances {
            assert_eq!(
                instance.slots,
                vec![None],
                "a probe reporting no GPU is one slot"
            );
        }
        release_all(instances)?;
        assert_eq!(provider.destroyed().len(), 2);
        assert!(provider.live().is_empty());
        Ok(())
    }

    #[test]
    fn a_probe_failure_tears_the_instance_down() -> Result<()> {
        // The instance acquires but its probe never runs: the instance is torn
        // down rather than left running with no slots.
        let (_dir, store, run) = acquisition_env();
        let lock = store.acquire_run_lock(&run)?;
        let provider = StubProvider::new(vec![offer("a", 100_000)]);
        let format = FormatId::new("stub.v1")?;
        let result = acquire_fleet(
            &fleet_config(1, FillPolicy::Strict),
            &provider,
            &store,
            &lock,
            &FleetMode::Local(PathBuf::from("/nonexistent/sima-worker")),
            &format,
            &exec(),
        );
        assert!(result.is_err(), "a probe failure fails the acquisition");
        assert_eq!(provider.destroyed().len(), 1, "the instance is torn down");
        assert!(provider.live().is_empty());
        Ok(())
    }

    #[test]
    fn a_vast_fleet_is_reached_over_ssh() -> Result<()> {
        // The transport mode is a pure function of the provider: vast over ssh,
        // read without touching the environment (only the provider itself reads
        // the key).
        let mut fleet = stub_fleet(1);
        fleet.provider = FleetProvider::Vast;
        assert!(matches!(transport_mode(&fleet)?, FleetMode::Ssh));
        Ok(())
    }

    #[test]
    fn the_stub_provider_lists_an_offer_per_requested_instance() -> Result<()> {
        let provider = provider_for(&stub_fleet(3))?;
        assert_eq!(provider.id(), "stub");
        assert_eq!(provider.offers()?.len(), 3);
        Ok(())
    }

    #[test]
    fn the_stub_provider_acquires_an_instance_that_reaches_ready() -> Result<()> {
        // The stub acquires: provisioning an offer yields an instance that its
        // own status call reports Ready with an ssh endpoint, which maps to a
        // transport target.
        let provider = provider_for(&stub_fleet(1))?;
        let offer = provider.offers()?.into_iter().next().expect("an offer");
        let Provision::Provisioned(instance) = provider.provision(&offer.id, "tag-0")? else {
            panic!("the stub provisions an always-available offer");
        };
        let InstanceStatus::Ready(endpoint) = provider.instance(&instance.id)? else {
            panic!("the stub instance is ready at once");
        };
        let target = endpoint_target(endpoint.clone());
        assert_eq!(target.host, endpoint.host);
        assert_eq!(target.port, endpoint.port);
        assert_eq!(target.user, endpoint.user);
        Ok(())
    }
}
