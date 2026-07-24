//! Fleet dispatch: the config's `provider` id resolved to a control-plane
//! backend and the transport mode its instances are reached through.
//!
//! The pipeline is where provider choice becomes concrete, so this is the one
//! edge from configuration to a boxed [`Provider`]. A run that names no
//! `[fleet]` never reaches here, so it constructs no provider and reads no
//! `VAST_API_KEY`.

use sima_core::Result;
use sima_provider::stub::StubProvider;
use sima_provider::{Offer, OfferId, Price, Provider, SshEndpoint};
use sima_provider_vast::{VastConfig, VastProvider};
use sima_transport::{FleetMode, SshTarget};

use crate::config::{FleetConfig, FleetProvider};
use crate::orchestrate::worker_binary;

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
    use std::time::Duration;

    use sima_provider::{InstanceStatus, Provision};

    use super::*;
    use crate::config::{FillPolicy, FleetConfig};

    /// A stub fleet requesting `count` instances, permissive constraints.
    fn stub_fleet(count: usize) -> FleetConfig {
        FleetConfig {
            provider: FleetProvider::Stub,
            count,
            fill: FillPolicy::Strict,
            image: "ghcr.io/alvatar/sima-worker:latest".to_string(),
            disk_gb: 32,
            ready_timeout: Duration::from_millis(600_000),
            ready_poll: Duration::from_millis(5_000),
            constraints: sima_provider::Constraints::default(),
            budget: sima_provider::Budget::default(),
        }
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
