//! [`VastProvider`]: the marketplace behind the provider contract.

use sima_core::Result;
use sima_provider::{
    InstanceId, InstanceStatus, Offer, OfferId, Provider, Provision, TaggedInstance,
};

use crate::client::VastClient;
use crate::config::VastConfig;
use crate::{instances, offers, rental};

/// The identifier ledger records carry for machines rented here. It is
/// public so a caller resolving a backend from a ledger record's provider
/// field matches on the id this crate answers with, rather than on a copy
/// of the literal.
pub const PROVIDER_ID: &str = "vastai";

/// The Vast.ai backend, holding the client every call goes through and the
/// shape of the rentals it creates.
pub struct VastProvider {
    /// The authenticated client for the configured API root.
    client: VastClient,
    /// The image, disk, and environment a rental is created with.
    config: VastConfig,
}

impl VastProvider {
    /// A backend reaching the API `config` names, renting the machines it
    /// describes.
    pub fn new(config: VastConfig) -> VastProvider {
        VastProvider {
            client: VastClient::new(&config.base_url, &config.api_key),
            config,
        }
    }
}

impl Provider for VastProvider {
    fn id(&self) -> &'static str {
        PROVIDER_ID
    }

    fn offers(&self) -> Result<Vec<Offer>> {
        offers::search(&self.client)
    }

    fn provision(&self, offer: &OfferId, tag: &str) -> Result<Provision> {
        rental::create(&self.client, &self.config, offer, tag)
    }

    fn instance(&self, id: &InstanceId) -> Result<InstanceStatus> {
        instances::status(&self.client, id)
    }

    /// Every instance the account holds, less those carrying no label: a
    /// ledger record exists only for a tag this backend wrote, so an
    /// unlabeled instance corresponds to no record.
    fn instances(&self) -> Result<Vec<TaggedInstance>> {
        instances::held(&self.client)
    }

    fn destroy(&self, id: &InstanceId) -> Result<()> {
        instances::destroy(&self.client, id)
    }
}

#[cfg(test)]
mod tests {
    use super::{PROVIDER_ID, VastProvider};
    use crate::config::VastConfig;
    use crate::test_server::{ScriptedAnswer, TestServer};
    use sima_core::Result;
    use sima_provider::{Constraints, Objective, Provider, select};

    #[test]
    fn the_backend_lists_the_marketplace_behind_the_contract() -> Result<()> {
        let server = TestServer::new(vec![ScriptedAnswer {
            status: 200,
            body: r#"{"offers": [{"id": 8123456, "gpu_name": "RTX_4090", "num_gpus": 2,
                "gpu_ram": 24564.0, "dph_total": 0.412, "reliability": 0.9871,
                "verification": "verified", "disk_space": 205.7, "inet_down": 1350.4,
                "geolocation": "Warsaw, PL"}]}"#
                .to_string(),
        }]);
        let provider = VastProvider::new(VastConfig {
            base_url: server.url(),
            api_key: "k-secret".to_string(),
            image: "ghcr.io/owner/sima-worker".to_string(),
            disk_gb: 64,
            env: None,
        });
        assert_eq!(provider.id(), PROVIDER_ID);
        let ranked = select(
            provider.offers()?,
            &Constraints::default(),
            Objective::CheapestPerHour,
        );
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].gpu_model, "RTX_4090");
        Ok(())
    }
}
