//! [`VastProvider`]: the marketplace behind the provider contract.

use sima_core::Result;
use sima_provider::{Offer, OfferId, Provision};

use crate::client::VastClient;
use crate::config::VastConfig;
use crate::{offers, rental};

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

    /// The marketplace's current on-demand offers, normalized.
    pub fn offers(&self) -> Result<Vec<Offer>> {
        offers::search(&self.client)
    }

    /// Rents `offer`, attaching `tag` to the created instance verbatim.
    pub fn provision(&self, offer: &OfferId, tag: &str) -> Result<Provision> {
        rental::create(&self.client, &self.config, offer, tag)
    }
}
