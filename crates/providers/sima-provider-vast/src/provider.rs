//! [`VastProvider`]: the marketplace behind the provider contract.

use sima_core::Result;
use sima_provider::Offer;

use crate::client::VastClient;
use crate::config::VastConfig;
use crate::offers;

/// The Vast.ai backend, holding the client every call goes through.
pub struct VastProvider {
    /// The authenticated client for the configured API root.
    client: VastClient,
}

impl VastProvider {
    /// A backend reaching the API `config` names, renting the machines it
    /// describes.
    pub fn new(config: VastConfig) -> VastProvider {
        VastProvider {
            client: VastClient::new(&config.base_url, &config.api_key),
        }
    }

    /// The marketplace's current on-demand offers, normalized.
    pub fn offers(&self) -> Result<Vec<Offer>> {
        offers::search(&self.client)
    }
}
