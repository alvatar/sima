//! [`VastConfig`]: what the backend needs beyond the provider contract.
//!
//! The trait's methods carry an offer and a tag and nothing else, so the
//! image a rental boots, the disk it gets, and the credentials the API
//! demands are configuration of the backend itself.

use std::collections::BTreeMap;

use sima_core::{Error, Result};

/// The marketplace's public API root.
pub const DEFAULT_BASE_URL: &str = "https://console.vast.ai";

/// The environment variable holding the API key.
pub const API_KEY_VAR: &str = "VAST_API_KEY";

/// Everything a [`VastProvider`](crate::VastProvider) needs: where the API
/// lives, the key that opens it, and the shape of the rentals it creates.
///
/// The key is read from the environment and stays there. Run configuration
/// is content-addressed and identity-bearing, so a key placed in config
/// would enter run hashes and the store.
#[derive(Debug, Clone)]
pub struct VastConfig {
    /// The API root every request is built against.
    pub base_url: String,
    /// The API key sent as a bearer token.
    pub api_key: String,
    /// The container image reference a rental boots.
    pub image: String,
    /// Disk to give the rental, in gigabytes.
    pub disk_gb: u64,
    /// Environment variables passed to the created instance. The API
    /// accepts only a JSON object of name-value pairs and rejects the
    /// CLI's `-e KEY=value` string form with `invalid_args`. `None`
    /// passes none.
    pub env: Option<BTreeMap<String, String>>,
}

impl VastConfig {
    /// Configuration for `image` at `disk_gb`, with the key read from
    /// `VAST_API_KEY` and the API at its public root.
    ///
    /// An absent or empty variable is [`Error::Provider`]: a backend
    /// without a key can reach nothing, and failing at construction names
    /// the cause once instead of at every call.
    pub fn from_env(image: &str, disk_gb: u64) -> Result<VastConfig> {
        VastConfig::keyed(
            std::env::var(API_KEY_VAR).unwrap_or_default(),
            image,
            disk_gb,
        )
    }

    /// Configuration carrying `api_key` verbatim, rejecting an empty one: a
    /// backend without a key reaches nothing, and failing at construction
    /// names the cause once instead of at every call.
    fn keyed(api_key: String, image: &str, disk_gb: u64) -> Result<VastConfig> {
        if api_key.is_empty() {
            return Err(Error::Provider(format!(
                "the vast.ai API key is read from {API_KEY_VAR}, which is unset or empty"
            )));
        }
        Ok(VastConfig {
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key,
            image: image.to_string(),
            disk_gb,
            env: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{API_KEY_VAR, DEFAULT_BASE_URL, VastConfig};
    use sima_core::Error;

    #[test]
    fn a_key_present_in_the_environment_configures_the_public_api_root() {
        let config = VastConfig::keyed("k-secret".to_string(), "ghcr.io/owner/image", 64)
            .expect("a present key configures");
        assert_eq!(config.base_url, DEFAULT_BASE_URL);
        assert_eq!(config.api_key, "k-secret");
        assert_eq!(config.image, "ghcr.io/owner/image");
        assert_eq!(config.disk_gb, 64);
        assert!(config.env.is_none());
    }

    #[test]
    fn an_empty_key_fails_construction_naming_the_variable_it_comes_from() {
        let failure = VastConfig::keyed(String::new(), "ghcr.io/owner/image", 64);
        assert!(matches!(
            failure,
            Err(Error::Provider(message)) if message.contains(API_KEY_VAR)
        ));
    }
}
