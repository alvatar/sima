//! The Vast.ai backend: the [`Provider`](sima_provider::Provider) contract
//! against a GPU marketplace's REST API.
//!
//! The crate is one of the backends under `crates/providers/`, a namespace
//! mirroring `crates/toolkits/`: each backend isolates the dependency set
//! its service needs, and only this crate carries an HTTP client.
//!
//! Everything the marketplace reports is normalized here, so what leaves
//! the crate is the provider-agnostic offer and instance model. Selection
//! stays above: the backend narrows its query to what defines its scope —
//! rentable machines on demand — and every constraint is applied by
//! [`select`](sima_provider::select).
//!
//! The API key is read from the environment and never enters run
//! configuration, which is content-addressed and would carry it into run
//! hashes and the store.

mod client;
mod config;
mod instances;
mod offers;
mod price;
mod provider;
mod rental;
#[cfg(test)]
mod test_server;

pub use config::{API_KEY_VAR, DEFAULT_BASE_URL, VastConfig};
pub use provider::VastProvider;
