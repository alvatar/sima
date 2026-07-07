//! Pipeline layer: the human-facing configuration in, a driven run out.
//!
//! A `sima.toml` is loaded and translated into the identity-bearing
//! [`sima_model::RunConfig`] plus the operational execution settings; the
//! format id dispatches to a [`Family`] — the executor that evaluates the
//! format's specs, the environment that enters task identity, and the
//! translation of the family-owned params section — and the generator id
//! dispatches to a generator with its own config translation. The pipeline
//! routes configuration sections to the family and generator code that owns
//! them; it never interprets their content.

mod config;
mod family;
mod stub;

pub use config::{LoadedConfig, load};
pub use family::{Family, family_for, generator_for, generator_params_for, params_for};
