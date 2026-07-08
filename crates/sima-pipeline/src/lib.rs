//! Pipeline layer: the human-facing configuration in, a driven run out.
//!
//! A `sima.toml` is loaded and translated into the identity-bearing
//! [`sima_model::RunConfig`] plus the operational execution settings; the
//! format id dispatches through [`sima_domains`] to the executor that
//! evaluates the format's specs, the environment that enters task identity,
//! and the translation of the domain-owned params section, and the generator
//! id dispatches to a generator with its own config translation. The pipeline
//! routes configuration sections to the domain and generator code that owns
//! them; it never interprets their content.

mod config;
mod orchestrate;
mod status;

pub use config::{LoadedConfig, load};
pub use orchestrate::orchestrate;
// The scheduler types a caller drives and observes runs through, re-exported
// so the CLI consumes one coherent surface.
pub use sima_scheduler::{LifecycleEvent, RunControl, RunOutcome};
pub use status::{RunState, RunStatus, status};
