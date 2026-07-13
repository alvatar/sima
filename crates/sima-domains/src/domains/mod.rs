//! Per-domain implementations: one module per domain.
//!
//! Each domain module supplies the pieces a format id binds — executor,
//! generator, codecs, environment, and the TOML translation of its config
//! sections — which [`crate::domain`] resolves through its id dispatch.

pub(crate) mod ca_evolution;
pub(crate) mod stub;
