//! Domains: the executable substance a format id binds.
//!
//! A domain groups everything the infrastructure needs to run one format's
//! candidates: the executor that evaluates its specs, the environment its
//! results depend on, the generator that produces its specs, the codecs that
//! give specs and params their canonical bytes, and the translation of the
//! human-facing TOML config sections into those bytes. The [`Domain`] type and
//! the id dispatch ([`domain_for`], [`generator_for`], and the two translation
//! entries) are the crate's surface; each domain's pieces live in its own
//! module under `domains/`.
//!
//! The pipeline calls this crate to resolve a config's format and generator
//! ids to code; the scheduler tests use the stub domain as a deterministic,
//! programmable substrate through a dev-dependency. `sima-contracts` sits
//! below and holds the traits alone.

mod domain;
mod domains;

pub use domain::{Domain, domain_for, generator_for, generator_params_for, params_for};
pub use domains::stub::{
    StubBehavior, StubExecutor, StubGenerator, StubGeneratorConfig, StubProgram,
};
