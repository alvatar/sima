//! Identity layer: the types whose bytes participate in content addressing.
//!
//! `sima-model` defines the identity-bearing vocabulary — spec, params,
//! environment, task identity and key, task result record, run config and
//! run id — as pure data with canonical [`sima_core::Enc`]/[`sima_core::Dec`]
//! encodings. Every canonical encoding opens with a str-framed domain tag,
//! and a value's id is the blake3 hash of its standalone bytes, so the id
//! doubles as the content-addressed object address. Observational and
//! human-readable data (journals, manifest JSON, execution metadata) lives
//! in higher crates.

mod canon;
mod env;
mod params;
mod spec;
mod task;

pub use env::{EnvComponent, EnvId, EnvValue, Environment};
pub use params::{Params, ParamsId};
pub use spec::{FormatId, Spec, SpecId};
pub use task::{TaskIdentity, TaskKey};
