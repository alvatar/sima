//! Identity layer: the types whose bytes participate in content addressing.
//!
//! `sima-model` defines the identity-bearing vocabulary — spec, params,
//! environment, task identity and key, task result record, run config and
//! run id — as pure data with canonical [`sima_core::Enc`]/[`sima_core::Dec`]
//! encodings. Every canonical encoding is a str-framed domain tag
//! (`sima.<name>.v1`) followed by the type's fields in the order its
//! `encode` method documents, and a value's id is the blake3 hash of its
//! standalone bytes, so the id doubles as the content-addressed object
//! address. Observational and
//! human-readable data (journals, manifest JSON, execution metadata) lives
//! in higher crates.

mod canonical;
mod environment;
mod params;
mod run_config;
mod spec;
mod task;
mod task_record;
#[cfg(test)]
mod testutil;

pub use environment::{Environment, EnvironmentComponent, EnvironmentId, EnvironmentValue};
pub use params::{Params, ParamsId};
pub use run_config::{GeneratorConfig, GeneratorId, RunConfig, RunId};
pub use spec::{FormatId, Spec, SpecId};
pub use task::{TaskIdentity, TaskKey};
pub use task_record::{ArtifactRef, TaskRecord};
