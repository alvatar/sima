//! Identity layer: the types whose bytes participate in content addressing.
//!
//! `sima-model` defines the identity-bearing vocabulary — spec, params,
//! environment, task identity and key, task result record, search config and
//! search id — as pure data with canonical [`sima_core::Enc`]/[`sima_core::Dec`]
//! encodings. Every canonical encoding is a length-prefixed string domain tag
//! (`sima.<name>.v1`) followed by the type's fields in the order its
//! `encode` method documents, and a value's id is the blake3 hash of its
//! standalone bytes, so the id doubles as the content-addressed object
//! address. Observational and
//! human-readable data (journals, manifest JSON, execution metadata) lives
//! in higher crates.
//!
//! The complete layout of every encoding, tag first, then fields in order.
//! Field notation follows the `sima-core` encode format: `str` and `bytes`
//! are a u64 little-endian byte length then the payload, integers are
//! little-endian at their natural width, `hash` is 32 raw digest bytes,
//! `opt_hash` is a flag byte (0 or 1) then the digest when present, `opt_u64`
//! is a flag byte (0 or 1) then the u64 when present:
//!
//! ```text
//! sima.spec.v1         str format ‖ bytes candidate
//! sima.params.v1       bytes params
//! sima.environment.v1  u64 count ‖ each: str name ‖ u8 arm ‖ (str version | hash digest)
//! sima.task.v1         hash spec ‖ hash params ‖ u64 seed ‖ hash environment ‖ opt_hash input-state
//! sima.task-record.v1  full sima.task.v1 encoding ‖ u64 count ‖ each: str name ‖ hash object
//! sima.run-config.v1   u64 root-seed ‖ opt_u64 segments ‖ str format ‖ str generator ‖
//!                      bytes generator-params ‖ full sima.params.v1 encoding
//! ```
//!
//! Embedded encodings (`sima.task.v1` inside a record, `sima.params.v1`
//! inside a search config) carry their own tag. A worked byte-by-byte example
//! lives in the `canonical` module docs, and every layout is pinned in hex
//! in its module's tests.

mod canonical;
mod environment;
mod params;
mod search_config;
mod spec;
mod task;
mod task_record;
#[cfg(test)]
mod testutil;

pub use environment::{Environment, EnvironmentComponent, EnvironmentId, EnvironmentValue};
pub use params::{Params, ParamsId};
pub use search_config::{GeneratorConfig, GeneratorId, SearchConfig, SearchId};
pub use spec::{FormatId, Spec, SpecId};
pub use task::{TaskIdentity, TaskKey};
pub use task_record::{ArtifactRef, TaskRecord};
