//! The surface an out-of-tree executor or generator is written against.
//!
//! This crate holds **re-exports and nothing else** — no types, no functions,
//! no logic. Its purpose is to name a surface: an implementation depends on
//! this crate alone, and the crates behind it stay free to reorganise as long
//! as the facade keeps naming the same items.
//!
//! What it publishes:
//!
//! - the two seams — [`Executor`] and [`Generator`] — with the vocabulary they
//!   exchange: [`TaskInput`], [`ExecutionContext`], [`Outcome`], [`Artifact`],
//!   [`Stats`], [`WorkerId`], [`STATE_ARTIFACT`], and the [`Checkpoint`]
//!   resume channel with its inert [`NoCheckpoint`] handle;
//! - the device vocabulary: [`DeviceBinding`] and [`DeviceClass`], which name
//!   the device an executor is built for, and [`DeviceInfo`] with
//!   [`DeviceType`], which are how a domain answers what its work runs on;
//! - the identity-bearing values a seam is handed: [`Spec`], [`Params`],
//!   [`FormatId`], [`GeneratorId`], and the [`Environment`] vocabulary;
//! - the foundations those values are built on: [`Error`] and [`Result`],
//!   [`struct@Hash`] and [`hash_bytes`], the [`Codec`]/[`Enc`]/[`Dec`] canonical
//!   encoding, and the [`prng`] module.
//!
//! [`prng`] is published because result-affecting randomness must be
//! bit-identical across substrates: a generator draws from it rather than from
//! a dependency whose stream can shift under semver, and the same arithmetic is
//! implemented on CPU and GPU.
//!
//! # What is deliberately absent
//!
//! The omissions are the surface's shape, not gaps in it. Each names a
//! responsibility that belongs to the other side of the seam:
//!
//! - **run-level configuration** (`RunConfig`, `RunId`, `GeneratorConfig`) is
//!   the orchestrator's, never an executor's;
//! - **identity and commitment** (`TaskKey`, `TaskIdentity`, `TaskRecord`,
//!   `ArtifactRef`) are the worker's side: an executor receives loaded bytes
//!   and returns artifacts, and the worker keys and commits them;
//! - **content addresses** (`SpecId`, `ParamsId`) address nothing an executor
//!   reaches, because it is handed resolved values rather than references;
//! - **transport framing** (`read_frame`, `write_frame`, `MAX_PAYLOAD`) carries
//!   a seam's values between processes and is the transport's own;
//! - **crash injection** (`crashpoint`) is test-only failure injection;
//! - **free-function hex** (`to_hex`, `from_hex`) is covered for a third party
//!   by [`Hash::from_hex`] and [`struct@Hash`]'s `Display`.

pub use sima_contracts::{
    Artifact, Checkpoint, DeviceBinding, DeviceClass, DeviceInfo, DeviceType, ExecutionContext,
    Executor, Generator, NoCheckpoint, Outcome, STATE_ARTIFACT, Stats, TaskInput, WorkerId,
};
pub use sima_core::prng;
pub use sima_core::{Codec, Dec, Enc, Error, Hash, Result, hash_bytes};
pub use sima_model::{
    Environment, EnvironmentComponent, EnvironmentId, EnvironmentValue, FormatId, GeneratorId,
    Params, Spec,
};
