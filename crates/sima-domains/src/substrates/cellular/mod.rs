//! The cellular kind: multi-channel `f32` grid state advanced by an update
//! kernel dispatched over it on the GPU.
//!
//! Reaction-diffusion, Neural CA, and Lenia are all this kind: they share the
//! grid state and the double-buffered dispatch harness, and differ only in the
//! update kernel, the genome, and the channel count. This module holds that
//! shared substrate. `reference` holds the CPU implementation the harness is
//! cross-checked against and the smoke kernels its own tests dispatch, compiled
//! only for those tests: a family supplies a kernel and a genome, never a
//! reference.
//!
//! # One substrate, two backends
//!
//! Which compute backend a kernel runs on is the `CellularOps` boundary in
//! `ops`. Above it everything is written once and monomorphized: the dispatch
//! harness in `harness`, the stats reduction in `reduce`, and the engine in
//! `backend`, which is the sole `CellularEngine` implementation. `WgslEngine`
//! and `CudaEngine` name two instantiations of it.
//!
//! Below the boundary sit `wgsl` and `cuda`, each about fifty lines of
//! translation onto one toolkit's surface, plus the kernels only that backend
//! can execute. The kernels are the deliberate transcription — a shader and a
//! CUDA source written against each other — and every constant the two must
//! agree on lives in `reduce`, so neither can drift into folding differently.

mod backend;
mod cuda;
mod engine;
mod grid;
mod harness;
mod ops;
mod prng;
mod reduce;
mod wgsl;

/// The CPU reference and the smoke kernels the substrate's own tests dispatch.
/// Scaffolding, so it is compiled only for them.
#[cfg(test)]
mod reference;

pub use grid::Grid;
pub use reduce::scalar_names;

pub(crate) use engine::{CellularEngine, CellularEvaluation, EvaluationInput};
pub(crate) use reduce::BLOCK_WIDTH;

/// The cellular engine on the WGSL backend.
pub(crate) type WgslEngine = backend::CellularBackend<wgsl::WgslOps>;
/// The cellular engine on the CUDA backend.
pub(crate) type CudaEngine = backend::CellularBackend<cuda::CudaOps>;
