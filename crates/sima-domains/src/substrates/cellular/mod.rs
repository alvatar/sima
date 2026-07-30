//! The cellular kind: multi-channel `f32` grid state advanced by an update
//! kernel dispatched over it on the GPU.
//!
//! Reaction-diffusion, Neural CA, and Lenia are all this kind: they share the
//! grid state and the double-buffered dispatch harness, and differ only in the
//! update kernel, the genome, and the channel count. This module holds that
//! shared substrate, along with [`CellularRule`], a CPU-reference contract used
//! solely to cross-check the harness against an independent implementation in
//! tests. Each family supplies its own kernel and genome, not a reference.
//!
//! Which compute backend a kernel runs on is the `CellularEngine` boundary: one
//! operation wide, with one implementation per backend. The dispatch harness
//! in `step` and the stats reduction in `reduce` are the WGSL implementation's
//! half of it, reached through `WgslEngine`; their CUDA counterparts live in
//! `cuda`, reached through `CudaEngine`. What the two share rather than
//! transcribe — the scalar naming, the channel bound, the partition count —
//! lives in `reduce` and is used by both.

mod cuda;
mod cuda_engine;
mod engine;
mod grid;
mod prng;
mod reduce;
mod reference;
mod step;
mod wgsl_engine;

pub use grid::Grid;
pub use reduce::scalar_names;
pub use reference::CellularRule;
pub use step::{Trajectory, run};

pub(crate) use cuda_engine::CudaEngine;
pub(crate) use engine::{CellularEngine, CellularEvaluation, EvaluationInput};
pub(crate) use reduce::{
    GridPair, MAX_CHANNELS, PARTITIONS, REDUCE_WGSL, ReduceKernels, name_scalars, reduce,
};
pub(crate) use wgsl_engine::WgslEngine;
