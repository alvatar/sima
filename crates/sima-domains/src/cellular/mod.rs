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
//! Which compute substrate a kernel runs on is the `CellularEngine` seam: one
//! operation wide, with one implementation per substrate. The dispatch harness
//! in `step` and the stats reduction in `reduce` are the WGSL implementation's
//! half of it, reached through `WgslEngine`.

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

pub(crate) use engine::{CellularEngine, CellularEvaluation, EvaluationInput};
pub(crate) use reduce::{GridPair, REDUCE_WGSL, ReduceKernels, reduce};
pub(crate) use wgsl_engine::WgslEngine;
