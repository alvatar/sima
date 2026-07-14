//! The cellular kind: multi-channel `f32` grid state advanced by a WGSL update
//! kernel dispatched over it on the GPU.
//!
//! Reaction-diffusion, Neural CA, and Lenia are all this kind: they share the
//! grid state and the double-buffered dispatch harness, and differ only in the
//! update kernel, the genome, and the channel count. This module holds that
//! shared substrate, along with [`CellularRule`], a CPU-reference contract used
//! solely to cross-check the harness against an independent implementation in
//! tests. Each family supplies its own kernel and genome, not a reference.

mod grid;
mod prng;
mod reference;
mod step;

pub use grid::Grid;
pub use reference::CellularRule;
pub use step::run;
