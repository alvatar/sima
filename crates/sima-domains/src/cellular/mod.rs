//! The cellular kind: multi-channel `f32` grid state advanced by a WGSL update
//! kernel dispatched over it, with a CPU reference the GPU path cross-checks
//! against.
//!
//! Reaction-diffusion, Neural CA, and Lenia are all this kind: they share the
//! grid state, the double-buffered dispatch harness, and the CPU-reference
//! scaffold, and differ only in the update kernel, the genome, and the channel
//! count. This module holds that shared substrate; each family supplies its own
//! kernel, genome, and reference.

mod grid;
mod reference;
mod step;

pub use grid::Grid;
pub use reference::CellularRule;
pub use step::run;
