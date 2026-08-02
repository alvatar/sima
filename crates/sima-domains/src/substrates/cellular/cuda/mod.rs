//! The CUDA half of the cellular substrate: the adapter onto
//! [`sima_toolkit_cuda`], and the kernels this backend ships.
//!
//! Everything the substrate does with a device is written once above the
//! [`CellularOps`](super::ops::CellularOps) boundary; what lives here is the
//! translation into one toolkit's surface, plus the kernels only this backend
//! can execute.
//!
//! The kernels are the deliberate per-backend transcription. `reduce.cu` is
//! written against `wgsl/shaders/reduce.wgsl` pass for pass, over the same
//! fixed partition topology, and the constants the two must agree on — the
//! channel bound, the partition count, the block width — come from
//! [`cellular::reduce`](super::reduce) rather than from either kernel.

mod ops;

pub(crate) use ops::CudaOps;
