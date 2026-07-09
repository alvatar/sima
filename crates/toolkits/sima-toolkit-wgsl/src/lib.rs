//! WGSL compute toolkit: run GPU compute kernels authored in WGSL without
//! writing raw Vulkan.
//!
//! A domain describes kernels and buffers and orchestrates dispatches through a
//! small surface — [`Context`], [`Buffer`], [`Kernel`] — while the toolkit hides
//! `ash`/Vulkan and compiles WGSL to SPIR-V in process with `naga`. It is an
//! execution backend a domain depends on, a compute library rather than an
//! executor: it holds no store handle and builds no run identity.

mod buffer;
mod compile;
mod context;
mod kernel;
mod selection;
mod validation;

pub use buffer::Buffer;
pub use compile::{COMPILER_ID, source_digest};
pub use context::Context;
pub use kernel::Kernel;
