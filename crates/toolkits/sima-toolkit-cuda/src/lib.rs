//! CUDA compute toolkit: run GPU compute kernels authored in CUDA C without
//! writing raw driver-API calls.
//!
//! A domain describes kernels and buffers and orchestrates dispatches through a
//! small surface — [`Context`], [`Buffer`], [`Kernel`] — while the toolkit hides
//! the CUDA driver API behind `cudarc`. It is an execution backend a domain
//! depends on, a compute library rather than an executor: it holds no store
//! handle and builds no run identity.
//!
//! # Kernels ship as PTX
//!
//! A kernel is authored in CUDA C, compiled to PTX once with [`compile`] under
//! [`PTX_OPTIONS`], and the PTX is committed beside its source.
//! [`Context::kernel`] takes that PTX text and the driver's just-in-time
//! compiler turns it into machine code for the card it is loaded on. Nothing
//! compiles CUDA C while a run executes, so no worker needs the CUDA toolkit —
//! only the driver, which arrives with the card.
//!
//! # Block dimensions
//!
//! Launches are one-dimensional. A kernel declares the width of its thread
//! block with `__launch_bounds__`, the toolkit reads that width back from the
//! loaded function, and [`Context::dispatch`] launches blocks of exactly that
//! width — so the block size lives in the kernel source, the way a WGSL
//! `@workgroup_size` does, and a caller sizing a grid reads it from
//! [`Kernel::block_width`] rather than repeating it.
//!
//! # Tests
//!
//! Tests split three ways by what they touch. Pure ones run anywhere and are
//! covered by `cargo test`. Tests that open a [`Context`] need an NVIDIA device
//! and are marked `#[ignore]`, so `cargo test` skips them and hosted CI stays
//! green with no device present:
//!
//! ```text
//! cargo test -p sima-toolkit-cuda -- --ignored
//! ```
//!
//! Tests that call [`compile`] additionally need the CUDA toolkit installed for
//! `libnvrtc`, and say so in their own `#[ignore]` reason.
//!
//! `cudarc` opens the CUDA libraries at run time, so the crate builds with no
//! CUDA toolkit and no driver present.

mod buffer;
mod compile;
mod context;
mod dispatch;
mod driver;
mod kernel;
mod selection;

pub use buffer::Buffer;
pub use compile::{COMPILER_ID, PTX_OPTIONS, compile};
pub use context::Context;
pub use kernel::Kernel;
pub use selection::{DeviceInfo, DeviceType, enumerate_devices, selected_device_desc};
