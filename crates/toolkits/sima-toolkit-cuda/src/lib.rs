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
//! Regenerating a committed PTX needs `libnvrtc`, and only that: it is a
//! userspace compiler that opens no device and needs no driver. It comes with
//! the CUDA toolkit, or on its own from the `nvidia-cuda-nvrtc-cu12` wheel, in
//! which case point `LD_LIBRARY_PATH` at the directory holding
//! `libnvrtc.so.12`. The `compile-ptx` example is the regeneration step:
//!
//! ```text
//! LD_LIBRARY_PATH=<dir> cargo run -p sima-toolkit-cuda --example compile-ptx \
//!   -- path/to/kernel.cu > path/to/kernel.ptx
//! ```
//!
//! Each kernel carries a regeneration test asserting its committed artifact is
//! exactly what its committed source compiles to. NVRTC stamps its own version
//! into the PTX header, so that test also pins which NVRTC produced the commit:
//! regenerating with a different one is a real change to what the device runs,
//! and it moves the digest the environment records.
//!
//! # Block dimensions
//!
//! Launches are one-dimensional. CUDA takes block dimensions at launch rather
//! than from the compiled artifact, so the width is stated twice and the
//! toolkit ties the two together: a kernel declares it in source with
//! `__launch_bounds__`, the caller passes the matching width to
//! [`Context::kernel`], and that call rejects a width the device cannot launch.
//! [`Context::dispatch`] then launches blocks of exactly that width, and a
//! caller sizing a grid reads it back from [`Kernel::block_width`] rather than
//! repeating the literal a third time.
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
//! Tests that call [`compile`] need `libnvrtc` instead of a device, and say so
//! in their own `#[ignore]` reason.
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
