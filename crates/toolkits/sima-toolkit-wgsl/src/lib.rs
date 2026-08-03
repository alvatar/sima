//! WGSL compute toolkit: run GPU compute kernels authored in WGSL without
//! writing raw Vulkan.
//!
//! A domain describes kernels and buffers and orchestrates dispatches through a
//! small surface — [`Context`], [`Buffer`], [`Kernel`] — while the toolkit hides
//! `ash`/Vulkan and compiles WGSL to SPIR-V in process with `naga`. It is an
//! execution backend a domain depends on, a compute library rather than an
//! executor: it holds no store handle and builds no run identity.
//!
//! # What pairs with the CUDA toolkit
//!
//! The two toolkits present the same shape — a [`Context`] that allocates
//! zeroed [`Buffer`]s, builds a [`Kernel`] at a stated block width, and
//! dispatches over reflected bindings, optionally carrying a [`BufferUpdate`]
//! — and differ only where the backends genuinely do:
//!
//! - **Identity.** This toolkit compiles WGSL in process, so a kernel reports
//!   [`source_digest`] and [`COMPILER_ID`] states the lowering. The CUDA
//!   toolkit loads committed PTX, so it reports the digest of that artifact and
//!   its compiler id names only what the artifact targets.
//! - **The compiler on the surface.** Lowering happens here at run time, so
//!   [`check`] is what a domain calls to validate a kernel without a device.
//!   CUDA compiles offline instead, so its surface carries the regeneration
//!   entry point and no run-time check.
//!
//! # Tests
//!
//! Tests split by whether they touch a real device. Compilation and identity
//! tests run anywhere; tests that create a [`Context`] need a Vulkan device.
//! Both run under a plain `cargo test`, so the device path is exercised on
//! every machine that has one and a device fault surfaces as a test failure
//! rather than a skipped test.
//!
//! `ash` loads the system Vulkan loader at runtime, so the crate builds with no
//! native toolchain and the device tests are the only part that needs hardware.

mod buffer;
mod compile;
mod context;
mod dispatch;
mod instance;
mod kernel;
mod selection;
mod validation;

pub use buffer::Buffer;
pub use compile::{COMPILER_ID, check, source_digest};
pub use context::Context;
pub use dispatch::BufferUpdate;
pub use kernel::Kernel;
pub use selection::{enumerate_devices, selected_device_desc};
