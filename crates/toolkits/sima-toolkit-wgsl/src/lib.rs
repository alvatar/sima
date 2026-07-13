//! WGSL compute toolkit: run GPU compute kernels authored in WGSL without
//! writing raw Vulkan.
//!
//! A domain describes kernels and buffers and orchestrates dispatches through a
//! small surface — [`Context`], [`Buffer`], [`Kernel`] — while the toolkit hides
//! `ash`/Vulkan and compiles WGSL to SPIR-V in process with `naga`. It is an
//! execution backend a domain depends on, a compute library rather than an
//! executor: it holds no store handle and builds no run identity.
//!
//! # Tests
//!
//! Tests split by whether they touch a real device. Compilation and identity
//! tests run anywhere and are covered by `cargo test`. Tests that create a
//! [`Context`] need a Vulkan device and are marked `#[ignore]`, so `cargo test`
//! skips them and hosted CI stays green with no device present. Run them on a
//! machine with a device:
//!
//! ```text
//! cargo test -p sima-toolkit-wgsl -- --ignored
//! ```
//!
//! `ash` loads the system Vulkan loader at runtime, so the crate builds with no
//! native toolchain and the device tests are the only part that needs hardware.

mod buffer;
mod compile;
mod context;
mod dispatch;
mod kernel;
mod selection;
mod validation;

pub use buffer::Buffer;
pub use compile::{COMPILER_ID, check, source_digest};
pub use context::Context;
pub use kernel::Kernel;
