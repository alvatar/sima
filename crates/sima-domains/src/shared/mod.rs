//! Executor machinery reused across domains, one layer below the concrete
//! [`domains`](crate::domains).
//!
//! A domain binds a format id to its executor, environment, generator, and
//! codecs. Where several domains would build their executors on the same
//! compute substrate, that substrate lives here rather than inside any one
//! domain. Today that is the [`cellular`] kind — the grid state, the
//! double-buffered dispatch harness, the `CellularEngine` seam and its
//! backends — shared by every reaction-diffusion, Neural CA, and Lenia domain.

pub mod cellular;
