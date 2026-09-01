//! The structural kinds a domain's executor is built on, one layer below the
//! concrete [`domains`](crate::domains).
//!
//! A substrate is the state shape and the dispatch machinery a family of rules
//! shares — distinct from the execution backend (Host, Wgsl, Cuda), which is
//! the engine that searches the dispatch. Where several domains would build their
//! executors on the same structural kind, that kind lives here rather than
//! inside any one domain. Today that is the [`cellular`] substrate — the grid
//! state, the double-buffered dispatch harness, and the `CellularEngine` boundary
//! over its backends — shared by every reaction-diffusion, Neural CA, and Lenia
//! domain.

pub mod cellular;
