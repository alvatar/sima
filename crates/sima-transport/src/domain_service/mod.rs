//! The domain service: how the orchestrator asks a program what its format
//! binds.
//!
//! Five of the seven things a format binds are read where a run is driven —
//! its environment, its device list, its params translation, its generator's
//! params translation, and its specs — so a program that lives in its own
//! binary answers them over a pipe. The other two, the executor and the device
//! description, are read inside a worker and cross the worker protocol
//! ([`crate::protocol`]) instead.
//!
//! - [`protocol`] — the message vocabulary both endpoints share.
//!
//! The session stays open for the run, so a program pays its startup cost once.

pub mod protocol;
