//! The execution transport: how the orchestrator converses with the
//! subprocess workers that host domain executors.
//!
//! The transport is operational machinery, never identity-bearing: nothing
//! that crosses it is hashed, and a run's manifests are byte-identical
//! whatever transport carried its attempts.
//!
//! - [`protocol`] — the wire protocol: frame IO and the message vocabulary
//!   both endpoints share.

pub mod protocol;

pub(crate) mod checkpoint_cadence;
