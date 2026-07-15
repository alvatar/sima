//! The execution transport: how the orchestrator converses with the
//! subprocess workers that host domain executors.
//!
//! The transport is operational machinery, never identity-bearing: nothing
//! that crosses it is hashed, and a run's manifests are byte-identical
//! whatever transport carried its attempts.

pub(crate) mod checkpoint_cadence;
