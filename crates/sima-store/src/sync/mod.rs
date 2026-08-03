//! Store-to-store synchronization: a have/want protocol over any byte pipe.
//!
//! Two stores exchange the task records within a caller-supplied key set and
//! the CAS objects those records reference, ending with the union of both.
//! The protocol is standalone here — its only consumers are tests until
//! `sima migrate` composes it — and it lives in `sima-store` over
//! [`sima_core::frame`], so it depends on nothing above the store.
//!
//! Scope is records and objects only. Checkpoints, placement, journals, and
//! manifests stay with their orchestrator: segments are the portable resume
//! point, placement re-binds greedily, and journals are observational.

mod engine;
mod message;

pub use engine::{ObjectScope, SyncReport, SyncRole};
