//! `sima migrate`: moving a run's orchestrator onto another machine.
//!
//! Everything built below this module distributes **workers** — the store and
//! the orchestrator stay here, and task inputs and results cross the wire
//! inline. Migration moves the **orchestrator**: the run's durable state
//! travels to the destination, a `sima run` process drives it there, and the
//! results come back.
//!
//! - [`destination`] — which machine, read from `[orchestrator].migrate`.
//! - [`far_config`] — where the run lives there, and what it reads when it
//!   arrives.

pub(crate) mod destination;
pub(crate) mod far_config;
