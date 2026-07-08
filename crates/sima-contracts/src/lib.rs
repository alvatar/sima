//! Contract layer: the two seams the search substrate runs candidates
//! through.
//!
//! A generator produces a run's candidate specs; an executor evaluates one
//! candidate and returns produced artifacts plus observational stats, or a
//! failure outcome. Both are pure compute over `sima-model` values — they
//! never touch the store, so the trust boundary "executors never touch
//! durable state" is visible in the crate graph: this crate depends on
//! `sima-model` and `sima-core` only.
//!
//! The distinction the contract encodes in the type system is the split
//! between identity inputs and execution context. An executor receives a
//! task input — spec, params, seed, environment, input-state — which
//! determines the task key and the committed artifacts, and an execution
//! context — attempt number and worker id — which it may read but which
//! never influences a committed artifact. Stats is the one output that may
//! reflect execution context; it is observational and destined for the
//! journal.
//!
//! The concrete implementations that satisfy these traits live in
//! `sima-domains`, one per format; this crate holds the traits and their
//! shared vocabulary alone.

mod executor;
mod generator;

pub use executor::{Artifact, ExecutionContext, Executor, Outcome, Stats, TaskInput, WorkerId};
pub use generator::Generator;
