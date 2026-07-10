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
//! journal. The third `execute` parameter, the checkpoint handle, carries the
//! crash-resume channel under the same discipline: the executor offers
//! continuation bytes and may adopt saved ones, while the handle owns every
//! write, so executors still never touch durable state and checkpoints never
//! influence committed bytes.
//!
//! The concrete implementations that satisfy these traits live in
//! `sima-domains`, one per format; this crate holds the traits and their
//! shared vocabulary alone.

mod checkpoint;
mod executor;
mod generator;

pub use checkpoint::{Checkpoint, NoCheckpoint};
pub use executor::{
    Artifact, ExecutionContext, Executor, Outcome, STATE_ARTIFACT, Stats, TaskInput, WorkerId,
};
pub use generator::Generator;
