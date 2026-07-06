//! Contract layer: the two seams the search substrate runs candidates
//! through, plus deterministic stub implementations of both.
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
//! The stub module supplies a seeded generator and a spec-programmed
//! executor (succeed, flaky, panic, sleep) so the scheduler
//! (M1.5) has a deterministic, programmable substrate to build against
//! without a GPU or real model families.

mod executor;
mod generator;
mod stub;

pub use executor::{Artifact, ExecutionContext, Executor, Outcome, Stats, TaskInput, WorkerId};
pub use generator::Generator;
pub use stub::{StubBehavior, StubExecutor, StubGenerator, StubGeneratorConfig, StubProgram};
