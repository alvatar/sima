//! Deterministic stub implementations of the two contracts.
//!
//! The stubs give the scheduler (M1.5) a programmable substrate with no GPU
//! and no store: a spec carries a [`StubProgram`] selecting one of a few
//! behaviors, a generator produces a run's specs from a seeded config, and an
//! executor evaluates one by reading its program.

mod generator;
mod program;

pub use generator::{StubGenerator, StubGeneratorConfig};
pub use program::{StubBehavior, StubProgram};
