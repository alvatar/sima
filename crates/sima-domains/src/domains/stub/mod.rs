//! The stub domain: a deterministic, programmable substrate.
//!
//! A spec carries a [`StubProgram`] selecting one of a few behaviors, the
//! [`StubGenerator`] produces a run's specs from a seeded config, and the
//! [`StubExecutor`] evaluates one by reading its program — no GPU and no
//! store. The translation module turns the domain's TOML config sections into
//! the canonical bytes the model carries, and binds the format id to its
//! executor and environment. This is what the infrastructure layers test
//! against in place of a real evaluation domain.

mod executor;
mod generator;
mod program;
mod state;
mod translation;

pub use executor::StubExecutor;
pub use generator::{StubGenerator, StubGeneratorConfig};
pub use program::{StubBehavior, StubProgram};
pub use state::StubState;
pub(crate) use translation::{ID, binding, generator_params, params};
