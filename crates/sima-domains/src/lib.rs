//! Domains: the executable substance a format id binds.
//!
//! A domain groups everything the infrastructure needs to run one format's
//! candidates: the executor that evaluates its specs, the environment its
//! results depend on, the generator that produces its specs, the codecs that
//! give specs and params their canonical bytes, and the translation of the
//! human-facing TOML config sections into those bytes. The id dispatch —
//! [`domain_for`], [`generator_for`], [`generators_for`] — is the crate's
//! surface; each domain's pieces live in its own module under `domains/`.
//!
//! Because this crate knows which execution backends the build compiles in and
//! which one each format runs through, it also answers what devices a program
//! can use: the [`devices`] module carries the enumeration the layers above
//! resolve device selectors against, asked about a format id.
//!
//! Below the concrete domains — the stub and `ca_evolution` — the
//! [`substrates`] layer holds the structural kinds a domain's executor is built
//! on. Today that is the [`cellular`](substrates::cellular) substrate: the
//! [`Grid`](substrates::cellular::Grid) state, the double-buffered dispatch
//! harness that advances it, and the stats reduction over the result. Each is
//! written once over an internal boundary and instantiated per execution
//! backend, so what a backend supplies is the translation onto its own toolkit
//! and the kernels only it can run.
//!
//! A format binds a [`Domain`](sima_contracts::Domain) object and nothing
//! else, which is the shape a program outside the workspace supplies, so a
//! built-in format is driven over exactly the contracts a third party writes
//! against.
//!
//! The pipeline calls this crate to resolve a config's format and generator
//! ids to code; the scheduler tests use the stub domain as a deterministic,
//! programmable substrate through a dev-dependency. `sima-contracts` sits
//! below and holds the traits alone.

pub mod devices;
mod dispatch;
mod domains;
pub mod substrates;

pub use dispatch::{domain_for, generator_for, generators_for};
pub use domains::ca_evolution::continuation::{decode_continuation, encode_continuation};
pub use domains::stub::{
    StubBehavior, StubExecutor, StubGenerator, StubGeneratorConfig, StubProgram, StubState,
};
