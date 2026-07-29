//! Domains: the executable substance a format id binds.
//!
//! A domain groups everything the infrastructure needs to run one format's
//! candidates: the executor that evaluates its specs, the environment its
//! results depend on, the generator that produces its specs, the codecs that
//! give specs and params their canonical bytes, and the translation of the
//! human-facing TOML config sections into those bytes. The [`Domain`] type and
//! the id dispatch ([`domain_for`], [`generator_for`], and the two translation
//! entries) are the crate's surface; each domain's pieces live in its own
//! module under `domains/`.
//!
//! Because this crate knows which execution backends the build compiles in and
//! which one each format runs through, it also answers what devices a program
//! can use: the [`devices`] module carries the enumeration the layers above
//! resolve device selectors against, asked about a format id.
//!
//! Below the concrete domains — the stub and `ca_evolution` — the
//! [`substrates`] layer holds the structural kinds a domain's executor is built
//! on. Today that is the [`cellular`](substrates::cellular) substrate: the
//! [`Grid`](substrates::cellular::Grid) state, the
//! [`run`](substrates::cellular::run) double-buffered dispatch harness, and the
//! [`CellularRule`](substrates::cellular::CellularRule) CPU-reference contract
//! used solely to cross-check that harness against an independent implementation
//! in tests.
//!
//! The same domains are reachable as objects through [`BuiltinDomain`] and
//! [`BuiltinGenerator`], the shape a program outside the workspace supplies,
//! so a built-in format can be driven over the seam a third party writes
//! against.
//!
//! The pipeline calls this crate to resolve a config's format and generator
//! ids to code; the scheduler tests use the stub domain as a deterministic,
//! programmable substrate through a dev-dependency. `sima-contracts` sits
//! below and holds the traits alone.

pub mod devices;
mod domain;
mod domains;
mod plug;
pub mod substrates;

pub use domain::{Domain, domain_for, generator_for, generator_params_for, params_for};
pub use domains::ca_evolution::continuation::{decode_continuation, encode_continuation};
pub use domains::stub::{
    StubBehavior, StubExecutor, StubGenerator, StubGeneratorConfig, StubProgram, StubState,
};
pub use plug::{BuiltinDomain, BuiltinGenerator, generators_for};
