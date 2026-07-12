//! The `ca_evolution` domain: a 2D cellular grid evolved by a reaction-diffusion
//! rule — today the Gray-Scott two-chemical system, whose candidates are the
//! four evolvable scalars of that update rule.
//!
//! The module holds [`CaEvolutionGenome`], the validated payload type the
//! domain's specs carry, [`CaEvolutionGeneratorConfig`], the generator config
//! naming the candidate count and the sampled box — each with its canonical
//! byte codec — [`CaEvolutionGenerator`], the seeded generator drawing a
//! run's candidates from that box, [`CaEvolutionPatch`], the ignition
//! configuration of the domain's initial grid, [`CaEvolutionParams`], the
//! run parameters framing one task's evaluation, and
//! [`CaEvolutionExecutor`], the GPU executor advancing that grid through the
//! domain's WGSL kernel. The translation module turns the domain's config
//! sections into those canonical bytes and binds the domain's id.

mod executor;
mod generator;
mod genome;
mod params;
mod patch;
mod translation;

pub use executor::CaEvolutionExecutor;
pub(crate) use executor::KERNEL_WGSL;
pub use generator::{CaEvolutionGenerator, CaEvolutionGeneratorConfig};
pub use genome::CaEvolutionGenome;
pub use params::CaEvolutionParams;
pub use patch::CaEvolutionPatch;
pub(crate) use translation::{ID, domain, generator_params, params};
