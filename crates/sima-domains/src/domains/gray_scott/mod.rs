//! The Gray-Scott reaction-diffusion domain: a two-chemical system on a 2D
//! grid whose candidates are the four evolvable scalars of its update rule.
//!
//! The module holds [`GrayScottGenome`], the validated payload type the
//! domain's specs carry, [`GrayScottGeneratorConfig`], the generator config
//! naming the candidate count and the sampled box — each with its canonical
//! byte codec — and [`GrayScottGenerator`], the seeded generator drawing a
//! run's candidates from that box. The translation module turns the domain's
//! `[run.generator]` section into those canonical config bytes and binds the
//! domain's id.

mod generator;
mod genome;
mod translation;

pub use generator::{GrayScottGenerator, GrayScottGeneratorConfig};
pub use genome::GrayScottGenome;
pub(crate) use translation::{ID, generator_params};
