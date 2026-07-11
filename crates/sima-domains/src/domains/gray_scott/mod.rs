//! The Gray-Scott reaction-diffusion domain: a two-chemical system on a 2D
//! grid whose candidates are the four evolvable scalars of its update rule.
//!
//! The module holds [`GrayScottGenome`], the validated payload type the
//! domain's specs carry, [`GrayScottGeneratorConfig`], the generator config
//! naming the candidate count and the sampled box — each with its canonical
//! byte codec — and [`GrayScottGenerator`], the seeded generator drawing a
//! run's candidates from that box.

mod generator;
mod genome;

pub use generator::{GrayScottGenerator, GrayScottGeneratorConfig};
pub use genome::GrayScottGenome;
