//! The Gray-Scott reaction-diffusion domain: a two-chemical system on a 2D
//! grid whose candidates are the four evolvable scalars of its update rule.
//!
//! The module holds [`GrayScottGenome`], the validated payload type the
//! domain's specs carry, and [`GrayScottGeneratorConfig`], the generator
//! config naming the candidate count and the sampled box — each with its
//! canonical byte codec.

mod generator;
mod genome;

pub use generator::GrayScottGeneratorConfig;
pub use genome::GrayScottGenome;
