//! The Gray-Scott reaction-diffusion domain: a two-chemical system on a 2D
//! grid whose candidates are the four evolvable scalars of its update rule.
//!
//! The module holds [`GrayScottGenome`], the validated payload type the
//! domain's specs carry.

mod genome;

pub use genome::GrayScottGenome;
