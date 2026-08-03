//! The Gray-Scott reaction-diffusion model: a two-chemical CA on a 2D grid whose
//! candidates are the four evolvable scalars of its update rule.
//!
//! [`GrayScottGenome`] is the spec payload (feed, kill, and the two diffusion
//! rates), [`GrayScottIgnition`] the model's slice of `[run.params]` (the
//! centered seeded patch over the fixed point), and [`GrayScottGenConfig`] the
//! sampling box. [`GrayScott`] binds them to the generic machinery through
//! [`CaModel`], with the reaction-diffusion kernel co-located in
//! `gray_scott.wgsl`.

mod gen_config;
mod genome;
mod ignition;
mod rule;

pub(crate) use rule::GrayScott;
// The rule's own types, re-exported: the CUDA program is the same rule on
// another backend and binds these unchanged.
pub(crate) use gen_config::GrayScottGenConfig;
pub(crate) use genome::GrayScottGenome;
pub(crate) use ignition::GrayScottIgnition;
