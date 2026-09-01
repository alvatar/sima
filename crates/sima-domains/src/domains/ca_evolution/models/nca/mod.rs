//! The asynchronous Neural Cellular Automaton: a grid of cells that update
//! their state channels by a small learned network, stochastically and out of
//! phase, with the update mask keyed on the absolute step the harness supplies.
//!
//! [`NcaGenome`] is the spec payload (the flat network weight vector),
//! [`NcaIgnition`] the model's slice of `[search.params]` (the centered seeded
//! patch), and [`NcaGenConfig`] the sampling box. The model binds them to the
//! generic machinery through [`CaModel`](super::super::model::CaModel), with the
//! asynchronous update kernel co-located in `nca.wgsl` and composed on top of the
//! shared WGSL PRNG. The model is stepped: its kernel reads the per-step index
//! from the harness and its committed state frames that step ahead of the grid.

mod gen_config;
mod genome;
mod ignition;
mod rule;

pub(crate) use rule::{CHANNELS, Nca};
