//! The `ca_evolution` domain: a generic cellular-automaton substrate behind the
//! [`CaModel`](model::CaModel) boundary, with one model per registered format id.
//!
//! The domain owns the shared machinery — [`CaExecutor<M>`](executor::CaExecutor),
//! [`CaGenerator<M>`](generator::CaGenerator), [`CaParams`](params::CaParams),
//! the [`seeded_patch`](ignition::seeded_patch) ignition primitive, and
//! [`CaDomain<M, E>`](domain::CaDomain) — and each model under `models/`
//! implements [`CaModel`](model::CaModel). `registry` maps a format or generator
//! id to its model and delegates.

pub(crate) mod continuation;
mod domain;
mod executor;
mod generator;
mod ignition;
mod model;
mod models;
mod params;
mod registry;
mod values;

#[cfg(test)]
mod toy_model;

pub(crate) use registry::{domain_for, generator_for};
