//! The `ca_evolution` domain: a generic cellular-automaton substrate behind the
//! [`CaModel`](model::CaModel) seam, with one model per registered format id.
//!
//! The domain owns the shared machinery — [`CaExecutor<M>`](executor::CaExecutor),
//! [`CaGenerator<M>`](generator::CaGenerator), [`CaParams`](params::CaParams),
//! the [`seeded_patch`](ignition::seeded_patch) ignition primitive, and
//! [`build_domain`](domain::build_domain) — and each model under `models/`
//! implements [`CaModel`](model::CaModel). This module is the registry: it maps
//! a format or generator id to its model and delegates. Adding a model is a new
//! module under `models/` plus one arm here; the generic machinery never
//! changes.

pub(crate) mod continuation;
mod domain;
mod executor;
mod generator;
mod ignition;
mod model;
mod models;
mod params;

#[cfg(test)]
mod toy_model;

use sima_contracts::Generator;
use sima_core::Result;
use sima_model::{FormatId, GeneratorId, Params};

use crate::domain::Domain;
use model::CaModel;
use models::gray_scott::GrayScott;
use models::nca::Nca;

/// Resolves a format id to one of this domain's models, binding its [`Domain`],
/// or `None` if no model claims it.
pub(crate) fn domain_for(format: &FormatId) -> Option<Result<Domain>> {
    match format.as_str() {
        GrayScott::FORMAT_ID => Some(domain::build_domain::<GrayScott>()),
        Nca::FORMAT_ID => Some(domain::build_domain::<Nca>()),
        _ => None,
    }
}

/// Resolves the `[run.params]` translation for a format id, or `None`.
/// `segmented` is whether the run divides candidates into segments; the models
/// forbid a `snapshot_when` predicate on a segmented run.
pub(crate) fn params_for(
    format: &FormatId,
    table: &toml::Table,
    segmented: bool,
) -> Option<Result<Params>> {
    match format.as_str() {
        GrayScott::FORMAT_ID => Some(params::translate::<GrayScott>(table, segmented)),
        Nca::FORMAT_ID => Some(params::translate::<Nca>(table, segmented)),
        _ => None,
    }
}

/// Resolves a generator id to one of this domain's models, or `None`.
pub(crate) fn generator_for(id: &GeneratorId) -> Option<Result<Box<dyn Generator>>> {
    match id.as_str() {
        GrayScott::FORMAT_ID => Some(
            generator::CaGenerator::<GrayScott>::new().map(|g| Box::new(g) as Box<dyn Generator>),
        ),
        Nca::FORMAT_ID => {
            Some(generator::CaGenerator::<Nca>::new().map(|g| Box::new(g) as Box<dyn Generator>))
        }
        _ => None,
    }
}

/// Resolves the `[run.generator]` translation for a generator id, or `None`.
pub(crate) fn generator_params_for(
    id: &GeneratorId,
    table: &toml::Table,
) -> Option<Result<Vec<u8>>> {
    match id.as_str() {
        GrayScott::FORMAT_ID => Some(generator::translate::<GrayScott>(table)),
        Nca::FORMAT_ID => Some(generator::translate::<Nca>(table)),
        _ => None,
    }
}
