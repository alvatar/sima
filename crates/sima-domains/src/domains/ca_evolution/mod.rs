//! The `ca_evolution` domain: a generic cellular-automaton substrate behind the
//! [`CaModel`](model::CaModel) boundary, with one model per registered format id.
//!
//! The domain owns the shared machinery — [`CaExecutor<M>`](executor::CaExecutor),
//! [`CaGenerator<M>`](generator::CaGenerator), [`CaParams`](params::CaParams),
//! the [`seeded_patch`](ignition::seeded_patch) ignition primitive, and
//! [`CaDomain<M, E>`](domain::CaDomain) — and each model under `models/`
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
mod values;

#[cfg(test)]
mod toy_model;

use sima_contracts::{Domain, Generator};
use sima_core::Result;
use sima_model::{FormatId, GeneratorId};

use crate::substrates::cellular::{CellularEngine, CudaEngine, WgslEngine};
use domain::CaDomain;
use model::CaModel;
use models::gray_scott::GrayScott;
use models::gray_scott_cuda::GrayScottCuda;
use models::nca::Nca;

/// Resolves a format id to one of this domain's models, or `None` if no model
/// claims it.
///
/// Each arm names both the model and the backend it runs on: the model
/// declares no engine, so a rule ported to a second backend is a second arm
/// beside the first, and a mismatched pairing is visible on one line.
pub(crate) fn domain_for(format: &FormatId) -> Option<Result<Box<dyn Domain>>> {
    /// Boxes one model-and-backend pairing as the domain a format binds.
    fn bound<M: CaModel, E: CellularEngine>() -> Result<Box<dyn Domain>> {
        Ok(Box::new(CaDomain::<M, E>::new()?))
    }
    match format.as_str() {
        GrayScott::FORMAT_ID => Some(bound::<GrayScott, WgslEngine>()),
        GrayScottCuda::FORMAT_ID => Some(bound::<GrayScottCuda, CudaEngine>()),
        Nca::FORMAT_ID => Some(bound::<Nca, WgslEngine>()),
        _ => None,
    }
}

/// Resolves a generator id to one of this domain's models, or `None`.
pub(crate) fn generator_for(id: &GeneratorId) -> Option<Result<Box<dyn Generator>>> {
    /// Boxes one model's generator.
    fn drawn<M: CaModel>() -> Result<Box<dyn Generator>> {
        Ok(Box::new(generator::CaGenerator::<M>::new()?))
    }
    match id.as_str() {
        GrayScott::FORMAT_ID => Some(drawn::<GrayScott>()),
        GrayScottCuda::FORMAT_ID => Some(drawn::<GrayScottCuda>()),
        Nca::FORMAT_ID => Some(drawn::<Nca>()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every registered program and the `EnvironmentId` its domain produces.
    ///
    /// The environment enters every task key, so a change to one of these
    /// invalidates every stored result of that program and forces it to be
    /// recomputed. Pinning the ids makes such a change an explicit edit of this
    /// table rather than a silent consequence of touching the code that
    /// assembles environments. A new program adds a row; an edited kernel
    /// changes one.
    const PINNED_ENVIRONMENT_IDS: [(&str, &str); 3] = [
        (
            "ca_evolution.gray_scott.v1",
            "bc6086ce7e256cd95cb1cc6849bce7db4c29ad30eff1684837042231c1d3c7ed",
        ),
        (
            "ca_evolution.gray_scott_cuda.v1",
            "bb2bf989e6b61745b98d33b8273435ece89387c7e638df6f40209f47cacdef23",
        ),
        (
            "ca_evolution.nca.v1",
            "3a1c5d539436120da0753d07b099aa96593d857b21dfdeeb9280e262d78750fe",
        ),
    ];

    #[test]
    fn every_registered_program_keeps_its_environment_id() -> Result<()> {
        for (id, pinned) in PINNED_ENVIRONMENT_IDS {
            let format = FormatId::new(id)?;
            let domain = domain_for(&format).expect("a registered program")?;
            assert_eq!(
                domain.environment().id().to_string(),
                pinned,
                "the environment of {id} changed, invalidating its stored results"
            );
        }
        Ok(())
    }

    #[test]
    fn a_program_is_registered_for_every_dispatch() -> Result<()> {
        // The two dispatches are separate matches over the same ids, so a
        // model added to one and forgotten in the other resolves for some
        // purposes and not others.
        for (id, _) in PINNED_ENVIRONMENT_IDS {
            let format = FormatId::new(id)?;
            let generator = GeneratorId::new(id)?;
            assert!(domain_for(&format).is_some(), "{id} binds a domain");
            assert!(
                generator_for(&generator).is_some(),
                "{id} binds a generator"
            );
        }
        Ok(())
    }
}
