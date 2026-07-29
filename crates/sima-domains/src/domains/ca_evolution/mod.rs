//! The `ca_evolution` domain: a generic cellular-automaton substrate behind the
//! [`CaModel`](model::CaModel) seam, with one model per registered format id.
//!
//! The domain owns the shared machinery — [`CaExecutor<M>`](executor::CaExecutor),
//! [`CaGenerator<M>`](generator::CaGenerator), [`CaParams`](params::CaParams),
//! the [`seeded_patch`](ignition::seeded_patch) ignition primitive, and
//! [`build_binding`](binding::build_binding) — and each model under `models/`
//! implements [`CaModel`](model::CaModel). This module is the registry: it maps
//! a format or generator id to its model and delegates. Adding a model is a new
//! module under `models/` plus one arm here; the generic machinery never
//! changes.

mod binding;
pub(crate) mod continuation;
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

use crate::format_binding::FormatBinding;
use crate::substrates::cellular::{CudaEngine, WgslEngine};
use model::CaModel;
use models::gray_scott::GrayScott;
use models::gray_scott_cuda::GrayScottCuda;
use models::nca::Nca;

/// Resolves a format id to one of this domain's models, binding its [`FormatBinding`],
/// or `None` if no model claims it.
///
/// Each arm names both the model and the backend it runs on: the model
/// declares no engine, so a rule ported to a second backend is a second arm
/// beside the first, and a mismatched pairing is visible on one line.
pub(crate) fn binding_for(format: &FormatId) -> Option<Result<FormatBinding>> {
    match format.as_str() {
        GrayScott::FORMAT_ID => Some(binding::build_binding::<GrayScott, WgslEngine>()),
        GrayScottCuda::FORMAT_ID => Some(binding::build_binding::<GrayScottCuda, CudaEngine>()),
        Nca::FORMAT_ID => Some(binding::build_binding::<Nca, WgslEngine>()),
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
        GrayScottCuda::FORMAT_ID => Some(params::translate::<GrayScottCuda>(table, segmented)),
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
        GrayScottCuda::FORMAT_ID => Some(
            generator::CaGenerator::<GrayScottCuda>::new()
                .map(|g| Box::new(g) as Box<dyn Generator>),
        ),
        Nca::FORMAT_ID => {
            Some(generator::CaGenerator::<Nca>::new().map(|g| Box::new(g) as Box<dyn Generator>))
        }
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
            let domain = binding_for(&format).expect("a registered program")?;
            assert_eq!(
                domain.environment.id().to_string(),
                pinned,
                "the environment of {id} changed, invalidating its stored results"
            );
        }
        Ok(())
    }

    #[test]
    fn a_program_is_registered_for_every_dispatch() -> Result<()> {
        // The four dispatches are separate matches over the same ids, so a
        // model added to one and forgotten in another resolves for some
        // purposes and not others.
        for (id, _) in PINNED_ENVIRONMENT_IDS {
            let format = FormatId::new(id)?;
            let generator = GeneratorId::new(id)?;
            assert!(binding_for(&format).is_some(), "{id} binds a domain");
            assert!(
                params_for(&format, &toml::Table::new(), false).is_some(),
                "{id} translates params"
            );
            assert!(
                generator_for(&generator).is_some(),
                "{id} binds a generator"
            );
        }
        Ok(())
    }
}
