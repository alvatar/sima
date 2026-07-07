//! [`Family`] and the static dispatches from config ids to code.
//!
//! A format id binds three things — this is what a `Family` groups: the
//! executor that evaluates specs of the format, the environment its results
//! depend on, and the translation of the family-owned `[run.params]` section
//! into the opaque canonical params bytes. Generators dispatch separately:
//! one format has one executor but many generators, and a generator owns the
//! translation of its own `[run.generator]` keys. Both dispatches are static
//! matches; unknown ids are [`Error::Validation`].

use sima_contracts::{Executor, Generator, StubGenerator};
use sima_core::{Error, Result};
use sima_model::{Environment, FormatId, GeneratorId, Params};

use crate::stub;

/// Everything a format id binds: the executor that evaluates specs of the
/// format and the environment its results depend on. The format's params
/// translation is the third binding, dispatched by [`params_for`].
pub struct Family {
    /// The format this family interprets.
    pub format: FormatId,
    /// The executor for the format's specs.
    pub executor: Box<dyn Executor + Sync>,
    /// The environment entering every task's identity.
    pub environment: Environment,
}

/// Static dispatch: the families this build knows. An unknown format id is
/// [`Error::Validation`].
pub fn family_for(format: &FormatId) -> Result<Family> {
    match format.as_str() {
        stub::ID => stub::family(),
        other => Err(Error::Validation(format!("unknown format id {other:?}"))),
    }
}

/// Params translation for the family: the `[run.params]` table into the
/// opaque canonical params bytes. The family owns and validates its keys.
pub fn params_for(format: &FormatId, table: &toml::Table) -> Result<Params> {
    match format.as_str() {
        stub::ID => stub::params(table),
        other => Err(Error::Validation(format!("unknown format id {other:?}"))),
    }
}

/// Static generator dispatch. An unknown generator id is
/// [`Error::Validation`].
pub fn generator_for(id: &GeneratorId) -> Result<Box<dyn Generator>> {
    match id.as_str() {
        stub::ID => Ok(Box::new(StubGenerator::new()?)),
        other => Err(Error::Validation(format!("unknown generator id {other:?}"))),
    }
}

/// Translation of the generator's own config table — the `[run.generator]`
/// section minus `id` — into its opaque params blob. The generator owns and
/// validates its keys.
pub fn generator_params_for(id: &GeneratorId, table: &toml::Table) -> Result<Vec<u8>> {
    match id.as_str() {
        stub::ID => stub::generator_params(table),
        other => Err(Error::Validation(format!("unknown generator id {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A validated format id.
    fn format(name: &str) -> FormatId {
        FormatId::new(name).expect("format id")
    }

    /// A validated generator id.
    fn generator(name: &str) -> GeneratorId {
        GeneratorId::new(name).expect("generator id")
    }

    #[test]
    fn an_unknown_format_id_is_rejected_by_both_dispatches() {
        let unknown = format("no-such-family.v1");
        for result in [
            family_for(&unknown).map(|_| ()),
            params_for(&unknown, &toml::Table::new()).map(|_| ()),
        ] {
            match result {
                Err(Error::Validation(msg)) => {
                    assert!(
                        msg.contains("no-such-family.v1"),
                        "the error names the id: {msg}"
                    );
                }
                other => panic!("expected Validation, got {other:?}"),
            }
        }
    }

    #[test]
    fn an_unknown_generator_id_is_rejected_by_both_dispatches() {
        let unknown = generator("no-such-generator.v1");
        for result in [
            generator_for(&unknown).map(|_| ()),
            generator_params_for(&unknown, &toml::Table::new()).map(|_| ()),
        ] {
            match result {
                Err(Error::Validation(msg)) => {
                    assert!(
                        msg.contains("no-such-generator.v1"),
                        "the error names the id: {msg}"
                    );
                }
                other => panic!("expected Validation, got {other:?}"),
            }
        }
    }

    #[test]
    fn the_stub_family_binds_executor_and_environment() -> Result<()> {
        let family = family_for(&format("stub.v1"))?;
        assert_eq!(family.format.as_str(), "stub.v1");
        // The executor answers for the family's format.
        assert_eq!(family.executor.format().as_str(), "stub.v1");
        // One environment component: the stub executor's version.
        assert_eq!(family.environment.components().len(), 1);
        assert_eq!(family.environment.components()[0].name(), "stub.executor");
        Ok(())
    }

    #[test]
    fn the_stub_generator_dispatches() -> Result<()> {
        let generator = generator_for(&generator("stub.v1"))?;
        assert_eq!(generator.id().as_str(), "stub.v1");
        Ok(())
    }
}
