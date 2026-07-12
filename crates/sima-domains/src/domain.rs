//! [`Domain`] and the static dispatches from config ids to code.
//!
//! A format id binds three things — this is what a `Domain` groups: the
//! executor that evaluates specs of the format, the environment its results
//! depend on, and the translation of the domain-owned `[run.params]` section
//! into the opaque canonical params bytes. Generators dispatch separately:
//! one format has one executor but many generators, and a generator owns the
//! translation of its own `[run.generator]` keys. Both dispatches are static
//! matches; unknown ids are [`Error::Validation`].

use sima_contracts::{Executor, Generator};
use sima_core::{Error, Result};
use sima_model::{Environment, FormatId, GeneratorId, Params};

use crate::domains::{ca_evolution, stub};

/// Everything a format id binds: the executor that evaluates specs of the
/// format and the environment its results depend on. The format's params
/// translation is the third binding, dispatched by [`params_for`].
pub struct Domain {
    /// The format this domain interprets.
    pub format: FormatId,
    /// The executor for the format's specs.
    pub executor: Box<dyn Executor + Sync>,
    /// The environment entering every task's identity.
    pub environment: Environment,
}

/// Static dispatch: the domains this build knows. An unknown format id is
/// [`Error::Validation`].
pub fn domain_for(format: &FormatId) -> Result<Domain> {
    match format.as_str() {
        stub::ID => stub::domain(),
        ca_evolution::ID => ca_evolution::domain(),
        other => Err(Error::Validation(format!("unknown format id {other:?}"))),
    }
}

/// Params translation for the domain: the `[run.params]` table into the
/// opaque canonical params bytes. The domain owns and validates its keys.
pub fn params_for(format: &FormatId, table: &toml::Table) -> Result<Params> {
    match format.as_str() {
        stub::ID => stub::params(table),
        ca_evolution::ID => ca_evolution::params(table),
        other => Err(Error::Validation(format!("unknown format id {other:?}"))),
    }
}

/// Static generator dispatch. An unknown generator id is
/// [`Error::Validation`].
pub fn generator_for(id: &GeneratorId) -> Result<Box<dyn Generator>> {
    match id.as_str() {
        stub::ID => Ok(Box::new(stub::StubGenerator::new()?)),
        ca_evolution::ID => Ok(Box::new(ca_evolution::CaEvolutionGenerator::new()?)),
        other => Err(Error::Validation(format!("unknown generator id {other:?}"))),
    }
}

/// Translation of the generator's own config table — the `[run.generator]`
/// section minus `id` — into its opaque params blob. The generator owns and
/// validates its keys.
pub fn generator_params_for(id: &GeneratorId, table: &toml::Table) -> Result<Vec<u8>> {
    match id.as_str() {
        stub::ID => stub::generator_params(table),
        ca_evolution::ID => ca_evolution::generator_params(table),
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
        let unknown = format("no-such-domain.v1");
        for result in [
            domain_for(&unknown).map(|_| ()),
            params_for(&unknown, &toml::Table::new()).map(|_| ()),
        ] {
            match result {
                Err(Error::Validation(msg)) => {
                    assert!(
                        msg.contains("no-such-domain.v1"),
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
    fn the_stub_domain_binds_executor_and_environment() -> Result<()> {
        let domain = domain_for(&format("stub.v1"))?;
        assert_eq!(domain.format.as_str(), "stub.v1");
        // The executor answers for the domain's format.
        assert_eq!(domain.executor.format().as_str(), "stub.v1");
        // One environment component: the stub executor's version.
        assert_eq!(domain.environment.components().len(), 1);
        assert_eq!(domain.environment.components()[0].name(), "stub.executor");
        Ok(())
    }

    #[test]
    fn the_stub_generator_dispatches() -> Result<()> {
        let generator = generator_for(&generator("stub.v1"))?;
        assert_eq!(generator.id().as_str(), "stub.v1");
        Ok(())
    }

    #[test]
    fn the_ca_evolution_domain_binds_executor_and_environment() -> Result<()> {
        use sima_model::EnvironmentValue;
        use sima_toolkit_wgsl::{COMPILER_ID, source_digest};

        // Constructing the domain here proves domain_for is device-free:
        // this test runs everywhere, GPU or not.
        let domain = domain_for(&format("ca_evolution.v1"))?;
        assert_eq!(domain.format.as_str(), "ca_evolution.v1");
        assert_eq!(domain.executor.format().as_str(), "ca_evolution.v1");
        // Three components: the executor version, the kernel source digest,
        // and the pinned compiler id — together they pin the compiled
        // SPIR-V in every task's identity.
        let components = domain.environment.components();
        assert_eq!(components.len(), 3);
        assert_eq!(components[0].name(), "ca_evolution.executor");
        assert_eq!(
            *components[0].value(),
            EnvironmentValue::Version("v1".to_string())
        );
        assert_eq!(components[1].name(), "ca_evolution.kernel");
        assert_eq!(
            *components[1].value(),
            EnvironmentValue::Digest(source_digest(ca_evolution::KERNEL_WGSL))
        );
        assert_eq!(components[2].name(), "wgsl.compiler");
        assert_eq!(
            *components[2].value(),
            EnvironmentValue::Version(COMPILER_ID.to_string())
        );
        Ok(())
    }

    #[test]
    fn the_ca_evolution_params_translation_dispatches() -> Result<()> {
        let table: toml::Table = r#"
            width = 64
            height = 48
            steps = 100
            dt = 1.0
            base_u = 0.5
            base_v = 0.25
            side_divisor = 8
            noise_width = 0.02
        "#
        .parse()
        .expect("parse test table");
        assert_eq!(
            params_for(&format("ca_evolution.v1"), &table)?.bytes,
            ca_evolution::params(&table)?.bytes
        );
        Ok(())
    }

    #[test]
    fn the_ca_evolution_generator_dispatches() -> Result<()> {
        let generator = generator_for(&generator("ca_evolution.v1"))?;
        assert_eq!(generator.id().as_str(), "ca_evolution.v1");
        Ok(())
    }

    #[test]
    fn the_ca_evolution_generator_translation_dispatches() -> Result<()> {
        let table: toml::Table = r#"
            count = 64
            feed = [0.01, 0.08]
            kill = [0.03, 0.07]
            diffusion_u = [0.16, 0.16]
            diffusion_v = [0.08, 0.08]
        "#
        .parse()
        .expect("parse test table");
        assert_eq!(
            generator_params_for(&generator("ca_evolution.v1"), &table)?,
            ca_evolution::generator_params(&table)?
        );
        Ok(())
    }
}
