//! The static dispatches from config ids to code.
//!
//! A format id binds one [`Domain`]: the executor that evaluates specs of the
//! format, the environment its results depend on, the devices its work runs on,
//! and the translation of the `[run.params]` section it owns. Generators
//! dispatch separately, because one format has one executor but many
//! generators, and a generator owns the translation of its own
//! `[run.generator]` keys.
//!
//! The in-tree formats are reached through exactly the contracts a program
//! outside the workspace implements, so nothing here is a shape of its own:
//! `sima-domains` answers with a `Domain` object the same way a spawned program
//! does. Both dispatches are static matches; unknown ids are
//! [`Error::Validation`].

use sima_contracts::{Domain, Generator};
use sima_core::{Error, Result};
use sima_model::{FormatId, GeneratorId};

use crate::domains::{ca_evolution, stub};

/// The domain a format id binds. The stub is matched directly; every other id
/// is offered to `ca_evolution`, whose registry claims its models. An unclaimed
/// id is [`Error::Validation`].
pub fn domain_for(format: &FormatId) -> Result<Box<dyn Domain>> {
    if format.as_str() == stub::ID {
        return Ok(Box::new(stub::StubDomain::new()?));
    }
    ca_evolution::domain_for(format).unwrap_or_else(|| {
        Err(Error::Validation(format!(
            "unknown format id {:?}",
            format.as_str()
        )))
    })
}

/// The generator `id` names, checked against the format the run declares.
///
/// A run states its format and its generator separately, and the two must
/// agree: a generator producing specs of another format would mint a run id
/// over the mismatch and fail only when the first spec is stored. Resolving
/// through the format catches it at load, before any store exists, and the
/// refusal names both ids since either one could be the typo.
pub fn generator_for(format: &FormatId, id: &GeneratorId) -> Result<Box<dyn Generator>> {
    let generator = resolve_generator(id)?;
    if generator.format() != format {
        return Err(Error::Validation(format!(
            "generator {:?} produces specs of format {:?}, and the run declares format {:?}",
            id.as_str(),
            generator.format().as_str(),
            format.as_str()
        )));
    }
    Ok(generator)
}

/// The generators this build offers for `format`.
///
/// Each program registers its generator under its format's id, so a format's
/// generators follow from the format itself; the format is resolved first, so
/// one this build does not carry fails naming the id rather than at the first
/// question.
pub fn generators_for(format: &FormatId) -> Result<Vec<Box<dyn Generator>>> {
    domain_for(format)?;
    Ok(vec![generator_for(
        format,
        &GeneratorId::new(format.as_str())?,
    )?])
}

/// The generator `id` names, whatever format it produces for.
fn resolve_generator(id: &GeneratorId) -> Result<Box<dyn Generator>> {
    if id.as_str() == stub::ID {
        return Ok(Box::new(stub::StubGenerator::new()?));
    }
    ca_evolution::generator_for(id).unwrap_or_else(|| {
        Err(Error::Validation(format!(
            "unknown generator id {:?}",
            id.as_str()
        )))
    })
}

#[cfg(test)]
mod tests {
    use sima_contracts::{DeviceBinding, DeviceClass};

    use super::*;

    /// A validated format id.
    fn format(name: &str) -> FormatId {
        FormatId::new(name).expect("format id")
    }

    /// A validated generator id.
    fn generator(name: &str) -> GeneratorId {
        GeneratorId::new(name).expect("generator id")
    }

    /// Every format this build carries.
    const FORMATS: [&str; 4] = [
        "stub.v1",
        "ca_evolution.gray_scott.v1",
        "ca_evolution.gray_scott_cuda.v1",
        "ca_evolution.nca.v1",
    ];

    #[test]
    fn an_unknown_format_id_is_rejected() {
        match domain_for(&format("no-such-domain.v1")).map(|_| ()) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains("no-such-domain.v1"),
                    "the error names the id: {msg}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_generator_id_is_rejected() {
        let unknown = generator("no-such-generator.v1");
        match generator_for(&format("stub.v1"), &unknown).map(|_| ()) {
            Err(Error::Validation(msg)) => {
                assert!(
                    msg.contains("no-such-generator.v1"),
                    "the error names the id: {msg}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn a_generator_of_another_format_is_rejected_naming_both() {
        // The mismatch a run can write: the format and the generator are
        // separate config keys, and a generator that draws for another format
        // would produce specs the run's executor cannot read.
        match generator_for(&format("stub.v1"), &generator("ca_evolution.nca.v1")).map(|_| ()) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains("ca_evolution.nca.v1"), "{msg}");
                assert!(msg.contains("stub.v1"), "{msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn every_format_binds_a_domain_and_offers_its_generator() -> Result<()> {
        // Both dispatches are separate matches over the same ids, so a format
        // added to one and forgotten in the other resolves for some purposes
        // and not others. Resolving here also proves the dispatch is
        // device-free: this test runs on a machine with no GPU at all.
        for name in FORMATS {
            let format = format(name);
            let domain = domain_for(&format)?;
            assert_eq!(domain.format(), &format);
            assert_eq!(domain.executor(None)?.format(), &format);

            let generators = generators_for(&format)?;
            assert_eq!(generators.len(), 1, "{name} offers one generator");
            assert_eq!(generators[0].id().as_str(), name);
            assert_eq!(generators[0].format(), &format);
        }
        Ok(())
    }

    #[test]
    fn the_stub_domain_carries_one_environment_component() -> Result<()> {
        let domain = domain_for(&format("stub.v1"))?;
        assert_eq!(domain.environment().components().len(), 1);
        assert_eq!(domain.environment().components()[0].name(), "stub.executor");
        Ok(())
    }

    #[test]
    fn a_ca_evolution_format_carries_its_four_components() -> Result<()> {
        // The crate-level dispatch claims no ca_evolution id itself: it offers
        // the id to the registry. The component digests are asserted in the
        // registry's own tests, which can read the model's kernel source.
        let domain = domain_for(&format("ca_evolution.gray_scott.v1"))?;
        let names: Vec<&str> = domain
            .environment()
            .components()
            .iter()
            .map(|c| c.name())
            .collect();
        assert_eq!(
            names,
            [
                "ca_evolution.gray_scott.executor",
                "ca_evolution.gray_scott.kernel",
                "ca_evolution.gray_scott.reduce",
                "wgsl.compiler",
            ]
        );
        Ok(())
    }

    #[test]
    fn a_bound_executor_constructs_without_touching_a_device() -> Result<()> {
        // A GPU domain's construction stays device-free whether or not a device
        // is named: the engine initializes lazily on the first execute, so this
        // test — and `orchestrate`, which builds domains before any store
        // mutation — runs on a machine with no GPU at all. The binding names a
        // class that need not exist here; nothing resolves it until execute.
        let binding = DeviceBinding {
            class: DeviceClass::new("dead:beef").expect("class id"),
            member: 0,
        };
        for name in ["ca_evolution.gray_scott.v1", "stub.v1"] {
            let format = format(name);
            let domain = domain_for(&format)?;
            assert_eq!(domain.executor(Some(&binding))?.format(), &format);
        }
        Ok(())
    }

    #[test]
    fn a_domain_translates_the_section_text_it_owns() -> Result<()> {
        // Configuration crosses as text, so the domain parses it with a TOML of
        // its own and answers with the canonical bytes its executor reads.
        let domain = domain_for(&format("stub.v1"))?;
        assert_eq!(
            domain.translate_config("hex = \"00ff\"\n", false)?.bytes,
            vec![0x00, 0xff]
        );
        // A run that states no params sends empty text, which is the table with
        // no keys rather than a parse failure.
        assert!(domain.translate_config("", false)?.bytes.is_empty());
        Ok(())
    }

    #[test]
    fn a_section_that_is_no_toml_is_a_validation_error() {
        let domain = domain_for(&format("stub.v1")).expect("a registered format");
        let error = domain
            .translate_config("this is not toml", false)
            .expect_err("a parse failure");
        assert!(matches!(error, Error::Validation(_)), "{error:?}");
    }

    #[test]
    fn a_generator_translates_its_own_section_text() -> Result<()> {
        // The generator owns its keys, so text and table reach the same bytes.
        let generators = generators_for(&format("stub.v1"))?;
        let text = "behaviors = [\"succeed\", \"reject\"]\n";
        assert_eq!(
            generators[0].translate_config(text)?,
            crate::domains::stub::generator_params(&text.parse::<toml::Table>().expect("a table"))?
        );
        Ok(())
    }

    #[test]
    fn an_unknown_format_offers_no_generator() {
        let Err(error) = generators_for(&format("no-such-domain.v1")) else {
            panic!("expected a format this build does not carry to offer nothing");
        };
        assert!(error.to_string().contains("no-such-domain.v1"), "{error}");
    }
}
