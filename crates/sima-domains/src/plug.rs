//! The in-tree domains behind the plug seam.
//!
//! [`BuiltinDomain`] and [`BuiltinGenerator`] present this crate's dispatches
//! as the objects a program outside the workspace supplies, so the formats
//! this build carries are reachable through exactly the seam a third party
//! writes against.
//!
//! Configuration crosses the seam as TOML text, so a plug parses the section
//! before handing it to the translation that owns its keys.

use sima_contracts::{DeviceBinding, DeviceInfo, DomainPlug, Executor, Generator, GeneratorPlug};
use sima_core::{Error, Result};
use sima_model::{Environment, FormatId, GeneratorId, Params};

use crate::domain::Domain;

/// One of this build's formats, behind the domain seam.
pub struct BuiltinDomain {
    domain: Domain,
}

impl BuiltinDomain {
    /// The plug for `format`, or a validation error naming an id this build
    /// does not carry.
    pub fn new(format: &FormatId) -> Result<BuiltinDomain> {
        Ok(BuiltinDomain {
            domain: crate::domain_for(format)?,
        })
    }
}

impl DomainPlug for BuiltinDomain {
    fn format(&self) -> &FormatId {
        &self.domain.format
    }

    fn environment(&self) -> &Environment {
        &self.domain.environment
    }

    fn executor(&self, device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
        (self.domain.executor)(device)
    }

    fn device_desc(&self, device: Option<&DeviceBinding>) -> Result<(String, String)> {
        (self.domain.device_desc)(device)
    }

    fn enumerate(&self) -> Result<Vec<DeviceInfo>> {
        (self.domain.enumerate)()
    }

    fn translate_params(&self, toml: &str, segmented: bool) -> Result<Params> {
        crate::params_for(&self.domain.format, &table(toml)?, segmented)
    }
}

/// One of this build's generators, behind the generator seam.
pub struct BuiltinGenerator {
    id: GeneratorId,
    format: FormatId,
}

impl GeneratorPlug for BuiltinGenerator {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    fn format(&self) -> &FormatId {
        &self.format
    }

    fn translate_params(&self, toml: &str) -> Result<Vec<u8>> {
        crate::generator_params_for(&self.id, &table(toml)?)
    }

    fn generator(&self) -> Result<Box<dyn Generator>> {
        crate::generator_for(&self.id)
    }
}

/// The generators this build offers for `format`.
///
/// Each program registers its generator under its format's id, so a format's
/// generators follow from the format itself; both dispatches are exercised
/// here, so a format this build does not carry — or one whose generator is
/// missing — fails naming the id rather than at the first question.
pub fn generators_for(format: &FormatId) -> Result<Vec<BuiltinGenerator>> {
    let id = GeneratorId::new(format.as_str())?;
    crate::domain_for(format)?;
    crate::generator_for(&id)?;
    Ok(vec![BuiltinGenerator {
        id,
        format: format.clone(),
    }])
}

/// Parses a configuration section's text into the table its translation reads.
/// Empty text is the table with no keys: a run that states no section.
fn table(toml: &str) -> Result<toml::Table> {
    toml.parse()
        .map_err(|e| Error::Validation(format!("the configuration section is no TOML: {e}")))
}

#[cfg(test)]
mod tests {
    use sima_contracts::{DomainPlug, GeneratorPlug};
    use sima_core::Result;
    use sima_model::FormatId;

    use super::*;
    use crate::{domain_for, generator_params_for, params_for};

    /// Every format this build carries.
    const FORMATS: [&str; 4] = [
        "stub.v1",
        "ca_evolution.gray_scott.v1",
        "ca_evolution.gray_scott_cuda.v1",
        "ca_evolution.nca.v1",
    ];

    /// A validated format id.
    fn format(name: &str) -> FormatId {
        FormatId::new(name).expect("format id")
    }

    #[test]
    fn a_plug_answers_for_the_format_it_was_built_for() -> Result<()> {
        for name in FORMATS {
            let format = format(name);
            let plug = BuiltinDomain::new(&format)?;
            assert_eq!(plug.format(), &format);
            // The environment is the one the dispatch supplies, so a run
            // reaching a format through a plug keeps its task keys.
            assert_eq!(plug.environment(), &domain_for(&format)?.environment);
        }
        Ok(())
    }

    #[test]
    fn a_plug_translates_the_section_text_to_the_canonical_bytes() -> Result<()> {
        // The seam carries the section as text, so the plug parses it and
        // answers what the in-process translation answers for the same table.
        let format = format("stub.v1");
        let plug = BuiltinDomain::new(&format)?;
        let text = "hex = \"00ff\"\n";
        assert_eq!(
            plug.translate_params(text, false)?,
            params_for(
                &format,
                &text.parse::<toml::Table>().expect("a table"),
                false
            )?
        );
        Ok(())
    }

    #[test]
    fn an_absent_section_translates_as_an_empty_table() -> Result<()> {
        // A run that states no params sends empty text, which is the table
        // with no keys rather than a parse failure.
        let format = format("stub.v1");
        assert_eq!(
            BuiltinDomain::new(&format)?.translate_params("", false)?,
            params_for(&format, &toml::Table::new(), false)?
        );
        Ok(())
    }

    #[test]
    fn a_section_that_is_no_toml_is_a_validation_error() {
        let plug = BuiltinDomain::new(&format("stub.v1")).expect("a registered format");
        let error = plug
            .translate_params("this is not toml", false)
            .expect_err("a parse failure");
        assert!(
            matches!(error, sima_core::Error::Validation(_)),
            "{error:?}"
        );
    }

    #[test]
    fn an_unknown_format_binds_no_plug() {
        let Err(error) = BuiltinDomain::new(&format("no-such-domain.v1")) else {
            panic!("expected a format this build does not carry to bind nothing");
        };
        assert!(error.to_string().contains("no-such-domain.v1"), "{error}");
    }

    #[test]
    fn every_format_offers_its_generator_under_its_own_id() -> Result<()> {
        // Every program in this build registers its generator under its
        // format's id, which is what makes the generators of a format
        // derivable from it.
        for name in FORMATS {
            let format = format(name);
            let generators = generators_for(&format)?;
            assert_eq!(generators.len(), 1, "{name} offers one generator");
            assert_eq!(generators[0].id().as_str(), name);
            assert_eq!(generators[0].format(), &format);
            assert_eq!(generators[0].generator()?.id().as_str(), name);
        }
        Ok(())
    }

    #[test]
    fn a_generator_plug_translates_the_section_text() -> Result<()> {
        let format = format("stub.v1");
        let generators = generators_for(&format)?;
        let text = "behaviors = [\"succeed\", \"reject\"]\n";
        assert_eq!(
            generators[0].translate_params(text)?,
            generator_params_for(
                generators[0].id(),
                &text.parse::<toml::Table>().expect("a table")
            )?
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
