//! The in-tree formats behind the contracts a program outside the workspace
//! implements.
//!
//! [`BuiltinDomain`] presents this crate's dispatch as the [`Domain`] object a
//! program supplies, so the formats this build carries are reachable through
//! exactly the contract a third party writes against. Generators need no such
//! wrapper: each in-tree generator implements [`Generator`] whole, its own
//! translation included.
//!
//! Configuration crosses as TOML text, so the domain parses the section before
//! handing it to the translation that owns its keys.

use sima_contracts::{DeviceBinding, DeviceInfo, Domain, Executor, Generator};
use sima_core::Result;
use sima_model::{Environment, FormatId, GeneratorId, Params};

use crate::domains::translate::table;
use crate::format_binding::FormatBinding;

/// One of this build's formats, behind the domain contract.
pub struct BuiltinDomain {
    binding: FormatBinding,
}

impl BuiltinDomain {
    /// The domain for `format`, or a validation error naming an id this build
    /// does not carry.
    pub fn new(format: &FormatId) -> Result<BuiltinDomain> {
        Ok(BuiltinDomain {
            binding: crate::binding_for(format)?,
        })
    }
}

impl Domain for BuiltinDomain {
    fn format(&self) -> &FormatId {
        &self.binding.format
    }

    fn environment(&self) -> &Environment {
        &self.binding.environment
    }

    fn executor(&self, device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
        (self.binding.executor)(device)
    }

    fn device_desc(&self, device: Option<&DeviceBinding>) -> Result<(String, String)> {
        (self.binding.device_desc)(device)
    }

    fn enumerate_devices(&self) -> Result<Vec<DeviceInfo>> {
        (self.binding.enumerate)()
    }

    fn translate_config(&self, toml: &str, segmented: bool) -> Result<Params> {
        crate::params_for(&self.binding.format, &table(toml)?, segmented)
    }
}

/// The generators this build offers for `format`.
///
/// Each program registers its generator under its format's id, so a format's
/// generators follow from the format itself; both dispatches are exercised
/// here, so a format this build does not carry — or one whose generator is
/// missing — fails naming the id rather than at the first question.
pub fn generators_for(format: &FormatId) -> Result<Vec<Box<dyn Generator>>> {
    crate::binding_for(format)?;
    Ok(vec![crate::generator_for(&GeneratorId::new(
        format.as_str(),
    )?)?])
}

#[cfg(test)]
mod tests {
    use sima_core::Result;
    use sima_model::FormatId;

    use super::*;
    use crate::{binding_for, params_for};

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
    fn a_domain_answers_for_the_format_it_was_built_for() -> Result<()> {
        for name in FORMATS {
            let format = format(name);
            let domain = BuiltinDomain::new(&format)?;
            assert_eq!(domain.format(), &format);
            // The environment is the one the dispatch supplies, so a run
            // reaching a format through the contract keeps its task keys.
            assert_eq!(domain.environment(), &binding_for(&format)?.environment);
        }
        Ok(())
    }

    #[test]
    fn a_domain_translates_the_section_text_to_the_canonical_bytes() -> Result<()> {
        // Configuration arrives as text, so the domain parses it and answers
        // what the in-process translation answers for the same table.
        let format = format("stub.v1");
        let domain = BuiltinDomain::new(&format)?;
        let text = "hex = \"00ff\"\n";
        assert_eq!(
            domain.translate_config(text, false)?,
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
            BuiltinDomain::new(&format)?.translate_config("", false)?,
            params_for(&format, &toml::Table::new(), false)?
        );
        Ok(())
    }

    #[test]
    fn a_section_that_is_no_toml_is_a_validation_error() {
        let domain = BuiltinDomain::new(&format("stub.v1")).expect("a registered format");
        let error = domain
            .translate_config("this is not toml", false)
            .expect_err("a parse failure");
        assert!(
            matches!(error, sima_core::Error::Validation(_)),
            "{error:?}"
        );
    }

    #[test]
    fn an_unknown_format_binds_no_domain() {
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
        }
        Ok(())
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
