//! What a program supplies to bind a format id: [`Domain`].
//!
//! A domain is the declaration side of a format: its environment, the devices
//! its work runs on, and the translation of the `[run.params]` section it owns.
//! It also builds the [`Executor`] that does the work, because the executor is
//! constructed in a worker process on a device chosen at run start.
//!
//! A trait rather than a struct of function pointers, because a domain holds
//! state: a renderer keeps its device and its loaded assets for the life of the
//! run.

use sima_core::Result;
use sima_model::{Environment, FormatId, Params};

use crate::{DeviceBinding, DeviceInfo, Executor};

/// Everything a format id binds, as the program that owns the format supplies
/// it.
///
/// One object carries the format's executor, the devices it runs on, the
/// environment its results depend on, and the translation of its own
/// configuration. Candidate production stays separate in [`crate::Generator`],
/// because one format has one executor and many generators.
pub trait Domain: Send + Sync {
    /// The format this domain interprets. A run over any other format is a
    /// validation failure, so the id is what a host checks each request
    /// against.
    fn format(&self) -> &FormatId;

    /// The environment entering every task's identity.
    fn environment(&self) -> &Environment;

    /// Builds the executor for the format's specs, bound to `device` — or, for
    /// `None`, to the execution backend's default selection.
    ///
    /// A constructor rather than a built executor, because the device is known
    /// only where execution happens.
    fn executor(&self, device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>>;

    /// Describes the device an executor built from the same binding computes
    /// on, as `(name, driver version)`; both empty for a format that uses no
    /// device.
    fn device_desc(&self, device: Option<&DeviceBinding>) -> Result<(String, String)>;

    /// Every device this format's work can run on, as its execution backend
    /// enumerates them. A format that opens no device answers with an empty
    /// list.
    fn enumerate(&self) -> Result<Vec<DeviceInfo>>;

    /// The `[run.params]` section as TOML text, translated to the canonical
    /// params bytes the format's executor reads. `segmented` is whether the run
    /// divides candidates into segments, which a format may forbid a setting to
    /// coexist with.
    ///
    /// The section crosses as text, so the domain parses it with a TOML of its
    /// own choosing.
    fn translate_params(&self, toml: &str, segmented: bool) -> Result<Params>;
}

/// `Domain` is dyn-compatible: a host holds one behind a trait object for the
/// life of a session. The auto-trait supertraits are part of the contract — a
/// domain is reached from the threads a run drives its workers on.
const _: fn() = || {
    fn _object_safe(_: &dyn Domain) {}
};

#[cfg(test)]
mod tests {
    use sima_core::Result;
    use sima_model::{
        Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorId, Params, Spec,
    };

    use crate::{DeviceBinding, DeviceInfo, Domain, Executor, Generator};

    /// A domain over a format that opens no device: enough of one to exercise
    /// the contract as an object.
    struct TestDomain {
        format: FormatId,
        environment: Environment,
    }

    impl Domain for TestDomain {
        fn format(&self) -> &FormatId {
            &self.format
        }

        fn environment(&self) -> &Environment {
            &self.environment
        }

        fn executor(&self, _device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
            Err(sima_core::Error::Validation("no executor here".to_string()))
        }

        fn device_desc(&self, _device: Option<&DeviceBinding>) -> Result<(String, String)> {
            Ok((String::new(), String::new()))
        }

        fn enumerate(&self) -> Result<Vec<DeviceInfo>> {
            Ok(Vec::new())
        }

        fn translate_params(&self, toml: &str, segmented: bool) -> Result<Params> {
            Ok(Params {
                bytes: format!("{toml}:{segmented}").into_bytes(),
            })
        }
    }

    /// A generator over the same format.
    struct TestGenerator {
        id: GeneratorId,
        format: FormatId,
    }

    impl Generator for TestGenerator {
        fn id(&self) -> &GeneratorId {
            &self.id
        }

        fn format(&self) -> &FormatId {
            &self.format
        }

        fn translate_params(&self, toml: &str) -> Result<Vec<u8>> {
            Ok(toml.as_bytes().to_vec())
        }

        fn generate(&self, root_seed: u64, _params: &[u8]) -> Result<Vec<Spec>> {
            Ok(vec![Spec {
                format: self.format.clone(),
                bytes: vec![root_seed as u8],
            }])
        }
    }

    /// The two components a program supplies.
    fn components() -> Result<(TestDomain, TestGenerator)> {
        let format = FormatId::new("domain-test.v1")?;
        let environment = Environment::new(vec![EnvironmentComponent::new(
            "domain-test.executor",
            EnvironmentValue::Version("v1".to_string()),
        )?])?;
        Ok((
            TestDomain {
                format: format.clone(),
                environment,
            },
            TestGenerator {
                id: GeneratorId::new("domain-test.v1")?,
                format,
            },
        ))
    }

    #[test]
    fn a_domain_answers_for_its_format_as_an_object() -> Result<()> {
        // What the host holds: the domain behind a trait object, answering the
        // questions a run asks of a format.
        let (domain, _) = components()?;
        let domain: &dyn Domain = &domain;
        assert_eq!(domain.format().as_str(), "domain-test.v1");
        assert_eq!(domain.environment().components().len(), 1);
        assert!(domain.enumerate()?.is_empty());
        assert_eq!(
            domain.translate_params("count = 3", true)?.bytes,
            b"count = 3:true"
        );
        Ok(())
    }

    #[test]
    fn a_generator_names_the_format_it_produces_for() -> Result<()> {
        let (_, generator) = components()?;
        let generator: &dyn Generator = &generator;
        assert_eq!(generator.id().as_str(), "domain-test.v1");
        assert_eq!(generator.format().as_str(), "domain-test.v1");
        assert_eq!(generator.translate_params("n = 1")?, b"n = 1");
        assert_eq!(
            generator.generate(7, &[])?[0].format.as_str(),
            "domain-test.v1"
        );
        Ok(())
    }

    #[test]
    fn both_components_configure_from_toml_text() -> Result<()> {
        // Configuration crosses as the text of the section, so a program parses
        // it with a toml of its own choosing and sima's stays its own.
        let (domain, generator) = components()?;
        let params: Params = domain.translate_params("hex = \"00\"", false)?;
        assert_eq!(params.bytes, b"hex = \"00\":false");
        assert_eq!(
            generator.translate_params("behaviors = []")?,
            b"behaviors = []"
        );
        Ok(())
    }
}
