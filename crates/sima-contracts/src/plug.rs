//! What a program supplies to bind a format id: [`DomainPlug`] and
//! [`GeneratorPlug`].
//!
//! A plug is the construction side of the two seams: [`Executor`] and
//! [`Generator`] evaluate and produce, and a plug is what a program hands over
//! so a run can reach them — the format's environment, the devices its work
//! runs on, and the translation of the program's own configuration.
//!
//! Traits rather than structs of function pointers, because a plug holds state:
//! a renderer keeps its device and its loaded assets for the life of the run.

use sima_core::Result;
use sima_model::{Environment, FormatId, GeneratorId, Params};

use crate::{DeviceBinding, DeviceInfo, Executor, Generator};

/// Everything a format id binds, as the program that owns the format supplies
/// it.
///
/// One object carries the format's executor, the devices it runs on, the
/// environment its results depend on, and the translation of its own
/// configuration. Generators target a format through [`GeneratorPlug`] and stay
/// separate, because one format has one executor and many generators.
pub trait DomainPlug: Send + Sync {
    /// The format this plug interprets. A run over any other format is a
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
    /// The section crosses as text, so the plug parses it with a TOML of its
    /// own choosing.
    fn translate_params(&self, toml: &str, segmented: bool) -> Result<Params>;
}

/// A generator targeting one format.
pub trait GeneratorPlug: Send + Sync {
    /// The generator id a run config names this generator by.
    fn id(&self) -> &GeneratorId;

    /// The format the specs this generator produces are of.
    fn format(&self) -> &FormatId;

    /// The `[run.generator]` section as TOML text, minus `id`, translated to
    /// the generator's opaque params blob.
    fn translate_params(&self, toml: &str) -> Result<Vec<u8>>;

    /// Builds the generator itself.
    fn generator(&self) -> Result<Box<dyn Generator>>;
}

/// Both plugs are dyn-compatible: a host holds one behind a trait object for
/// the life of a session. The auto-trait supertraits are part of the contract —
/// a plug is reached from the threads a run drives its workers on.
const _: fn() = || {
    fn _domain_object_safe(_: &dyn DomainPlug) {}
    fn _generator_object_safe(_: &dyn GeneratorPlug) {}
};

#[cfg(test)]
mod tests {
    use sima_core::Result;
    use sima_model::{
        Environment, EnvironmentComponent, EnvironmentValue, FormatId, GeneratorId, Params, Spec,
    };

    use crate::{DeviceBinding, DeviceInfo, DomainPlug, Executor, Generator, GeneratorPlug};

    /// A plug over a format that opens no device: enough of one to exercise
    /// the seam as an object.
    struct TestPlug {
        format: FormatId,
        environment: Environment,
    }

    impl DomainPlug for TestPlug {
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

    /// A generator plug over the same format.
    struct TestGeneratorPlug {
        id: GeneratorId,
        format: FormatId,
    }

    impl GeneratorPlug for TestGeneratorPlug {
        fn id(&self) -> &GeneratorId {
            &self.id
        }

        fn format(&self) -> &FormatId {
            &self.format
        }

        fn translate_params(&self, toml: &str) -> Result<Vec<u8>> {
            Ok(toml.as_bytes().to_vec())
        }

        fn generator(&self) -> Result<Box<dyn Generator>> {
            Err(sima_core::Error::Validation(
                "no generator here".to_string(),
            ))
        }
    }

    /// The pair a test drives through the two seams.
    fn plugs() -> Result<(TestPlug, TestGeneratorPlug)> {
        let format = FormatId::new("plug-test.v1")?;
        let environment = Environment::new(vec![EnvironmentComponent::new(
            "plug-test.executor",
            EnvironmentValue::Version("v1".to_string()),
        )?])?;
        Ok((
            TestPlug {
                format: format.clone(),
                environment,
            },
            TestGeneratorPlug {
                id: GeneratorId::new("plug-test.v1")?,
                format,
            },
        ))
    }

    #[test]
    fn a_domain_plug_answers_for_its_format_as_an_object() -> Result<()> {
        // What the host holds: the plug behind a trait object, answering the
        // questions a run asks of a format.
        let (plug, _) = plugs()?;
        let plug: &dyn DomainPlug = &plug;
        assert_eq!(plug.format().as_str(), "plug-test.v1");
        assert_eq!(plug.environment().components().len(), 1);
        assert!(plug.enumerate()?.is_empty());
        assert_eq!(
            plug.translate_params("count = 3", true)?.bytes,
            b"count = 3:true"
        );
        Ok(())
    }

    #[test]
    fn a_generator_plug_names_the_format_it_produces_for() -> Result<()> {
        let (_, plug) = plugs()?;
        let plug: &dyn GeneratorPlug = &plug;
        assert_eq!(plug.id().as_str(), "plug-test.v1");
        assert_eq!(plug.format().as_str(), "plug-test.v1");
        assert_eq!(plug.translate_params("n = 1")?, b"n = 1");
        Ok(())
    }

    #[test]
    fn a_plug_configures_from_toml_text() -> Result<()> {
        // Configuration crosses as the text of the section, so a plug parses
        // it with a toml of its own choosing and sima's stays its own.
        let (plug, generator) = plugs()?;
        let params: Params = plug.translate_params("hex = \"00\"", false)?;
        assert_eq!(params.bytes, b"hex = \"00\":false");
        assert_eq!(
            generator.translate_params("behaviors = []")?,
            b"behaviors = []"
        );
        Ok(())
    }

    /// A spec list is what a generator plug's generator produces; the type is
    /// part of the seam even where this plug builds none.
    #[test]
    fn a_generator_plug_produces_specs_of_its_format() -> Result<()> {
        let (_, plug) = plugs()?;
        assert!(plug.generator().is_err());
        let spec = Spec {
            format: plug.format().clone(),
            bytes: vec![7],
        };
        assert_eq!(spec.format.as_str(), "plug-test.v1");
        Ok(())
    }
}
