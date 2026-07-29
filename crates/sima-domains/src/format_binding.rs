//! [`FormatBinding`] and the static dispatches from config ids to code.
//!
//! A format id binds three things — this is what a `FormatBinding` groups: the
//! executor that evaluates specs of the format, the environment its results
//! depend on, and the translation of the domain-owned `[run.params]` section
//! into the opaque canonical params bytes. Generators dispatch separately:
//! one format has one executor but many generators, and a generator owns the
//! translation of its own `[run.generator]` keys. Both dispatches are static
//! matches; unknown ids are [`Error::Validation`].

use sima_contracts::{DeviceBinding, DeviceInfo, Executor, Generator};
use sima_core::{Error, Result};
use sima_model::{Environment, FormatId, GeneratorId, Params};

use crate::domains::{ca_evolution, stub};

/// Everything a format id binds: the executor that evaluates specs of the
/// format and the environment its results depend on. The format's params
/// translation is the third binding, dispatched by [`params_for`].
pub struct FormatBinding {
    /// The format this domain interprets.
    pub format: FormatId,
    /// Builds the executor for the format's specs, bound to the given device —
    /// or, for `None`, to the execution backend's default selection.
    ///
    /// A constructor rather than a built executor, because the device is known
    /// only where execution happens: the parent side reads a domain for its
    /// environment, params, and stats and never executes, while a worker learns
    /// its device at handshake time and builds the executor then.
    pub executor: fn(Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>>,
    /// Describes the device the executor built from the same binding computes
    /// on, as `(name, driver version)`; both empty for a domain that uses no
    /// device.
    ///
    /// Resolved without building an executor, so a worker can report its
    /// device at handshake time while the backend's engine stays lazy. A
    /// binding naming a device the machine does not have is an error here. The
    /// driver version is operational provenance the journal records: the one
    /// variable an environment hash cannot see across machines of one class.
    pub device_desc: fn(Option<&DeviceBinding>) -> Result<(String, String)>,
    /// Every device this domain's work can run on, as its execution backend
    /// enumerates them.
    ///
    /// Only that backend's devices can hold the work, so the enumeration
    /// travels with the domain rather than being selected from a list of
    /// backends this build knows. A domain that opens no device answers with an
    /// empty list, which the layers above read as a worker that needs no
    /// device.
    pub enumerate: fn() -> Result<Vec<DeviceInfo>>,
    /// The environment entering every task's identity.
    pub environment: Environment,
}

/// Static dispatch: the domains this build knows. The stub is matched directly;
/// every other id is offered to `ca_evolution`, whose registry claims its
/// models. An unclaimed id is [`Error::Validation`].
pub fn binding_for(format: &FormatId) -> Result<FormatBinding> {
    if format.as_str() == stub::ID {
        return stub::binding();
    }
    ca_evolution::binding_for(format).unwrap_or_else(|| {
        Err(Error::Validation(format!(
            "unknown format id {:?}",
            format.as_str()
        )))
    })
}

/// Params translation for the domain: the `[run.params]` table into the opaque
/// canonical params bytes. The domain owns and validates its keys. `segmented`
/// is whether the run divides candidates into segments — a domain may forbid a
/// setting that a segment chain cannot honor; the stub ignores it.
pub fn params_for(format: &FormatId, table: &toml::Table, segmented: bool) -> Result<Params> {
    if format.as_str() == stub::ID {
        return stub::params(table);
    }
    ca_evolution::params_for(format, table, segmented).unwrap_or_else(|| {
        Err(Error::Validation(format!(
            "unknown format id {:?}",
            format.as_str()
        )))
    })
}

/// Static generator dispatch. An unknown generator id is [`Error::Validation`].
pub fn generator_for(id: &GeneratorId) -> Result<Box<dyn Generator>> {
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
    use sima_contracts::DeviceClass;

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
            binding_for(&unknown).map(|_| ()),
            params_for(&unknown, &toml::Table::new(), false).map(|_| ()),
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
    fn an_unknown_generator_id_is_rejected() {
        let unknown = generator("no-such-generator.v1");
        match generator_for(&unknown).map(|_| ()) {
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
    fn the_stub_domain_binds_executor_and_environment() -> Result<()> {
        let domain = binding_for(&format("stub.v1"))?;
        assert_eq!(domain.format.as_str(), "stub.v1");
        // The executor answers for the domain's format.
        assert_eq!((domain.executor)(None)?.format().as_str(), "stub.v1");
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
    fn a_ca_evolution_model_id_delegates_to_the_registry() -> Result<()> {
        // The crate-level dispatch claims no ca_evolution id itself: it offers
        // the id to the registry. Binding here proves binding_for is device-free —
        // this test runs everywhere, GPU or not — and that the delegation reaches
        // the Gray-Scott model. The environment component digest is asserted in
        // the registry's own tests, which can read the model's kernel source.
        let domain = binding_for(&format("ca_evolution.gray_scott.v1"))?;
        assert_eq!(domain.format.as_str(), "ca_evolution.gray_scott.v1");
        assert_eq!(
            (domain.executor)(None)?.format().as_str(),
            "ca_evolution.gray_scott.v1"
        );
        let names: Vec<&str> = domain
            .environment
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
    fn a_ca_evolution_generator_delegates() -> Result<()> {
        let generator = generator_for(&generator("ca_evolution.gray_scott.v1"))?;
        assert_eq!(generator.id().as_str(), "ca_evolution.gray_scott.v1");
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
        let domain = binding_for(&format("ca_evolution.gray_scott.v1"))?;
        assert_eq!(
            (domain.executor)(Some(&binding))?.format().as_str(),
            "ca_evolution.gray_scott.v1"
        );
        Ok(())
    }

    #[test]
    fn the_stub_executor_ignores_the_binding() -> Result<()> {
        // The stub uses no device, so a binding changes nothing about it.
        let binding = DeviceBinding {
            class: DeviceClass::new("8086:7d51").expect("class id"),
            member: 0,
        };
        let domain = binding_for(&format("stub.v1"))?;
        assert_eq!(
            (domain.executor)(Some(&binding))?.format().as_str(),
            "stub.v1"
        );
        Ok(())
    }
}
