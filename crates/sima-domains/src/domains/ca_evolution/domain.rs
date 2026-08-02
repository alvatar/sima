//! [`CaDomain`]: one CA model on one backend, as the [`Domain`] a format binds.

use sima_contracts::{DeviceBinding, DeviceInfo, Domain, Executor};
use sima_core::{Result, hash_bytes};
use sima_model::{Environment, EnvironmentComponent, EnvironmentValue, FormatId, Params};

use super::executor::CaExecutor;
use super::model::CaModel;
use super::params;
use crate::domains::translate::table;
use crate::substrates::cellular::CellularEngine;

/// The model `M` running on the backend `E`, behind the domain contract.
///
/// Zero state beyond the identity it answers with: the executor is built per
/// device where execution happens, and everything else — the environment, the
/// device list, the configuration translation — is derived from `M` and `E`
/// alone. `M` and `E` ride along through monomorphization, so no field names
/// either.
pub(crate) struct CaDomain<M: CaModel, E: CellularEngine> {
    format: FormatId,
    environment: Environment,
    model: std::marker::PhantomData<fn() -> (M, E)>,
}

impl<M: CaModel, E: CellularEngine> CaDomain<M, E> {
    /// Assembles the domain for the model `M` on the backend `E`, with the
    /// four-component environment its results depend on: the executor's own
    /// version, the blake3 digest of the update-kernel source, the digest of the
    /// backend's reduction kernel, and the backend's pinned compiler identity.
    /// The first three names derive from `M::NAME`, the fourth from `E`.
    ///
    /// Together these pin what the device actually executes: editing either
    /// kernel or changing the compiler changes every task key, forcing
    /// re-execution instead of silently reusing stale results. The reduction
    /// joins the environment because its output gates committed bytes, exactly
    /// as the update kernel's does. Two backends give one rule two distinct
    /// environments, so neither one's results are invalidated by work on the
    /// other.
    ///
    /// Every component is computed device-free, so resolving a format never
    /// needs a GPU.
    pub(crate) fn new() -> Result<CaDomain<M, E>> {
        Ok(CaDomain {
            format: FormatId::new(M::FORMAT_ID)?,
            environment: Environment::new(vec![
                EnvironmentComponent::new(
                    format!("{}.executor", M::NAME),
                    EnvironmentValue::Version(M::VERSION.to_string()),
                )?,
                EnvironmentComponent::new(
                    format!("{}.kernel", M::NAME),
                    EnvironmentValue::Digest(hash_bytes(M::KERNEL_SOURCE.as_bytes())),
                )?,
                EnvironmentComponent::new(
                    format!("{}.reduce", M::NAME),
                    EnvironmentValue::Digest(E::reduce_digest()),
                )?,
                EnvironmentComponent::new(
                    E::COMPILER_COMPONENT,
                    EnvironmentValue::Version(E::COMPILER_ID.to_string()),
                )?,
            ])?,
            model: std::marker::PhantomData,
        })
    }
}

impl<M: CaModel, E: CellularEngine> Domain for CaDomain<M, E> {
    fn format(&self) -> &FormatId {
        &self.format
    }

    fn environment(&self) -> &Environment {
        &self.environment
    }

    fn executor(&self, device: Option<&DeviceBinding>) -> Result<Box<dyn Executor + Sync>> {
        Ok(Box::new(CaExecutor::<M, E>::new(device)?))
    }

    fn device_desc(&self, device: Option<&DeviceBinding>) -> Result<(String, String)> {
        E::device_desc(device)
    }

    fn enumerate_devices(&self) -> Result<Vec<DeviceInfo>> {
        E::enumerate_devices()
    }

    fn translate_config(&self, toml: &str, segmented: bool) -> Result<Params> {
        // The model is known statically here, so the section reaches its own
        // translation without a second dispatch through the format id.
        params::translate::<M>(&table(toml)?, segmented)
    }
}
