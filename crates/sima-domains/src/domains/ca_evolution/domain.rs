//! [`build_domain`]: assembles a [`Domain`] from a CA model and the backend
//! it runs on.

use sima_core::{Result, hash_bytes};
use sima_model::{Environment, EnvironmentComponent, EnvironmentValue, FormatId};

use super::executor::CaExecutor;
use super::model::CaModel;
use crate::domain::Domain;
use crate::substrates::cellular::CellularEngine;

/// Assembles the [`Domain`] for the model `M` running on the backend `E`: the
/// executor and a four-component environment — the executor's own version, the
/// blake3 digest of the update-kernel source, the digest of the backend's
/// reduction kernel, and the backend's pinned compiler identity. The first
/// three names derive from `M::NAME`, the fourth from `E`.
///
/// Together these pin what the device actually executes: editing either kernel
/// or changing the compiler changes every task key, forcing re-execution
/// instead of silently reusing stale results. The reduction joins the
/// environment because its output gates committed bytes, exactly as the update
/// kernel's does. Two backends give one rule two distinct environments, so
/// neither one's results are invalidated by work on the other.
///
/// All components are computed device-free, so
/// [`domain_for`](crate::domain_for) never needs a GPU.
pub(crate) fn build_domain<M: CaModel, E: CellularEngine>() -> Result<Domain> {
    Ok(Domain {
        format: FormatId::new(M::FORMAT_ID)?,
        // Captures nothing, so it coerces to the plain fn pointer the field
        // holds; `M` and `E` ride along through monomorphization.
        executor: |device| Ok(Box::new(CaExecutor::<M, E>::new(device)?)),
        device_desc: E::device_desc,
        enumerate: E::enumerate,
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
    })
}
