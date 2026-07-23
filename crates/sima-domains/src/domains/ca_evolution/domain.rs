//! [`build_domain`]: assembles a [`Domain`] from a CA model.

use sima_core::Result;
use sima_model::{Environment, EnvironmentComponent, EnvironmentValue, FormatId};
use sima_toolkit_wgsl::{COMPILER_ID, selected_device_desc, source_digest};

use super::executor::CaExecutor;
use super::model::CaModel;
use crate::cellular::REDUCE_WGSL;
use crate::domain::Domain;

/// Assembles the [`Domain`] for the model `M`: the GPU executor and a
/// four-component environment — the executor's own version, the blake3 digest
/// of the WGSL update-kernel source, the digest of the reduction-kernel source,
/// and the pinned WGSL compiler id. The component names derive from `M::NAME`.
/// The two source digests and the compiler id together pin the compiled
/// SPIR-V: editing either shader or upgrading the compiler changes every task
/// key, forcing re-execution instead of silently reusing stale results. The
/// reduction joins the environment because its output gates committed bytes,
/// exactly as the update kernel's does. All components are computed device-free,
/// so [`domain_for`](crate::domain_for) never needs a GPU.
pub(crate) fn build_domain<M: CaModel>() -> Result<Domain> {
    Ok(Domain {
        format: FormatId::new(M::FORMAT_ID)?,
        // Captures nothing, so it coerces to the plain fn pointer the field
        // holds; `M` rides along through monomorphization.
        executor: |device| Ok(Box::new(CaExecutor::<M>::new(device)?)),
        // The toolkit speaks plain device ids; this is where the binding maps
        // to them.
        device_desc: |device| {
            selected_device_desc(device.map(|d| (d.vendor_id, d.device_id, d.member)))
        },
        environment: Environment::new(vec![
            EnvironmentComponent::new(
                format!("{}.executor", M::NAME),
                EnvironmentValue::Version(M::VERSION.to_string()),
            )?,
            EnvironmentComponent::new(
                format!("{}.kernel", M::NAME),
                EnvironmentValue::Digest(source_digest(M::KERNEL_WGSL)),
            )?,
            EnvironmentComponent::new(
                format!("{}.reduce", M::NAME),
                EnvironmentValue::Digest(source_digest(REDUCE_WGSL)),
            )?,
            EnvironmentComponent::new(
                "wgsl.compiler",
                EnvironmentValue::Version(COMPILER_ID.to_string()),
            )?,
        ])?,
    })
}
