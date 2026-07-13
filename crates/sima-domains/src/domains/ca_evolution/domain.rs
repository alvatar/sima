//! [`build_domain`]: assembles a [`Domain`] from a CA model.

use sima_core::Result;
use sima_model::{Environment, EnvironmentComponent, EnvironmentValue, FormatId};
use sima_toolkit_wgsl::{COMPILER_ID, source_digest};

use super::executor::CaExecutor;
use super::model::CaModel;
use crate::domain::Domain;

/// Assembles the [`Domain`] for the model `M`: the GPU executor and a
/// three-component environment — the executor's own version, the blake3 digest
/// of the WGSL kernel source, and the pinned WGSL compiler id. The component
/// names derive from `M::NAME`. Source digest and compiler id together pin the
/// compiled SPIR-V: editing the shader or upgrading the compiler changes every
/// task key, forcing re-execution instead of silently reusing stale results.
/// All three components are computed device-free, so
/// [`domain_for`](crate::domain_for) never needs a GPU.
pub(crate) fn build_domain<M: CaModel>() -> Result<Domain> {
    Ok(Domain {
        format: FormatId::new(M::FORMAT_ID)?,
        executor: Box::new(CaExecutor::<M>::new()?),
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
                "wgsl.compiler",
                EnvironmentValue::Version(COMPILER_ID.to_string()),
            )?,
        ])?,
    })
}
