//! Compute-pipeline creation from compiled WGSL.

use std::ffi::CString;

use ash::vk;

use sima_core::{Error, Hash, Result};

use crate::compile::{self, COMPILER_ID};
use crate::context::Context;

/// A compiled compute kernel: its shader pipeline plus the identity inputs a
/// domain records.
///
/// The kernel holds a cloned `ash::Device` and frees its pipeline, pipeline
/// layout, and descriptor-set layout on drop, with null guards keeping a
/// partially-constructed value sound. The shader module is a build-time
/// temporary, destroyed once the pipeline is built. Drop performs no
/// synchronization under the owning context's wait-idle-before-drop contract.
pub struct Kernel {
    device: ash::Device,
    descriptor_set_layout: vk::DescriptorSetLayout,
    pipeline_layout: vk::PipelineLayout,
    pipeline: vk::Pipeline,
    /// Group-0 storage-buffer binding numbers, ascending; the order buffers
    /// bind at dispatch.
    bindings: Vec<u32>,
    source_digest: Hash,
}

impl Kernel {
    /// blake3 of the WGSL source bytes — an identity input for a domain.
    pub fn source_digest(&self) -> Hash {
        self.source_digest
    }

    /// Canonical compiler-identity string — an identity input for a domain.
    pub fn compiler_id(&self) -> &str {
        COMPILER_ID
    }

    /// The compute pipeline handle.
    pub(crate) fn pipeline(&self) -> vk::Pipeline {
        self.pipeline
    }

    /// The pipeline layout handle.
    pub(crate) fn pipeline_layout(&self) -> vk::PipelineLayout {
        self.pipeline_layout
    }

    /// The group-0 descriptor-set layout handle.
    pub(crate) fn descriptor_set_layout(&self) -> vk::DescriptorSetLayout {
        self.descriptor_set_layout
    }

    /// The group-0 storage-buffer binding numbers, ascending.
    pub(crate) fn bindings(&self) -> &[u32] {
        &self.bindings
    }
}

impl Drop for Kernel {
    fn drop(&mut self) {
        // SAFETY: this value solely owns each handle; the owning context has
        // drained GPU work referencing them, and the null checks keep a
        // partially-constructed value sound.
        unsafe {
            if self.pipeline != vk::Pipeline::null() {
                self.device.destroy_pipeline(self.pipeline, None);
            }
            if self.pipeline_layout != vk::PipelineLayout::null() {
                self.device
                    .destroy_pipeline_layout(self.pipeline_layout, None);
            }
            if self.descriptor_set_layout != vk::DescriptorSetLayout::null() {
                self.device
                    .destroy_descriptor_set_layout(self.descriptor_set_layout, None);
            }
        }
    }
}

impl Context {
    /// Compiles WGSL and builds a compute pipeline for the named entry point.
    ///
    /// The group-0 storage bindings reflected from the shader become the
    /// descriptor-set layout; the pipeline layout carries that one set and no
    /// push-constant range.
    pub fn kernel(&self, wgsl: &str, entry: &str) -> Result<Kernel> {
        let compiled = compile::compile(wgsl, entry)?;
        let bindings = compile::storage_bindings(&compiled.module);
        let device = self.device();

        let shader_info = vk::ShaderModuleCreateInfo::default().code(&compiled.spirv);
        // SAFETY: `shader_info` and the SPIR-V slice it borrows live through the
        // call; the words came from the naga emitter for this module.
        let shader_module = unsafe { device.create_shader_module(&shader_info, None) }
            .map_err(|e| Error::Backend(format!("create shader module: {e}")))?;
        let shader_module = ShaderModuleGuard::new(device, shader_module);

        // Partially-built value carrying rollback: an early return drops it and
        // Drop frees whatever handles are set.
        let mut kernel = Kernel {
            device: device.clone(),
            descriptor_set_layout: vk::DescriptorSetLayout::null(),
            pipeline_layout: vk::PipelineLayout::null(),
            pipeline: vk::Pipeline::null(),
            bindings,
            source_digest: compile::source_digest(wgsl),
        };
        kernel.descriptor_set_layout = create_set_layout(device, &kernel.bindings)?;
        kernel.pipeline_layout = create_pipeline_layout(device, kernel.descriptor_set_layout)?;
        kernel.pipeline =
            create_compute_pipeline(device, kernel.pipeline_layout, shader_module.module, entry)?;
        Ok(kernel)
    }
}

/// Builds the group-0 descriptor-set layout: one `STORAGE_BUFFER` binding per
/// reflected binding number, visible to the compute stage.
fn create_set_layout(device: &ash::Device, bindings: &[u32]) -> Result<vk::DescriptorSetLayout> {
    let layout_bindings: Vec<vk::DescriptorSetLayoutBinding> = bindings
        .iter()
        .map(|&binding| {
            vk::DescriptorSetLayoutBinding::default()
                .binding(binding)
                .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
                .descriptor_count(1)
                .stage_flags(vk::ShaderStageFlags::COMPUTE)
        })
        .collect();
    let info = vk::DescriptorSetLayoutCreateInfo::default().bindings(&layout_bindings);
    // SAFETY: `info` and the `layout_bindings` it borrows live through the call.
    unsafe { device.create_descriptor_set_layout(&info, None) }
        .map_err(|e| Error::Backend(format!("create descriptor set layout: {e}")))
}

/// Builds a pipeline layout carrying the one descriptor set and no push
/// constants.
fn create_pipeline_layout(
    device: &ash::Device,
    set_layout: vk::DescriptorSetLayout,
) -> Result<vk::PipelineLayout> {
    let info =
        vk::PipelineLayoutCreateInfo::default().set_layouts(std::slice::from_ref(&set_layout));
    // SAFETY: `info` borrows `set_layout` via from_ref and lives through the call.
    unsafe { device.create_pipeline_layout(&info, None) }
        .map_err(|e| Error::Backend(format!("create pipeline layout: {e}")))
}

/// Builds the compute pipeline for `entry` from the shader module.
fn create_compute_pipeline(
    device: &ash::Device,
    pipeline_layout: vk::PipelineLayout,
    shader_module: vk::ShaderModule,
    entry: &str,
) -> Result<vk::Pipeline> {
    let entry_name = CString::new(entry)
        .map_err(|_| Error::Backend(format!("entry point name '{entry}' contains a NUL byte")))?;
    let stage = vk::PipelineShaderStageCreateInfo::default()
        .stage(vk::ShaderStageFlags::COMPUTE)
        .module(shader_module)
        .name(&entry_name);
    let info = vk::ComputePipelineCreateInfo::default()
        .stage(stage)
        .layout(pipeline_layout);
    // SAFETY: `info`, the `stage` it embeds, and `entry_name` live through the
    // call; the module and layout are valid handles created above.
    let pipelines = unsafe {
        device.create_compute_pipelines(
            vk::PipelineCache::null(),
            std::slice::from_ref(&info),
            None,
        )
    }
    .map_err(|(_, e)| Error::Backend(format!("create compute pipeline: {e}")))?;
    pipelines
        .first()
        .copied()
        .ok_or_else(|| Error::Backend("compute pipeline creation returned no pipeline".to_string()))
}

/// Owns a shader module for the duration of a pipeline build, destroying it on
/// drop. The module is a build-time temporary: once the pipeline is created it
/// is no longer referenced.
struct ShaderModuleGuard {
    device: ash::Device,
    module: vk::ShaderModule,
}

impl ShaderModuleGuard {
    fn new(device: &ash::Device, module: vk::ShaderModule) -> Self {
        Self {
            device: device.clone(),
            module,
        }
    }
}

impl Drop for ShaderModuleGuard {
    fn drop(&mut self) {
        // SAFETY: the guard solely owns the module and pipeline creation has
        // finished or failed by the time it drops, so no pending call references it.
        unsafe {
            if self.module != vk::ShaderModule::null() {
                self.device.destroy_shader_module(self.module, None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_digest;

    /// The shipped compute kernel with two group-0 storage buffers.
    const SMOKE_WGSL: &str = include_str!("../shaders/smoke.wgsl");

    /// Requires a real Vulkan device.
    #[test]
    fn kernel_reports_identity_inputs() {
        let context = Context::new().expect("create compute context");
        let kernel = context.kernel(SMOKE_WGSL, "main").expect("build kernel");
        assert_eq!(kernel.source_digest(), source_digest(SMOKE_WGSL));
        assert_eq!(kernel.compiler_id(), COMPILER_ID);
    }
}
