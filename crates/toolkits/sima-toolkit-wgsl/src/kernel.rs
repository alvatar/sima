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
    block_width: u32,
    source_digest: Hash,
}

impl Kernel {
    /// blake3 of the WGSL source bytes — an identity input for a domain.
    ///
    /// The WGSL is compiled in process, so the source is what identifies the
    /// kernel and [`compiler_id`](Kernel::compiler_id) states how it was
    /// lowered. The CUDA toolkit pairs differently: it hashes the committed PTX,
    /// the artifact the device executes, and its compiler id names only what
    /// that artifact targets.
    pub fn source_digest(&self) -> Hash {
        self.source_digest
    }

    /// Canonical compiler-identity string — an identity input for a domain.
    pub fn compiler_id(&self) -> &str {
        COMPILER_ID
    }

    /// Threads per workgroup along x. A caller sizing a grid divides its
    /// element count by this, so the width is stated once — in the shader — and
    /// read back rather than repeated at the dispatch site.
    pub fn block_width(&self) -> u32 {
        self.block_width
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
    /// Compiles WGSL and builds a compute pipeline for the named entry point,
    /// at the thread-block width the caller will size its grids by.
    ///
    /// The group-0 storage bindings reflected from the shader become the
    /// descriptor-set layout; the pipeline layout carries that one set and no
    /// push-constant range.
    ///
    /// `block_width` is the thread count per workgroup along x. The shader
    /// declares it too, with `@workgroup_size`, so the two are checked against
    /// each other here: a caller sizing a grid by a width the shader does not
    /// launch at would cover the wrong element count, which no dispatch would
    /// report. Widths beyond what the device launches are rejected here as
    /// well, before any pipeline is built.
    pub fn kernel(&self, wgsl: &str, entry: &str, block_width: u32) -> Result<Kernel> {
        let compiled = compile::compile(wgsl, entry)?;
        check_block_width(self.limits(), &compiled.module, entry, block_width)?;
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
            block_width,
            source_digest: compile::source_digest(wgsl),
        };
        kernel.descriptor_set_layout = create_set_layout(device, &kernel.bindings)?;
        kernel.pipeline_layout = create_pipeline_layout(device, kernel.descriptor_set_layout)?;
        kernel.pipeline =
            create_compute_pipeline(device, kernel.pipeline_layout, shader_module.module, entry)?;
        Ok(kernel)
    }
}

/// Confirms `block_width` is what the shader declares and what the device can
/// launch.
///
/// The declared size is checked on all three axes: this toolkit launches
/// one-dimensional grids, so a shader declaring depth on y or z would have its
/// extra invocations run with no caller sizing for them.
fn check_block_width(
    limits: &vk::PhysicalDeviceLimits,
    module: &naga::Module,
    entry: &str,
    block_width: u32,
) -> Result<()> {
    // The device limit is checked first, as the CUDA side checks it first, so
    // one bad width reports the same failure class on either backend.
    let maximum =
        limits.max_compute_work_group_size[0].min(limits.max_compute_work_group_invocations);
    if block_width == 0 || block_width > maximum {
        return Err(Error::Backend(format!(
            "kernel entry point '{entry}' asks for {block_width} threads per workgroup; this \
             device takes 1..={maximum}"
        )));
    }
    let declared = compile::workgroup_size(module, entry)?;
    if declared != [block_width, 1, 1] {
        return Err(Error::Backend(format!(
            "kernel entry point '{entry}' declares workgroup size {declared:?}; the caller sizes \
             its grids by {block_width} threads along x"
        )));
    }
    Ok(())
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

    /// The width the shipped smoke shader declares.
    const SMOKE_WIDTH: u32 = 64;

    /// Building a kernel against a context needs a Vulkan device.
    mod on_device {
        use super::*;

        #[test]
        fn kernel_reports_identity_inputs() {
            let context = Context::new().expect("create compute context");
            let kernel = context
                .kernel(SMOKE_WGSL, "main", SMOKE_WIDTH)
                .expect("build kernel");
            assert_eq!(kernel.source_digest(), source_digest(SMOKE_WGSL));
            assert_eq!(kernel.compiler_id(), COMPILER_ID);
            assert_eq!(kernel.block_width(), SMOKE_WIDTH);
        }

        #[test]
        fn a_block_width_the_shader_does_not_declare_is_rejected() {
            // A caller sizing its grid by 32 while the shader launches 64 would
            // dispatch half the groups the elements need, and nothing at
            // dispatch time could tell.
            let context = Context::new().expect("create compute context");
            match context.kernel(SMOKE_WGSL, "main", 32) {
                Err(Error::Backend(message)) => {
                    assert!(message.contains("workgroup size"), "{message}");
                }
                Err(other) => panic!("expected a backend width error, got {other:?}"),
                Ok(_) => panic!("expected a mismatched block width to be rejected"),
            }
        }

        /// A shader declaring a workgroup no device launches, so the caller's
        /// width agrees with the source and the device limit is what refuses
        /// it. Asking for a width the source does not declare would report the
        /// other failure and leave this branch untested.
        const WIDE_WGSL: &str = r#"
@group(0) @binding(0) var<storage, read> in_buf: array<u32>;

@compute @workgroup_size(2048)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    if (gid.x >= arrayLength(&in_buf)) { return; }
}
"#;

        #[test]
        fn a_block_width_the_device_cannot_launch_is_rejected() {
            // Caught before the pipeline is built, so an impossible width is a
            // clear toolkit error rather than an opaque dispatch failure later.
            // Vulkan guarantees only 128 invocations per workgroup, so 2048 is
            // past what any device here launches.
            let context = Context::new().expect("create compute context");
            match context.kernel(WIDE_WGSL, "main", 2048) {
                Err(Error::Backend(message)) => {
                    assert!(message.contains("threads per workgroup"), "{message}");
                }
                Err(other) => panic!("expected a backend width error, got {other:?}"),
                Ok(_) => panic!("expected an unlaunchable block width to be rejected"),
            }
        }
    }
}
