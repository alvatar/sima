//! Binding buffers to a kernel and dispatching workgroups.

use ash::vk;

use sima_core::{Error, Result};

use crate::buffer::Buffer;
use crate::context::Context;
use crate::kernel::Kernel;

impl Context {
    /// Binds `buffers` to the kernel's group-0 storage bindings in order and
    /// dispatches `groups` workgroups.
    ///
    /// One buffer is required per reflected binding, matched by position to the
    /// ascending binding numbers. A transient descriptor pool and set carry the
    /// bindings for this one dispatch and are freed after it completes. A
    /// leading barrier makes each buffer's prior writes — from a transfer
    /// upload or an earlier dispatch — available to the shader, so a buffer a
    /// previous dispatch wrote can be read by this one.
    pub fn dispatch(&self, kernel: &Kernel, buffers: &[&Buffer], groups: [u32; 3]) -> Result<()> {
        let bindings = kernel.bindings();
        if buffers.len() != bindings.len() {
            return Err(Error::Gpu(format!(
                "dispatch expects {} buffers for the kernel's bindings, got {}",
                bindings.len(),
                buffers.len()
            )));
        }
        let device = self.device();
        let pool = create_descriptor_pool(device, bindings.len() as u32)?;
        let pool = DescriptorPoolGuard::new(device, pool);
        let descriptor_set = allocate_set(device, pool.pool, kernel.descriptor_set_layout())?;

        let buffer_infos: Vec<vk::DescriptorBufferInfo> = buffers
            .iter()
            .map(|buffer| {
                vk::DescriptorBufferInfo::default()
                    .buffer(buffer.buffer)
                    .offset(0)
                    .range(vk::WHOLE_SIZE)
            })
            .collect();
        let writes: Vec<vk::WriteDescriptorSet> = bindings
            .iter()
            .zip(&buffer_infos)
            .map(|(&binding, info)| {
                vk::WriteDescriptorSet::default()
                    .dst_set(descriptor_set)
                    .dst_binding(binding)
                    .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
                    .buffer_info(std::slice::from_ref(info))
            })
            .collect();
        // SAFETY: `writes` and the `buffer_infos` they borrow live through the
        // call; `descriptor_set` was allocated from `pool` against the kernel's
        // layout, so each binding number is valid.
        unsafe { device.update_descriptor_sets(&writes, &[]) };

        self.submit_immediate(|command_buffer| {
            record_dispatch(
                device,
                command_buffer,
                kernel,
                buffers,
                descriptor_set,
                groups,
            );
        })
        // `pool` drops here, after the fence wait, freeing the set with it.
    }
}

/// Records the barrier, pipeline and set binds, and the dispatch.
fn record_dispatch(
    device: &ash::Device,
    command_buffer: vk::CommandBuffer,
    kernel: &Kernel,
    buffers: &[&Buffer],
    descriptor_set: vk::DescriptorSet,
    groups: [u32; 3],
) {
    let barriers: Vec<vk::BufferMemoryBarrier> = buffers
        .iter()
        .map(|buffer| {
            vk::BufferMemoryBarrier::default()
                .src_access_mask(vk::AccessFlags::TRANSFER_WRITE | vk::AccessFlags::SHADER_WRITE)
                .dst_access_mask(vk::AccessFlags::SHADER_READ | vk::AccessFlags::SHADER_WRITE)
                .src_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
                .dst_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
                .buffer(buffer.buffer)
                .offset(0)
                .size(buffer.size)
        })
        .collect();
    // SAFETY: `command_buffer` is recording; the kernel, buffers, and descriptor
    // set outlive the fence-waited submission; `barriers` lives through the
    // barrier call and every other argument is a valid handle.
    unsafe {
        device.cmd_pipeline_barrier(
            command_buffer,
            vk::PipelineStageFlags::TRANSFER | vk::PipelineStageFlags::COMPUTE_SHADER,
            vk::PipelineStageFlags::COMPUTE_SHADER,
            vk::DependencyFlags::empty(),
            &[],
            &barriers,
            &[],
        );
        device.cmd_bind_pipeline(
            command_buffer,
            vk::PipelineBindPoint::COMPUTE,
            kernel.pipeline(),
        );
        device.cmd_bind_descriptor_sets(
            command_buffer,
            vk::PipelineBindPoint::COMPUTE,
            kernel.pipeline_layout(),
            0,
            std::slice::from_ref(&descriptor_set),
            &[],
        );
        device.cmd_dispatch(command_buffer, groups[0], groups[1], groups[2]);
    }
}

/// Creates a transient descriptor pool holding one set of `count` storage
/// buffers.
fn create_descriptor_pool(device: &ash::Device, count: u32) -> Result<vk::DescriptorPool> {
    let pool_size = vk::DescriptorPoolSize::default()
        .ty(vk::DescriptorType::STORAGE_BUFFER)
        .descriptor_count(count);
    let info = vk::DescriptorPoolCreateInfo::default()
        .max_sets(1)
        .pool_sizes(std::slice::from_ref(&pool_size));
    // SAFETY: `info` and the `pool_size` it borrows live through the call.
    unsafe { device.create_descriptor_pool(&info, None) }
        .map_err(|e| Error::Gpu(format!("create descriptor pool: {e}")))
}

/// Allocates one descriptor set of `layout` from `pool`.
fn allocate_set(
    device: &ash::Device,
    pool: vk::DescriptorPool,
    layout: vk::DescriptorSetLayout,
) -> Result<vk::DescriptorSet> {
    let info = vk::DescriptorSetAllocateInfo::default()
        .descriptor_pool(pool)
        .set_layouts(std::slice::from_ref(&layout));
    // SAFETY: `pool` has capacity for this set and `info` borrows `layout` via
    // from_ref through the call; one layout in, so sets[0] is present on success.
    let sets = unsafe { device.allocate_descriptor_sets(&info) }
        .map_err(|e| Error::Gpu(format!("allocate descriptor set: {e}")))?;
    sets.first()
        .copied()
        .ok_or_else(|| Error::Gpu("descriptor set allocation returned no set".to_string()))
}

/// Owns a transient descriptor pool, destroying it (and the set within) on drop.
struct DescriptorPoolGuard {
    device: ash::Device,
    pool: vk::DescriptorPool,
}

impl DescriptorPoolGuard {
    fn new(device: &ash::Device, pool: vk::DescriptorPool) -> Self {
        Self {
            device: device.clone(),
            pool,
        }
    }
}

impl Drop for DescriptorPoolGuard {
    fn drop(&mut self) {
        // SAFETY: the dispatch that used this pool fence-completed before the
        // drop, so no in-flight work references the pool or its set.
        unsafe {
            if self.pool != vk::DescriptorPool::null() {
                self.device.destroy_descriptor_pool(self.pool, None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped compute kernel: `out[i] = in[i] * 2 + 1`.
    const SMOKE_WGSL: &str = include_str!("../shaders/smoke.wgsl");

    /// Requires a real Vulkan device.
    #[test]
    fn dispatch_applies_the_kernel() {
        let context = Context::new().expect("create compute context");
        let kernel = context.kernel(SMOKE_WGSL, "main").expect("build kernel");
        let input: [u32; 4] = [1, 2, 3, 4];
        let bytes: &[u8] = bytemuck::cast_slice(&input);
        let in_buffer = context.buffer(bytes.len()).expect("input buffer");
        let out_buffer = context.buffer(bytes.len()).expect("output buffer");
        context.upload(&in_buffer, bytes).expect("upload input");

        context
            .dispatch(&kernel, &[&in_buffer, &out_buffer], [1, 1, 1])
            .expect("dispatch");

        let read_back = context.download(&out_buffer).expect("download output");
        let output: &[u32] = bytemuck::cast_slice(&read_back);
        assert_eq!(output, [3, 5, 7, 9]);
    }

    /// Requires a real Vulkan device.
    ///
    /// A dispatch that reads a buffer a prior dispatch wrote must observe that
    /// output: the toolkit orders cross-dispatch shader-write to shader-read
    /// visibility through the leading buffer barrier's source scope. Applying
    /// `out = in * 2 + 1` twice, ping-ponging the two buffers, yields
    /// `in * 4 + 3`.
    #[test]
    fn a_dispatch_observes_a_prior_dispatchs_writes() {
        let context = Context::new().expect("create compute context");
        let kernel = context.kernel(SMOKE_WGSL, "main").expect("build kernel");
        let input: [u32; 4] = [1, 2, 3, 4];
        let bytes: &[u8] = bytemuck::cast_slice(&input);
        let a = context.buffer(bytes.len()).expect("buffer a");
        let b = context.buffer(bytes.len()).expect("buffer b");
        context.upload(&a, bytes).expect("upload input");

        // First pass writes b = a * 2 + 1 through the shader; the second pass
        // reads that shader output back out of b and writes a.
        context
            .dispatch(&kernel, &[&a, &b], [1, 1, 1])
            .expect("first dispatch");
        context
            .dispatch(&kernel, &[&b, &a], [1, 1, 1])
            .expect("second dispatch");

        let read_back = context.download(&a).expect("download");
        let output: &[u32] = bytemuck::cast_slice(&read_back);
        // (in * 2 + 1) * 2 + 1 = in * 4 + 3.
        assert_eq!(output, [7, 11, 15, 19]);
    }
}
