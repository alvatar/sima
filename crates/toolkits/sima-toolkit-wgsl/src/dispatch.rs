//! Binding buffers to a kernel and dispatching workgroups.

use ash::vk;

use sima_core::{Error, Result};

use crate::buffer::Buffer;
use crate::context::Context;
use crate::kernel::Kernel;

/// The largest update a dispatch carries, in bytes: what Vulkan writes inline
/// from a command buffer. Both backends hold the same bound, so an update a
/// domain writes is portable between them.
const MAX_UPDATE: usize = 65536;

/// A small host-side write folded into a dispatch's own submission.
///
/// The bytes land in `buffer` before the kernel reads it, inside the one
/// command buffer the dispatch submits, so a value that changes per dispatch —
/// a step index, a pass number — costs no submission, no fence wait, and no
/// staging allocation of its own.
///
/// The updated buffer binds after every buffer in the dispatch's own list. That
/// is what makes the write sound to express: the update holds it exclusively
/// while the rest of the bindings are shared.
pub struct BufferUpdate<'a> {
    pub buffer: &'a mut Buffer,
    /// The bytes to write, from the start of the buffer: a whole number of
    /// 4-byte words, at most 64 KiB, and no longer than the buffer.
    pub bytes: &'a [u8],
}

impl Context {
    /// The largest workgroup count this device launches, per axis.
    ///
    /// Vulkan guarantees only 65535 on each axis, so a caller sizing a grid by
    /// its element count has to check: past the limit the dispatch is refused
    /// by the driver rather than clamped, and on a machine that happens to
    /// allow more it would search. Reading the device's own figure keeps the
    /// check exact rather than conservative.
    pub fn max_groups(&self) -> Result<[u32; 3]> {
        Ok(self.limits().max_compute_work_group_count)
    }

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
        self.dispatch_bound(kernel, buffers, groups, None)
    }

    /// Dispatches as [`dispatch`](Context::dispatch) does, with `update` applied
    /// to its own buffer first and that buffer bound last.
    ///
    /// The write is recorded into the dispatch's command buffer ahead of the
    /// leading barrier, so the same barrier that makes prior writes visible to
    /// the shader covers this one: the bytes land before the kernel that reads
    /// them, in one submission rather than two.
    pub fn dispatch_with_update(
        &self,
        kernel: &Kernel,
        buffers: &[&Buffer],
        update: BufferUpdate<'_>,
        groups: [u32; 3],
    ) -> Result<()> {
        check_update(update.bytes, update.buffer.size)?;
        let mut bound: Vec<&Buffer> = buffers.to_vec();
        bound.push(update.buffer);
        self.dispatch_bound(kernel, &bound, groups, Some(update.bytes))
    }

    /// The one dispatch path: validate the binding count, build the descriptor
    /// set, and submit the recorded command buffer.
    fn dispatch_bound(
        &self,
        kernel: &Kernel,
        buffers: &[&Buffer],
        groups: [u32; 3],
        update: Option<&[u8]>,
    ) -> Result<()> {
        let bindings = kernel.bindings();
        if buffers.len() != bindings.len() {
            return Err(Error::Backend(format!(
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
                update,
            );
        })
        // `pool` drops here, after the fence wait, freeing the set with it.
    }
}

/// Confirms an update fits what Vulkan writes inline and what the buffer holds.
fn check_update(bytes: &[u8], size: vk::DeviceSize) -> Result<()> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) || bytes.len() > MAX_UPDATE {
        return Err(Error::Backend(format!(
            "a dispatch update is 4 to {MAX_UPDATE} bytes in whole 4-byte words, got {}",
            bytes.len()
        )));
    }
    if bytes.len() as vk::DeviceSize > size {
        return Err(Error::Backend(format!(
            "a dispatch update of {} bytes exceeds buffer size {size}",
            bytes.len()
        )));
    }
    Ok(())
}

/// Records the update, the barrier, the pipeline and set binds, and the
/// dispatch.
///
/// The update is written before the barrier so the barrier's transfer source
/// scope covers it: the same edge that makes an earlier upload or dispatch
/// visible to the shader makes these bytes visible too. The update always
/// targets the last-bound buffer, which is where the caller's `BufferUpdate`
/// put it.
#[allow(clippy::too_many_arguments)]
fn record_dispatch(
    device: &ash::Device,
    command_buffer: vk::CommandBuffer,
    kernel: &Kernel,
    buffers: &[&Buffer],
    descriptor_set: vk::DescriptorSet,
    groups: [u32; 3],
    update: Option<&[u8]>,
) {
    if let (Some(bytes), Some(target)) = (update, buffers.last()) {
        // SAFETY: `command_buffer` is recording; `target` outlives the
        // fence-waited submission, and `check_update` held the size to a whole
        // number of 4-byte words within both the inline bound and the buffer.
        unsafe {
            device.cmd_update_buffer(command_buffer, target.buffer, 0, bytes);
        }
    }
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
        .map_err(|e| Error::Backend(format!("create descriptor pool: {e}")))
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
        .map_err(|e| Error::Backend(format!("allocate descriptor set: {e}")))?;
    sets.first()
        .copied()
        .ok_or_else(|| Error::Backend("descriptor set allocation returned no set".to_string()))
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
    /// The workgroup width that shader declares.
    const SMOKE_WIDTH: u32 = 64;

    #[test]
    fn an_update_is_whole_words_within_the_inline_bound() {
        // The three ways an update can be malformed, each rejected before any
        // command buffer is recorded.
        assert!(check_update(&[], 64).is_err(), "empty");
        assert!(check_update(&[0; 6], 64).is_err(), "not whole 4-byte words");
        assert!(
            check_update(&[0; MAX_UPDATE + 4], 1 << 20).is_err(),
            "past the inline bound"
        );
        check_update(&[0; 8], 64).expect("eight bytes into a 64-byte buffer");
    }

    #[test]
    fn an_update_longer_than_its_buffer_is_rejected() {
        let error = check_update(&[0; 16], 8).expect_err("past the buffer");
        let Error::Backend(message) = error else {
            panic!("expected a backend error");
        };
        assert!(message.contains("exceeds buffer size 8"), "{message}");
    }

    /// Every dispatch test launches a kernel, which needs a Vulkan device.
    mod on_device {
        use super::*;

        #[test]
        fn dispatch_applies_the_kernel() {
            let context = Context::new().expect("create compute context");
            let kernel = context
                .kernel(SMOKE_WGSL, "main", SMOKE_WIDTH)
                .expect("build kernel");
            let input: [u32; 4] = [1, 2, 3, 4];
            let bytes: &[u8] = bytemuck::cast_slice(&input);
            let mut in_buffer = context.buffer(bytes.len()).expect("input buffer");
            let out_buffer = context.buffer(bytes.len()).expect("output buffer");
            context.upload(&mut in_buffer, bytes).expect("upload input");

            context
                .dispatch(&kernel, &[&in_buffer, &out_buffer], [1, 1, 1])
                .expect("dispatch");

            let read_back = context.download(&out_buffer).expect("download output");
            let output: &[u32] = bytemuck::cast_slice(&read_back);
            assert_eq!(output, [3, 5, 7, 9]);
        }

        /// A dispatch that reads a buffer a prior dispatch wrote must observe that
        /// output: the toolkit orders cross-dispatch shader-write to shader-read
        /// visibility through the leading buffer barrier's source scope. Applying
        /// `out = in * 2 + 1` twice, ping-ponging the two buffers, yields
        /// `in * 4 + 3`.
        #[test]
        fn a_dispatch_observes_a_prior_dispatchs_writes() {
            let context = Context::new().expect("create compute context");
            let kernel = context
                .kernel(SMOKE_WGSL, "main", SMOKE_WIDTH)
                .expect("build kernel");
            let input: [u32; 4] = [1, 2, 3, 4];
            let bytes: &[u8] = bytemuck::cast_slice(&input);
            let mut a = context.buffer(bytes.len()).expect("buffer a");
            let b = context.buffer(bytes.len()).expect("buffer b");
            context.upload(&mut a, bytes).expect("upload input");

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
}
