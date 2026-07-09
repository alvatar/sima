//! Device-local storage buffers and host/device transfers through staging.

use ash::vk;

use sima_core::{Error, Result};

use crate::context::Context;
use crate::selection::find_memory_type;

/// A device-local storage buffer plus its backing allocation, freed on drop.
///
/// The buffer holds a cloned `ash::Device` so its drop frees the Vulkan objects
/// directly, with null guards keeping a partially-constructed value sound. Drop
/// performs no synchronization: the owning [`Context`] drains the device before
/// teardown under the wait-idle-before-drop contract.
pub struct Buffer {
    device: ash::Device,
    pub(crate) buffer: vk::Buffer,
    memory: vk::DeviceMemory,
    pub(crate) size: vk::DeviceSize,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        // SAFETY: this value solely owns both handles; the owning context has
        // drained GPU work referencing them, and the null checks keep a
        // partially-constructed value (buffer created, allocation failed) sound.
        unsafe {
            if self.buffer != vk::Buffer::null() {
                self.device.destroy_buffer(self.buffer, None);
            }
            if self.memory != vk::DeviceMemory::null() {
                self.device.free_memory(self.memory, None);
            }
        }
    }
}

impl Context {
    /// Allocates a device-local storage buffer of `size` bytes.
    ///
    /// Usage is `STORAGE | TRANSFER_SRC | TRANSFER_DST`, so the buffer can bind
    /// to a kernel and take part in uploads and downloads. `size` must be
    /// greater than zero: Vulkan rejects a zero-sized buffer.
    pub fn buffer(&self, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Err(Error::Gpu(
                "buffer size must be greater than zero".to_string(),
            ));
        }
        create_buffer(
            self.device(),
            self.memory_properties(),
            size as vk::DeviceSize,
            vk::BufferUsageFlags::STORAGE_BUFFER
                | vk::BufferUsageFlags::TRANSFER_SRC
                | vk::BufferUsageFlags::TRANSFER_DST,
            vk::MemoryPropertyFlags::DEVICE_LOCAL,
        )
    }

    /// Copies host bytes into a device-local buffer through a staging buffer.
    ///
    /// The host writes into a host-visible staging buffer, then a transfer copy
    /// carries the bytes to the device-local destination. `data` must not
    /// exceed the destination's size.
    pub fn upload(&self, dst: &Buffer, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let byte_len = data.len() as vk::DeviceSize;
        if byte_len > dst.size {
            return Err(Error::Gpu(format!(
                "upload of {byte_len} bytes exceeds buffer size {}",
                dst.size
            )));
        }
        let staging = create_buffer(
            self.device(),
            self.memory_properties(),
            byte_len,
            vk::BufferUsageFlags::TRANSFER_SRC,
            vk::MemoryPropertyFlags::HOST_VISIBLE | vk::MemoryPropertyFlags::HOST_COHERENT,
        )?;
        // SAFETY: `staging` is host-visible and coherent, freshly created, and no
        // other mapping of its memory is live on this thread.
        let mapped = unsafe {
            self.device()
                .map_memory(staging.memory, 0, byte_len, vk::MemoryMapFlags::empty())
        }
        .map_err(|e| Error::Gpu(format!("map staging buffer: {e}")))?;
        // SAFETY: `data.len()` bytes fit the mapped range (staging is sized to
        // exactly that); the unmap pairs with the successful map above.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), mapped.cast::<u8>(), data.len());
            self.device().unmap_memory(staging.memory);
        }
        let region = vk::BufferCopy::default().size(byte_len);
        self.submit_immediate(|command_buffer| {
            // SAFETY: `command_buffer` is recording; `staging` and `dst` outlive
            // the submission, which is fence-waited before `staging` drops. Host
            // writes to coherent staging are made visible to the transfer by the
            // queue submit itself, so no leading barrier is needed.
            unsafe {
                self.device().cmd_copy_buffer(
                    command_buffer,
                    staging.buffer,
                    dst.buffer,
                    std::slice::from_ref(&region),
                );
            }
        })
    }

    /// Copies a device-local buffer back to host bytes through a staging buffer.
    ///
    /// Returns the buffer's full contents. A leading barrier makes the source's
    /// prior writes — from an upload transfer or a dispatch — available to the
    /// readback copy, and a trailing barrier makes the copied bytes available to
    /// the host read.
    pub fn download(&self, src: &Buffer) -> Result<Vec<u8>> {
        let staging = create_buffer(
            self.device(),
            self.memory_properties(),
            src.size,
            vk::BufferUsageFlags::TRANSFER_DST,
            vk::MemoryPropertyFlags::HOST_VISIBLE | vk::MemoryPropertyFlags::HOST_COHERENT,
        )?;
        let region = vk::BufferCopy::default().size(src.size);
        self.submit_immediate(|command_buffer| {
            record_readback(self.device(), command_buffer, src, &staging, region);
        })?;
        // SAFETY: `staging` is host-visible and coherent and the readback copy
        // has fence-completed, so `src.size` bytes are valid to read; no other
        // mapping is live.
        let mapped = unsafe {
            self.device()
                .map_memory(staging.memory, 0, src.size, vk::MemoryMapFlags::empty())
        }
        .map_err(|e| Error::Gpu(format!("map staging buffer: {e}")))?;
        let mut out = vec![0u8; src.size as usize];
        // SAFETY: `src.size` bytes fit both the mapped range and `out`; the unmap
        // pairs with the successful map above.
        unsafe {
            std::ptr::copy_nonoverlapping(mapped.cast::<u8>(), out.as_mut_ptr(), out.len());
            self.device().unmap_memory(staging.memory);
        }
        Ok(out)
    }
}

/// Records the readback of `src` into host-visible `staging`.
///
/// The leading barrier makes the source's prior transfer or compute writes
/// available to the copy; the trailing barrier makes the copy's writes
/// available to the host read of the coherent staging memory.
fn record_readback(
    device: &ash::Device,
    command_buffer: vk::CommandBuffer,
    src: &Buffer,
    staging: &Buffer,
    region: vk::BufferCopy,
) {
    let to_copy = vk::BufferMemoryBarrier::default()
        .src_access_mask(vk::AccessFlags::TRANSFER_WRITE | vk::AccessFlags::SHADER_WRITE)
        .dst_access_mask(vk::AccessFlags::TRANSFER_READ)
        .src_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
        .dst_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
        .buffer(src.buffer)
        .offset(0)
        .size(src.size);
    let to_host = vk::BufferMemoryBarrier::default()
        .src_access_mask(vk::AccessFlags::TRANSFER_WRITE)
        .dst_access_mask(vk::AccessFlags::HOST_READ)
        .src_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
        .dst_queue_family_index(vk::QUEUE_FAMILY_IGNORED)
        .buffer(staging.buffer)
        .offset(0)
        .size(staging.size);
    // SAFETY: `command_buffer` is recording; `src` and `staging` outlive the
    // fence-waited submission; every barrier/copy struct is stack-local and
    // referenced through from_ref slices that live through each call.
    unsafe {
        device.cmd_pipeline_barrier(
            command_buffer,
            vk::PipelineStageFlags::TRANSFER | vk::PipelineStageFlags::COMPUTE_SHADER,
            vk::PipelineStageFlags::TRANSFER,
            vk::DependencyFlags::empty(),
            &[],
            std::slice::from_ref(&to_copy),
            &[],
        );
        device.cmd_copy_buffer(
            command_buffer,
            src.buffer,
            staging.buffer,
            std::slice::from_ref(&region),
        );
        device.cmd_pipeline_barrier(
            command_buffer,
            vk::PipelineStageFlags::TRANSFER,
            vk::PipelineStageFlags::HOST,
            vk::DependencyFlags::empty(),
            &[],
            std::slice::from_ref(&to_host),
            &[],
        );
    }
}

/// Allocates a buffer and its backing memory with the given usage and memory
/// class, binding the two before returning.
fn create_buffer(
    device: &ash::Device,
    memory_properties: &vk::PhysicalDeviceMemoryProperties,
    size: vk::DeviceSize,
    usage: vk::BufferUsageFlags,
    required_memory: vk::MemoryPropertyFlags,
) -> Result<Buffer> {
    let buffer_info = vk::BufferCreateInfo::default()
        .size(size)
        .usage(usage)
        .sharing_mode(vk::SharingMode::EXCLUSIVE);
    // SAFETY: `buffer_info` is stack-local through the call; `device` is alive.
    let raw = unsafe { device.create_buffer(&buffer_info, None) }
        .map_err(|e| Error::Gpu(format!("create buffer: {e}")))?;
    // From here the partially-built value carries rollback: an early return
    // drops it and Drop releases whatever exists (memory is still null).
    let mut buffer = Buffer {
        device: device.clone(),
        buffer: raw,
        memory: vk::DeviceMemory::null(),
        size,
    };
    // SAFETY: `buffer.buffer` was just created from `device`; both are alive.
    let requirements = unsafe { device.get_buffer_memory_requirements(buffer.buffer) };
    let memory_type_index = find_memory_type(
        memory_properties,
        requirements.memory_type_bits,
        required_memory,
    )?;
    let alloc_info = vk::MemoryAllocateInfo::default()
        .allocation_size(requirements.size)
        .memory_type_index(memory_type_index);
    // SAFETY: `alloc_info` lives through the call; `memory_type_index` was
    // validated against `requirements.memory_type_bits` by find_memory_type.
    buffer.memory = unsafe { device.allocate_memory(&alloc_info, None) }
        .map_err(|e| Error::Gpu(format!("allocate buffer memory: {e}")))?;
    // SAFETY: buffer and memory were both created from `device`; offset 0 is
    // within the allocation.
    unsafe { device.bind_buffer_memory(buffer.buffer, buffer.memory, 0) }
        .map_err(|e| Error::Gpu(format!("bind buffer memory: {e}")))?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn buffer_rejects_zero_size() {
        let context = Context::new().expect("create compute context");
        assert!(matches!(context.buffer(0), Err(Error::Gpu(_))));
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn buffer_round_trips_bytes() {
        let context = Context::new().expect("create compute context");
        let data: Vec<u8> = (0..=255).collect();
        let buffer = context.buffer(data.len()).expect("allocate buffer");
        context.upload(&buffer, &data).expect("upload");
        let read_back = context.download(&buffer).expect("download");
        assert_eq!(read_back, data);
    }
}
