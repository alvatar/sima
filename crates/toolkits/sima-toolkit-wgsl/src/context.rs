//! The Vulkan device context: the toolkit's owner of the true Vulkan lifetime.

use ash::vk;

use sima_contracts::DeviceClass;
use sima_core::{Error, Result};

use crate::instance::{self, InstanceGuard};
use crate::selection::{self, DeviceChoice};
use crate::validation::{self, ValidationMessenger};

/// Owns the Vulkan instance, logical device, compute queue, and command pool
/// for one headless compute context.
///
/// It is the single owner of the true Vulkan lifetime. Buffers and kernels
/// created from it each hold a cloned `ash::Device` and free their own objects
/// on drop; the context outlives them and drains the device before teardown.
/// The contract is wait-idle-before-drop: the context's own [`Drop`] calls
/// `device_wait_idle` before destroying anything, so buffer and kernel drops
/// need no synchronization of their own.
pub struct Context {
    /// Vulkan entry, kept alive for the whole instance/device lifetime.
    _entry: ash::Entry,
    instance: ash::Instance,
    device: ash::Device,
    queue: vk::Queue,
    command_pool: vk::CommandPool,
    memory_properties: vk::PhysicalDeviceMemoryProperties,
    limits: vk::PhysicalDeviceLimits,
    device_name: String,
    driver_version: String,
    /// Debug-utils messenger, present only when validation is enabled.
    validation: Option<ValidationMessenger>,
}

impl Context {
    /// Creates a headless compute context on the auto-selected device.
    ///
    /// The pick is the shared selection policy's: the `SIMA_GPU_DEVICE`
    /// enumeration index when it is set, and otherwise discrete before
    /// integrated before virtual before CPU, with the lowest enumeration index
    /// breaking ties.
    pub fn new() -> Result<Context> {
        Context::build(selection::select_physical_device)
    }

    /// Creates a headless compute context on the given member of the given
    /// device class.
    ///
    /// `member` counts within the class, ordered by Vulkan enumeration index —
    /// the numbering [`enumerate_devices`](crate::enumerate_devices) reports.
    /// An absent class or a member out of range is an [`Error::Backend`] naming
    /// the request and what exists.
    pub fn for_class(class: &DeviceClass, member: u32) -> Result<Context> {
        Context::build(|instance| selection::select_class_member(instance, class, member))
    }

    /// Builds a context around the device `select` picks.
    ///
    /// Validation is enabled only when [`validation::resolve_validation_request`]
    /// approves it. Every construction step rolls back through a guard, so a
    /// mid-build failure leaves no orphaned Vulkan object.
    fn build(select: impl FnOnce(&ash::Instance) -> Result<DeviceChoice>) -> Result<Context> {
        // The entry is stored in `Context::_entry`, keeping the loader resident
        // for the whole lifetime of the instance and device derived from it.
        let entry = instance::load_entry()?;
        let validation_enabled = validation::resolve_validation_request(&entry);
        let instance = InstanceGuard::new(instance::create(&entry, validation_enabled)?);

        let DeviceChoice {
            physical_device,
            queue_family_index,
        } = select(instance.get()?)?;
        // SAFETY: `physical_device` was returned by `select`, which enumerated
        // it from `instance`; both are alive on this frame.
        let properties = unsafe {
            instance
                .get()?
                .get_physical_device_properties(physical_device)
        };
        let memory_properties = unsafe {
            instance
                .get()?
                .get_physical_device_memory_properties(physical_device)
        };
        let device_name = selection::device_name(&properties);
        let driver_version = selection::driver_version(properties.driver_version);

        let queue_priority = [1.0_f32];
        let queue_info = vk::DeviceQueueCreateInfo::default()
            .queue_family_index(queue_family_index)
            .queue_priorities(&queue_priority);
        let device_info =
            vk::DeviceCreateInfo::default().queue_create_infos(std::slice::from_ref(&queue_info));
        // SAFETY: `physical_device` came from `instance`; `device_info` and the
        // `queue_info` it references live on this frame through the call.
        let device = unsafe {
            instance
                .get()?
                .create_device(physical_device, &device_info, None)
        }
        .map_err(|e| Error::Backend(format!("create logical device: {e}")))?;
        let device = DeviceGuard::new(device);
        // SAFETY: `device` was just created with one queue in `queue_family_index`
        // at priority index 0, so that queue is valid.
        let queue = unsafe { device.get()?.get_device_queue(queue_family_index, 0) };

        let pool_info = vk::CommandPoolCreateInfo::default()
            .queue_family_index(queue_family_index)
            .flags(vk::CommandPoolCreateFlags::TRANSIENT);
        // SAFETY: `pool_info` lives through the call; `queue_family_index` is the
        // family the device's queue was created from.
        let command_pool = unsafe { device.get()?.create_command_pool(&pool_info, None) }
            .map_err(|e| Error::Backend(format!("create command pool: {e}")))?;
        let command_pool = CommandPoolGuard::new(device.get()?, command_pool);

        // Created after every other fallible step so a failure here is the last
        // thing that can happen; the guards above still roll back on this path.
        let validation = validation_enabled
            .then(|| ValidationMessenger::create(&entry, instance.get()?))
            .transpose()?;

        Ok(Context {
            _entry: entry,
            instance: instance.finish()?,
            device: device.finish()?,
            queue,
            command_pool: command_pool.finish()?,
            memory_properties,
            limits: properties.limits,
            device_name,
            driver_version,
            validation,
        })
    }

    /// The selected device's reported name, for provenance.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// The selected device's reported driver version, for provenance.
    pub fn driver_version(&self) -> &str {
        &self.driver_version
    }

    /// The logical device, for objects that hold a cloned handle.
    pub(crate) fn device(&self) -> &ash::Device {
        &self.device
    }

    /// The device's memory properties, for memory-type selection.
    pub(crate) fn memory_properties(&self) -> &vk::PhysicalDeviceMemoryProperties {
        &self.memory_properties
    }

    /// The device's reported limits, for the bounds a kernel build and a
    /// dispatch are checked against.
    pub(crate) fn limits(&self) -> &vk::PhysicalDeviceLimits {
        &self.limits
    }

    /// Records a one-time command buffer through `recorder`, submits it to the
    /// compute queue, and blocks until its fence signals.
    ///
    /// Every transfer and dispatch is one such submission: the queue drains it
    /// before the next begins, trading batching for a simple, obviously correct
    /// ordering. The command buffer and fence are freed on every path.
    pub(crate) fn submit_immediate(&self, recorder: impl FnOnce(vk::CommandBuffer)) -> Result<()> {
        let mut submission = OneTimeSubmit::begin(&self.device, self.command_pool)?;
        let command_buffer = submission.command_buffer;
        let begin_info = vk::CommandBufferBeginInfo::default()
            .flags(vk::CommandBufferUsageFlags::ONE_TIME_SUBMIT);
        // SAFETY: the buffer was just allocated in the initial state and is local
        // to this call; `begin_info` lives through it and starts recording.
        unsafe {
            self.device
                .begin_command_buffer(command_buffer, &begin_info)
        }
        .map_err(|e| Error::Backend(format!("begin command buffer: {e}")))?;
        recorder(command_buffer);
        // SAFETY: the buffer is recording after `begin` and the recorder has run,
        // so ending it into the executable state is legal.
        unsafe { self.device.end_command_buffer(command_buffer) }
            .map_err(|e| Error::Backend(format!("end command buffer: {e}")))?;
        let submit_info =
            vk::SubmitInfo::default().command_buffers(std::slice::from_ref(&command_buffer));
        // SAFETY: the buffer is executable; `submit_info` borrows it via from_ref
        // and lives through the call; the fence is signalled only by this submit.
        unsafe {
            self.device.queue_submit(
                self.queue,
                std::slice::from_ref(&submit_info),
                submission.fence,
            )
        }
        .map_err(|e| Error::Backend(format!("submit command buffer: {e}")))?;
        submission.submitted = true;
        // SAFETY: the fence was passed to the submit above and is the only wait
        // target; the slice lives through the call.
        unsafe {
            self.device
                .wait_for_fences(std::slice::from_ref(&submission.fence), true, u64::MAX)
        }
        .map_err(|e| Error::Backend(format!("wait for fence: {e}")))?;
        Ok(())
    }
}

impl Drop for Context {
    /// Releases Vulkan resources in dependency-safe order.
    fn drop(&mut self) {
        // SAFETY: device_wait_idle drains in-flight submissions so no queue work
        // still references the device; the command pool is destroyed before the
        // device that owns it, the validation messenger before the instance it
        // was created on, and the device before the instance it came from.
        unsafe {
            let _ = self.device.device_wait_idle();
            self.device.destroy_command_pool(self.command_pool, None);
            self.device.destroy_device(None);
            if let Some(validation) = self.validation.take() {
                validation.destroy();
            }
            self.instance.destroy_instance(None);
        }
    }
}

/// Rolls back a created logical device until construction transfers it.
struct DeviceGuard {
    device: Option<ash::Device>,
}

impl DeviceGuard {
    fn new(device: ash::Device) -> Self {
        Self {
            device: Some(device),
        }
    }

    fn get(&self) -> Result<&ash::Device> {
        self.device
            .as_ref()
            .ok_or_else(|| Error::Backend("Vulkan device guard used after finish".to_string()))
    }

    fn finish(mut self) -> Result<ash::Device> {
        self.device
            .take()
            .ok_or_else(|| Error::Backend("Vulkan device guard finished twice".to_string()))
    }
}

impl Drop for DeviceGuard {
    fn drop(&mut self) {
        // SAFETY: the guard owns the logical device until `finish` transfers it;
        // on a rollback path it drains and destroys the device itself.
        unsafe {
            if let Some(device) = self.device.take() {
                let _ = device.device_wait_idle();
                device.destroy_device(None);
            }
        }
    }
}

/// Rolls back a created command pool until construction transfers it.
struct CommandPoolGuard {
    device: ash::Device,
    command_pool: Option<vk::CommandPool>,
}

impl CommandPoolGuard {
    fn new(device: &ash::Device, command_pool: vk::CommandPool) -> Self {
        Self {
            device: device.clone(),
            command_pool: Some(command_pool),
        }
    }

    fn finish(mut self) -> Result<vk::CommandPool> {
        self.command_pool
            .take()
            .ok_or_else(|| Error::Backend("command pool guard finished twice".to_string()))
    }
}

impl Drop for CommandPoolGuard {
    fn drop(&mut self) {
        // SAFETY: the cloned device outlives this call; on a rollback path the
        // `DeviceGuard` still holds the owning device, destroyed after this pool.
        unsafe {
            if let Some(command_pool) = self.command_pool.take() {
                self.device.destroy_command_pool(command_pool, None);
            }
        }
    }
}

/// Owns a one-time command buffer and its fence, freeing both on every path.
///
/// If the submission was issued but the fence wait failed, the drop drains the
/// device before freeing so no queue work still references the buffer.
struct OneTimeSubmit {
    device: ash::Device,
    command_pool: vk::CommandPool,
    command_buffer: vk::CommandBuffer,
    fence: vk::Fence,
    submitted: bool,
}

impl OneTimeSubmit {
    fn begin(device: &ash::Device, command_pool: vk::CommandPool) -> Result<Self> {
        let alloc_info = vk::CommandBufferAllocateInfo::default()
            .command_pool(command_pool)
            .level(vk::CommandBufferLevel::PRIMARY)
            .command_buffer_count(1);
        // SAFETY: `command_pool` is owned by the caller's context; `alloc_info`
        // lives through the call and requests exactly one buffer.
        let command_buffers = unsafe { device.allocate_command_buffers(&alloc_info) }
            .map_err(|e| Error::Backend(format!("allocate command buffer: {e}")))?;
        let command_buffer = *command_buffers
            .first()
            .ok_or_else(|| Error::Backend("no command buffer allocated".to_string()))?;
        // SAFETY: default (unsignaled) fence create info is stack-local.
        let fence = match unsafe { device.create_fence(&vk::FenceCreateInfo::default(), None) } {
            Ok(fence) => fence,
            Err(e) => {
                // SAFETY: the buffer was just allocated from `command_pool`; free
                // it before returning so the failure orphans nothing.
                unsafe {
                    device
                        .free_command_buffers(command_pool, std::slice::from_ref(&command_buffer));
                }
                return Err(Error::Backend(format!("create submit fence: {e}")));
            }
        };
        Ok(Self {
            device: device.clone(),
            command_pool,
            command_buffer,
            fence,
            submitted: false,
        })
    }
}

impl Drop for OneTimeSubmit {
    fn drop(&mut self) {
        // SAFETY: this value solely owns the buffer and fence; a completed or
        // never-issued submission frees directly, while an issued-but-unwaited
        // one drains the device first so no in-flight work references them.
        unsafe {
            if self.submitted {
                let _ = self.device.device_wait_idle();
            }
            self.device.free_command_buffers(
                self.command_pool,
                std::slice::from_ref(&self.command_buffer),
            );
            self.device.destroy_fence(self.fence, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opening_an_absent_device_class_fails() {
        let absent = DeviceClass::new("dead:beef").expect("class id");
        assert!(matches!(
            Context::for_class(&absent, 0),
            Err(Error::Backend(_))
        ));
    }

    /// Opening a context needs a Vulkan device.
    mod on_device {
        use super::*;

        #[test]
        fn context_reports_device_provenance() {
            let context = Context::new().expect("create compute context");
            assert!(!context.device_name().is_empty());
            assert!(!context.driver_version().is_empty());
        }

        #[test]
        fn a_context_opens_on_every_enumerated_device() {
            let devices = crate::enumerate_devices().expect("enumerate devices");
            assert!(!devices.is_empty(), "at least one compute-capable device");
            for device in &devices {
                let context = Context::for_class(&device.class, device.member)
                    .expect("open the enumerated device");
                // The context opened the class member that was asked for, not
                // whichever device the default policy prefers.
                assert_eq!(context.device_name(), device.name);
            }
        }
    }
}
