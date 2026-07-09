//! The Vulkan device context: the toolkit's owner of the true Vulkan lifetime.

use ash::vk;

use sima_core::{Error, Result};

use crate::selection::{self, DeviceChoice};
use crate::validation::{
    self, ValidationMessenger, debug_utils_extension_name, validation_layer_names,
};

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
    queue_family_index: u32,
    command_pool: vk::CommandPool,
    memory_properties: vk::PhysicalDeviceMemoryProperties,
    device_name: String,
    driver_version: String,
    /// Debug-utils messenger, present only when validation is enabled.
    validation: Option<ValidationMessenger>,
}

impl Context {
    /// Creates a headless compute context on the auto-selected device.
    ///
    /// The device is chosen by [`selection::select_physical_device`]; validation
    /// is enabled only when [`validation::resolve_validation_request`] approves
    /// it. Every construction step rolls back through a guard, so a mid-build
    /// failure leaves no orphaned Vulkan object.
    pub fn new() -> Result<Context> {
        // SAFETY: loads the platform Vulkan loader; the returned Entry is stored
        // in `Context::_entry`, keeping the library loaded for the whole lifetime
        // of the instance and device derived from it.
        let entry = unsafe { ash::Entry::load() }
            .map_err(|e| Error::Gpu(format!("load Vulkan loader: {e}")))?;

        let app_name = c"sima";
        let app_info = vk::ApplicationInfo::default()
            .application_name(app_name)
            .application_version(vk::make_api_version(0, 1, 0, 0))
            .engine_name(app_name)
            .engine_version(vk::make_api_version(0, 1, 0, 0))
            .api_version(vk::API_VERSION_1_3);

        let validation_enabled = validation::resolve_validation_request(&entry);
        let layer_names = validation_layer_names();
        let debug_extensions = [debug_utils_extension_name()];
        let mut instance_info = vk::InstanceCreateInfo::default().application_info(&app_info);
        if validation_enabled {
            instance_info = instance_info
                .enabled_layer_names(&layer_names)
                .enabled_extension_names(&debug_extensions);
        }
        // SAFETY: `instance_info`, the `app_info` it references, and the optional
        // layer/extension arrays are stack-local through this call; `entry` stays
        // loaded above.
        let instance = unsafe { entry.create_instance(&instance_info, None) }
            .map_err(|e| Error::Gpu(format!("create Vulkan instance: {e}")))?;
        let instance = InstanceGuard::new(instance);

        let DeviceChoice {
            physical_device,
            queue_family_index,
        } = selection::select_physical_device(instance.get()?)?;
        // SAFETY: `physical_device` was returned by `select_physical_device`,
        // which enumerated it from `instance`; both are alive on this frame.
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
        .map_err(|e| Error::Gpu(format!("create logical device: {e}")))?;
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
            .map_err(|e| Error::Gpu(format!("create command pool: {e}")))?;
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
            queue_family_index,
            command_pool: command_pool.finish()?,
            memory_properties,
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

/// Rolls back a created instance until construction transfers it to `Context`.
struct InstanceGuard {
    instance: Option<ash::Instance>,
}

impl InstanceGuard {
    fn new(instance: ash::Instance) -> Self {
        Self {
            instance: Some(instance),
        }
    }

    fn get(&self) -> Result<&ash::Instance> {
        self.instance
            .as_ref()
            .ok_or_else(|| Error::Gpu("Vulkan instance guard used after finish".to_string()))
    }

    fn finish(mut self) -> Result<ash::Instance> {
        self.instance
            .take()
            .ok_or_else(|| Error::Gpu("Vulkan instance guard finished twice".to_string()))
    }
}

impl Drop for InstanceGuard {
    fn drop(&mut self) {
        // SAFETY: the guard owns the instance until `finish` transfers it; on a
        // rollback path no later owner exists to destroy it.
        unsafe {
            if let Some(instance) = self.instance.take() {
                instance.destroy_instance(None);
            }
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
            .ok_or_else(|| Error::Gpu("Vulkan device guard used after finish".to_string()))
    }

    fn finish(mut self) -> Result<ash::Device> {
        self.device
            .take()
            .ok_or_else(|| Error::Gpu("Vulkan device guard finished twice".to_string()))
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
            .ok_or_else(|| Error::Gpu("command pool guard finished twice".to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn context_reports_device_provenance() {
        let context = Context::new().expect("create compute context");
        assert!(!context.device_name().is_empty());
        assert!(!context.driver_version().is_empty());
    }
}
