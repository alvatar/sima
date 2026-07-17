//! Vulkan loader entry and instance lifetime.
//!
//! Every path that touches Vulkan starts with an instance: the [`Context`]
//! keeps one for the life of its device, while physical-device queries create a
//! short-lived one and destroy it before returning.
//!
//! [`Context`]: crate::Context

use ash::vk;

use sima_core::{Error, Result};

use crate::validation::{debug_utils_extension_name, validation_layer_names};

/// Loads the platform Vulkan loader.
///
/// The returned entry must outlive every instance created from it: it keeps the
/// loader library resident.
pub(crate) fn load_entry() -> Result<ash::Entry> {
    // SAFETY: loads the platform Vulkan loader; the caller keeps the returned
    // Entry alive for the whole lifetime of the instances derived from it.
    unsafe { ash::Entry::load() }.map_err(|e| Error::Gpu(format!("load Vulkan loader: {e}")))
}

/// Creates an instance on `entry`, enabling the validation layer and its
/// debug-utils extension when `validation_enabled`.
pub(crate) fn create(entry: &ash::Entry, validation_enabled: bool) -> Result<ash::Instance> {
    let app_name = c"sima";
    let app_info = vk::ApplicationInfo::default()
        .application_name(app_name)
        .application_version(vk::make_api_version(0, 1, 0, 0))
        .engine_name(app_name)
        .engine_version(vk::make_api_version(0, 1, 0, 0))
        .api_version(vk::API_VERSION_1_3);

    let layer_names = validation_layer_names();
    let debug_extensions = [debug_utils_extension_name()];
    let mut instance_info = vk::InstanceCreateInfo::default().application_info(&app_info);
    if validation_enabled {
        instance_info = instance_info
            .enabled_layer_names(&layer_names)
            .enabled_extension_names(&debug_extensions);
    }
    // SAFETY: `instance_info`, the `app_info` it references, and the optional
    // layer/extension arrays are stack-local through this call; `entry` is
    // borrowed and keeps the loader resident.
    unsafe { entry.create_instance(&instance_info, None) }
        .map_err(|e| Error::Gpu(format!("create Vulkan instance: {e}")))
}

/// Runs `query` against an instance created solely to inspect physical devices,
/// destroying it before returning.
///
/// Validation stays off: the query creates no device and submits no work, so the
/// layer would only cost startup time.
pub(crate) fn with_query_instance<T>(query: impl FnOnce(&ash::Instance) -> Result<T>) -> Result<T> {
    let entry = load_entry()?;
    let instance = InstanceGuard::new(create(&entry, false)?);
    // The guard destroys the instance as this scope ends, on both paths.
    query(instance.get()?)
}

/// Rolls back a created instance until its owner takes it, if one ever does.
pub(crate) struct InstanceGuard {
    instance: Option<ash::Instance>,
}

impl InstanceGuard {
    pub(crate) fn new(instance: ash::Instance) -> Self {
        Self {
            instance: Some(instance),
        }
    }

    pub(crate) fn get(&self) -> Result<&ash::Instance> {
        self.instance
            .as_ref()
            .ok_or_else(|| Error::Gpu("Vulkan instance guard used after finish".to_string()))
    }

    pub(crate) fn finish(mut self) -> Result<ash::Instance> {
        self.instance
            .take()
            .ok_or_else(|| Error::Gpu("Vulkan instance guard finished twice".to_string()))
    }
}

impl Drop for InstanceGuard {
    fn drop(&mut self) {
        // SAFETY: the guard owns the instance until `finish` transfers it; when
        // it still holds one, no later owner exists to destroy it.
        unsafe {
            if let Some(instance) = self.instance.take() {
                instance.destroy_instance(None);
            }
        }
    }
}
