//! Opt-in Vulkan validation-layer support.
//!
//! Setting `SIMA_VULKAN_VALIDATION=1` requests `VK_LAYER_KHRONOS_validation`
//! and a debug-utils messenger at instance creation, routing validation
//! warnings and errors to stderr. Off by default and zero cost when off:
//! without the environment switch no layer is requested and no messenger
//! exists. When the switch is set but the layer is not installed, instance
//! creation prints a marker to stderr and continues without validation.

use std::ffi::{CStr, c_char, c_void};

use ash::{ext, vk};

use sima_core::{Error, Result};

/// The Khronos validation layer name.
const VALIDATION_LAYER: &CStr = c"VK_LAYER_KHRONOS_validation";

/// Whether the environment opts into validation (`SIMA_VULKAN_VALIDATION` set
/// to anything but empty or `0`).
fn validation_requested() -> bool {
    std::env::var("SIMA_VULKAN_VALIDATION").is_ok_and(|value| !value.is_empty() && value != "0")
}

/// Whether the Khronos validation layer is installed on this system.
fn validation_layer_available(entry: &ash::Entry) -> bool {
    // SAFETY: `entry` is a loaded Vulkan entry; the call takes no handles.
    unsafe { entry.enumerate_instance_layer_properties() }
        .map(|layers| {
            layers
                .iter()
                .any(|layer| layer.layer_name_as_c_str() == Ok(VALIDATION_LAYER))
        })
        .unwrap_or(false)
}

/// Resolves the environment request against layer availability, printing a
/// stderr marker for each outcome. Returns whether validation should be
/// enabled for the instance being built.
pub(crate) fn resolve_validation_request(entry: &ash::Entry) -> bool {
    if !validation_requested() {
        return false;
    }
    if validation_layer_available(entry) {
        eprintln!("vulkan validation: enabled");
        true
    } else {
        eprintln!(
            "vulkan validation: layer unavailable \
             (SIMA_VULKAN_VALIDATION is set but VK_LAYER_KHRONOS_validation is not installed); \
             continuing without validation"
        );
        false
    }
}

/// The layer-name pointer array for instance creation when validation is on.
pub(crate) fn validation_layer_names() -> [*const c_char; 1] {
    [VALIDATION_LAYER.as_ptr()]
}

/// The debug-utils instance-extension name pointer.
pub(crate) fn debug_utils_extension_name() -> *const c_char {
    ext::debug_utils::NAME.as_ptr()
}

/// Live debug-utils messenger for one instance.
///
/// Owned by the [`Context`](crate::Context), which destroys it via
/// [`Self::destroy`] before the instance itself is destroyed.
pub(crate) struct ValidationMessenger {
    loader: ext::debug_utils::Instance,
    messenger: vk::DebugUtilsMessengerEXT,
}

impl ValidationMessenger {
    /// Creates the messenger on an instance built with the validation layer and
    /// the debug-utils extension enabled.
    pub(crate) fn create(entry: &ash::Entry, instance: &ash::Instance) -> Result<Self> {
        let loader = ext::debug_utils::Instance::new(entry, instance);
        let info = vk::DebugUtilsMessengerCreateInfoEXT::default()
            .message_severity(
                vk::DebugUtilsMessageSeverityFlagsEXT::WARNING
                    | vk::DebugUtilsMessageSeverityFlagsEXT::ERROR,
            )
            .message_type(
                vk::DebugUtilsMessageTypeFlagsEXT::VALIDATION
                    | vk::DebugUtilsMessageTypeFlagsEXT::PERFORMANCE,
            )
            .pfn_user_callback(Some(validation_callback));
        // SAFETY: `info` is stack-local through the call; the callback is a
        // plain function with no captured state.
        let messenger = unsafe { loader.create_debug_utils_messenger(&info, None) }
            .map_err(|e| Error::Gpu(format!("create validation messenger: {e}")))?;
        Ok(Self { loader, messenger })
    }

    /// Destroys the messenger. The caller must not have destroyed the instance
    /// the messenger was created on yet.
    pub(crate) fn destroy(&self) {
        // SAFETY: forwarded from the caller's ordering contract; the handle was
        // created by `create` and is destroyed exactly once (the Context drops
        // this value right before destroying the instance).
        unsafe {
            self.loader
                .destroy_debug_utils_messenger(self.messenger, None);
        }
    }
}

/// Debug-utils callback: routes every message to stderr.
unsafe extern "system" fn validation_callback(
    severity: vk::DebugUtilsMessageSeverityFlagsEXT,
    _types: vk::DebugUtilsMessageTypeFlagsEXT,
    data: *const vk::DebugUtilsMessengerCallbackDataEXT<'_>,
    _user_data: *mut c_void,
) -> vk::Bool32 {
    // SAFETY: the implementation passes a valid callback-data pointer whose
    // message is a NUL-terminated string for the duration of the call.
    let message = unsafe {
        data.as_ref()
            .and_then(|data| (!data.p_message.is_null()).then(|| CStr::from_ptr(data.p_message)))
            .map(|message| message.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    eprintln!("vulkan validation [{severity:?}]: {message}");
    // The spec requires FALSE: the triggering call proceeds.
    vk::FALSE
}
