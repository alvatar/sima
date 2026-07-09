//! Physical-device selection, memory-type queries, and provenance decoding.

use std::ffi::CStr;

use ash::vk;

use sima_core::{Error, Result};

/// A compute-capable physical device and the queue family that carries compute.
pub(crate) struct DeviceChoice {
    pub physical_device: vk::PhysicalDevice,
    pub queue_family_index: u32,
}

/// Selects the physical device to run on.
///
/// Keeps only devices exposing a compute queue family, then applies the
/// `SIMA_GPU_DEVICE` enumeration-index override when set, or otherwise picks
/// deterministically: discrete before integrated before virtual before CPU
/// before other, with the lowest enumeration index breaking ties.
pub(crate) fn select_physical_device(instance: &ash::Instance) -> Result<DeviceChoice> {
    // SAFETY: `instance` is borrowed and alive; the returned handles are owned
    // by it and used only while this function runs.
    let devices = unsafe { instance.enumerate_physical_devices() }
        .map_err(|e| Error::Gpu(format!("enumerate physical devices: {e}")))?;

    // Compute-capable devices, each keyed by its enumeration index.
    let mut candidates: Vec<(usize, DeviceChoice, vk::PhysicalDeviceType)> = Vec::new();
    for (index, &physical_device) in devices.iter().enumerate() {
        let Some(queue_family_index) = compute_queue_family(instance, physical_device) else {
            continue;
        };
        // SAFETY: `physical_device` was enumerated from `instance` above; both
        // are alive on this stack frame.
        let properties = unsafe { instance.get_physical_device_properties(physical_device) };
        candidates.push((
            index,
            DeviceChoice {
                physical_device,
                queue_family_index,
            },
            properties.device_type,
        ));
    }
    if candidates.is_empty() {
        return Err(Error::Gpu(
            "no Vulkan device exposes a compute queue family".to_string(),
        ));
    }

    let ranking: Vec<(usize, vk::PhysicalDeviceType)> =
        candidates.iter().map(|(i, _, t)| (*i, *t)).collect();
    let winner = choose_device(&ranking, requested_device_index()?)?;
    let choice = candidates
        .into_iter()
        .find(|(i, _, _)| *i == winner)
        .map(|(_, choice, _)| choice)
        .ok_or_else(|| Error::Gpu("selected device index has no candidate".to_string()))?;
    Ok(choice)
}

/// Picks the winning enumeration index from `(index, type)` pairs.
///
/// With `requested` set, the named index wins when it is compute-capable and
/// fails otherwise; without it, the lowest `(type rank, index)` pair wins.
/// Pure over the ranking inputs so the policy is verifiable without a device.
fn choose_device(
    candidates: &[(usize, vk::PhysicalDeviceType)],
    requested: Option<usize>,
) -> Result<usize> {
    if let Some(requested) = requested {
        return candidates
            .iter()
            .find(|(index, _)| *index == requested)
            .map(|(index, _)| *index)
            .ok_or_else(|| {
                Error::Gpu(format!(
                    "SIMA_GPU_DEVICE={requested} does not name a compute-capable device"
                ))
            });
    }
    candidates
        .iter()
        .min_by_key(|(index, device_type)| (type_rank(*device_type), *index))
        .map(|(index, _)| *index)
        .ok_or_else(|| Error::Gpu("no compute-capable device to select".to_string()))
}

/// Preference order across device types; lower ranks are preferred.
fn type_rank(device_type: vk::PhysicalDeviceType) -> u8 {
    match device_type {
        vk::PhysicalDeviceType::DISCRETE_GPU => 0,
        vk::PhysicalDeviceType::INTEGRATED_GPU => 1,
        vk::PhysicalDeviceType::VIRTUAL_GPU => 2,
        vk::PhysicalDeviceType::CPU => 3,
        _ => 4,
    }
}

/// The `SIMA_GPU_DEVICE` override as an enumeration index, if set and valid.
fn requested_device_index() -> Result<Option<usize>> {
    match std::env::var("SIMA_GPU_DEVICE") {
        Ok(value) => value.trim().parse::<usize>().map(Some).map_err(|_| {
            Error::Gpu(format!(
                "SIMA_GPU_DEVICE must be a device index, got {value:?}"
            ))
        }),
        Err(_) => Ok(None),
    }
}

/// The first queue family index supporting compute on this device, if any.
fn compute_queue_family(
    instance: &ash::Instance,
    physical_device: vk::PhysicalDevice,
) -> Option<u32> {
    // SAFETY: `physical_device` came from this `instance`; the returned Vec is
    // owned and outlives the query.
    let families = unsafe { instance.get_physical_device_queue_family_properties(physical_device) };
    families
        .iter()
        .position(|family| family.queue_flags.contains(vk::QueueFlags::COMPUTE))
        .map(|index| index as u32)
}

/// Finds a memory type satisfying both the requirement bitmask and the
/// required property flags.
pub(crate) fn find_memory_type(
    memory_properties: &vk::PhysicalDeviceMemoryProperties,
    memory_type_bits: u32,
    required: vk::MemoryPropertyFlags,
) -> Result<u32> {
    for index in 0..memory_properties.memory_type_count as usize {
        let supported = (memory_type_bits & (1_u32 << index)) != 0;
        if supported
            && memory_properties.memory_types[index]
                .property_flags
                .contains(required)
        {
            return Ok(index as u32);
        }
    }
    Err(Error::Gpu(format!(
        "no Vulkan memory type satisfies {required:?}"
    )))
}

/// The device's reported name, for provenance.
pub(crate) fn device_name(properties: &vk::PhysicalDeviceProperties) -> String {
    // SAFETY: `device_name` is a VK_MAX_PHYSICAL_DEVICE_NAME_SIZE fixed array
    // that Vulkan guarantees NUL-terminated; the CStr borrows it for this call.
    let name = unsafe { CStr::from_ptr(properties.device_name.as_ptr()) };
    name.to_string_lossy().into_owned()
}

/// The device's reported driver version, decoded with the standard Vulkan
/// version layout. Vendor drivers may pack this field differently; the value
/// is operational provenance, not identity.
pub(crate) fn driver_version(raw: u32) -> String {
    format!(
        "{}.{}.{}",
        vk::api_version_major(raw),
        vk::api_version_minor(raw),
        vk::api_version_patch(raw)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_rank_orders_discrete_first() {
        assert!(
            type_rank(vk::PhysicalDeviceType::DISCRETE_GPU)
                < type_rank(vk::PhysicalDeviceType::INTEGRATED_GPU)
        );
        assert!(
            type_rank(vk::PhysicalDeviceType::INTEGRATED_GPU)
                < type_rank(vk::PhysicalDeviceType::VIRTUAL_GPU)
        );
        assert!(
            type_rank(vk::PhysicalDeviceType::VIRTUAL_GPU) < type_rank(vk::PhysicalDeviceType::CPU)
        );
        assert!(type_rank(vk::PhysicalDeviceType::CPU) < type_rank(vk::PhysicalDeviceType::OTHER));
    }

    #[test]
    fn deterministic_pick_prefers_discrete_over_lower_index() {
        let candidates = [
            (0, vk::PhysicalDeviceType::INTEGRATED_GPU),
            (1, vk::PhysicalDeviceType::DISCRETE_GPU),
        ];
        assert_eq!(choose_device(&candidates, None).expect("pick a device"), 1);
    }

    #[test]
    fn deterministic_pick_breaks_ties_by_lowest_index() {
        let candidates = [
            (2, vk::PhysicalDeviceType::DISCRETE_GPU),
            (0, vk::PhysicalDeviceType::DISCRETE_GPU),
            (1, vk::PhysicalDeviceType::DISCRETE_GPU),
        ];
        assert_eq!(choose_device(&candidates, None).expect("pick a device"), 0);
    }

    #[test]
    fn override_selects_named_index() {
        let candidates = [
            (0, vk::PhysicalDeviceType::DISCRETE_GPU),
            (1, vk::PhysicalDeviceType::CPU),
        ];
        assert_eq!(
            choose_device(&candidates, Some(1)).expect("named index wins"),
            1
        );
    }

    #[test]
    fn override_out_of_range_is_rejected() {
        let candidates = [(0, vk::PhysicalDeviceType::DISCRETE_GPU)];
        assert!(matches!(
            choose_device(&candidates, Some(7)),
            Err(Error::Gpu(_))
        ));
    }

    #[test]
    fn driver_version_decodes_standard_layout() {
        let packed = vk::make_api_version(0, 1, 2, 3);
        assert_eq!(driver_version(packed), "1.2.3");
    }
}
