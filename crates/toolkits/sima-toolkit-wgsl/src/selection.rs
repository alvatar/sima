//! Physical-device enumeration, memory-type queries, and provenance decoding.
//!
//! This module is the Vulkan half of device selection: it enumerates what the
//! loader exposes and translates each candidate into the vocabulary
//! [`sima_contracts`] selects over. The policy itself — class minting, member
//! numbering, the `SIMA_GPU_DEVICE` override, the default type ranking — is
//! shared with every other backend and lives there, so the two backends cannot
//! spell one card's class two ways or rank two devices differently.

use std::ffi::CStr;

use ash::vk;

use sima_contracts::{DeviceClass, DeviceInfo, DeviceType};
use sima_core::{Error, Result};

use crate::instance;

/// A compute-capable physical device and the queue family that carries compute.
pub(crate) struct DeviceChoice {
    pub physical_device: vk::PhysicalDevice,
    pub queue_family_index: u32,
}

/// Every compute-capable physical device, with member indices assigned per
/// class in enumeration order.
///
/// Creates and destroys an instance of its own, so callers learn what hardware
/// exists without holding a [`Context`](crate::Context). A machine without
/// Vulkan — no loader library, or a loader whose driver search comes up empty —
/// has no such device, so it enumerates as an empty list, never an error; the
/// worker's `--enumerate-devices` probe relies on this to answer "none" on a driverless
/// host instead of failing the probe.
pub fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
    let devices = instance::with_query_instance_or_none(|instance| {
        let candidates = compute_capable_devices(instance)?;
        let classes: Vec<DeviceClass> = candidates
            .iter()
            .map(|candidate| candidate.class())
            .collect();
        Ok(candidates
            .iter()
            .zip(sima_contracts::number_members(&classes))
            .zip(classes.iter())
            .map(|((candidate, member), class)| DeviceInfo {
                class: class.clone(),
                name: device_name(&candidate.properties),
                device_type: device_type_of(candidate.properties.device_type),
                member,
            })
            .collect())
    })?;
    Ok(devices.unwrap_or_default())
}

/// The name and driver version of the device that `device` — or, for `None`,
/// the default selection policy — would open, in that order.
///
/// Resolved over an instance of its own, so a caller can report the device it is
/// bound to before any GPU engine is initialized. Name and driver resolve
/// together over the one short-lived instance the query opens.
pub fn selected_device_desc(device: Option<(&DeviceClass, u32)>) -> Result<(String, String)> {
    instance::with_query_instance(|instance| {
        let choice = match device {
            Some((class, member)) => select_class_member(instance, class, member)?,
            None => select_physical_device(instance)?,
        };
        // SAFETY: `choice.physical_device` was enumerated from `instance` inside
        // this closure; both are alive here.
        let properties = unsafe { instance.get_physical_device_properties(choice.physical_device) };
        Ok((
            device_name(&properties),
            driver_version(properties.driver_version),
        ))
    })
}

/// A compute-capable device with everything the selection policies read.
struct Candidate {
    index: usize,
    choice: DeviceChoice,
    properties: vk::PhysicalDeviceProperties,
}

impl Candidate {
    /// The class this device belongs to, minted from its configuration space.
    fn class(&self) -> DeviceClass {
        sima_contracts::class_of(self.properties.vendor_id, self.properties.device_id)
    }
}

/// Every physical device exposing a compute queue family, in enumeration order.
fn compute_capable_devices(instance: &ash::Instance) -> Result<Vec<Candidate>> {
    // SAFETY: `instance` is borrowed and alive; the returned handles are owned
    // by it and used only while the caller holds it.
    let devices = unsafe { instance.enumerate_physical_devices() }
        .map_err(|e| Error::Backend(format!("enumerate physical devices: {e}")))?;

    let mut candidates = Vec::new();
    for (index, &physical_device) in devices.iter().enumerate() {
        let Some(queue_family_index) = compute_queue_family(instance, physical_device) else {
            continue;
        };
        // SAFETY: `physical_device` was enumerated from `instance` above; both
        // are alive on this stack frame.
        let properties = unsafe { instance.get_physical_device_properties(physical_device) };
        candidates.push(Candidate {
            index,
            choice: DeviceChoice {
                physical_device,
                queue_family_index,
            },
            properties,
        });
    }
    Ok(candidates)
}

/// Takes the candidate at enumeration index `winner`.
fn take_candidate(candidates: Vec<Candidate>, winner: usize) -> Result<DeviceChoice> {
    candidates
        .into_iter()
        .find(|candidate| candidate.index == winner)
        .map(|candidate| candidate.choice)
        .ok_or_else(|| Error::Backend("selected device index has no candidate".to_string()))
}

/// Selects the physical device to run on.
///
/// Keeps only devices exposing a compute queue family, then hands the shared
/// policy the `(index, type)` ranking it decides over.
pub(crate) fn select_physical_device(instance: &ash::Instance) -> Result<DeviceChoice> {
    let candidates = compute_capable_devices(instance)?;
    let ranking: Vec<(usize, DeviceType)> = candidates
        .iter()
        .map(|candidate| {
            (
                candidate.index,
                device_type_of(candidate.properties.device_type),
            )
        })
        .collect();
    let winner =
        sima_contracts::choose_device(&ranking, sima_contracts::requested_device_index()?)?;
    take_candidate(candidates, winner)
}

/// Selects the given member of the given device class.
pub(crate) fn select_class_member(
    instance: &ash::Instance,
    class: &DeviceClass,
    member: u32,
) -> Result<DeviceChoice> {
    let candidates = compute_capable_devices(instance)?;
    let classes: Vec<(DeviceClass, usize)> = candidates
        .iter()
        .map(|candidate| (candidate.class(), candidate.index))
        .collect();
    let winner = sima_contracts::resolve_member(&classes, class, member)?;
    take_candidate(candidates, winner)
}

/// The category Vulkan reports for a device, in the shared vocabulary.
fn device_type_of(device_type: vk::PhysicalDeviceType) -> DeviceType {
    match device_type {
        vk::PhysicalDeviceType::DISCRETE_GPU => DeviceType::Discrete,
        vk::PhysicalDeviceType::INTEGRATED_GPU => DeviceType::Integrated,
        vk::PhysicalDeviceType::VIRTUAL_GPU => DeviceType::Virtual,
        vk::PhysicalDeviceType::CPU => DeviceType::Cpu,
        _ => DeviceType::Other,
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
    Err(Error::Backend(format!(
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
    fn every_vulkan_device_category_maps_to_the_shared_vocabulary() {
        // The shared ranking decides over these, so a category translated to
        // the wrong one would silently change which device a machine picks.
        for (reported, shared) in [
            (vk::PhysicalDeviceType::DISCRETE_GPU, DeviceType::Discrete),
            (
                vk::PhysicalDeviceType::INTEGRATED_GPU,
                DeviceType::Integrated,
            ),
            (vk::PhysicalDeviceType::VIRTUAL_GPU, DeviceType::Virtual),
            (vk::PhysicalDeviceType::CPU, DeviceType::Cpu),
            (vk::PhysicalDeviceType::OTHER, DeviceType::Other),
        ] {
            assert_eq!(device_type_of(reported), shared);
        }
    }

    #[test]
    fn driver_version_decodes_standard_layout() {
        let packed = vk::make_api_version(0, 1, 2, 3);
        assert_eq!(driver_version(packed), "1.2.3");
    }

    #[test]
    fn naming_an_absent_device_fails_to_resolve() {
        // A worker answers `Ready` with this name, so this is where a binding
        // onto hardware the machine does not have is caught: executor
        // construction is lazy and would not notice until the first task.
        let absent = DeviceClass::new("dead:beef").expect("class id");
        assert!(matches!(
            selected_device_desc(Some((&absent, 0))),
            Err(Error::Backend(_))
        ));
    }

    #[test]
    fn enumeration_never_fails_for_want_of_a_driver() {
        // The probe answers "none" on a machine with no Vulkan rather than
        // failing, which is what the worker's enumeration relies on.
        enumerate_devices().expect("enumeration answers on any machine");
    }

    /// Enumeration answers from the Vulkan loader, so this one needs a device.
    mod on_device {
        use super::*;

        #[test]
        fn enumeration_reports_compute_capable_devices() {
            let devices = enumerate_devices().expect("enumerate devices");
            assert!(!devices.is_empty(), "at least one compute-capable device");
            for device in &devices {
                assert!(!device.name.is_empty());
                let (name, driver) = selected_device_desc(Some((&device.class, device.member)))
                    .expect("resolve the device description");
                assert_eq!(name, device.name);
                // The driver version is operational provenance; it decodes to the
                // standard three-part layout and is never empty for a real device.
                assert!(!driver.is_empty(), "driver version reported");
            }
        }
    }
}
