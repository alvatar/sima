//! Physical-device enumeration and selection, memory-type queries, and
//! provenance decoding.
//!
//! Two selection policies share one enumeration pass: the default picks a
//! device by type rank, while a caller naming a device class picks the class
//! member it asks for. Both policies are pure functions over the enumerated
//! candidates, so they are verifiable without a device.

use std::ffi::CStr;

use ash::vk;

use sima_core::{Error, Result};

use crate::instance;

/// A compute-capable physical device and the queue family that carries compute.
pub(crate) struct DeviceChoice {
    pub physical_device: vk::PhysicalDevice,
    pub queue_family_index: u32,
}

/// A compute-capable physical device as enumerated: what it is, what it is
/// called, and which card it is among identical ones.
///
/// A device class is the `(vendor_id, device_id)` pair — two identical cards
/// are one class with two members — and `member` is the position within the
/// class, ordered by Vulkan enumeration index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceInfo {
    pub vendor_id: u32,
    pub device_id: u32,
    pub name: String,
    pub device_type: DeviceType,
    pub member: u32,
}

/// The device categories Vulkan reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceType {
    Discrete,
    Integrated,
    Virtual,
    Cpu,
    Other,
}

impl DeviceType {
    fn from_vk(device_type: vk::PhysicalDeviceType) -> DeviceType {
        match device_type {
            vk::PhysicalDeviceType::DISCRETE_GPU => DeviceType::Discrete,
            vk::PhysicalDeviceType::INTEGRATED_GPU => DeviceType::Integrated,
            vk::PhysicalDeviceType::VIRTUAL_GPU => DeviceType::Virtual,
            vk::PhysicalDeviceType::CPU => DeviceType::Cpu,
            _ => DeviceType::Other,
        }
    }

    /// Preference order across device types; lower ranks are preferred.
    fn rank(self) -> u8 {
        match self {
            DeviceType::Discrete => 0,
            DeviceType::Integrated => 1,
            DeviceType::Virtual => 2,
            DeviceType::Cpu => 3,
            DeviceType::Other => 4,
        }
    }
}

/// Every compute-capable physical device, with member indices assigned per
/// class in enumeration order.
///
/// Creates and destroys an instance of its own, so callers learn what hardware
/// exists without holding a [`Context`](crate::Context).
pub fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
    instance::with_query_instance(|instance| {
        let candidates = compute_capable_devices(instance)?;
        // Members count within a class, so each class gets its own running
        // index as enumeration order is walked.
        let mut members_seen: Vec<((u32, u32), u32)> = Vec::new();
        let mut devices = Vec::with_capacity(candidates.len());
        for candidate in &candidates {
            let class = (
                candidate.properties.vendor_id,
                candidate.properties.device_id,
            );
            let member = match members_seen.iter_mut().find(|(seen, _)| *seen == class) {
                Some((_, next)) => {
                    *next += 1;
                    *next - 1
                }
                None => {
                    members_seen.push((class, 1));
                    0
                }
            };
            devices.push(DeviceInfo {
                vendor_id: class.0,
                device_id: class.1,
                name: device_name(&candidate.properties),
                device_type: DeviceType::from_vk(candidate.properties.device_type),
                member,
            });
        }
        Ok(devices)
    })
}

/// The name of the device that `device` — or, for `None`, the default selection
/// policy — would open.
///
/// Resolved over an instance of its own, so a caller can report the device it is
/// bound to before any GPU engine is initialized.
pub fn selected_device_name(device: Option<(u32, u32, u32)>) -> Result<String> {
    instance::with_query_instance(|instance| {
        let choice = match device {
            Some((vendor_id, device_id, member)) => {
                select_class_member(instance, vendor_id, device_id, member)?
            }
            None => select_physical_device(instance)?,
        };
        // SAFETY: `choice.physical_device` was enumerated from `instance` inside
        // this closure; both are alive here.
        let properties = unsafe { instance.get_physical_device_properties(choice.physical_device) };
        Ok(device_name(&properties))
    })
}

/// A compute-capable device with everything the selection policies read.
struct Candidate {
    index: usize,
    choice: DeviceChoice,
    properties: vk::PhysicalDeviceProperties,
}

/// Every physical device exposing a compute queue family, in enumeration order.
fn compute_capable_devices(instance: &ash::Instance) -> Result<Vec<Candidate>> {
    // SAFETY: `instance` is borrowed and alive; the returned handles are owned
    // by it and used only while the caller holds it.
    let devices = unsafe { instance.enumerate_physical_devices() }
        .map_err(|e| Error::Gpu(format!("enumerate physical devices: {e}")))?;

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
    if candidates.is_empty() {
        return Err(Error::Gpu(
            "no Vulkan device exposes a compute queue family".to_string(),
        ));
    }
    Ok(candidates)
}

/// Takes the candidate at enumeration index `winner`.
fn take_candidate(candidates: Vec<Candidate>, winner: usize) -> Result<DeviceChoice> {
    candidates
        .into_iter()
        .find(|candidate| candidate.index == winner)
        .map(|candidate| candidate.choice)
        .ok_or_else(|| Error::Gpu("selected device index has no candidate".to_string()))
}

/// Selects the physical device to run on.
///
/// Keeps only devices exposing a compute queue family, then applies the
/// `SIMA_GPU_DEVICE` enumeration-index override when set, or otherwise picks
/// deterministically: discrete before integrated before virtual before CPU
/// before other, with the lowest enumeration index breaking ties.
pub(crate) fn select_physical_device(instance: &ash::Instance) -> Result<DeviceChoice> {
    let candidates = compute_capable_devices(instance)?;
    let ranking: Vec<(usize, vk::PhysicalDeviceType)> = candidates
        .iter()
        .map(|candidate| (candidate.index, candidate.properties.device_type))
        .collect();
    let winner = choose_device(&ranking, requested_device_index()?)?;
    take_candidate(candidates, winner)
}

/// Selects the given member of the given device class.
pub(crate) fn select_class_member(
    instance: &ash::Instance,
    vendor_id: u32,
    device_id: u32,
    member: u32,
) -> Result<DeviceChoice> {
    let candidates = compute_capable_devices(instance)?;
    let classes: Vec<(u32, u32, usize)> = candidates
        .iter()
        .map(|candidate| {
            (
                candidate.properties.vendor_id,
                candidate.properties.device_id,
                candidate.index,
            )
        })
        .collect();
    let winner = resolve_member(&classes, vendor_id, device_id, member)?;
    take_candidate(candidates, winner)
}

/// Picks the enumeration index of one member of a device class from
/// `(vendor id, device id, enumeration index)` triples.
///
/// Members of a class are ordered by enumeration index, so `member` counts
/// within the class alone. Pure over the candidate list so the mapping is
/// verifiable without a device.
fn resolve_member(
    candidates: &[(u32, u32, usize)],
    vendor_id: u32,
    device_id: u32,
    member: u32,
) -> Result<usize> {
    let mut members: Vec<usize> = candidates
        .iter()
        .filter(|(vendor, device, _)| *vendor == vendor_id && *device == device_id)
        .map(|(_, _, index)| *index)
        .collect();
    members.sort_unstable();
    if members.is_empty() {
        return Err(Error::Gpu(format!(
            "no compute-capable device {vendor_id:04x}:{device_id:04x} exists; present: {}",
            render_classes(candidates)
        )));
    }
    members.get(member as usize).copied().ok_or_else(|| {
        Error::Gpu(format!(
            "device {vendor_id:04x}:{device_id:04x} has {} member(s); member {member} requested",
            members.len()
        ))
    })
}

/// The candidates' classes as `vendor:device` hex, each listed once.
fn render_classes(candidates: &[(u32, u32, usize)]) -> String {
    let mut classes: Vec<String> = Vec::new();
    for (vendor_id, device_id, _) in candidates {
        let class = format!("{vendor_id:04x}:{device_id:04x}");
        if !classes.contains(&class) {
            classes.push(class);
        }
    }
    classes.join(", ")
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
    DeviceType::from_vk(device_type).rank()
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

    /// Two identical cards and one of another class, in enumeration order.
    const CANDIDATES: [(u32, u32, usize); 3] = [
        (0x8086, 0x7d51, 0),
        (0x10de, 0x2d39, 1),
        (0x10de, 0x2d39, 2),
    ];

    #[test]
    fn member_zero_selects_the_first_card_of_its_class() {
        assert_eq!(
            resolve_member(&CANDIDATES, 0x10de, 0x2d39, 0).expect("first member"),
            1
        );
    }

    #[test]
    fn members_are_ordered_by_enumeration_index() {
        assert_eq!(
            resolve_member(&CANDIDATES, 0x10de, 0x2d39, 1).expect("second member"),
            2
        );
        // The class's own members are consecutive here, but its position among
        // all candidates is not: member indices count within the class only.
        assert_eq!(
            resolve_member(&CANDIDATES, 0x8086, 0x7d51, 0).expect("sole member"),
            0
        );
    }

    #[test]
    fn member_order_follows_enumeration_even_when_candidates_are_unsorted() {
        let unsorted = [(0x10de, 0x2d39, 2), (0x10de, 0x2d39, 1)];
        assert_eq!(
            resolve_member(&unsorted, 0x10de, 0x2d39, 0).expect("lowest index first"),
            1
        );
    }

    #[test]
    fn unknown_class_error_names_the_request_and_what_exists() {
        let error = resolve_member(&CANDIDATES, 0x1002, 0x1234, 0).expect_err("absent class");
        let Error::Gpu(message) = error else {
            panic!("expected a Gpu error");
        };
        assert!(
            message.contains("1002:1234"),
            "names the request: {message}"
        );
        assert!(
            message.contains("10de:2d39"),
            "names what exists: {message}"
        );
        assert!(
            message.contains("8086:7d51"),
            "names what exists: {message}"
        );
    }

    #[test]
    fn member_out_of_range_error_names_the_member_count() {
        let error = resolve_member(&CANDIDATES, 0x8086, 0x7d51, 1).expect_err("one member only");
        let Error::Gpu(message) = error else {
            panic!("expected a Gpu error");
        };
        assert!(message.contains("8086:7d51"), "names the class: {message}");
        assert!(message.contains('1'), "names the member count: {message}");
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn naming_an_absent_device_fails_to_resolve() {
        // A worker answers `Ready` with this name, so this is where a binding
        // onto hardware the machine does not have is caught: executor
        // construction is lazy and would not notice until the first task.
        assert!(matches!(
            selected_device_name(Some((0xdead, 0xbeef, 0))),
            Err(Error::Gpu(_))
        ));
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn enumeration_reports_compute_capable_devices() {
        let devices = enumerate_devices().expect("enumerate devices");
        assert!(!devices.is_empty(), "at least one compute-capable device");
        for device in &devices {
            assert!(!device.name.is_empty());
            let name =
                selected_device_name(Some((device.vendor_id, device.device_id, device.member)))
                    .expect("resolve the device name");
            assert_eq!(name, device.name);
        }
    }
}
