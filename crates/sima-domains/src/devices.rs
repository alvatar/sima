//! What compute devices this build can run on.
//!
//! The domains layer is where the set of compiled-in execution backends is
//! known, so it is where "what devices exist" is answered for the layers above:
//! they ask this crate rather than depending on a toolkit directly.
//!
//! The types here are the vocabulary those layers hold. They are the backends'
//! answers translated into one shape, so a card two backends both discover
//! appears once and the question stays "what hardware is here" rather than
//! "what can each backend reach". Which substrate a card is used through is the
//! program's declaration, made in the run config by choosing a format id.

use serde::{Deserialize, Serialize};
use sima_core::Result;

/// A compute-capable device as enumerated: what it is, what it is called, and
/// which card it is among identical ones.
///
/// A device class is the `(vendor_id, device_id)` pair — two identical cards
/// are one class with two members — and `member` is the position within the
/// class, ordered by the backend's enumeration order.
///
/// The serde form is the `sima-worker --enumerate` probe's wire shape: a
/// human-readable device list, never identity-bearing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub vendor_id: u32,
    pub device_id: u32,
    pub name: String,
    pub device_type: DeviceType,
    pub member: u32,
}

/// The categories a device falls into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    Discrete,
    Integrated,
    Virtual,
    Cpu,
    Other,
}

/// Every compute-capable device this build can run on: the union of what the
/// backends discover, each card listed once.
pub fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
    let wgsl: Vec<DeviceInfo> = sima_toolkit_wgsl::enumerate_devices()?
        .into_iter()
        .map(from_wgsl)
        .collect();
    let cuda: Vec<DeviceInfo> = sima_toolkit_cuda::enumerate_devices()?
        .into_iter()
        .map(from_cuda)
        .collect();
    Ok(merge(wgsl, cuda))
}

/// Concatenates two backends' enumerations, dropping a device a later one
/// repeats.
///
/// An NVIDIA card on a machine with both a Vulkan driver and a CUDA driver is
/// discovered twice, and it is one card: the layers above count devices to
/// place workers, so listing it twice would place two workers on one GPU.
/// Identity is the `(vendor_id, device_id, member)` triple, which is exactly
/// what a device binding names, and the first backend to report a card fixes
/// its reported name and type.
fn merge(first: Vec<DeviceInfo>, second: Vec<DeviceInfo>) -> Vec<DeviceInfo> {
    let mut devices = first;
    for device in second {
        let seen = devices.iter().any(|present| {
            (present.vendor_id, present.device_id, present.member)
                == (device.vendor_id, device.device_id, device.member)
        });
        if !seen {
            devices.push(device);
        }
    }
    devices
}

/// The WGSL toolkit's enumerated device in this module's vocabulary. One of the
/// two sites that know both, mirroring the mapping of a `DeviceBinding` onto a
/// toolkit's device triple.
fn from_wgsl(device: sima_toolkit_wgsl::DeviceInfo) -> DeviceInfo {
    DeviceInfo {
        vendor_id: device.vendor_id,
        device_id: device.device_id,
        name: device.name,
        device_type: match device.device_type {
            sima_toolkit_wgsl::DeviceType::Discrete => DeviceType::Discrete,
            sima_toolkit_wgsl::DeviceType::Integrated => DeviceType::Integrated,
            sima_toolkit_wgsl::DeviceType::Virtual => DeviceType::Virtual,
            sima_toolkit_wgsl::DeviceType::Cpu => DeviceType::Cpu,
            sima_toolkit_wgsl::DeviceType::Other => DeviceType::Other,
        },
        member: device.member,
    }
}

/// The CUDA toolkit's enumerated device in this module's vocabulary. CUDA
/// discovers GPUs alone, so its two categories map straight across and the
/// remaining ones never arise from it.
fn from_cuda(device: sima_toolkit_cuda::DeviceInfo) -> DeviceInfo {
    DeviceInfo {
        vendor_id: device.vendor_id,
        device_id: device.device_id,
        name: device.name,
        device_type: match device.device_type {
            sima_toolkit_cuda::DeviceType::Discrete => DeviceType::Discrete,
            sima_toolkit_cuda::DeviceType::Integrated => DeviceType::Integrated,
        },
        member: device.member,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One enumerated device in this module's vocabulary.
    fn device(vendor_id: u32, device_id: u32, name: &str, member: u32) -> DeviceInfo {
        DeviceInfo {
            vendor_id,
            device_id,
            name: name.to_string(),
            device_type: DeviceType::Discrete,
            member,
        }
    }

    #[test]
    fn every_wgsl_device_category_has_a_counterpart() {
        for (backend, neutral) in [
            (
                sima_toolkit_wgsl::DeviceType::Discrete,
                DeviceType::Discrete,
            ),
            (
                sima_toolkit_wgsl::DeviceType::Integrated,
                DeviceType::Integrated,
            ),
            (sima_toolkit_wgsl::DeviceType::Virtual, DeviceType::Virtual),
            (sima_toolkit_wgsl::DeviceType::Cpu, DeviceType::Cpu),
            (sima_toolkit_wgsl::DeviceType::Other, DeviceType::Other),
        ] {
            let device = sima_toolkit_wgsl::DeviceInfo {
                vendor_id: 0x10de,
                device_id: 0x2d39,
                name: "NVIDIA RTX PRO 2000".to_string(),
                device_type: backend,
                member: 0,
            };
            assert_eq!(from_wgsl(device).device_type, neutral);
        }
    }

    #[test]
    fn every_cuda_device_category_has_a_counterpart() {
        for (backend, neutral) in [
            (
                sima_toolkit_cuda::DeviceType::Discrete,
                DeviceType::Discrete,
            ),
            (
                sima_toolkit_cuda::DeviceType::Integrated,
                DeviceType::Integrated,
            ),
        ] {
            let device = sima_toolkit_cuda::DeviceInfo {
                vendor_id: 0x10de,
                device_id: 0x2d39,
                name: "NVIDIA RTX PRO 2000".to_string(),
                device_type: backend,
                member: 0,
            };
            assert_eq!(from_cuda(device).device_type, neutral);
        }
    }

    #[test]
    fn a_wgsl_device_carries_through_the_conversion_verbatim() {
        let device = sima_toolkit_wgsl::DeviceInfo {
            vendor_id: 0x8086,
            device_id: 0x7d51,
            name: "Intel(R) Graphics (ARL)".to_string(),
            device_type: sima_toolkit_wgsl::DeviceType::Integrated,
            member: 1,
        };
        assert_eq!(
            from_wgsl(device),
            DeviceInfo {
                vendor_id: 0x8086,
                device_id: 0x7d51,
                name: "Intel(R) Graphics (ARL)".to_string(),
                device_type: DeviceType::Integrated,
                member: 1,
            }
        );
    }

    #[test]
    fn a_cuda_device_carries_through_the_conversion_verbatim() {
        let device = sima_toolkit_cuda::DeviceInfo {
            vendor_id: 0x10de,
            device_id: 0x2684,
            name: "NVIDIA GeForce RTX 4090".to_string(),
            device_type: sima_toolkit_cuda::DeviceType::Discrete,
            member: 1,
        };
        assert_eq!(
            from_cuda(device),
            DeviceInfo {
                vendor_id: 0x10de,
                device_id: 0x2684,
                name: "NVIDIA GeForce RTX 4090".to_string(),
                device_type: DeviceType::Discrete,
                member: 1,
            }
        );
    }

    #[test]
    fn a_host_reached_by_one_backend_alone_enumerates_unchanged() {
        // The union is a concatenation, so a machine only one backend discovers
        // enumerates exactly the list that backend reports, in its order.
        let vulkan = vec![
            device(0x8086, 0x7d51, "Intel(R) Graphics (ARL)", 0),
            device(0x10de, 0x2d39, "NVIDIA RTX PRO 2000", 0),
        ];
        assert_eq!(merge(vulkan.clone(), Vec::new()), vulkan);
        assert_eq!(merge(Vec::new(), vulkan.clone()), vulkan);
    }

    #[test]
    fn a_card_both_backends_discover_is_listed_once() {
        // The dev machine: an Intel iGPU and an NVIDIA card that Vulkan and CUDA
        // both report. Three entries in, two out — one per physical card.
        let vulkan = vec![
            device(0x8086, 0x7d51, "Intel(R) Graphics (ARL)", 0),
            device(0x10de, 0x2d39, "NVIDIA RTX PRO 2000", 0),
        ];
        let cuda = vec![device(
            0x10de,
            0x2d39,
            "NVIDIA RTX PRO 2000 Blackwell Laptop GPU",
            0,
        )];
        let merged = merge(vulkan.clone(), cuda);
        assert_eq!(merged, vulkan, "the first report of a card is the one kept");
    }

    #[test]
    fn members_of_one_class_are_distinct_devices() {
        // Two identical cards are one class with two members, and the second
        // member is a card of its own — never folded into the first.
        let vulkan = vec![device(0x10de, 0x2684, "NVIDIA GeForce RTX 4090", 0)];
        let cuda = vec![
            device(0x10de, 0x2684, "NVIDIA GeForce RTX 4090", 0),
            device(0x10de, 0x2684, "NVIDIA GeForce RTX 4090", 1),
        ];
        let merged = merge(vulkan, cuda);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].member, 0);
        assert_eq!(merged[1].member, 1);
    }

    #[test]
    fn enumeration_answers_on_a_machine_with_no_device_at_all() {
        // Neither backend faults for want of a driver, so the probe the worker
        // runs answers rather than failing.
        enumerate_devices().expect("enumeration answers on any machine");
    }
}
