//! What compute devices this build can run on.
//!
//! The domains layer is where the set of compiled-in execution backends is
//! known, so it is where "what devices exist" is answered for the layers above:
//! they ask this crate rather than depending on a toolkit directly.
//!
//! The types here are the vocabulary those layers hold. They are the backends'
//! answers translated into one shape, so a second backend extends
//! [`enumerate_devices`]'s body — enumerate both, concatenate — while its
//! callers keep holding these types.

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

/// Every compute-capable device this build can run on.
pub fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
    Ok(sima_toolkit_wgsl::enumerate_devices()?
        .into_iter()
        .map(from_wgsl)
        .collect())
}

/// The WGSL toolkit's enumerated device in this module's vocabulary. The one
/// site that knows both, mirroring the mapping of a `DeviceBinding` onto the
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_backend_device_category_has_a_counterpart() {
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
    fn a_device_carries_through_the_conversion_verbatim() {
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
}
