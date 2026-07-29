//! What compute devices a program can run on.
//!
//! The domains layer is where the set of compiled-in execution backends is
//! known, so it is where "what devices can be used" is answered for the layers
//! above: they ask this crate rather than depending on a toolkit directly.
//!
//! The question is asked about a format id, never about a machine. A backend
//! reaches only the devices its own driver stack exposes, and the two stacks
//! disagree on real hosts: a rented instance whose Vulkan loader cannot
//! initialize the NVIDIA driver offers a WGSL program the CPU rasterizer alone
//! while CUDA opens the card there, and a laptop's Intel integrated GPU is a
//! Vulkan device that CUDA cannot open at all. A list of everything present
//! would therefore hand a program devices its backend faults on, so each
//! domain names its [`Backend`] and the enumeration follows the program.

use serde::{Deserialize, Serialize};
use sima_contracts::DeviceClass;
use sima_core::Result;
use sima_model::FormatId;

/// A compute-capable device as enumerated: what it is, what it is called, and
/// which one it is among the interchangeable members of its class.
///
/// The class is minted by the backend that enumerated the device, and `member`
/// is the position within it, ordered by that backend's enumeration order.
///
/// The serde form is the `sima-worker --enumerate` probe's wire shape: a
/// human-readable device list, never identity-bearing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub class: DeviceClass,
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

/// The execution backend a domain runs its work through, and so the one that
/// decides which of this machine's devices are available to it.
///
/// Every domain names one. It is what turns a format id into an enumeration,
/// and it is a property of the program rather than of the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Computes in the worker process itself, opening no device.
    Host,
    /// The WGSL toolkit, over any Vulkan driver present.
    Wgsl,
    /// The CUDA toolkit, over the NVIDIA driver.
    Cuda,
}

/// Every device the program bound to `format` can run on.
///
/// Resolving the format is what selects the backend: the registration table
/// pairs each program with the backend it executes through, so nothing above
/// this crate has to know which backends the build compiles in.
pub fn enumerate_devices(format: &FormatId) -> Result<Vec<DeviceInfo>> {
    devices_of(crate::domain_for(format)?.backend)
}

/// One backend's devices in this module's vocabulary.
fn devices_of(backend: Backend) -> Result<Vec<DeviceInfo>> {
    match backend {
        // A program that opens no device enumerates none, and the layers above
        // read that as a worker needing no device rather than as a bare host.
        Backend::Host => Ok(Vec::new()),
        Backend::Wgsl => sima_toolkit_wgsl::enumerate_devices()?
            .into_iter()
            .map(from_wgsl)
            .collect(),
        Backend::Cuda => sima_toolkit_cuda::enumerate_devices()?
            .into_iter()
            .map(from_cuda)
            .collect(),
    }
}

/// The WGSL toolkit's enumerated device in this module's vocabulary. One of the
/// two sites that know both, mirroring the mapping of a `DeviceBinding` onto a
/// toolkit's device triple.
fn from_wgsl(device: sima_toolkit_wgsl::DeviceInfo) -> Result<DeviceInfo> {
    Ok(DeviceInfo {
        class: DeviceClass::new(device.class)?,
        name: device.name,
        device_type: match device.device_type {
            sima_toolkit_wgsl::DeviceType::Discrete => DeviceType::Discrete,
            sima_toolkit_wgsl::DeviceType::Integrated => DeviceType::Integrated,
            sima_toolkit_wgsl::DeviceType::Virtual => DeviceType::Virtual,
            sima_toolkit_wgsl::DeviceType::Cpu => DeviceType::Cpu,
            sima_toolkit_wgsl::DeviceType::Other => DeviceType::Other,
        },
        member: device.member,
    })
}

/// The CUDA toolkit's enumerated device in this module's vocabulary. CUDA
/// discovers GPUs alone, so its two categories map straight across and the
/// remaining ones never arise from it.
fn from_cuda(device: sima_toolkit_cuda::DeviceInfo) -> Result<DeviceInfo> {
    Ok(DeviceInfo {
        class: DeviceClass::new(device.class)?,
        name: device.name,
        device_type: match device.device_type {
            sima_toolkit_cuda::DeviceType::Discrete => DeviceType::Discrete,
            sima_toolkit_cuda::DeviceType::Integrated => DeviceType::Integrated,
        },
        member: device.member,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A validated format id.
    fn format(name: &str) -> FormatId {
        FormatId::new(name).expect("format id")
    }

    /// The backend the program bound to `name` runs on.
    fn backend_of(name: &str) -> Backend {
        crate::domain_for(&format(name))
            .expect("a registered format")
            .backend
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
                class: sima_toolkit_wgsl::class_of(0x10de, 0x2d39),
                name: "NVIDIA RTX PRO 2000".to_string(),
                device_type: backend,
                member: 0,
            };
            assert_eq!(from_wgsl(device).expect("convert").device_type, neutral);
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
                class: sima_toolkit_cuda::class_of(0x10de, 0x2d39),
                name: "NVIDIA RTX PRO 2000".to_string(),
                device_type: backend,
                member: 0,
            };
            assert_eq!(from_cuda(device).expect("convert").device_type, neutral);
        }
    }

    #[test]
    fn a_wgsl_device_carries_through_the_conversion_verbatim() {
        let device = sima_toolkit_wgsl::DeviceInfo {
            class: sima_toolkit_wgsl::class_of(0x8086, 0x7d51),
            name: "Intel(R) Graphics (ARL)".to_string(),
            device_type: sima_toolkit_wgsl::DeviceType::Integrated,
            member: 1,
        };
        assert_eq!(
            from_wgsl(device).expect("convert"),
            DeviceInfo {
                class: DeviceClass::new("8086:7d51").expect("class id"),
                name: "Intel(R) Graphics (ARL)".to_string(),
                device_type: DeviceType::Integrated,
                member: 1,
            }
        );
    }

    #[test]
    fn a_cuda_device_carries_through_the_conversion_verbatim() {
        let device = sima_toolkit_cuda::DeviceInfo {
            class: sima_toolkit_cuda::class_of(0x10de, 0x2684),
            name: "NVIDIA GeForce RTX 4090".to_string(),
            device_type: sima_toolkit_cuda::DeviceType::Discrete,
            member: 1,
        };
        assert_eq!(
            from_cuda(device).expect("convert"),
            DeviceInfo {
                class: DeviceClass::new("10de:2684").expect("class id"),
                name: "NVIDIA GeForce RTX 4090".to_string(),
                device_type: DeviceType::Discrete,
                member: 1,
            }
        );
    }

    #[test]
    fn a_wgsl_program_enumerates_the_wgsl_backend() {
        // The rented-host case: Vulkan there reaches the CPU rasterizer alone
        // while CUDA opens the card. Enumerating anything CUDA-only for this
        // program would bind its worker to a device Vulkan cannot open.
        assert_eq!(backend_of("ca_evolution.gray_scott.v1"), Backend::Wgsl);
        assert_eq!(backend_of("ca_evolution.nca.v1"), Backend::Wgsl);
    }

    #[test]
    fn a_cuda_program_enumerates_the_cuda_backend() {
        // The mirror case: the laptop's Intel integrated GPU is a Vulkan device
        // CUDA cannot open at all. Enumerating anything Vulkan-only for this
        // program would bind its worker to a device the CUDA driver faults on.
        assert_eq!(backend_of("ca_evolution.gray_scott_cuda.v1"), Backend::Cuda);
    }

    #[test]
    fn a_program_that_opens_no_device_enumerates_none() {
        // The stub computes in the worker process, so it has no device to be
        // placed on and the layers above derive a deviceless worker.
        assert_eq!(backend_of("stub.v1"), Backend::Host);
        assert!(
            devices_of(Backend::Host)
                .expect("the host backend always answers")
                .is_empty()
        );
    }

    #[test]
    fn enumeration_answers_on_a_machine_with_no_device_at_all() {
        // No backend faults for want of a driver, so the probe the worker runs
        // answers rather than failing, whichever program it is asked about.
        for name in [
            "stub.v1",
            "ca_evolution.gray_scott.v1",
            "ca_evolution.gray_scott_cuda.v1",
        ] {
            enumerate_devices(&format(name)).expect("enumeration answers on any machine");
        }
    }

    #[test]
    fn an_unknown_format_has_no_devices_to_enumerate() {
        // The probe resolves the format before it touches a backend, so a
        // format this build does not know is a validation error rather than an
        // empty list that would read as a machine with no hardware.
        assert!(enumerate_devices(&format("no-such-domain.v1")).is_err());
    }
}
