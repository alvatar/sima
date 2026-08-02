//! Device enumeration and the mapping from a PCI device class onto a CUDA
//! ordinal.
//!
//! This module is the CUDA half of device selection: it enumerates what the
//! driver exposes and translates each candidate into the vocabulary
//! [`sima_contracts`] selects over. The policy itself — class minting, member
//! numbering, the `SIMA_GPU_DEVICE` override, the default type ranking — is
//! shared with every other backend and lives there, so the two backends cannot
//! spell one card's class two ways or rank two devices differently.
//!
//! CUDA indexes its devices by ordinal while the layers above name a card by
//! its PCI class and the member index within that class. This module is where
//! the two meet: the ordinal's PCI bus identifier resolves to the class through
//! the kernel's PCI configuration, and members are numbered per class in
//! ordinal order.

use std::ffi::c_char;
use std::path::PathBuf;

use cudarc::driver::result;
use cudarc::driver::sys;

use sima_contracts::{DeviceClass, DeviceInfo, DeviceType};
use sima_core::{Error, Result};

use crate::driver;

/// Every CUDA device, with member indices assigned per class in ordinal order.
///
/// A machine without CUDA — no driver library, or a driver that refuses to
/// initialize — has no such device, so it enumerates as an empty list, never an
/// error; the worker's `--enumerate-devices` probe relies on this to answer "none" on a
/// driverless host instead of failing the probe.
pub fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
    let devices = driver::with_driver_or_none(|| {
        let candidates = cuda_devices()?;
        let classes: Vec<DeviceClass> = candidates
            .iter()
            .map(|candidate| candidate.class.clone())
            .collect();
        Ok(candidates
            .into_iter()
            .zip(sima_contracts::number_members(&classes))
            .map(|(candidate, member)| DeviceInfo {
                class: candidate.class,
                name: candidate.name,
                device_type: candidate.device_type,
                member,
            })
            .collect())
    })?;
    Ok(devices.unwrap_or_default())
}

/// The name and driver version of the device that `device` — or, for `None`,
/// the default selection policy — would open, in that order.
///
/// Resolved without creating a context, so a caller can report the device it is
/// bound to before any GPU engine is initialized.
pub fn selected_device_desc(device: Option<(&DeviceClass, u32)>) -> Result<(String, String)> {
    let ordinal = resolve_ordinal(device)?;
    let cu_device = result::device::get(ordinal as i32)
        .map_err(|e| driver::backend_error("open the CUDA device", e))?;
    let name = result::device::get_name(cu_device)
        .map_err(|e| driver::backend_error("read the CUDA device name", e))?;
    Ok((name, driver_version()?))
}

/// The ordinal the binding resolves to, or the one the default policy picks.
/// Initializes the driver, so a machine without one fails here naming that.
pub(crate) fn resolve_ordinal(device: Option<(&DeviceClass, u32)>) -> Result<usize> {
    driver::initialize()?;
    let candidates = cuda_devices()?;
    match device {
        Some((class, member)) => {
            let classes: Vec<(DeviceClass, usize)> = candidates
                .iter()
                .map(|candidate| (candidate.class.clone(), candidate.ordinal))
                .collect();
            sima_contracts::resolve_member(&classes, class, member)
        }
        None => {
            let ranking: Vec<(usize, DeviceType)> = candidates
                .iter()
                .map(|candidate| (candidate.ordinal, candidate.device_type))
                .collect();
            sima_contracts::choose_device(&ranking, sima_contracts::requested_device_index()?)
        }
    }
}

/// The CUDA driver's version, as `major.minor`. Operational provenance the
/// journal records, never identity.
pub(crate) fn driver_version() -> Result<String> {
    let mut version: std::ffi::c_int = 0;
    // SAFETY: the driver is initialized by the caller and `version` is a live
    // stack slot the call writes exactly one int into.
    unsafe { sys::cuDriverGetVersion(&mut version) }
        .result()
        .map_err(|e| driver::backend_error("read the CUDA driver version", e))?;
    // The driver reports one integer, 1000 * major + 10 * minor.
    Ok(format!("{}.{}", version / 1000, (version % 1000) / 10))
}

/// A CUDA device with everything the selection policies read.
struct Candidate {
    ordinal: usize,
    class: DeviceClass,
    name: String,
    device_type: DeviceType,
}

/// Every CUDA device, in ordinal order. The driver must already be initialized.
fn cuda_devices() -> Result<Vec<Candidate>> {
    let count = result::device::get_count()
        .map_err(|e| driver::backend_error("count the CUDA devices", e))?;
    let mut candidates = Vec::with_capacity(count.max(0) as usize);
    for ordinal in 0..count {
        let cu_device = result::device::get(ordinal)
            .map_err(|e| driver::backend_error("open the CUDA device", e))?;
        let name = result::device::get_name(cu_device)
            .map_err(|e| driver::backend_error("read the CUDA device name", e))?;
        // SAFETY: `cu_device` was returned by `cuDeviceGet` above and the
        // attribute is a plain integer query taking no pointers of ours.
        let integrated = unsafe {
            result::device::get_attribute(
                cu_device,
                sys::CUdevice_attribute_enum::CU_DEVICE_ATTRIBUTE_INTEGRATED,
            )
        }
        .map_err(|e| driver::backend_error("read the CUDA device integration attribute", e))?;
        candidates.push(Candidate {
            ordinal: ordinal as usize,
            class: pci_class(cu_device)?,
            name,
            // A CUDA device is a GPU, either a card of its own or one sharing
            // the host's memory, so these two categories are the whole set the
            // shared vocabulary can see from here.
            device_type: if integrated != 0 {
                DeviceType::Integrated
            } else {
                DeviceType::Discrete
            },
        });
    }
    Ok(candidates)
}

/// The class of a CUDA device, read from its configuration space.
///
/// CUDA reports a device's PCI bus identifier but not the vendor and device
/// identifiers of its configuration space, which are what a class is minted
/// from. The bus identifier names the device's directory under
/// `/sys/bus/pci/devices`, where the kernel publishes both.
fn pci_class(cu_device: sys::CUdevice) -> Result<DeviceClass> {
    let bus_id = pci_bus_id(cu_device)?;
    let directory = PathBuf::from("/sys/bus/pci/devices").join(&bus_id);
    let vendor_id = pci_id(&directory.join("vendor"), &bus_id)?;
    let device_id = pci_id(&directory.join("device"), &bus_id)?;
    Ok(sima_contracts::class_of(vendor_id, device_id))
}

/// A device's PCI bus identifier as `domain:bus:device.function`, lowercased to
/// match the kernel's directory naming.
fn pci_bus_id(cu_device: sys::CUdevice) -> Result<String> {
    // The driver documents `domain:bus:device.function` with two-digit fields;
    // this is comfortably longer, and the driver NUL-terminates within it.
    let mut raw = [0 as c_char; 32];
    // SAFETY: `cu_device` came from `cuDeviceGet` and `raw` is a live stack
    // buffer of exactly the length passed alongside it.
    unsafe { sys::cuDeviceGetPCIBusId(raw.as_mut_ptr(), raw.len() as std::ffi::c_int, cu_device) }
        .result()
        .map_err(|e| driver::backend_error("read the CUDA device PCI bus identifier", e))?;
    // SAFETY: the call above wrote a NUL-terminated string into `raw`, which is
    // alive for this borrow.
    let bus_id = unsafe { std::ffi::CStr::from_ptr(raw.as_ptr()) };
    Ok(bus_id.to_string_lossy().to_lowercase())
}

/// One `0x`-prefixed hexadecimal identifier from a PCI device's sysfs entry.
fn pci_id(path: &std::path::Path, bus_id: &str) -> Result<u32> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        Error::Backend(format!(
            "read the PCI configuration of CUDA device {bus_id} at {}: {e}",
            path.display()
        ))
    })?;
    let digits = text.trim().trim_start_matches("0x");
    u32::from_str_radix(digits, 16).map_err(|e| {
        Error::Backend(format!(
            "parse the PCI configuration of CUDA device {bus_id} at {}: {text:?} is not a \
             hexadecimal identifier: {e}",
            path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enumeration_never_fails_for_want_of_a_driver() {
        // The probe answers "none" on a machine with no CUDA rather than
        // failing, which is what the worker's enumeration relies on.
        enumerate_devices().expect("enumeration answers on any machine");
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

    /// Enumeration answers from the driver, so this one needs an NVIDIA device.
    mod on_device {
        use super::*;

        #[test]
        fn enumeration_reports_every_cuda_device() {
            let devices = enumerate_devices().expect("enumerate devices");
            assert!(!devices.is_empty(), "at least one CUDA device");
            for device in &devices {
                assert!(!device.name.is_empty());
                // Every CUDA device is an NVIDIA card, so the class's vendor half
                // is fixed and the device half comes from its PCI configuration.
                assert!(
                    device.class.as_str().starts_with("10de:"),
                    "an NVIDIA vendor id: {}",
                    device.class
                );
                let (name, driver) = selected_device_desc(Some((&device.class, device.member)))
                    .expect("resolve the device description");
                assert_eq!(name, device.name);
                assert!(!driver.is_empty(), "driver version reported");
            }
        }
    }
}
