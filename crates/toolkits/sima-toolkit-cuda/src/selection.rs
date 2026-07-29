//! Device enumeration and selection, and the mapping from a PCI device class
//! onto a CUDA ordinal.
//!
//! Two selection policies share one enumeration pass: the default picks a
//! device by type rank, while a caller naming a device class picks the class
//! member it asks for. Both policies are pure functions over the enumerated
//! candidates, so they are verifiable without a device.
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

use sima_core::{Error, Result};

use crate::driver;

/// A compute-capable device as enumerated: what it is, what it is called, and
/// which card it is among identical ones.
///
/// The class is minted here from the device's configuration-space identifiers,
/// and `member` is the position within the class, ordered by CUDA ordinal. Two
/// identical cards are one class with two members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceInfo {
    pub class: String,
    pub name: String,
    pub device_type: DeviceType,
    pub member: u32,
}

/// The class of a device with configuration-space identifiers.
///
/// This backend defines its own class names, so it is the one that reads them
/// back: every comparison against a caller's class goes through this spelling.
pub fn class_of(vendor_id: u32, device_id: u32) -> String {
    format!("{vendor_id:04x}:{device_id:04x}")
}

/// The device categories CUDA reports. A CUDA device is a GPU, either a card of
/// its own or one sharing the host's memory, so the two categories are the
/// whole set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceType {
    Discrete,
    Integrated,
}

impl DeviceType {
    /// Preference order across device types; lower ranks are preferred.
    fn rank(self) -> u8 {
        match self {
            DeviceType::Discrete => 0,
            DeviceType::Integrated => 1,
        }
    }
}

/// Every CUDA device, with member indices assigned per class in ordinal order.
///
/// A machine without CUDA — no driver library, or a driver that refuses to
/// initialize — has no such device, so it enumerates as an empty list, never an
/// error; the worker's `--enumerate` probe relies on this to answer "none" on a
/// driverless host instead of failing the probe.
pub fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
    let devices = driver::with_driver_or_none(|| {
        let candidates = cuda_devices()?;
        // Members count within a class, so each class gets its own running
        // index as ordinal order is walked.
        let mut members_seen: Vec<(String, u32)> = Vec::new();
        let mut devices = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let class = candidate.class;
            let member = match members_seen.iter_mut().find(|(seen, _)| *seen == class) {
                Some((_, next)) => {
                    *next += 1;
                    *next - 1
                }
                None => {
                    members_seen.push((class.clone(), 1));
                    0
                }
            };
            devices.push(DeviceInfo {
                class,
                name: candidate.name,
                device_type: candidate.device_type,
                member,
            });
        }
        Ok(devices)
    })?;
    Ok(devices.unwrap_or_default())
}

/// The name and driver version of the device that `device` — or, for `None`,
/// the default selection policy — would open, in that order.
///
/// Resolved without creating a context, so a caller can report the device it is
/// bound to before any GPU engine is initialized.
pub fn selected_device_desc(device: Option<(&str, u32)>) -> Result<(String, String)> {
    let ordinal = resolve_ordinal(device)?;
    let cu_device = result::device::get(ordinal as i32)
        .map_err(|e| driver::backend_error("open the CUDA device", e))?;
    let name = result::device::get_name(cu_device)
        .map_err(|e| driver::backend_error("read the CUDA device name", e))?;
    Ok((name, driver_version()?))
}

/// The ordinal the binding resolves to, or the one the default policy picks.
/// Initializes the driver, so a machine without one fails here naming that.
pub(crate) fn resolve_ordinal(device: Option<(&str, u32)>) -> Result<usize> {
    driver::initialize()?;
    let candidates = cuda_devices()?;
    match device {
        Some((class, member)) => {
            let classes: Vec<(String, usize)> = candidates
                .iter()
                .map(|candidate| (candidate.class.clone(), candidate.ordinal))
                .collect();
            resolve_member(&classes, class, member)
        }
        None => {
            let ranking: Vec<(usize, DeviceType)> = candidates
                .iter()
                .map(|candidate| (candidate.ordinal, candidate.device_type))
                .collect();
            choose_device(&ranking)
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
    class: String,
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
/// identifiers of its configuration space, which are what this backend mints a
/// class from. The bus identifier names the device's directory under
/// `/sys/bus/pci/devices`, where the kernel publishes both.
fn pci_class(cu_device: sys::CUdevice) -> Result<String> {
    let bus_id = pci_bus_id(cu_device)?;
    let directory = PathBuf::from("/sys/bus/pci/devices").join(&bus_id);
    let vendor_id = pci_id(&directory.join("vendor"), &bus_id)?;
    let device_id = pci_id(&directory.join("device"), &bus_id)?;
    Ok(class_of(vendor_id, device_id))
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

/// Picks the ordinal of one member of a device class from `(class, ordinal)`
/// pairs.
///
/// Members of a class are ordered by ordinal, so `member` counts within the
/// class alone. Pure over the candidate list so the mapping is verifiable
/// without a device.
fn resolve_member(candidates: &[(String, usize)], class: &str, member: u32) -> Result<usize> {
    let mut members: Vec<usize> = candidates
        .iter()
        .filter(|(candidate, _)| candidate == class)
        .map(|(_, ordinal)| *ordinal)
        .collect();
    members.sort_unstable();
    if members.is_empty() {
        return Err(Error::Backend(format!(
            "no CUDA device {class} exists; present: {}",
            render_classes(candidates)
        )));
    }
    members.get(member as usize).copied().ok_or_else(|| {
        Error::Backend(format!(
            "CUDA device {class} has {} member(s); member {member} requested",
            members.len()
        ))
    })
}

/// The candidates' classes, each listed once.
fn render_classes(candidates: &[(String, usize)]) -> String {
    if candidates.is_empty() {
        return "no CUDA device".to_string();
    }
    let mut classes: Vec<&str> = Vec::new();
    for (class, _) in candidates {
        if !classes.contains(&class.as_str()) {
            classes.push(class);
        }
    }
    classes.join(", ")
}

/// Picks the winning ordinal from `(ordinal, type)` pairs: the lowest
/// `(type rank, ordinal)` pair. Pure over the ranking inputs so the policy is
/// verifiable without a device.
fn choose_device(candidates: &[(usize, DeviceType)]) -> Result<usize> {
    candidates
        .iter()
        .min_by_key(|(ordinal, device_type)| (device_type.rank(), *ordinal))
        .map(|(ordinal, _)| *ordinal)
        .ok_or_else(|| Error::Backend("no CUDA device to select".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_rank_orders_discrete_first() {
        assert!(DeviceType::Discrete.rank() < DeviceType::Integrated.rank());
    }

    #[test]
    fn deterministic_pick_prefers_discrete_over_lower_ordinal() {
        let candidates = [(0, DeviceType::Integrated), (1, DeviceType::Discrete)];
        assert_eq!(choose_device(&candidates).expect("pick a device"), 1);
    }

    #[test]
    fn deterministic_pick_breaks_ties_by_lowest_ordinal() {
        let candidates = [
            (2, DeviceType::Discrete),
            (0, DeviceType::Discrete),
            (1, DeviceType::Discrete),
        ];
        assert_eq!(choose_device(&candidates).expect("pick a device"), 0);
    }

    #[test]
    fn selecting_from_nothing_is_rejected() {
        assert!(matches!(choose_device(&[]), Err(Error::Backend(_))));
    }

    /// Two identical cards and one of another class, in ordinal order.
    fn candidates() -> Vec<(String, usize)> {
        vec![
            (class_of(0x10de, 0x2684), 0),
            (class_of(0x10de, 0x2d39), 1),
            (class_of(0x10de, 0x2d39), 2),
        ]
    }

    #[test]
    fn a_class_is_minted_as_the_vendor_device_hex_pair() {
        // The rendered form is a contract: a config selector matches this exact
        // string, and the journal and placement slots spell a class the same
        // way. It is also the spelling the WGSL backend mints, so one physical
        // card is one class name whichever backend reached it.
        assert_eq!(class_of(0x10de, 0x2d39), "10de:2d39");
        assert_eq!(class_of(0x10de, 0x2684), "10de:2684");
    }

    #[test]
    fn member_zero_selects_the_first_card_of_its_class() {
        assert_eq!(
            resolve_member(&candidates(), "10de:2d39", 0).expect("first member"),
            1
        );
    }

    #[test]
    fn members_are_ordered_by_ordinal() {
        assert_eq!(
            resolve_member(&candidates(), "10de:2d39", 1).expect("second member"),
            2
        );
        // The class's own members are consecutive here, but its position among
        // all candidates is not: member indices count within the class only.
        assert_eq!(
            resolve_member(&candidates(), "10de:2684", 0).expect("sole member"),
            0
        );
    }

    #[test]
    fn member_order_follows_ordinal_even_when_candidates_are_unsorted() {
        let unsorted = [(class_of(0x10de, 0x2d39), 2), (class_of(0x10de, 0x2d39), 1)];
        assert_eq!(
            resolve_member(&unsorted, "10de:2d39", 0).expect("lowest ordinal first"),
            1
        );
    }

    #[test]
    fn unknown_class_error_names_the_request_and_what_exists() {
        // The Intel card of a mixed machine: enumerated by the WGSL backend,
        // never by this one, so binding the CUDA program to it fails here.
        let error = resolve_member(&candidates(), "8086:7d51", 0).expect_err("absent class");
        let Error::Backend(message) = error else {
            panic!("expected a backend error");
        };
        assert!(
            message.contains("8086:7d51"),
            "names the request: {message}"
        );
        assert!(message.contains("CUDA"), "names the substrate: {message}");
        assert!(
            message.contains("10de:2d39") && message.contains("10de:2684"),
            "names what exists: {message}"
        );
    }

    #[test]
    fn an_absent_class_on_a_machine_with_no_cuda_device_says_so() {
        let error = resolve_member(&[], "10de:2d39", 0).expect_err("nothing to select");
        let Error::Backend(message) = error else {
            panic!("expected a backend error");
        };
        assert!(message.contains("no CUDA device"), "{message}");
    }

    #[test]
    fn member_out_of_range_error_names_the_member_count() {
        let error = resolve_member(&candidates(), "10de:2684", 1).expect_err("one member only");
        let Error::Backend(message) = error else {
            panic!("expected a backend error");
        };
        assert!(message.contains("10de:2684"), "names the class: {message}");
        assert!(message.contains('1'), "names the member count: {message}");
    }

    #[test]
    fn enumeration_never_fails_for_want_of_a_driver() {
        // The probe answers "none" on a machine with no CUDA rather than
        // failing, which is what the worker's enumeration relies on.
        enumerate_devices().expect("enumeration answers on any machine");
    }

    /// Requires an NVIDIA device.
    #[test]
    fn enumeration_reports_every_cuda_device() {
        let devices = enumerate_devices().expect("enumerate devices");
        assert!(!devices.is_empty(), "at least one CUDA device");
        for device in &devices {
            assert!(!device.name.is_empty());
            // Every CUDA device is an NVIDIA card, so the class's vendor half
            // is fixed and the device half comes from its PCI configuration.
            assert!(
                device.class.starts_with("10de:"),
                "an NVIDIA vendor id: {}",
                device.class
            );
            let (name, driver) = selected_device_desc(Some((device.class.as_str(), device.member)))
                .expect("resolve the device description");
            assert_eq!(name, device.name);
            assert!(!driver.is_empty(), "driver version reported");
        }
    }

    /// Requires an NVIDIA device.
    #[test]
    fn naming_an_absent_device_fails_to_resolve() {
        // A worker answers `Ready` with this name, so this is where a binding
        // onto hardware the machine does not have is caught: executor
        // construction is lazy and would not notice until the first task.
        assert!(matches!(
            selected_device_desc(Some(("dead:beef", 0))),
            Err(Error::Backend(_))
        ));
    }
}
