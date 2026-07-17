//! Device selectors: naming the devices a run spreads its workers over, and
//! resolving those names against the hardware that is actually here.
//!
//! Resolution happens where a run starts, never where a config is read: a
//! selector names real hardware, and reading a config must work on a machine
//! with no GPU at all — `sima status` and `sima report` never enumerate.

use sima_contracts::DeviceClass;
use sima_core::{Error, Result};
use sima_domains::devices::DeviceInfo;
use sima_scheduler::DeviceEntry;

/// One `[[execution.device]]` entry as written: which device, and how many
/// workers on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceSelector {
    /// A case-insensitive substring of the device's reported name, or its
    /// exact `vendor:device` hex pair.
    pub select: String,
    /// Worker processes to run on the selected device.
    pub workers: usize,
}

/// Resolves each selector against `enumerated`, pairing it with the class it
/// names and that class's card count.
///
/// Each selector must name exactly one class: zero matches, several classes,
/// or two selectors landing on one class are all validation errors, and each
/// names every device that exists so the fix is a copy of one line.
pub fn resolve(
    selectors: &[DeviceSelector],
    enumerated: &[DeviceInfo],
) -> Result<Vec<DeviceEntry>> {
    let mut entries: Vec<DeviceEntry> = Vec::with_capacity(selectors.len());
    // The selector each entry came from, so a collision can name both texts —
    // the two lines the reader has to reconcile.
    let mut resolved_from: Vec<&str> = Vec::with_capacity(selectors.len());
    for selector in selectors {
        let matched: Vec<&DeviceInfo> = enumerated
            .iter()
            .filter(|device| matches(&selector.select, device))
            .collect();
        // Members of one class are interchangeable, so a selector naming a
        // class with several cards has named one thing, not many.
        let mut classes: Vec<DeviceClass> = Vec::new();
        for device in &matched {
            let class = class_of(device);
            if !classes.contains(&class) {
                classes.push(class);
            }
        }
        let [class] = classes.as_slice() else {
            return Err(Error::Validation(format!(
                "device selector {:?} matches {}; {}",
                selector.select,
                match classes.len() {
                    0 => "no device".to_string(),
                    n => format!("{n} devices: {}", render_classes(&classes)),
                },
                render_available(enumerated),
            )));
        };
        if let Some(position) = entries.iter().position(|entry| entry.class == *class) {
            return Err(Error::Validation(format!(
                "device selectors {:?} and {:?} both match {} ({class}); \
                 each device takes one entry",
                resolved_from[position], selector.select, entries[position].name
            )));
        }
        entries.push(DeviceEntry {
            class: *class,
            // Every member of a class reports the same name; the first is the
            // class's name.
            name: matched[0].name.clone(),
            workers: selector.workers,
            members: matched.len() as u32,
        });
        resolved_from.push(&selector.select);
    }
    Ok(entries)
}

/// Whether `select` names `device`: its exact `vendor:device` hex pair, or a
/// case-insensitive substring of its name.
fn matches(select: &str, device: &DeviceInfo) -> bool {
    if select.eq_ignore_ascii_case(&class_of(device).to_string()) {
        return true;
    }
    device.name.to_lowercase().contains(&select.to_lowercase())
}

/// The class a device belongs to.
fn class_of(device: &DeviceInfo) -> DeviceClass {
    DeviceClass {
        vendor_id: device.vendor_id,
        device_id: device.device_id,
    }
}

/// The classes as `vendor:device`, for an ambiguous-selector message.
fn render_classes(classes: &[DeviceClass]) -> String {
    classes
        .iter()
        .map(DeviceClass::to_string)
        .collect::<Vec<String>>()
        .join(", ")
}

/// Every enumerated device as `name (vendor:device)`, so a failed selector
/// shows what could have been written instead.
fn render_available(enumerated: &[DeviceInfo]) -> String {
    if enumerated.is_empty() {
        return "no compute-capable device is present".to_string();
    }
    let mut listed: Vec<String> = Vec::new();
    for device in enumerated {
        let rendered = format!("{} ({})", device.name, class_of(device));
        if !listed.contains(&rendered) {
            listed.push(rendered);
        }
    }
    format!("present: {}", listed.join(", "))
}

#[cfg(test)]
mod tests {
    use sima_domains::devices::DeviceType;

    use super::*;

    /// One enumerated device.
    fn info(vendor_id: u32, device_id: u32, name: &str, member: u32) -> DeviceInfo {
        DeviceInfo {
            vendor_id,
            device_id,
            name: name.to_string(),
            device_type: DeviceType::Discrete,
            member,
        }
    }

    /// An Intel iGPU and an NVIDIA dGPU, the shape of a laptop.
    fn two_devices() -> Vec<DeviceInfo> {
        vec![
            info(0x8086, 0x7d51, "Intel(R) Graphics (ARL)", 0),
            info(
                0x10de,
                0x2d39,
                "NVIDIA RTX PRO 2000 Blackwell Laptop GPU",
                0,
            ),
        ]
    }

    /// A selector over `select` carrying one worker.
    fn select(select: &str) -> DeviceSelector {
        DeviceSelector {
            select: select.to_string(),
            workers: 1,
        }
    }

    #[test]
    fn a_name_substring_selects_its_device() -> Result<()> {
        let entries = resolve(&[select("NVIDIA")], &two_devices())?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].class.to_string(), "10de:2d39");
        assert_eq!(entries[0].members, 1);
        assert_eq!(entries[0].workers, 1);
        Ok(())
    }

    #[test]
    fn a_name_substring_ignores_case() -> Result<()> {
        let entries = resolve(&[select("nvidia")], &two_devices())?;
        assert_eq!(entries[0].class.to_string(), "10de:2d39");
        Ok(())
    }

    #[test]
    fn an_exact_id_pair_selects_its_device() -> Result<()> {
        let entries = resolve(&[select("8086:7d51")], &two_devices())?;
        assert_eq!(entries[0].class.to_string(), "8086:7d51");
        assert_eq!(entries[0].name, "Intel(R) Graphics (ARL)");
        Ok(())
    }

    #[test]
    fn identical_cards_are_one_class_of_several_members() -> Result<()> {
        // Two of the same card: one selector names both, because members are
        // interchangeable by declaration.
        let pair = vec![
            info(0x10de, 0x2d39, "NVIDIA RTX PRO 2000", 0),
            info(0x10de, 0x2d39, "NVIDIA RTX PRO 2000", 1),
        ];
        let entries = resolve(&[select("NVIDIA")], &pair)?;
        assert_eq!(entries.len(), 1, "one class");
        assert_eq!(entries[0].members, 2, "with two cards");
        Ok(())
    }

    #[test]
    fn a_selector_matching_nothing_lists_what_is_present() {
        let error = resolve(&[select("AMD")], &two_devices()).expect_err("no AMD here");
        let Error::Validation(message) = error else {
            panic!("expected a validation error");
        };
        assert!(message.contains("\"AMD\""), "names the selector: {message}");
        assert!(
            message.contains("Intel(R) Graphics (ARL) (8086:7d51)"),
            "lists every device with its ids: {message}"
        );
        assert!(
            message.contains("(10de:2d39)"),
            "lists every device: {message}"
        );
    }

    #[test]
    fn a_selector_matching_several_classes_is_ambiguous() {
        // Two different NVIDIA cards: "NVIDIA" names both, so the run would
        // not know which to place work on.
        let two_nvidias = vec![
            info(0x10de, 0x2d39, "NVIDIA RTX PRO 2000", 0),
            info(0x10de, 0x2684, "NVIDIA RTX 4090", 0),
        ];
        let error = resolve(&[select("NVIDIA")], &two_nvidias).expect_err("ambiguous");
        let Error::Validation(message) = error else {
            panic!("expected a validation error");
        };
        assert!(message.contains("2 devices"), "says how many: {message}");
        assert!(message.contains("10de:2d39"), "names them: {message}");
        assert!(message.contains("10de:2684"), "names them: {message}");
    }

    #[test]
    fn two_selectors_may_not_name_the_same_device() {
        let error = resolve(&[select("NVIDIA"), select("10de:2d39")], &two_devices())
            .expect_err("one device, two entries");
        let Error::Validation(message) = error else {
            panic!("expected a validation error");
        };
        // Both selector texts, so the reader knows which two lines collided.
        assert!(
            message.contains("\"NVIDIA\""),
            "names the first selector: {message}"
        );
        assert!(
            message.contains("\"10de:2d39\""),
            "names the second selector: {message}"
        );
        assert!(
            message.contains("NVIDIA RTX PRO 2000 Blackwell Laptop GPU (10de:2d39)"),
            "names the device both matched: {message}"
        );
    }

    #[test]
    fn no_devices_at_all_is_reported_as_such() {
        let error = resolve(&[select("NVIDIA")], &[]).expect_err("nothing to select");
        let Error::Validation(message) = error else {
            panic!("expected a validation error");
        };
        assert!(message.contains("no compute-capable device is present"));
    }

    #[test]
    fn every_selector_resolves_in_order() -> Result<()> {
        let entries = resolve(
            &[
                DeviceSelector {
                    select: "NVIDIA".to_string(),
                    workers: 3,
                },
                DeviceSelector {
                    select: "8086:7d51".to_string(),
                    workers: 1,
                },
            ],
            &two_devices(),
        )?;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].workers, 3);
        assert_eq!(entries[1].workers, 1);
        Ok(())
    }
}
