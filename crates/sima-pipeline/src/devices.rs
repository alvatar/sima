//! Device selectors: naming the devices a search spreads its workers over, and
//! resolving those names against the devices its program can actually open
//! here.
//!
//! Resolution happens where a search starts, never where a config is read: a
//! selector names real hardware, and reading a config must work on a machine
//! with no GPU at all — `sima status` and `sima report` never enumerate.

use sima_contracts::{DeviceBinding, DeviceClass};
use sima_core::{Error, Result};
use sima_domains::devices::{DeviceInfo, DeviceType};
use sima_scheduler::DeviceEntry;

/// One `[[orchestrator.device]]` entry as written: which device, and how many
/// workers on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceSelector {
    /// A case-insensitive substring of the device's reported name, or the
    /// exact class its execution backend minted.
    pub select: String,
    /// Worker processes to run on the selected device.
    pub workers: usize,
}

/// The devices a machine's workers may be placed on: every enumerated device,
/// minus the CPU ones whenever a non-CPU device is present.
///
/// A host whose graphics stack works enumerates the CPU rasterizer beside its
/// card, and a machine with a GPU is used for the GPU: placing a worker on the
/// rasterizer would spend the machine running the slowest device on it. When
/// every enumerated device is a CPU they all stand — a host that offers this
/// program no GPU still gets workers.
///
/// The devices are the ones the search's program can open, since the probe asked
/// about its format, so every device this yields is a place the search can
/// actually put a worker.
pub(crate) fn usable(devices: &[DeviceInfo]) -> impl Iterator<Item = &DeviceInfo> {
    let has_gpu = devices
        .iter()
        .any(|device| device.device_type != DeviceType::Cpu);
    devices
        .iter()
        .filter(move |device| !has_gpu || device.device_type != DeviceType::Cpu)
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
            if !classes.contains(&device.class) {
                classes.push(device.class.clone());
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
            class: (*class).clone(),
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

/// Parses the `sima-worker --enumerate-devices` probe's stdout — one JSON
/// [`DeviceInfo`] per line — into the device list to resolve a remote's
/// selectors against. Blank lines are ignored; a line that is not a valid
/// device object is [`Error::Validation`]. Empty output is a machine with no
/// compute device, an empty list, not an error.
pub fn parse_enumeration(text: &str) -> Result<Vec<DeviceInfo>> {
    let mut devices = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let device: DeviceInfo = serde_json::from_str(line).map_err(|e| {
            Error::Validation(format!(
                "device enumeration line {line:?} is not a device: {e}"
            ))
        })?;
        devices.push(device);
    }
    Ok(devices)
}

/// Whether `select` names `device`: its exact class, or a case-insensitive
/// substring of its name.
fn matches(select: &str, device: &DeviceInfo) -> bool {
    if select.eq_ignore_ascii_case(device.class.as_str()) {
        return true;
    }
    device.name.to_lowercase().contains(&select.to_lowercase())
}

/// The classes, for an ambiguous-selector message.
fn render_classes(classes: &[DeviceClass]) -> String {
    classes
        .iter()
        .map(DeviceClass::to_string)
        .collect::<Vec<String>>()
        .join(", ")
}

/// Every enumerated device as `name (class)`, so a failed selector shows what
/// could have been written instead.
fn render_available(enumerated: &[DeviceInfo]) -> String {
    if enumerated.is_empty() {
        return "no compute-capable device is present".to_string();
    }
    let mut listed: Vec<String> = Vec::new();
    for device in enumerated {
        let rendered = format!("{} ({})", device.name, device.class);
        if !listed.contains(&rendered) {
            listed.push(rendered);
        }
    }
    format!("present: {}", listed.join(", "))
}

/// One worker slot per usable device, each bound to it; an enumeration
/// reporting no device at all yields a single deviceless worker — the stub
/// testing path, and any device-free machine.
///
/// Three places derive a worker layout from one enumeration and must agree on
/// what a machine offers: a rented machine's slots, the far-side config a
/// migration synthesizes for one, and a search whose layout its program's own
/// enumeration decides. [`usable`] is the rule they share.
pub(crate) fn derived_slots(devices: &[DeviceInfo]) -> Vec<Option<DeviceBinding>> {
    if devices.is_empty() {
        return vec![None];
    }
    usable(devices)
        .map(|device| {
            Some(DeviceBinding {
                class: device.class.clone(),
                member: device.member,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use sima_domains::devices::DeviceType;

    use super::*;

    /// One enumerated device.
    fn info(class: &str, name: &str, member: u32) -> DeviceInfo {
        DeviceInfo {
            class: DeviceClass::new(class).expect("class id"),
            name: name.to_string(),
            device_type: DeviceType::Discrete,
            member,
        }
    }

    /// An Intel iGPU and an NVIDIA dGPU, a typical laptop device set.
    fn two_devices() -> Vec<DeviceInfo> {
        vec![
            info("8086:7d51", "Intel(R) Graphics (ARL)", 0),
            info("10de:2d39", "NVIDIA RTX PRO 2000 Blackwell Laptop GPU", 0),
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
            info("10de:2d39", "NVIDIA RTX PRO 2000", 0),
            info("10de:2d39", "NVIDIA RTX PRO 2000", 1),
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
        // Two different NVIDIA cards: "NVIDIA" names both, so the search would
        // not know which to place work on.
        let two_nvidias = vec![
            info("10de:2d39", "NVIDIA RTX PRO 2000", 0),
            info("10de:2684", "NVIDIA RTX 4090", 0),
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

    #[test]
    fn a_probe_output_round_trips_through_the_parser() -> Result<()> {
        // The probe writes one JSON device per line; the parser reads exactly
        // what `sima-worker --enumerate-devices` serializes.
        let devices = two_devices();
        let text = devices
            .iter()
            .map(|d| serde_json::to_string(d).expect("device to JSON"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(parse_enumeration(&text)?, devices);
        Ok(())
    }

    #[test]
    fn probe_output_with_no_devices_parses_as_an_empty_list() -> Result<()> {
        // A remote with no compute device: empty output is an empty list, not
        // an error. Blank lines and trailing whitespace are ignored.
        assert!(parse_enumeration("")?.is_empty());
        assert!(parse_enumeration("\n  \n")?.is_empty());
        Ok(())
    }

    #[test]
    fn a_malformed_probe_line_is_rejected() {
        let error = parse_enumeration("{not valid json").expect_err("not a device");
        let Error::Validation(message) = error else {
            panic!("expected a validation error");
        };
        assert!(message.contains("is not a device"), "{message}");
    }
}
