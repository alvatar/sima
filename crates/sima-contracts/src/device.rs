//! The device an executor computes on, and the selection of one from what a
//! backend enumerated.
//!
//! The vocabulary — [`DeviceClass`], [`DeviceInfo`], [`DeviceBinding`] — is what
//! a domain and the layers above it speak. The selection below is the policy
//! that turns an enumerated candidate list into the one device a context opens:
//! class minting, member numbering, the `SIMA_GPU_DEVICE` override, and the
//! default type ranking.
//!
//! The policy lives here rather than in an execution backend because it is the
//! same policy for every backend, and because minting classes in one place is
//! what makes one physical card one class name whichever backend reached it.
//! Every function is pure over the candidate list a backend supplies, so the
//! policy is verifiable without a device.

use std::fmt;

use serde::{Deserialize, Serialize};
use sima_core::{Error, Result};

/// Longest class id, in bytes.
const MAX_CLASS_LEN: usize = 64;

/// The environment variable overriding the default device pick with an
/// enumeration index.
const DEVICE_OVERRIDE: &str = "SIMA_GPU_DEVICE";

/// The compute device an executor is bound to: a device class and the member
/// within it.
///
/// A binding says *where* an executor computes; it grants no state access, so
/// executors stay pure compute under it.
///
/// Operational data, never identity: a binding never enters a task key, an
/// environment, or a record, so it carries no canonical encoding. The frame
/// encoding that carries it to a worker belongs to the transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceBinding {
    pub class: DeviceClass,
    /// The position within the class, ordered as the execution backend
    /// enumerates its devices.
    pub member: u32,
}

impl DeviceBinding {
    /// The class this binding names.
    pub fn class(&self) -> &DeviceClass {
        &self.class
    }
}

/// A kind of compute device, named by the execution backend that enumerates it.
///
/// The backend mints the string and sima compares, hashes, and renders it
/// without interpreting it. What tells two devices apart when they cannot stand
/// in for each other therefore belongs inside the string: a backend over
/// configuration-space identifiers mints `8086:7d51`, and a backend whose
/// devices are partitioned mints the partition profile alongside, so members of
/// one class stay interchangeable.
///
/// Two identical cards are one class with two members. Members are
/// interchangeable by declaration — that is what makes them one class — so a
/// class carries no member: work bound to a class may run on any of them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct DeviceClass(String);

impl DeviceClass {
    /// A class named by `id`, which is 1 to 64 bytes of `[a-z0-9._:-]`.
    ///
    /// The charset is what keeps a class legible where it surfaces — a config
    /// selector, a journal line, a placement slot — and what keeps one spelling
    /// per class, so a device is never named twice under two casings.
    pub fn new(id: impl Into<String>) -> Result<DeviceClass> {
        let id = id.into();
        if id.is_empty() || id.len() > MAX_CLASS_LEN {
            return Err(Error::Validation(format!(
                "device class {id:?} is {} bytes; a class id is 1 to {MAX_CLASS_LEN}",
                id.len()
            )));
        }
        if let Some(bad) = id.chars().find(|c| !is_class_char(*c)) {
            return Err(Error::Validation(format!(
                "device class {id:?} carries {bad:?}; a class id is lowercase \
                 alphanumerics and any of `. _ : -`"
            )));
        }
        Ok(DeviceClass(id))
    }

    /// The class as the backend minted it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for DeviceClass {
    type Error = Error;

    /// Validates a class read back from a serde form — a placement slot, an
    /// enumeration probe line — so a name no backend could have minted fails
    /// where it is read rather than travelling on as a class nothing matches.
    fn try_from(id: String) -> Result<DeviceClass> {
        DeviceClass::new(id)
    }
}

impl fmt::Display for DeviceClass {
    /// Renders the class as the backend minted it, the spelling that names a
    /// device in configuration, diagnostics, and the run journal.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Whether `c` may appear in a class id.
fn is_class_char(c: char) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | ':' | '-')
}

/// A compute-capable device as enumerated: what it is, what it is called, and
/// which one it is among the interchangeable members of its class.
///
/// A domain answers with a list of these when asked what its work can run on,
/// so the type is part of what a domain outside this workspace is written
/// against.
///
/// The serde form is the device list's wire shape — human-readable, never
/// identity-bearing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceInfo {
    /// The class, as the backend that enumerated this device minted it.
    pub class: DeviceClass,
    /// The device's own reported name.
    pub name: String,
    pub device_type: DeviceType,
    /// The position within the class, ordered as the backend enumerates.
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

impl DeviceType {
    /// Preference order across device types; lower ranks are preferred.
    ///
    /// This is the default pick's whole policy: real compute hardware before
    /// software fallbacks, so a machine with both runs on the card.
    pub fn rank(self) -> u8 {
        match self {
            DeviceType::Discrete => 0,
            DeviceType::Integrated => 1,
            DeviceType::Virtual => 2,
            DeviceType::Cpu => 3,
            DeviceType::Other => 4,
        }
    }
}

/// The class of a device with the configuration-space identifiers a backend
/// read for it.
///
/// Every backend mints its class names here, so one physical card is one class
/// name whichever backend reached it: a config selector written against a
/// Vulkan enumeration matches the same card under CUDA, and the journal and
/// placement slots spell it the same way.
pub fn class_of(vendor_id: u32, device_id: u32) -> DeviceClass {
    // Lowercase hex either side of a colon is inside the class charset and ten
    // bytes at most, so the minted id is valid by construction and nothing here
    // can fail. `a_minted_class_is_a_valid_class_id` holds that in place.
    DeviceClass(format!("{vendor_id:04x}:{device_id:04x}"))
}

/// The member index of each candidate within its own class, in the order the
/// backend enumerated them.
///
/// Two identical cards are one class with two members, so the count runs per
/// class rather than over the whole list: the second card of a class is member
/// 1 however many other devices sit between them.
pub fn number_members(classes: &[DeviceClass]) -> Vec<u32> {
    // Each class carries its own running count, so a class's next member is
    // whatever that count stands at when the class is met again.
    let mut seen: Vec<(&DeviceClass, u32)> = Vec::new();
    let mut members = Vec::with_capacity(classes.len());
    for class in classes {
        match seen.iter_mut().find(|(known, _)| *known == class) {
            Some((_, next)) => {
                members.push(*next);
                *next += 1;
            }
            None => {
                members.push(0);
                seen.push((class, 1));
            }
        }
    }
    members
}

/// Picks the enumeration index of one member of a device class from
/// `(class, index)` pairs.
///
/// Members of a class are ordered by enumeration index, so `member` counts
/// within the class alone. The index is whatever the backend numbers its
/// devices by — a Vulkan enumeration position, a CUDA ordinal — and travels
/// back to it unchanged.
pub fn resolve_member(
    candidates: &[(DeviceClass, usize)],
    class: &DeviceClass,
    member: u32,
) -> Result<usize> {
    let mut members: Vec<usize> = candidates
        .iter()
        .filter(|(candidate, _)| candidate == class)
        .map(|(_, index)| *index)
        .collect();
    members.sort_unstable();
    if members.is_empty() {
        return Err(Error::Backend(format!(
            "no compute-capable device {class} exists; present: {}",
            render_classes(candidates)
        )));
    }
    members.get(member as usize).copied().ok_or_else(|| {
        Error::Backend(format!(
            "device {class} has {} member(s); member {member} requested",
            members.len()
        ))
    })
}

/// Picks the winning enumeration index from `(index, type)` pairs.
///
/// With `requested` set, the named index wins when it is compute-capable and
/// fails otherwise; without it, the lowest `(type rank, index)` pair wins, so
/// the pick is deterministic across runs on one machine.
pub fn choose_device(
    candidates: &[(usize, DeviceType)],
    requested: Option<usize>,
) -> Result<usize> {
    if let Some(requested) = requested {
        return candidates
            .iter()
            .find(|(index, _)| *index == requested)
            .map(|(index, _)| *index)
            .ok_or_else(|| {
                Error::Backend(format!(
                    "{DEVICE_OVERRIDE}={requested} does not name a compute-capable device"
                ))
            });
    }
    candidates
        .iter()
        .min_by_key(|(index, device_type)| (device_type.rank(), *index))
        .map(|(index, _)| *index)
        .ok_or_else(|| Error::Backend("no compute-capable device to select".to_string()))
}

/// The `SIMA_GPU_DEVICE` override as an enumeration index, if set.
///
/// The index is the backend's own numbering, so the variable names the same
/// device the enumeration probe listed at that position.
pub fn requested_device_index() -> Result<Option<usize>> {
    match std::env::var(DEVICE_OVERRIDE) {
        Ok(value) => value.trim().parse::<usize>().map(Some).map_err(|_| {
            Error::Backend(format!(
                "{DEVICE_OVERRIDE} must be a device index, got {value:?}"
            ))
        }),
        Err(_) => Ok(None),
    }
}

/// The candidates' classes, each listed once, for an error that has to say what
/// the machine does have.
fn render_classes(candidates: &[(DeviceClass, usize)]) -> String {
    if candidates.is_empty() {
        return "none".to_string();
    }
    let mut classes: Vec<&str> = Vec::new();
    for (class, _) in candidates {
        if !classes.contains(&class.as_str()) {
            classes.push(class.as_str());
        }
    }
    classes.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A class that must be valid.
    fn class(id: &str) -> DeviceClass {
        DeviceClass::new(id).expect("a valid class id")
    }

    #[test]
    fn a_class_renders_the_name_the_backend_minted() {
        // The rendered form is a contract: a config selector matches this exact
        // string (`[[orchestrator.device]] select = "8086:7d51"`), and the journal
        // and placement slots spell a class the same way. A change of width,
        // separator, or case would leave selector matching silently missing its
        // device; here it fails loudly instead.
        assert_eq!(class("8086:7d51").to_string(), "8086:7d51");
        assert_eq!(class("8086:7d51").as_str(), "8086:7d51");
    }

    #[test]
    fn a_class_admits_a_partition_profile_beside_the_pair() {
        // A partitioned card reports one pair for every slice, so the profile
        // is what the backend adds to keep members interchangeable.
        assert_eq!(class("10de:2330:1g.10gb").to_string(), "10de:2330:1g.10gb");
    }

    #[test]
    fn a_partition_profile_is_a_class_of_its_own() {
        // Two profiles of one card differ in memory by up to a factor of four,
        // so work bound to one may not run on the other: they are two classes,
        // and neither is the bare pair.
        let whole = class("10de:2330");
        let small = class("10de:2330:1g.10gb");
        let large = class("10de:2330:4g.40gb");
        assert_ne!(whole, small);
        assert_ne!(small, large);
    }

    #[test]
    fn a_class_id_is_one_to_sixty_four_bytes() {
        assert!(DeviceClass::new("").is_err());
        assert!(DeviceClass::new("a".repeat(64)).is_ok());
        assert!(DeviceClass::new("a".repeat(65)).is_err());
    }

    #[test]
    fn a_class_id_stays_within_the_legible_charset() {
        // The charset is what keeps a class readable where it surfaces, and
        // what keeps one spelling per class: an uppercase or spaced variant
        // would name the same device twice.
        assert!(DeviceClass::new("8086:7D51").is_err());
        assert!(DeviceClass::new("8086 7d51").is_err());
        assert!(DeviceClass::new("8086/7d51").is_err());
        assert!(DeviceClass::new("a-b_c.d:0").is_ok());
    }

    #[test]
    fn a_rejected_class_id_names_itself() {
        let Err(Error::Validation(message)) = DeviceClass::new("8086:7D51") else {
            panic!("expected a validation error");
        };
        assert!(message.contains("8086:7D51"), "{message}");
    }

    #[test]
    fn a_binding_names_its_class() {
        let binding = DeviceBinding {
            class: class("10de:2d39"),
            member: 1,
        };
        assert_eq!(binding.class().as_str(), "10de:2d39");
    }

    #[test]
    fn a_class_is_minted_as_the_vendor_device_hex_pair() {
        // The rendered form is a contract: a config selector matches this exact
        // string, and the journal and placement slots spell a class the same
        // way. A change of width, separator, or case would leave selector
        // matching silently missing its device. Both execution backends mint
        // through this one function, so a card enumerated by either gets the
        // same name and neither can drift from the other.
        assert_eq!(class_of(0x8086, 0x7d51).to_string(), "8086:7d51");
        assert_eq!(class_of(0x10de, 0x2d39).to_string(), "10de:2d39");
    }

    #[test]
    fn a_minted_class_is_a_valid_class_id() {
        // Minting bypasses `DeviceClass::new`, so the charset and length bound
        // are held here instead: every pair of identifiers, extremes included,
        // renders inside them.
        for (vendor, device) in [(0, 0), (0xffff, 0xffff), (0x10de, 0x2d39)] {
            let minted = class_of(vendor, device);
            assert_eq!(
                DeviceClass::new(minted.as_str()).expect("a minted class validates"),
                minted
            );
        }
    }

    #[test]
    fn type_rank_orders_real_hardware_first() {
        // The default pick reads this order alone, so a machine offering both a
        // card and a software rasterizer runs on the card.
        let ranks = [
            DeviceType::Discrete,
            DeviceType::Integrated,
            DeviceType::Virtual,
            DeviceType::Cpu,
            DeviceType::Other,
        ]
        .map(DeviceType::rank);
        assert!(ranks.is_sorted(), "{ranks:?}");
    }

    #[test]
    fn deterministic_pick_prefers_discrete_over_lower_index() {
        let candidates = [(0, DeviceType::Integrated), (1, DeviceType::Discrete)];
        assert_eq!(choose_device(&candidates, None).expect("pick a device"), 1);
    }

    #[test]
    fn deterministic_pick_breaks_ties_by_lowest_index() {
        let candidates = [
            (2, DeviceType::Discrete),
            (0, DeviceType::Discrete),
            (1, DeviceType::Discrete),
        ];
        assert_eq!(choose_device(&candidates, None).expect("pick a device"), 0);
    }

    #[test]
    fn selecting_from_nothing_is_rejected() {
        assert!(matches!(choose_device(&[], None), Err(Error::Backend(_))));
    }

    #[test]
    fn the_override_selects_the_named_index() {
        // The override names an index the default policy would rank last, which
        // is what makes it an override rather than a hint.
        let candidates = [(0, DeviceType::Discrete), (1, DeviceType::Cpu)];
        assert_eq!(
            choose_device(&candidates, Some(1)).expect("the named index wins"),
            1
        );
    }

    #[test]
    fn an_override_out_of_range_is_rejected() {
        let candidates = [(0, DeviceType::Discrete)];
        let Err(Error::Backend(message)) = choose_device(&candidates, Some(7)) else {
            panic!("expected a backend error");
        };
        assert!(message.contains("SIMA_GPU_DEVICE=7"), "{message}");
    }

    /// Two identical cards and one of another class, in enumeration order.
    fn candidates() -> Vec<(DeviceClass, usize)> {
        vec![
            (class_of(0x8086, 0x7d51), 0),
            (class_of(0x10de, 0x2d39), 1),
            (class_of(0x10de, 0x2d39), 2),
        ]
    }

    #[test]
    fn members_are_numbered_within_their_own_class() {
        // The pair of identical cards is one class with two members, and the
        // card of another class between them takes no number from either.
        let classes = [
            class_of(0x10de, 0x2d39),
            class_of(0x8086, 0x7d51),
            class_of(0x10de, 0x2d39),
        ];
        assert_eq!(number_members(&classes), [0, 0, 1]);
    }

    #[test]
    fn member_zero_selects_the_first_card_of_its_class() {
        assert_eq!(
            resolve_member(&candidates(), &class_of(0x10de, 0x2d39), 0).expect("first member"),
            1
        );
    }

    #[test]
    fn members_are_ordered_by_enumeration_index() {
        assert_eq!(
            resolve_member(&candidates(), &class_of(0x10de, 0x2d39), 1).expect("second member"),
            2
        );
        // The class's own members are consecutive here, but its position among
        // all candidates is not: member indices count within the class only.
        assert_eq!(
            resolve_member(&candidates(), &class_of(0x8086, 0x7d51), 0).expect("sole member"),
            0
        );
    }

    #[test]
    fn member_order_follows_enumeration_even_when_candidates_are_unsorted() {
        let unsorted = [(class_of(0x10de, 0x2d39), 2), (class_of(0x10de, 0x2d39), 1)];
        assert_eq!(
            resolve_member(&unsorted, &class_of(0x10de, 0x2d39), 0).expect("lowest index first"),
            1
        );
    }

    #[test]
    fn an_unknown_class_error_names_the_request_and_what_exists() {
        let error =
            resolve_member(&candidates(), &class_of(0x1002, 0x1234), 0).expect_err("absent class");
        let Error::Backend(message) = error else {
            panic!("expected a backend error");
        };
        assert!(
            message.contains("1002:1234"),
            "names the request: {message}"
        );
        assert!(
            message.contains("10de:2d39") && message.contains("8086:7d51"),
            "names what exists: {message}"
        );
    }

    #[test]
    fn an_absent_class_on_a_machine_with_no_device_at_all_says_so() {
        let error = resolve_member(&[], &class_of(0x10de, 0x2d39), 0).expect_err("nothing to pick");
        let Error::Backend(message) = error else {
            panic!("expected a backend error");
        };
        assert!(message.contains("present: none"), "{message}");
    }

    #[test]
    fn a_member_out_of_range_error_names_the_member_count() {
        let error =
            resolve_member(&candidates(), &class_of(0x8086, 0x7d51), 1).expect_err("one member");
        let Error::Backend(message) = error else {
            panic!("expected a backend error");
        };
        assert!(message.contains("8086:7d51"), "names the class: {message}");
        assert!(message.contains('1'), "names the member count: {message}");
    }
}
