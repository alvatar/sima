//! The device an executor computes on.

use std::fmt;

use serde::{Deserialize, Serialize};
use sima_core::{Error, Result};

/// Longest class id, in bytes.
const MAX_CLASS_LEN: usize = 64;

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
        // string (`[[execution.device]] select = "8086:7d51"`), and the journal
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
}
