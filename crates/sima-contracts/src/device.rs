//! The device an executor computes on.

use std::fmt;

/// The compute device an executor is bound to: a device class and the member
/// within it.
///
/// A binding says *where* an executor computes; it grants no state access, so
/// executors stay pure compute under it.
///
/// Operational data, never identity: a binding never enters a task key, an
/// environment, or a record, so it carries no canonical encoding. The frame
/// encoding that carries it to a worker belongs to the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceBinding {
    pub vendor_id: u32,
    pub device_id: u32,
    /// The position within the class, ordered as the execution backend
    /// enumerates its devices.
    pub member: u32,
}

impl DeviceBinding {
    /// The class this binding names.
    pub fn class(&self) -> DeviceClass {
        DeviceClass {
            vendor_id: self.vendor_id,
            device_id: self.device_id,
        }
    }
}

/// A kind of compute device: the `(vendor_id, device_id)` pair the execution
/// backend reports.
///
/// Two identical cards are one class with two members. Members are
/// interchangeable by declaration — that is what makes them one class — so a
/// class carries no member: work bound to a class may run on any of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeviceClass {
    pub vendor_id: u32,
    pub device_id: u32,
}

impl fmt::Display for DeviceClass {
    /// Renders the class as `vendor:device` in hex, the spelling that names a
    /// device in configuration, diagnostics, and the run journal.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04x}:{:04x}", self.vendor_id, self.device_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_binding_names_its_class_without_its_member() {
        let binding = DeviceBinding {
            vendor_id: 0x10de,
            device_id: 0x2d39,
            member: 3,
        };
        assert_eq!(
            binding.class(),
            DeviceClass {
                vendor_id: 0x10de,
                device_id: 0x2d39,
            }
        );
    }

    #[test]
    fn members_of_one_class_share_it() {
        let first = DeviceBinding {
            vendor_id: 0x10de,
            device_id: 0x2d39,
            member: 0,
        };
        let second = DeviceBinding { member: 1, ..first };
        assert_eq!(first.class(), second.class());
    }

    #[test]
    fn a_class_renders_as_vendor_device_hex() {
        let class = DeviceClass {
            vendor_id: 0x8086,
            device_id: 0x7d51,
        };
        assert_eq!(class.to_string(), "8086:7d51");
    }
}
