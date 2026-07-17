//! The device an executor computes on.

/// The compute device an executor is bound to: a device class and the member
/// within it.
///
/// A class is the `(vendor_id, device_id)` pair as the execution backend
/// reports it — two identical cards are one class with two members — and
/// `member` is the position within the class. A binding says *where* an
/// executor computes; it grants no state access, so executors stay pure
/// compute under it.
///
/// Operational data, never identity: a binding never enters a task key, an
/// environment, or a record, so it carries no canonical encoding. The frame
/// encoding that carries it to a worker belongs to the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceBinding {
    pub vendor_id: u32,
    pub device_id: u32,
    pub member: u32,
}
