//! [`CellularOps`]: the device operations the cellular substrate needs from a
//! compute backend.
//!
//! Everything above this boundary — the dispatch harness, the stats reduction,
//! the engine — is written once and monomorphized per backend. The adapters
//! that satisfy it live under `wgsl` and `cuda` and are about fifty lines each:
//! they translate these calls into one toolkit's own surface and answer for the
//! backend's identity.
//!
//! The boundary is deliberately internal rather than a trait on the toolkit
//! surface. A toolkit is a compute library that knows nothing of grids or
//! scalars, and each isolates its own dependency set; making it implement a
//! domain's trait would tie the two together in the direction the layering
//! forbids.

use sima_contracts::{DeviceBinding, DeviceInfo};
use sima_core::Result;

/// A compute backend the cellular substrate dispatches through.
///
/// The associated types are the backend's own allocation and kernel handles,
/// so nothing here boxes or erases them: a `Trajectory` holds the backend's
/// buffers directly.
pub(crate) trait CellularOps: Send + Sized + 'static {
    /// A device allocation of untyped bytes.
    type Buffer;
    /// A kernel loaded onto the device. `Send` because an engine holding one
    /// is built on the worker thread that will run it.
    type Kernel: Send;

    /// The name of the environment component that pins this backend's
    /// compiler. Each backend names its own, so a domain's environment says
    /// which compiler it depends on rather than only what that compiler is.
    const COMPILER_COMPONENT: &'static str;

    /// Canonical identity of the compiler and its output-affecting options,
    /// recorded in a domain's environment beside its kernel digest.
    const COMPILER_ID: &'static str;

    /// This backend's stats reduction as it ships: shader source on one
    /// backend, committed PTX on the other. Its digest is what enters every
    /// environment built on this backend.
    const REDUCE_SOURCE: &'static str;

    /// The entry point every cellular kernel on this backend declares.
    const ENTRY: &'static str;

    /// Every device this backend can open, in the domains layer's vocabulary.
    fn enumerate_devices() -> Result<Vec<DeviceInfo>>;

    /// The device's name and driver version, resolved without opening a
    /// device, for the worker handshake.
    fn device_desc(device: Option<&DeviceBinding>) -> Result<(String, String)>;

    /// Opens `device`, or the backend's default selection for `None`.
    fn open(device: Option<&DeviceBinding>) -> Result<Self>;

    /// Loads `source` and takes `entry` from it, at the given block width.
    fn kernel(&self, source: &str, entry: &str, block_width: u32) -> Result<Self::Kernel>;

    /// Allocates `size` zeroed bytes on the device.
    fn buffer(&self, size: usize) -> Result<Self::Buffer>;

    /// Copies host bytes into the head of a device allocation.
    fn upload(&self, dst: &mut Self::Buffer, bytes: &[u8]) -> Result<()>;

    /// Copies a device allocation back, in full.
    fn download(&self, src: &Self::Buffer) -> Result<Vec<u8>>;

    /// Binds `bound` in order and dispatches `groups` blocks.
    fn dispatch(
        &self,
        kernel: &Self::Kernel,
        bound: &[&Self::Buffer],
        groups: [u32; 3],
    ) -> Result<()>;

    /// Dispatches with `bytes` written into `update` first, inside the
    /// dispatch's own submission, and `update` bound after `bound`.
    fn dispatch_with_update(
        &self,
        kernel: &Self::Kernel,
        bound: &[&Self::Buffer],
        update: &mut Self::Buffer,
        bytes: &[u8],
        groups: [u32; 3],
    ) -> Result<()>;

    /// The largest group count this device launches along x.
    fn max_groups_x(&self) -> Result<u32>;
}
