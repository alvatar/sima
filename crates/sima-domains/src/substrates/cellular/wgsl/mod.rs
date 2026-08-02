//! The WGSL half of the cellular substrate: the adapter onto
//! [`sima_toolkit_wgsl`], and the shaders this backend ships.
//!
//! Everything the substrate does with a device is written once above the
//! [`CellularOps`](super::ops::CellularOps) boundary; what lives here is the
//! translation into one toolkit's surface, plus the kernels only this backend
//! can execute.

mod ops;

pub(crate) use ops::WgslOps;
