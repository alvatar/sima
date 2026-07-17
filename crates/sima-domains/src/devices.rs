//! What compute devices this build can run on.
//!
//! The domains layer is where the set of compiled-in execution backends is
//! known, so it is where "what devices exist" is answered for the layers above:
//! they ask this crate rather than depending on a toolkit directly.
//!
//! Every domain's backend is the WGSL toolkit, so its enumeration is the whole
//! answer today; a second backend would widen this module, not its callers.

pub use sima_toolkit_wgsl::{DeviceInfo, DeviceType, enumerate_devices};
