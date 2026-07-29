//! The CUDA device context: the toolkit's owner of the device and its stream.

use std::sync::Arc;

use cudarc::driver::{CudaContext, CudaStream};

use sima_core::Result;

use crate::driver;
use crate::selection;

/// Owns the CUDA context and the stream every transfer and dispatch is
/// submitted to.
///
/// `cudarc` reference-counts the context behind the stream, and buffers and
/// kernels created here each hold their own reference, so teardown order takes
/// care of itself: the device outlives everything derived from it whatever the
/// drop order.
pub struct Context {
    stream: Arc<CudaStream>,
    device_name: String,
    driver_version: String,
}

impl Context {
    /// Creates a compute context on the auto-selected device: discrete before
    /// integrated, with the lowest CUDA ordinal breaking ties.
    pub fn new() -> Result<Context> {
        Context::build(None)
    }

    /// Creates a compute context on the given member of the given device class.
    ///
    /// The class is one this backend minted, and `member` counts within it,
    /// ordered by CUDA ordinal — the numbering
    /// [`enumerate_devices`](crate::enumerate_devices) reports. An absent class
    /// or a member out of range is an
    /// [`Error::Backend`](sima_core::Error::Backend) naming the request and
    /// what exists.
    pub fn for_class(class: &str, member: u32) -> Result<Context> {
        Context::build(Some((class, member)))
    }

    /// Builds a context on the device the binding resolves to, or on the
    /// default selection for `None`.
    fn build(device: Option<(&str, u32)>) -> Result<Context> {
        let ordinal = selection::resolve_ordinal(device)?;
        let context = CudaContext::new(ordinal)
            .map_err(|e| driver::backend_error("create the CUDA context", e))?;
        let device_name = context
            .name()
            .map_err(|e| driver::backend_error("read the CUDA device name", e))?;
        Ok(Context {
            stream: context.default_stream(),
            device_name,
            driver_version: selection::driver_version()?,
        })
    }

    /// The selected device's reported name, for provenance.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// The CUDA driver's reported version, for provenance.
    pub fn driver_version(&self) -> &str {
        &self.driver_version
    }

    /// The stream every transfer and dispatch is submitted to.
    pub(crate) fn stream(&self) -> &Arc<CudaStream> {
        &self.stream
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::Error;

    /// Requires an NVIDIA device.
    #[test]
    fn context_reports_device_provenance() {
        let context = Context::new().expect("create compute context");
        assert!(!context.device_name().is_empty());
        assert!(!context.driver_version().is_empty());
    }

    /// Requires an NVIDIA device.
    #[test]
    fn a_context_opens_on_every_enumerated_device() {
        let devices = crate::enumerate_devices().expect("enumerate devices");
        assert!(!devices.is_empty(), "at least one CUDA device");
        for device in &devices {
            let context = Context::for_class(&device.class, device.member)
                .expect("open the enumerated device");
            // The context opened the class member that was asked for, not
            // whichever device the default policy prefers.
            assert_eq!(context.device_name(), device.name);
        }
    }

    /// Requires an NVIDIA device.
    #[test]
    fn opening_an_absent_device_class_fails() {
        assert!(matches!(
            Context::for_class("dead:beef", 0),
            Err(Error::Backend(_))
        ));
    }
}
