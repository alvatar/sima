//! CUDA driver entry: loading the driver library, initializing it, and turning
//! driver failures into [`Error::Gpu`].
//!
//! Every path that touches CUDA starts here. The driver library is opened at
//! run time, so a machine with no NVIDIA driver installed loads nothing and
//! reports no devices instead of failing.

use std::sync::OnceLock;

use cudarc::driver::result;
use cudarc::driver::result::DriverError;

use sima_core::{Error, Result};

/// The driver library, opened once and kept resident for the process.
///
/// `cudarc` opens the same library itself on its first call; this probe exists
/// because that path panics when the library is absent, and an absent driver is
/// an answer this toolkit reports rather than a failure. Opening it here first
/// keeps the library loaded for the rest of the process, so `cudarc`'s own load
/// resolves to the same image.
fn driver_library() -> Option<&'static libloading::Library> {
    static LIBRARY: OnceLock<Option<libloading::Library>> = OnceLock::new();
    LIBRARY
        .get_or_init(|| {
            // The same candidate names `cudarc` searches, in the same order, so
            // the probe answers exactly whether `cudarc` will find the library.
            ["cuda", "nvcuda"]
                .into_iter()
                .flat_map(cudarc::get_lib_name_candidates)
                // SAFETY: opening the platform CUDA driver library runs its
                // initializers; the handle is never unloaded, so nothing can
                // observe it disappearing.
                .find_map(|name| unsafe { libloading::Library::new(name) }.ok())
        })
        .as_ref()
}

/// Initializes the CUDA driver, which every other driver call requires.
///
/// Repeated calls are the driver's own no-op after the first success.
pub(crate) fn initialize() -> Result<()> {
    if driver_library().is_none() {
        return Err(Error::Gpu(
            "no CUDA driver library is installed on this machine".to_string(),
        ));
    }
    result::init().map_err(|e| gpu_error("initialize the CUDA driver", e))
}

/// Runs `query` against an initialized driver, resolving to `None` on a machine
/// where CUDA cannot run instead of an error.
///
/// Two states mean no CUDA device can exist here: the driver library is absent,
/// and `cuInit` refuses. `cuInit` has no single defined answer for "nothing is
/// installed" — a missing kernel module, absent device nodes, and a userspace
/// and kernel driver at different versions each report their own code — so any
/// initialization failure is read as the machine having no CUDA device. For a
/// caller asking what hardware the machine has, that is an answer. Every
/// failure after initialization stays an error, and a caller that names a
/// device ([`selected_device_desc`](crate::selected_device_desc),
/// [`Context::new`](crate::Context::new)) initializes through
/// [`initialize`] instead, so it receives the driver's own refusal.
pub(crate) fn with_driver_or_none<T>(query: impl FnOnce() -> Result<T>) -> Result<Option<T>> {
    if driver_library().is_none() || result::init().is_err() {
        return Ok(None);
    }
    query().map(Some)
}

/// Wraps a driver failure as [`Error::Gpu`], naming the operation and the
/// driver's own name and description of the failure.
pub(crate) fn gpu_error(operation: &str, error: DriverError) -> Error {
    Error::Gpu(format!("{operation}: {}", render(error)))
}

/// The driver's name and description of an error, as `NAME: description`.
/// Either half is omitted when the driver cannot render it.
fn render(error: DriverError) -> String {
    let name = error
        .error_name()
        .ok()
        .and_then(|name| name.to_str().ok())
        .unwrap_or("CUDA error");
    match error
        .error_string()
        .ok()
        .and_then(|text| text.to_str().ok())
    {
        Some(text) => format!("{name}: {text}"),
        None => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_machine_without_a_usable_driver_resolves_to_none() -> Result<()> {
        // The probe answers for whatever this machine has: with no usable CUDA
        // driver the query never runs and the answer is None, and with one the
        // query runs and its value comes back. Neither is an error, which is
        // what the enumeration path relies on.
        let ran = with_driver_or_none(|| Ok(7))?;
        match ran {
            None => assert!(
                driver_library().is_none() || result::init().is_err(),
                "None is reported only when CUDA cannot initialize"
            ),
            Some(value) => assert_eq!(value, 7),
        }
        Ok(())
    }

    #[test]
    fn a_query_failure_after_initialization_stays_an_error() {
        // Only the driver's own absence resolves to None; a failure the query
        // itself raises propagates, so a real fault is never read as "no
        // hardware here".
        if driver_library().is_none() || result::init().is_err() {
            return;
        }
        let result = with_driver_or_none(|| -> Result<()> { Err(Error::Gpu("probe".to_string())) });
        assert!(matches!(result, Err(Error::Gpu(_))));
    }
}
