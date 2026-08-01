//! Loading a kernel from committed PTX, with the thread-block width it runs at.

use cudarc::driver::CudaFunction;
use cudarc::driver::sys;
use cudarc::nvrtc::Ptx;

use sima_core::{Error, Hash, Result, hash_bytes};

use crate::compile::COMPILER_ID;
use crate::context::Context;
use crate::driver;

/// A kernel loaded from PTX: its device function, the width of the thread block
/// it launches with, and the identity input a domain records.
///
/// The function keeps its module and the module keeps the context alive, so a
/// kernel is safe to hold for as long as it is used.
pub struct Kernel {
    function: CudaFunction,
    block_width: u32,
    ptx_digest: Hash,
}

impl Kernel {
    /// blake3 of the PTX text — an identity input for a domain.
    ///
    /// The PTX is what the driver executes, so hashing it covers the compiled
    /// artifact exactly, and the compiler that produced it needs no separate
    /// version to be trusted.
    pub fn ptx_digest(&self) -> Hash {
        self.ptx_digest
    }

    /// Canonical identity of the form this kernel executes in — an identity
    /// input for a domain.
    pub fn compiler_id(&self) -> &str {
        COMPILER_ID
    }

    /// Threads per block along x. A caller sizing a grid divides its element
    /// count by this, so the width is stated once and read back rather than
    /// repeated at the launch site.
    pub fn block_width(&self) -> u32 {
        self.block_width
    }

    /// The device function to launch.
    pub(crate) fn function(&self) -> &CudaFunction {
        &self.function
    }
}

impl Context {
    /// Loads `ptx`, takes the named entry point from it, and fixes the width of
    /// the thread blocks it launches with.
    ///
    /// The driver's just-in-time compiler turns the PTX into machine code for
    /// this device as the module loads, so a PTX targeting an architecture the
    /// device cannot run fails here rather than at the first launch.
    ///
    /// `block_width` is the thread count per block along x, matching the
    /// `__launch_bounds__` the kernel source declares: CUDA takes the block
    /// dimensions at launch rather than from the compiled artifact, so the
    /// width is supplied here and every launch of this kernel uses it.
    pub fn kernel(&self, ptx: &str, entry: &str, block_width: u32) -> Result<Kernel> {
        let context = self.stream().context();
        let maximum = context
            .attribute(sys::CUdevice_attribute_enum::CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK)
            .map_err(|e| driver::backend_error("read the device's maximum block width", e))?;
        if block_width == 0 || i64::from(block_width) > i64::from(maximum) {
            return Err(Error::Backend(format!(
                "kernel entry point '{entry}' asks for {block_width} threads per block; this \
                 device takes 1..={maximum}"
            )));
        }
        let module = context
            .load_module(Ptx::from_src(ptx))
            .map_err(|e| driver::backend_error("load the PTX module", e))?;
        let function = module.load_function(entry).map_err(|e| {
            driver::backend_error(
                &format!("take entry point '{entry}' from the PTX module"),
                e,
            )
        })?;
        Ok(Kernel {
            function,
            block_width,
            ptx_digest: hash_bytes(ptx.as_bytes()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::hash_bytes;

    /// The shipped compute kernel's committed PTX: `out[i] = in[i] * 2 + 1`.
    const SMOKE_PTX: &str = include_str!("../kernels/smoke.ptx");

    /// Requires `libnvrtc`.
    #[test]
    fn the_committed_ptx_reproduces_from_its_source() {
        // The committed artifact is what a device loads, so it must be exactly
        // what the committed source compiles to.
        assert_eq!(
            crate::compile(include_str!("../kernels/smoke.cu")).expect("compile the kernel"),
            SMOKE_PTX
        );
    }

    #[test]
    fn the_committed_ptx_is_what_the_toolkit_promises_a_caller() {
        // Both identities a kernel reports come from the artifact and the
        // pinned target, so they can be checked without a device.
        assert!(SMOKE_PTX.contains(".entry main_kernel("));
        assert!(SMOKE_PTX.contains(".target sm_75"));
        assert_eq!(COMPILER_ID, "ptx; arch=compute_75");
    }

    /// Loading a module and launching from it needs an NVIDIA device.
    mod on_device {
        use super::*;

        #[test]
        fn kernel_reports_identity_inputs() {
            let context = Context::new().expect("create compute context");
            let kernel = context
                .kernel(SMOKE_PTX, "main_kernel", 64)
                .expect("build kernel");
            assert_eq!(kernel.ptx_digest(), hash_bytes(SMOKE_PTX.as_bytes()));
            assert_eq!(kernel.compiler_id(), COMPILER_ID);
            assert_eq!(kernel.block_width(), 64);
        }

        #[test]
        fn an_entry_point_the_module_does_not_declare_is_rejected() {
            let context = Context::new().expect("create compute context");
            match context.kernel(SMOKE_PTX, "no_such_entry", 64) {
                Err(Error::Backend(message)) => {
                    assert!(message.contains("no_such_entry"), "{message}");
                }
                Err(other) => panic!("expected a backend lookup error, got {other:?}"),
                Ok(_) => panic!("expected an unknown entry point to be rejected"),
            }
        }

        #[test]
        fn malformed_ptx_is_rejected_by_the_driver() {
            let context = Context::new().expect("create compute context");
            match context.kernel("this is not PTX", "main_kernel", 64) {
                Err(Error::Backend(message)) => {
                    assert!(message.contains("load the PTX module"), "{message}");
                }
                Err(other) => panic!("expected a backend load error, got {other:?}"),
                Ok(_) => panic!("expected malformed PTX to be rejected"),
            }
        }

        #[test]
        fn a_block_width_the_device_cannot_launch_is_rejected() {
            // Caught before the module loads, so a mistyped width is a clear
            // toolkit error rather than an opaque launch failure later.
            let context = Context::new().expect("create compute context");
            for width in [0, 1 << 20] {
                match context.kernel("", "main_kernel", width) {
                    Err(Error::Backend(message)) => {
                        assert!(message.contains("threads per block"), "{message}");
                    }
                    Err(other) => panic!("expected a backend width error, got {other:?}"),
                    Ok(_) => panic!("expected block width {width} to be rejected"),
                }
            }
        }
    }
}
