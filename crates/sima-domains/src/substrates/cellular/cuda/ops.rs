//! [`CudaOps`]: the cellular substrate's device operations on the CUDA backend.

use sima_contracts::{DeviceBinding, DeviceInfo};
use sima_core::Result;
use sima_toolkit_cuda::{Buffer, BufferUpdate, Context, Kernel, selected_device_desc};

use crate::substrates::cellular::ops::CellularOps;

/// The CUDA backend: an NVIDIA context, and the toolkit calls the substrate's
/// harness and reduction are written against.
pub(crate) struct CudaOps {
    context: Context,
}

impl CellularOps for CudaOps {
    type Buffer = Buffer;
    type Kernel = Kernel;

    const COMPILER_COMPONENT: &'static str = "cuda.compiler";
    const COMPILER_ID: &'static str = sima_toolkit_cuda::COMPILER_ID;
    /// The committed PTX of the reduction, not the CUDA C beside it: the
    /// artifact a worker loads is what executes, and it is what a regenerated
    /// kernel changes.
    const REDUCE_SOURCE: &'static str = include_str!("kernels/reduce.ptx");
    /// `main` is spoken for in C++, so the convention's single entry point
    /// takes the name the toolkit's own kernels use.
    const ENTRY: &'static str = "main_kernel";

    fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
        sima_toolkit_cuda::enumerate_devices()
    }

    fn device_desc(device: Option<&DeviceBinding>) -> Result<(String, String)> {
        selected_device_desc(device.map(|d| (d.class(), d.member)))
    }

    fn open(device: Option<&DeviceBinding>) -> Result<CudaOps> {
        let context = match device {
            Some(device) => Context::for_class(device.class(), device.member)?,
            None => Context::new()?,
        };
        Ok(CudaOps { context })
    }

    fn kernel(&self, source: &str, entry: &str, block_width: u32) -> Result<Kernel> {
        self.context.kernel(source, entry, block_width)
    }

    fn buffer(&self, size: usize) -> Result<Buffer> {
        self.context.buffer(size)
    }

    fn upload(&self, dst: &mut Buffer, bytes: &[u8]) -> Result<()> {
        self.context.upload(dst, bytes)
    }

    fn download(&self, src: &Buffer) -> Result<Vec<u8>> {
        self.context.download(src)
    }

    fn dispatch(&self, kernel: &Kernel, bound: &[&Buffer], groups: [u32; 3]) -> Result<()> {
        self.context.dispatch(kernel, bound, groups)
    }

    fn dispatch_with_update(
        &self,
        kernel: &Kernel,
        bound: &[&Buffer],
        update: &mut Buffer,
        bytes: &[u8],
        groups: [u32; 3],
    ) -> Result<()> {
        self.context.dispatch_with_update(
            kernel,
            bound,
            BufferUpdate {
                buffer: update,
                bytes,
            },
            groups,
        )
    }

    fn max_groups_x(&self) -> Result<u32> {
        Ok(self.context.max_groups()?[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::hash_bytes;
    use sima_toolkit_cuda::{PTX_OPTIONS, compile};

    use crate::substrates::cellular::{CellularEngine, CudaEngine, WgslEngine};

    /// The CUDA C the committed PTX is generated from.
    const REDUCE_CU: &str = include_str!("kernels/reduce.cu");

    #[test]
    fn the_committed_ptx_declares_the_four_entry_points() {
        // A module missing an entry point would fail only on a machine with a
        // device; the names are in the text, so this checks them anywhere.
        for entry in ["pass1", "combine1", "pass2", "combine2"] {
            assert!(
                CudaOps::REDUCE_SOURCE.contains(&format!(".entry {entry}(")),
                "the committed PTX declares {entry}"
            );
        }
    }

    #[test]
    fn the_committed_ptx_targets_the_architecture_the_options_name() {
        assert!(
            PTX_OPTIONS.contains(&"--gpu-architecture=compute_75"),
            "the committed PTX is generated for compute_75"
        );
        assert!(
            CudaOps::REDUCE_SOURCE.contains(".target sm_75"),
            "the committed PTX targets sm_75"
        );
    }

    /// Requires `libnvrtc`.
    #[test]
    fn the_committed_ptx_reproduces_from_its_source() {
        // The committed artifact is what executes, so it must be exactly what
        // the committed source compiles to. A mismatch means one of the two was
        // edited without the other, or that this NVRTC differs from the one
        // that produced the commit — the version is in the PTX header.
        assert_eq!(
            compile(REDUCE_CU).expect("compile the reduction"),
            CudaOps::REDUCE_SOURCE,
            "regenerate with the compile step in the crate's kernel documentation"
        );
    }

    #[test]
    fn enumeration_answers_on_a_machine_with_no_cuda_device() {
        // The probe the worker runs must answer rather than fail on a host with
        // no NVIDIA driver, so the orchestrator reads "no device" instead of a
        // failed probe.
        CudaOps::enumerate_devices().expect("enumeration answers on any machine");
    }

    #[test]
    fn the_reduction_digest_hashes_the_committed_ptx() {
        // The environment component this feeds is what makes a regenerated
        // reduction invalidate every task key of every domain on this backend,
        // so it must cover the artifact that executes.
        assert_eq!(
            CudaEngine::reduce_digest(),
            hash_bytes(CudaOps::REDUCE_SOURCE.as_bytes())
        );
    }

    #[test]
    fn the_compiler_component_is_distinct_from_the_other_backends() {
        // The component name enters the environment, so two backends must name
        // different ones: that is what keeps one backend's stored results out
        // of the other's task keys.
        assert_eq!(CudaEngine::COMPILER_COMPONENT, "cuda.compiler");
        assert_ne!(
            CudaEngine::COMPILER_COMPONENT,
            WgslEngine::COMPILER_COMPONENT
        );
        assert_eq!(CudaEngine::COMPILER_ID, sima_toolkit_cuda::COMPILER_ID);
    }
}
