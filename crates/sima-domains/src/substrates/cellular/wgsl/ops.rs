//! [`WgslOps`]: the cellular substrate's device operations on the WGSL backend.

use sima_contracts::{DeviceBinding, DeviceInfo};
use sima_core::Result;
use sima_toolkit_wgsl::{Buffer, BufferUpdate, Context, Kernel, selected_device_desc};

use crate::substrates::cellular::ops::CellularOps;

/// The WGSL backend: a Vulkan context, and the toolkit calls the substrate's
/// harness and reduction are written against.
pub(crate) struct WgslOps {
    context: Context,
}

impl CellularOps for WgslOps {
    type Buffer = Buffer;
    type Kernel = Kernel;

    const COMPILER_COMPONENT: &'static str = "wgsl.compiler";
    const COMPILER_ID: &'static str = sima_toolkit_wgsl::COMPILER_ID;
    /// The reduction shader. Its digest joins every environment on this
    /// backend, so editing it must change task keys exactly as editing a step
    /// kernel does.
    const REDUCE_SOURCE: &'static str = include_str!("shaders/reduce.wgsl");
    /// WGSL puts no name in the way, so the convention's single entry point
    /// takes the obvious one.
    const ENTRY: &'static str = "main";

    fn enumerate_devices() -> Result<Vec<DeviceInfo>> {
        sima_toolkit_wgsl::enumerate_devices()
    }

    fn device_desc(device: Option<&DeviceBinding>) -> Result<(String, String)> {
        selected_device_desc(device.map(|d| (d.class(), d.member)))
    }

    fn open(device: Option<&DeviceBinding>) -> Result<WgslOps> {
        let context = match device {
            Some(device) => Context::for_class(device.class(), device.member)?,
            None => Context::new()?,
        };
        Ok(WgslOps { context })
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
    use sima_toolkit_wgsl::check;

    use crate::substrates::cellular::{CellularEngine, WgslEngine};

    #[test]
    fn every_reduction_pass_compiles_device_free() -> Result<()> {
        // The four entry points share one module, so each one is compiled here
        // rather than only on a machine with a device.
        for entry in ["pass1", "combine1", "pass2", "combine2"] {
            check(WgslOps::REDUCE_SOURCE, entry)?;
        }
        Ok(())
    }

    #[test]
    fn enumeration_answers_on_a_machine_with_no_vulkan_device() {
        // The probe the worker runs must answer rather than fail on a host
        // whose Vulkan loader finds no driver, so the orchestrator reads "no
        // device" instead of a failed probe.
        WgslOps::enumerate_devices().expect("enumeration answers on any machine");
    }

    #[test]
    fn the_reduction_digest_hashes_the_shader_source() {
        // The environment component this feeds is what makes an edit to the
        // reduction invalidate every task key of every domain on this backend,
        // so it must cover the shader text exactly.
        assert_eq!(
            WgslEngine::reduce_digest(),
            hash_bytes(WgslOps::REDUCE_SOURCE.as_bytes())
        );
    }

    #[test]
    fn the_compiler_component_pins_the_toolkit_identity() {
        // The value is the toolkit's own pinned constant, so a naga upgrade
        // that changes emitted SPIR-V moves every task key on this backend.
        assert_eq!(WgslEngine::COMPILER_COMPONENT, "wgsl.compiler");
        assert_eq!(WgslEngine::COMPILER_ID, sima_toolkit_wgsl::COMPILER_ID);
    }
}
