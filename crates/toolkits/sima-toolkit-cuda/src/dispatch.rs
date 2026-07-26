//! Binding buffers to a kernel and launching thread blocks.

use cudarc::driver::{LaunchConfig, PushKernelArg};

use sima_core::Result;

use crate::buffer::Buffer;
use crate::context::Context;
use crate::driver;
use crate::kernel::Kernel;

impl Context {
    /// Binds `buffers` to the kernel's parameters in order and launches
    /// `groups` thread blocks of the kernel's own width.
    ///
    /// Each buffer becomes one pointer parameter, matched by position to the
    /// kernel's declaration, so a kernel taking four pointers is dispatched
    /// with four buffers in the order it declares them. The launch is
    /// submitted to the context's stream and drained before returning: the
    /// stream already orders one dispatch's writes against the next one's
    /// reads, and draining trades batching for failures that surface at the
    /// call that caused them.
    pub fn dispatch(&self, kernel: &Kernel, buffers: &[&Buffer], groups: [u32; 3]) -> Result<()> {
        let config = LaunchConfig {
            grid_dim: (groups[0], groups[1], groups[2]),
            block_dim: (kernel.block_width(), 1, 1),
            shared_mem_bytes: 0,
        };
        let function = kernel.function();
        let mut launch = self.stream().launch_builder(function);
        for buffer in buffers {
            launch.arg(buffer.bytes());
        }
        // SAFETY: the kernel's parameters are pointers, one per bound buffer,
        // and each buffer outlives the drained launch below. What the kernel
        // does within those allocations is the kernel's own contract: it reads
        // its bounds from the buffers it is given, as every kernel in this
        // project does.
        unsafe { launch.launch(config) }.map_err(|e| driver::gpu_error("launch the kernel", e))?;
        self.stream()
            .synchronize()
            .map_err(|e| driver::gpu_error("drain the stream after a dispatch", e))
    }
}
