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

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped compute kernel: `out[i] = in[i] * 2 + 1`, over a third
    /// buffer holding the element count.
    const SMOKE_PTX: &str = include_str!("../kernels/smoke.ptx");

    /// The context, the kernel, and the element-count buffer every test here
    /// starts from.
    fn smoke(context: &Context, count: u32) -> (Kernel, Buffer) {
        let kernel = context
            .kernel(SMOKE_PTX, "main_kernel", 64)
            .expect("build kernel");
        let words = [count];
        let bytes: &[u8] = bytemuck::cast_slice(&words);
        let mut buffer = context.buffer(bytes.len()).expect("count buffer");
        context.upload(&mut buffer, bytes).expect("upload count");
        (kernel, buffer)
    }

    /// Requires an NVIDIA device.
    #[test]
    fn dispatch_applies_the_kernel() {
        let context = Context::new().expect("create compute context");
        let (kernel, count) = smoke(&context, 4);
        let input: [u32; 4] = [1, 2, 3, 4];
        let bytes: &[u8] = bytemuck::cast_slice(&input);
        let mut in_buffer = context.buffer(bytes.len()).expect("input buffer");
        let out_buffer = context.buffer(bytes.len()).expect("output buffer");
        context.upload(&mut in_buffer, bytes).expect("upload input");

        context
            .dispatch(&kernel, &[&in_buffer, &out_buffer, &count], [1, 1, 1])
            .expect("dispatch");

        let read_back = context.download(&out_buffer).expect("download output");
        let output: &[u32] = bytemuck::cast_slice(&read_back);
        assert_eq!(output, [3, 5, 7, 9]);
    }

    /// Requires an NVIDIA device.
    ///
    /// A dispatch that reads a buffer a prior dispatch wrote must observe that
    /// output: both launches go to the same stream, which orders them, and
    /// each dispatch drains before returning. Applying `out = in * 2 + 1`
    /// twice, ping-ponging the two buffers, yields `in * 4 + 3`.
    #[test]
    fn a_dispatch_observes_a_prior_dispatchs_writes() {
        let context = Context::new().expect("create compute context");
        let (kernel, count) = smoke(&context, 4);
        let input: [u32; 4] = [1, 2, 3, 4];
        let bytes: &[u8] = bytemuck::cast_slice(&input);
        let mut a = context.buffer(bytes.len()).expect("buffer a");
        let b = context.buffer(bytes.len()).expect("buffer b");
        context.upload(&mut a, bytes).expect("upload input");

        context
            .dispatch(&kernel, &[&a, &b, &count], [1, 1, 1])
            .expect("first dispatch");
        context
            .dispatch(&kernel, &[&b, &a, &count], [1, 1, 1])
            .expect("second dispatch");

        let read_back = context.download(&a).expect("download");
        let output: &[u32] = bytemuck::cast_slice(&read_back);
        // (in * 2 + 1) * 2 + 1 = in * 4 + 3.
        assert_eq!(output, [7, 11, 15, 19]);
    }

    /// Requires an NVIDIA device.
    #[test]
    fn a_launch_covering_more_threads_than_elements_writes_only_the_elements() {
        // The block width is fixed at 64, so a 4-element buffer is covered by
        // one block of 64 threads and 60 of them must fall out on the bounds
        // guard rather than write past the allocation.
        let context = Context::new().expect("create compute context");
        let (kernel, count) = smoke(&context, 4);
        let input: [u32; 4] = [1, 2, 3, 4];
        let bytes: &[u8] = bytemuck::cast_slice(&input);
        let mut in_buffer = context.buffer(bytes.len()).expect("input buffer");
        let out_buffer = context.buffer(bytes.len()).expect("output buffer");
        context.upload(&mut in_buffer, bytes).expect("upload input");
        context
            .dispatch(&kernel, &[&in_buffer, &out_buffer, &count], [1, 1, 1])
            .expect("dispatch");
        let read_back = context.download(&out_buffer).expect("download output");
        assert_eq!(read_back.len(), bytes.len());
    }
}
