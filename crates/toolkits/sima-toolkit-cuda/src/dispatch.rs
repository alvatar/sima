//! Binding buffers to a kernel and launching thread blocks.

use cudarc::driver::{LaunchConfig, PushKernelArg};

use sima_core::{Error, Result};

use crate::buffer::Buffer;
use crate::context::Context;
use crate::driver;
use crate::kernel::Kernel;

/// The largest update a dispatch carries, in bytes. Both backends hold the
/// same bound — it is what Vulkan writes inline from a command buffer — so an
/// update a domain writes is portable between them.
const MAX_UPDATE: usize = 65536;

/// A small host-side write folded into a dispatch's own stream ordering.
///
/// The bytes land in `buffer` before the kernel reads it, submitted to the same
/// stream as the launch, so a value that changes per dispatch — a step index, a
/// pass number — costs no drain and no allocation of its own.
///
/// The updated buffer binds after every buffer in the dispatch's own list. That
/// is what makes the write sound to express: the update holds it exclusively
/// while the rest of the bindings are shared.
pub struct BufferUpdate<'a> {
    pub buffer: &'a mut Buffer,
    /// The bytes to write, from the start of the buffer: a whole number of
    /// 4-byte words, at most 64 KiB, and no longer than the buffer.
    pub bytes: &'a [u8],
}

impl Context {
    /// Binds `buffers` to the kernel's parameters in order and launches
    /// `groups` thread blocks of the kernel's own width.
    ///
    /// Each buffer becomes one pointer parameter, matched by position to the
    /// kernel's declaration, so a kernel taking four pointers is dispatched
    /// with four buffers in the order it declares them. A count that disagrees
    /// with the declaration is rejected here: the driver takes whatever a
    /// launch pushes, so an unchecked mismatch would leave the kernel reading a
    /// pointer that was never supplied. The launch is submitted to the
    /// context's stream and drained before returning: the stream already orders
    /// one dispatch's writes against the next one's reads, and draining trades
    /// batching for failures that surface at the call that caused them.
    pub fn dispatch(&self, kernel: &Kernel, buffers: &[&Buffer], groups: [u32; 3]) -> Result<()> {
        if buffers.len() != kernel.params() {
            return Err(Error::Backend(format!(
                "dispatch expects {} buffers for the kernel's parameters, got {}",
                kernel.params(),
                buffers.len()
            )));
        }
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
        unsafe { launch.launch(config) }
            .map_err(|e| driver::backend_error("launch the kernel", e))?;
        self.stream()
            .synchronize()
            .map_err(|e| driver::backend_error("drain the stream after a dispatch", e))
    }

    /// Dispatches as [`dispatch`](Context::dispatch) does, with `update`
    /// applied to its own buffer first and that buffer bound last.
    ///
    /// The copy goes onto the stream the launch is submitted to, which orders
    /// it against the kernel's read: the bytes land before the kernel that
    /// reads them, with no drain between the two.
    pub fn dispatch_with_update(
        &self,
        kernel: &Kernel,
        buffers: &[&Buffer],
        update: BufferUpdate<'_>,
        groups: [u32; 3],
    ) -> Result<()> {
        check_update(update.bytes, update.buffer.len())?;
        self.upload(update.buffer, update.bytes)?;
        let mut bound: Vec<&Buffer> = buffers.to_vec();
        bound.push(update.buffer);
        self.dispatch(kernel, &bound, groups)
    }
}

/// Confirms an update fits the portable bound and what the buffer holds.
fn check_update(bytes: &[u8], size: usize) -> Result<()> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) || bytes.len() > MAX_UPDATE {
        return Err(Error::Backend(format!(
            "a dispatch update is 4 to {MAX_UPDATE} bytes in whole 4-byte words, got {}",
            bytes.len()
        )));
    }
    if bytes.len() > size {
        return Err(Error::Backend(format!(
            "a dispatch update of {} bytes exceeds buffer size {size}",
            bytes.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped compute kernel: `out[i] = in[i] * 2 + 1`, over a third
    /// buffer holding the element count.
    const SMOKE_PTX: &str = include_str!("../kernels/smoke.ptx");

    #[test]
    fn an_update_is_whole_words_within_the_portable_bound() {
        // The bound is Vulkan's, held here too so an update a domain writes
        // behaves the same on either backend rather than passing on one and
        // failing on the other.
        assert!(check_update(&[], 64).is_err(), "empty");
        assert!(check_update(&[0; 6], 64).is_err(), "not whole 4-byte words");
        assert!(
            check_update(&[0; MAX_UPDATE + 4], 1 << 20).is_err(),
            "past the portable bound"
        );
        check_update(&[0; 8], 64).expect("eight bytes into a 64-byte buffer");
    }

    #[test]
    fn an_update_longer_than_its_buffer_is_rejected() {
        let error = check_update(&[0; 16], 8).expect_err("past the buffer");
        let Error::Backend(message) = error else {
            panic!("expected a backend error");
        };
        assert!(message.contains("exceeds buffer size 8"), "{message}");
    }

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

    /// Every dispatch test launches a kernel, which needs an NVIDIA device.
    mod on_device {
        use super::*;

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
}
