//! End-to-end smoke test spanning context, buffer, kernel, and dispatch.
//!
//! Requires a real Vulkan device, which the `on_device` suffix states.

use sima_toolkit_wgsl::{COMPILER_ID, Context, source_digest};

/// The shipped smoke kernel: `out[i] = in[i] * 2 + 1`.
const SMOKE_WGSL: &str = include_str!("../shaders/smoke.wgsl");
/// The workgroup width that shader declares.
const SMOKE_WIDTH: u32 = 64;

#[test]
fn smoke_kernel_runs_end_to_end_on_device() {
    let context = Context::new().expect("create compute context");
    let kernel = context
        .kernel(SMOKE_WGSL, "main", SMOKE_WIDTH)
        .expect("build kernel");

    // A count that is not a multiple of the workgroup size exercises the
    // in-shader bounds guard against the rounded-up workgroup count.
    let input: Vec<u32> = (0..1000u32).collect();
    let bytes: &[u8] = bytemuck::cast_slice(&input);
    let in_buffer = context.buffer(bytes.len()).expect("input buffer");
    let out_buffer = context.buffer(bytes.len()).expect("output buffer");
    context.upload(&in_buffer, bytes).expect("upload input");

    // The grid is sized by the width the kernel reports rather than a literal
    // repeated here, which is what the kernel build made possible.
    let groups = input.len().div_ceil(kernel.block_width() as usize) as u32;
    context
        .dispatch(&kernel, &[&in_buffer, &out_buffer], [groups, 1, 1])
        .expect("dispatch");

    let read_back = context.download(&out_buffer).expect("download output");
    let output: &[u32] = bytemuck::cast_slice(&read_back);
    let expected: Vec<u32> = input.iter().map(|&value| value * 2 + 1).collect();
    assert_eq!(output, expected.as_slice());

    // The identity inputs a domain records are stable across the run.
    assert_eq!(kernel.source_digest(), source_digest(SMOKE_WGSL));
    assert_eq!(kernel.compiler_id(), COMPILER_ID);
}
