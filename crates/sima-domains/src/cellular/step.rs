//! The double-buffered GPU dispatch harness that advances a [`Grid`].

use sima_core::Result;
use sima_toolkit_wgsl::{Buffer, Context, Kernel};

use crate::cellular::Grid;

/// Advances `initial` by `steps` double-buffered dispatches of `kernel`,
/// returning the resulting grid.
///
/// Each step dispatches `kernel` over the whole grid with one invocation per
/// cell, ping-ponging between two device buffers. The bindings follow the
/// cellular-kind convention, ascending to match the kernel's declared bindings
/// positionally:
///
/// - binding 0 the input grid, binding 1 the output grid,
/// - binding 2 the dimensions `[width, height, channels]`,
/// - bindings 3+ the `params` storage buffers in order (empty for a
///   parameterless kernel),
/// - then, when `step_base` is `Some`, the per-step index buffer as the last
///   binding.
///
/// The dimensions and params buffers are bound unchanged every step while
/// bindings 0 and 1 swap.
///
/// `step_base` transports the per-step index to the kernel. When `Some(base)`,
/// one two-word `u32` buffer is created, and before dispatch `i` the value
/// `base + i` is uploaded to it as `[lo, hi]` and bound last, so a kernel that
/// depends on the absolute step reads it as a `u64`. The dispatch's leading
/// transfer barrier orders each upload against the shader read. When `None`, no
/// step buffer is created or bound and the dispatch is byte-identical to a run
/// without one.
///
/// `steps == 0` returns a clone of `initial` without dispatching. The harness
/// is neighborhood-agnostic: a small stencil and a large-radius convolution
/// are both just the `kernel` argument.
pub fn run(
    context: &Context,
    kernel: &Kernel,
    initial: &Grid,
    steps: u32,
    params: &[&Buffer],
    step_base: Option<u64>,
) -> Result<Grid> {
    if steps == 0 {
        return Ok(initial.clone());
    }

    let width = initial.width();
    let height = initial.height();
    let channels = initial.channels();
    let payload = initial.data();
    let byte_len = std::mem::size_of_val(payload);

    // Two ping-pong buffers for the state and one fixed dimensions buffer.
    let a = context.buffer(byte_len)?;
    let b = context.buffer(byte_len)?;
    let dims_values = [width, height, channels];
    let dims = context.buffer(std::mem::size_of_val(&dims_values))?;
    context.upload(&dims, bytemuck::cast_slice(&dims_values))?;
    // A f32 -> u8 cast is alignment-safe, so the payload uploads zero-copy.
    context.upload(&a, bytemuck::cast_slice(payload))?;

    // The per-step index buffer, created once and re-uploaded each dispatch when
    // the model opts in. Two u32 words carry the step as a little-endian u64.
    let step_buffer = match step_base {
        Some(_) => Some(context.buffer(2 * std::mem::size_of::<u32>())?),
        None => None,
    };

    // One workgroup covers 64 cells along x; round the cell count up.
    let cell_count = width as u64 * height as u64;
    let groups = [cell_count.div_ceil(64) as u32, 1, 1];

    // `src` holds the current state, `dst` receives the next. Swapping after
    // each dispatch leaves `src` on the most recently written buffer for both
    // even and odd step counts.
    let mut src = &a;
    let mut dst = &b;
    for i in 0..steps {
        let mut bound: Vec<&Buffer> = Vec::with_capacity(3 + params.len() + 1);
        bound.push(src);
        bound.push(dst);
        bound.push(&dims);
        bound.extend_from_slice(params);
        if let (Some(base), Some(buffer)) = (step_base, step_buffer.as_ref()) {
            let step = base + u64::from(i);
            let words = [step as u32, (step >> 32) as u32];
            context.upload(buffer, bytemuck::cast_slice(&words))?;
            bound.push(buffer);
        }
        context.dispatch(kernel, &bound, groups)?;
        std::mem::swap(&mut src, &mut dst);
    }

    // Rebuild the payload from little-endian bytes four at a time: a u8 -> f32
    // cast of the unaligned download buffer would be unsound.
    let bytes = context.download(src)?;
    let data: Vec<f32> = bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect();
    Grid::new(width, height, channels, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The neighborhood-max kernel that exercises the path. Its bindings and
    /// dispatch match the cellular-kind convention the harness encodes.
    const SMOKE_WGSL: &str = include_str!("../../shaders/smoke.wgsl");

    /// A one-channel probe kernel that writes the low word of the per-step index
    /// into every cell. It declares the step buffer at binding 3 (no params), so
    /// after `steps` dispatches from `base` every cell holds `base + steps - 1`.
    const STEP_PROBE_WGSL: &str = r#"
@group(0) @binding(0) var<storage, read> in_grid: array<f32>;
@group(0) @binding(1) var<storage, read_write> out_grid: array<f32>;
@group(0) @binding(2) var<storage, read> dims: array<u32>;
@group(0) @binding(3) var<storage, read> step_words: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let cell = gid.x;
    if (cell >= dims[0] * dims[1]) { return; }
    // The test keeps the step well under 2^24, so the f32 holds it exactly.
    out_grid[cell] = f32(step_words[0]);
}
"#;

    /// Builds a context and compiles the smoke kernel, or panics with context.
    fn smoke(context: &Context) -> Kernel {
        context
            .kernel(SMOKE_WGSL, "main")
            .expect("build smoke kernel")
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn run_advances_one_step() {
        let context = Context::new().expect("create compute context");
        let kernel = smoke(&context);
        // 5x1x1: with height 1 the up/down neighbors alias the cell, so a step
        // is a toroidal 3-point max along x over [1, 4, 2, 5, 3]:
        //   x0=max(1,3,4)=4, x1=max(4,1,2)=4, x2=max(2,4,5)=5,
        //   x3=max(5,2,3)=5, x4=max(3,5,1)=5.
        let initial = Grid::new(5, 1, 1, vec![1.0, 4.0, 2.0, 5.0, 3.0]).expect("grid");
        let result = run(&context, &kernel, &initial, 1, &[], None).expect("run");
        assert_eq!(result.data(), &[4.0, 4.0, 5.0, 5.0, 5.0]);
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn run_reduces_each_channel_independently() {
        let context = Context::new().expect("create compute context");
        let kernel = smoke(&context);
        // 3x1x2, cell-major interleaved. Channel 0 along x is [1, 4, 7], so
        // every cell's toroidal 3-point max is 7. Channel 1 is [9, 2, 5], so
        // every cell's max is 9. The two channels never mix.
        let initial = Grid::new(3, 1, 2, vec![1.0, 9.0, 4.0, 2.0, 7.0, 5.0]).expect("grid");
        let result = run(&context, &kernel, &initial, 1, &[], None).expect("run");
        assert_eq!(result.data(), &[7.0, 9.0, 7.0, 9.0, 7.0, 9.0]);
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn run_composes_across_steps() {
        let context = Context::new().expect("create compute context");
        let kernel = smoke(&context);
        let initial = Grid::new(4, 3, 2, (0..24).map(|i| (i % 7) as f32).collect()).expect("grid");

        // Advancing k steps must equal advancing one step k times, for even and
        // odd k alike. A ping-pong that returns the wrong buffer for even step
        // counts would break this self-consistency.
        let two = run(&context, &kernel, &initial, 2, &[], None).expect("two");
        let one = run(&context, &kernel, &initial, 1, &[], None).expect("one");
        let one_then_one = run(&context, &kernel, &one, 1, &[], None).expect("one then one");
        assert_eq!(two.to_bytes(), one_then_one.to_bytes());

        let three = run(&context, &kernel, &initial, 3, &[], None).expect("three");
        let two_then_one = run(&context, &kernel, &two, 1, &[], None).expect("two then one");
        assert_eq!(three.to_bytes(), two_then_one.to_bytes());
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn run_zero_steps_returns_the_input() {
        let context = Context::new().expect("create compute context");
        let kernel = smoke(&context);
        let initial = Grid::new(3, 2, 2, (0..12).map(|i| i as f32).collect()).expect("grid");
        let result = run(&context, &kernel, &initial, 0, &[], None).expect("run");
        assert_eq!(result.to_bytes(), initial.to_bytes());
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn run_transports_the_per_step_index() {
        // A kernel reading the step buffer observes `base + i` on dispatch `i`.
        // After `steps` dispatches from `base`, the last dispatch wrote
        // `base + steps - 1` into every cell, and the ping-pong returns that
        // buffer.
        let context = Context::new().expect("create compute context");
        let kernel = context
            .kernel(STEP_PROBE_WGSL, "main")
            .expect("build step probe kernel");
        let initial = Grid::new(4, 4, 1, vec![0.0; 16]).expect("grid");
        let (base, steps) = (100u64, 5u32);
        let result = run(&context, &kernel, &initial, steps, &[], Some(base)).expect("run");
        let expected = (base + u64::from(steps) - 1) as f32;
        assert!(
            result.data().iter().all(|&v| v == expected),
            "every cell must hold base + steps - 1 = {expected}, got {:?}",
            result.data()
        );
    }
}
