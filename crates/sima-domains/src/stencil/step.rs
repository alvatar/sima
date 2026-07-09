//! The double-buffered GPU dispatch harness that advances a [`Grid`].

use sima_core::Result;
use sima_toolkit_wgsl::{Buffer, Context, Kernel};

use crate::stencil::Grid;

/// Advances `initial` by `steps` double-buffered dispatches of `kernel`,
/// returning the resulting grid.
///
/// Each step dispatches `kernel` over the whole grid with one invocation per
/// cell, ping-ponging between two device buffers. The bindings follow the
/// stencil-kind convention: binding 0 the input grid, binding 1 the output
/// grid, binding 2 the dimensions `[width, height, channels]`, and bindings
/// 3+ the `params` storage buffers in order (empty for a parameterless
/// kernel). The dimensions and params buffers are bound unchanged every step
/// while bindings 0 and 1 swap.
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

    // One workgroup covers 64 cells along x; round the cell count up.
    let cell_count = width as u64 * height as u64;
    let groups = [cell_count.div_ceil(64) as u32, 1, 1];

    // `src` holds the current state, `dst` receives the next. Swapping after
    // each dispatch leaves `src` on the most recently written buffer for both
    // even and odd step counts.
    let mut src = &a;
    let mut dst = &b;
    for _ in 0..steps {
        let mut bound: Vec<&Buffer> = Vec::with_capacity(3 + params.len());
        bound.push(src);
        bound.push(dst);
        bound.push(&dims);
        bound.extend_from_slice(params);
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
    /// dispatch match the stencil-kind convention the harness encodes.
    const SMOKE_WGSL: &str = include_str!("../shaders/smoke.wgsl");

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
        let result = run(&context, &kernel, &initial, 1, &[]).expect("run");
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
        let result = run(&context, &kernel, &initial, 1, &[]).expect("run");
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
        let two = run(&context, &kernel, &initial, 2, &[]).expect("two");
        let one = run(&context, &kernel, &initial, 1, &[]).expect("one");
        let one_then_one = run(&context, &kernel, &one, 1, &[]).expect("one then one");
        assert_eq!(two.to_bytes(), one_then_one.to_bytes());

        let three = run(&context, &kernel, &initial, 3, &[]).expect("three");
        let two_then_one = run(&context, &kernel, &two, 1, &[]).expect("two then one");
        assert_eq!(three.to_bytes(), two_then_one.to_bytes());
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn run_zero_steps_returns_the_input() {
        let context = Context::new().expect("create compute context");
        let kernel = smoke(&context);
        let initial = Grid::new(3, 2, 2, (0..12).map(|i| i as f32).collect()).expect("grid");
        let result = run(&context, &kernel, &initial, 0, &[]).expect("run");
        assert_eq!(result.to_bytes(), initial.to_bytes());
    }
}
