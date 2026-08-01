//! The double-buffered GPU dispatch harness that advances a [`Grid`].

use sima_core::Result;
use sima_toolkit_wgsl::{Buffer, Context, Kernel};

use crate::substrates::cellular::Grid;

/// The result of a [`run`]: the two ping-pong buffers left resident on the
/// device — the final grid $G_N$ and the step before it $G_{N-1}$ — over the
/// context that produced them.
///
/// Downloading the final grid and reducing over the pair are separate
/// operations on this handle: a caller that only needs stats never pays for the
/// full grid readback, and the reduction reads the two buffers in place.
pub struct Trajectory<'a> {
    context: &'a Context,
    /// The most recently written buffer: the final grid $G_N$.
    current: Buffer,
    /// The buffer written the step before: $G_{N-1}$. Equal in content to
    /// `current` when the run took no steps.
    previous: Buffer,
    width: u32,
    height: u32,
    channels: u32,
}

impl<'a> Trajectory<'a> {
    /// The final grid buffer $G_N$, resident on the device.
    pub fn current(&self) -> &Buffer {
        &self.current
    }

    /// The grid one step before the final, $G_{N-1}$, resident on the device.
    pub fn previous(&self) -> &Buffer {
        &self.previous
    }

    /// The cell count of the grid: `width * height`.
    pub fn cell_count(&self) -> u32 {
        self.width * self.height
    }

    /// The channel count of the grid.
    pub fn channels(&self) -> u32 {
        self.channels
    }

    /// Downloads the final grid and rebuilds it into a [`Grid`].
    pub fn grid(&self) -> Result<Grid> {
        // Rebuild the payload from little-endian bytes four at a time: a u8 ->
        // f32 cast of the unaligned download buffer would be unsound.
        let bytes = self.context.download(&self.current)?;
        let data: Vec<f32> = bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();
        Grid::new(self.width, self.height, self.channels, data)
    }
}

/// Advances `initial` by `steps` double-buffered dispatches of `kernel`,
/// returning a [`Trajectory`] over the two buffers left resident.
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
/// `steps == 0` dispatches nothing and leaves both buffers holding `initial`,
/// so a reduction over the pair reports no activity. The harness is
/// neighborhood-agnostic: a small stencil and a large-radius convolution are
/// both just the `kernel` argument.
pub fn run<'a>(
    context: &'a Context,
    kernel: &Kernel,
    initial: &Grid,
    steps: u32,
    params: &[&Buffer],
    step_base: Option<u64>,
) -> Result<Trajectory<'a>> {
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
    // Both buffers start on `initial` so a zero-step run leaves the pair equal.
    context.upload(&a, bytemuck::cast_slice(payload))?;
    context.upload(&b, bytemuck::cast_slice(payload))?;

    // The per-step index buffer, created once and re-uploaded each dispatch when
    // the model opts in. Two u32 words carry the step as a little-endian u64.
    let step_buffer = match step_base {
        Some(_) => Some(context.buffer(2 * std::mem::size_of::<u32>())?),
        None => None,
    };

    // One workgroup covers 64 cells along x; round the cell count up.
    let cell_count = width as u64 * height as u64;
    let groups = [cell_count.div_ceil(64) as u32, 1, 1];

    // `current_is_a` tracks which buffer holds the latest state. Each dispatch
    // reads the current buffer and writes the other; after the swap the written
    // buffer becomes current. This leaves `current` on the most recently
    // written buffer and `previous` on the one before it, for even and odd step
    // counts alike.
    let mut current_is_a = true;
    for i in 0..steps {
        let (src, dst) = if current_is_a { (&a, &b) } else { (&b, &a) };
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
        current_is_a = !current_is_a;
    }

    let (current, previous) = if current_is_a { (a, b) } else { (b, a) };
    Ok(Trajectory {
        context,
        current,
        previous,
        width,
        height,
        channels,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The neighborhood-max kernel that exercises the path. Its bindings and
    /// dispatch match the cellular-kind convention the harness encodes.
    const SMOKE_WGSL: &str = include_str!("../../../shaders/smoke.wgsl");

    /// A one-channel probe kernel that adds the low word of the per-step index to
    /// every cell. It declares the step buffer at binding 3 (no params), so after
    /// `steps` dispatches from `base` every cell holds the sum of the step
    /// indices `base ..= base + steps - 1`. Accumulating makes every dispatch's
    /// upload contribute, so a wrong intermediate step index is caught, not only
    /// a wrong final one.
    const STEP_PROBE_WGSL: &str = r#"
@group(0) @binding(0) var<storage, read> in_grid: array<f32>;
@group(0) @binding(1) var<storage, read_write> out_grid: array<f32>;
@group(0) @binding(2) var<storage, read> dims: array<u32>;
@group(0) @binding(3) var<storage, read> step_words: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let cell = gid.x;
    if (cell >= dims[0] * dims[1]) { return; }
    // The test keeps the running sum well under 2^24, so the f32 holds it exactly.
    out_grid[cell] = in_grid[cell] + f32(step_words[0]);
}
"#;

    /// Builds a context and compiles the smoke kernel, or panics with context.
    fn smoke(context: &Context) -> Kernel {
        context
            .kernel(SMOKE_WGSL, "main")
            .expect("build smoke kernel")
    }

    /// Stepping a grid dispatches the kernel, which needs a real Vulkan device.
    mod on_device {
        use super::*;

        #[test]
        fn run_advances_one_step() {
            let context = Context::new().expect("create compute context");
            let kernel = smoke(&context);
            // 5x1x1: with height 1 the up/down neighbors alias the cell, so a step
            // is a toroidal 3-point max along x over [1, 4, 2, 5, 3]:
            //   x0=max(1,3,4)=4, x1=max(4,1,2)=4, x2=max(2,4,5)=5,
            //   x3=max(5,2,3)=5, x4=max(3,5,1)=5.
            let initial = Grid::new(5, 1, 1, vec![1.0, 4.0, 2.0, 5.0, 3.0]).expect("grid");
            let result = run(&context, &kernel, &initial, 1, &[], None)
                .expect("run")
                .grid()
                .expect("grid");
            assert_eq!(result.data(), &[4.0, 4.0, 5.0, 5.0, 5.0]);
        }

        #[test]
        fn run_reduces_each_channel_independently() {
            let context = Context::new().expect("create compute context");
            let kernel = smoke(&context);
            // 3x1x2, cell-major interleaved. Channel 0 along x is [1, 4, 7], so
            // every cell's toroidal 3-point max is 7. Channel 1 is [9, 2, 5], so
            // every cell's max is 9. The two channels never mix.
            let initial = Grid::new(3, 1, 2, vec![1.0, 9.0, 4.0, 2.0, 7.0, 5.0]).expect("grid");
            let result = run(&context, &kernel, &initial, 1, &[], None)
                .expect("run")
                .grid()
                .expect("grid");
            assert_eq!(result.data(), &[7.0, 9.0, 7.0, 9.0, 7.0, 9.0]);
        }

        #[test]
        fn run_composes_across_steps() {
            let context = Context::new().expect("create compute context");
            let kernel = smoke(&context);
            let initial =
                Grid::new(4, 3, 2, (0..24).map(|i| (i % 7) as f32).collect()).expect("grid");

            // Advancing k steps must equal advancing one step k times, for even and
            // odd k alike. A ping-pong that returns the wrong buffer for even step
            // counts would break this self-consistency.
            let two = run(&context, &kernel, &initial, 2, &[], None)
                .expect("two")
                .grid()
                .expect("grid");
            let one = run(&context, &kernel, &initial, 1, &[], None)
                .expect("one")
                .grid()
                .expect("grid");
            let one_then_one = run(&context, &kernel, &one, 1, &[], None)
                .expect("one then one")
                .grid()
                .expect("grid");
            assert_eq!(two.to_bytes(), one_then_one.to_bytes());

            let three = run(&context, &kernel, &initial, 3, &[], None)
                .expect("three")
                .grid()
                .expect("grid");
            let two_then_one = run(&context, &kernel, &two, 1, &[], None)
                .expect("two then one")
                .grid()
                .expect("grid");
            assert_eq!(three.to_bytes(), two_then_one.to_bytes());
        }

        #[test]
        fn run_zero_steps_returns_the_input() {
            let context = Context::new().expect("create compute context");
            let kernel = smoke(&context);
            let initial = Grid::new(3, 2, 2, (0..12).map(|i| i as f32).collect()).expect("grid");
            let result = run(&context, &kernel, &initial, 0, &[], None)
                .expect("run")
                .grid()
                .expect("grid");
            assert_eq!(result.to_bytes(), initial.to_bytes());
        }

        #[test]
        fn run_transports_the_per_step_index() {
            // A kernel reading the step buffer observes `base + i` on dispatch `i`.
            // The probe accumulates, so after `steps` dispatches from `base` every
            // cell holds the sum of `base ..= base + steps - 1`, and a wrong index on
            // any single dispatch — not only the last — changes the total.
            let context = Context::new().expect("create compute context");
            let kernel = context
                .kernel(STEP_PROBE_WGSL, "main")
                .expect("build step probe kernel");
            let initial = Grid::new(4, 4, 1, vec![0.0; 16]).expect("grid");
            let (base, steps) = (100u64, 5u32);
            let result = run(&context, &kernel, &initial, steps, &[], Some(base))
                .expect("run")
                .grid()
                .expect("grid");
            // Sum of base ..= base + steps - 1 = 100 + 101 + 102 + 103 + 104 = 510.
            let expected = (base..base + u64::from(steps)).sum::<u64>() as f32;
            assert!(
                result.data().iter().all(|&v| v == expected),
                "every cell must hold the summed step indices {expected}, got {:?}",
                result.data()
            );
        }
    }
}
