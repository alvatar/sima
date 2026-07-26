//! The double-buffered CUDA dispatch harness that advances a [`Grid`].
//!
//! The CUDA counterpart of [`cellular::step`](crate::cellular::step), dispatch
//! for dispatch: the same ping-pong over two device buffers, the same argument
//! order, and the same per-step index transport.

use sima_core::Result;
use sima_toolkit_cuda::{Buffer, Context, Kernel};

use crate::cellular::Grid;

/// The threads per block every cellular kernel is launched with, matching the
/// WGSL side's `@workgroup_size(64)`. CUDA takes the block dimensions at launch
/// rather than from the compiled module, so the kernel declares the same width
/// with `__launch_bounds__(64)` and the toolkit checks the two agree.
pub(crate) const BLOCK_WIDTH: u32 = 64;

/// The result of a [`run`]: the two ping-pong buffers left resident on the
/// device — the final grid $G_N$ and the step before it $G_{N-1}$ — over the
/// context that produced them.
///
/// Downloading the final grid and reducing over the pair are separate
/// operations on this handle: a caller that only needs stats never pays for the
/// full grid readback, and the reduction reads the two buffers in place.
pub(crate) struct Trajectory<'a> {
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

impl Trajectory<'_> {
    /// The final grid buffer $G_N$, resident on the device.
    pub(crate) fn current(&self) -> &Buffer {
        &self.current
    }

    /// The grid one step before the final, $G_{N-1}$, resident on the device.
    pub(crate) fn previous(&self) -> &Buffer {
        &self.previous
    }

    /// The cell count of the grid: `width * height`.
    pub(crate) fn cell_count(&self) -> u32 {
        self.width * self.height
    }

    /// The channel count of the grid.
    pub(crate) fn channels(&self) -> u32 {
        self.channels
    }

    /// Downloads the final grid and rebuilds it into a [`Grid`].
    pub(crate) fn grid(&self) -> Result<Grid> {
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
/// Each step dispatches `kernel` over the whole grid with one thread per cell,
/// ping-ponging between two device buffers. The kernel's parameters follow the
/// cellular-kind convention, in the order they are declared:
///
/// - parameter 0 the input grid, parameter 1 the output grid,
/// - parameter 2 the dimensions `[width, height, channels]`,
/// - parameters 3+ the `params` buffers in order (empty for a parameterless
///   kernel),
/// - then, when `step_base` is `Some`, the per-step index buffer last.
///
/// The dimensions and params buffers are passed unchanged every step while
/// parameters 0 and 1 swap.
///
/// `step_base` transports the per-step index to the kernel. When `Some(base)`,
/// one two-word `u32` buffer is created, and before dispatch `i` the value
/// `base + i` is uploaded to it and passed last, so a kernel that depends on the
/// absolute step reads it as a `u64`. The upload runs on the same stream as the
/// launches, which orders it against the kernel's read. When `None`, no step
/// buffer is created or passed and the launch is identical to a run without one.
///
/// `steps == 0` dispatches nothing and leaves both buffers holding `initial`,
/// so a reduction over the pair reports no activity.
pub(crate) fn run<'a>(
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
    let mut a = context.buffer(byte_len)?;
    let mut b = context.buffer(byte_len)?;
    let dims_values = [width, height, channels];
    let mut dims = context.buffer(std::mem::size_of_val(&dims_values))?;
    context.upload(&mut dims, bytemuck::cast_slice(&dims_values))?;
    // A f32 -> u8 cast is alignment-safe, so the payload uploads zero-copy.
    // Both buffers start on `initial` so a zero-step run leaves the pair equal.
    context.upload(&mut a, bytemuck::cast_slice(payload))?;
    context.upload(&mut b, bytemuck::cast_slice(payload))?;

    // The per-step index buffer, created once and re-uploaded each dispatch when
    // the model opts in. Two u32 words carry the step as a little-endian u64.
    let mut step_buffer = match step_base {
        Some(_) => Some(context.buffer(2 * std::mem::size_of::<u32>())?),
        None => None,
    };

    // One block covers 64 cells along x; round the cell count up.
    let cell_count = width as u64 * height as u64;
    let groups = [cell_count.div_ceil(u64::from(BLOCK_WIDTH)) as u32, 1, 1];

    // `current_is_a` tracks which buffer holds the latest state. Each dispatch
    // reads the current buffer and writes the other; after the swap the written
    // buffer becomes current. This leaves `current` on the most recently
    // written buffer and `previous` on the one before it, for even and odd step
    // counts alike.
    let mut current_is_a = true;
    for i in 0..steps {
        // The step upload borrows the buffer mutably, so it happens before the
        // argument list is built rather than inside it.
        if let (Some(base), Some(buffer)) = (step_base, step_buffer.as_mut()) {
            let step = base + u64::from(i);
            let words = [step as u32, (step >> 32) as u32];
            context.upload(buffer, bytemuck::cast_slice(&words))?;
        }
        let (src, dst) = if current_is_a { (&a, &b) } else { (&b, &a) };
        let mut bound: Vec<&Buffer> = Vec::with_capacity(3 + params.len() + 1);
        bound.push(src);
        bound.push(dst);
        bound.push(&dims);
        bound.extend_from_slice(params);
        if let Some(buffer) = step_buffer.as_ref() {
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

    /// The smoke kernel that exercises the path: a toroidal neighborhood max,
    /// whose parameters and dispatch match the cellular-kind convention the
    /// harness encodes. The CUDA transcription of `shaders/smoke.wgsl`, loaded
    /// from committed PTX exactly as a model's kernel is.
    const SMOKE_PTX: &str = include_str!("../../../kernels/smoke.ptx");

    /// Requires `libnvrtc`.
    #[test]
    fn the_committed_ptx_reproduces_from_its_source() {
        assert_eq!(
            sima_toolkit_cuda::compile(include_str!("../../../kernels/smoke.cu"))
                .expect("compile the smoke kernel"),
            SMOKE_PTX
        );
    }

    /// Requires a CUDA device.
    #[test]
    fn run_advances_one_step() {
        let context = Context::new().expect("create compute context");
        let kernel = context
            .kernel(SMOKE_PTX, "main_kernel", BLOCK_WIDTH)
            .expect("build smoke kernel");
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

    /// Requires a CUDA device.
    #[test]
    fn run_composes_across_steps() {
        // Advancing k steps must equal advancing one step k times, for even and
        // odd k alike. A ping-pong that returns the wrong buffer for even step
        // counts would break this self-consistency.
        let context = Context::new().expect("create compute context");
        let kernel = context
            .kernel(SMOKE_PTX, "main_kernel", BLOCK_WIDTH)
            .expect("build smoke kernel");
        let initial = Grid::new(4, 3, 2, (0..24).map(|i| (i % 7) as f32).collect()).expect("grid");
        let advance = |grid: &Grid, steps: u32| {
            run(&context, &kernel, grid, steps, &[], None)
                .expect("run")
                .grid()
                .expect("grid")
        };
        let one = advance(&initial, 1);
        let two = advance(&initial, 2);
        assert_eq!(two.to_bytes(), advance(&one, 1).to_bytes());
        let three = advance(&initial, 3);
        assert_eq!(three.to_bytes(), advance(&two, 1).to_bytes());
    }

    /// Requires a CUDA device.
    #[test]
    fn run_zero_steps_returns_the_input() {
        let context = Context::new().expect("create compute context");
        let kernel = context
            .kernel(SMOKE_PTX, "main_kernel", BLOCK_WIDTH)
            .expect("build smoke kernel");
        let initial = Grid::new(3, 2, 2, (0..12).map(|i| i as f32).collect()).expect("grid");
        let result = run(&context, &kernel, &initial, 0, &[], None)
            .expect("run")
            .grid()
            .expect("grid");
        assert_eq!(result.to_bytes(), initial.to_bytes());
    }
}
