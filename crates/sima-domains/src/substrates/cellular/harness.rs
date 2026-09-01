//! The double-buffered dispatch harness that advances a [`Grid`], written once
//! over the [`CellularOps`] boundary and monomorphized per backend.

use sima_core::{Error, Result};

use crate::substrates::cellular::ops::CellularOps;
use crate::substrates::cellular::{BLOCK_WIDTH, Grid};

/// The result of a [`search`]: the two ping-pong buffers left resident on the
/// device — the final grid $G_N$ and the step before it $G_{N-1}$ — over the
/// backend that produced them.
///
/// Downloading the final grid and reducing over the pair are separate
/// operations on this handle: a caller that only needs stats never pays for the
/// full grid readback, and the reduction reads the two buffers in place.
pub(crate) struct Trajectory<'a, O: CellularOps> {
    ops: &'a O,
    /// The most recently written buffer: the final grid $G_N$.
    current: O::Buffer,
    /// The buffer written the step before: $G_{N-1}$. Equal in content to
    /// `current` when the search took no steps.
    previous: O::Buffer,
    width: u32,
    height: u32,
    channels: u32,
    /// `width * height`, taken from the grid that was advanced rather than
    /// recomputed: [`Grid::new`] already refused an extent whose product does
    /// not fit a `u32`.
    cell_count: u32,
}

impl<O: CellularOps> Trajectory<'_, O> {
    /// The final grid buffer $G_N$, resident on the device.
    pub(crate) fn current(&self) -> &O::Buffer {
        &self.current
    }

    /// The grid one step before the final, $G_{N-1}$, resident on the device.
    pub(crate) fn previous(&self) -> &O::Buffer {
        &self.previous
    }

    /// The cell count of the grid: `width * height`.
    pub(crate) fn cell_count(&self) -> u32 {
        self.cell_count
    }

    /// The channel count of the grid.
    pub(crate) fn channels(&self) -> u32 {
        self.channels
    }

    /// Downloads the final grid and rebuilds it into a [`Grid`].
    pub(crate) fn grid(&self) -> Result<Grid> {
        // Rebuild the payload from little-endian bytes four at a time: a u8 ->
        // f32 cast of the unaligned download buffer would be unsound. The
        // buffer was sized at four bytes per f32, so the remainder is empty and
        // the chunks are the whole of it.
        let bytes = self.ops.download(&self.current)?;
        let data: Vec<f32> = bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|chunk| f32::from_le_bytes(*chunk))
            .collect();
        Grid::new(self.width, self.height, self.channels, data)
    }
}

/// Advances `initial` by `steps` double-buffered dispatches of `kernel`,
/// returning a [`Trajectory`] over the two buffers left resident.
///
/// Each step dispatches `kernel` over the whole grid with one invocation per
/// cell, ping-ponging between two device buffers. The bindings follow the
/// cellular-kind convention, in the order the kernel declares them:
///
/// - binding 0 the input grid, binding 1 the output grid,
/// - binding 2 the dimensions `[width, height, channels]`,
/// - bindings 3+ the `params` buffers in order (empty for a parameterless
///   kernel),
/// - then, when `step_base` is `Some`, the per-step index buffer last.
///
/// The dimensions and params buffers are bound unchanged every step while
/// bindings 0 and 1 swap.
///
/// `step_base` transports the per-step index to the kernel. When `Some(base)`,
/// one two-word `u32` buffer is created, and dispatch `i` carries the value
/// `base + i` into it as `[lo, hi]` inside its own submission, bound last, so a
/// kernel that depends on the absolute step reads it as a `u64`. When `None`,
/// no step buffer is created or bound and the dispatch is byte-identical to a
/// search without one.
///
/// `steps == 0` dispatches nothing and leaves both buffers holding `initial`,
/// so a reduction over the pair reports no activity. The harness is
/// neighborhood-agnostic: a small stencil and a large-radius convolution are
/// both just the `kernel` argument.
pub(crate) fn search<'a, O: CellularOps>(
    ops: &'a O,
    kernel: &O::Kernel,
    initial: &Grid,
    steps: u32,
    params: &[&O::Buffer],
    step_base: Option<u64>,
) -> Result<Trajectory<'a, O>> {
    let width = initial.width();
    let height = initial.height();
    let channels = initial.channels();
    let cell_count = initial.cell_count();
    let payload = initial.data();
    let byte_len = std::mem::size_of_val(payload);

    // The grid the kernel will cover, refused here if this device cannot launch
    // enough groups for it — before anything is allocated.
    let groups = group_count(initial, BLOCK_WIDTH, ops.max_groups_x()?)?;

    // Two ping-pong buffers for the state and one fixed dimensions buffer.
    let mut a = ops.buffer(byte_len)?;
    let mut b = ops.buffer(byte_len)?;
    let dims_values = [width, height, channels];
    let mut dims = ops.buffer(std::mem::size_of_val(&dims_values))?;
    ops.upload(&mut dims, bytemuck::cast_slice(&dims_values))?;
    // A f32 -> u8 cast is alignment-safe, so the payload uploads zero-copy.
    // Both buffers start on `initial` so a zero-step search leaves the pair equal.
    ops.upload(&mut a, bytemuck::cast_slice(payload))?;
    ops.upload(&mut b, bytemuck::cast_slice(payload))?;

    // The per-step index buffer, created once and rewritten by each dispatch
    // when the model opts in. Two u32 words carry the step as a little-endian
    // u64.
    let mut step_buffer = match step_base {
        Some(_) => Some(ops.buffer(2 * std::mem::size_of::<u32>())?),
        None => None,
    };

    // `current_is_a` tracks which buffer holds the latest state. Each dispatch
    // reads the current buffer and writes the other; after the swap the written
    // buffer becomes current. This leaves `current` on the most recently
    // written buffer and `previous` on the one before it, for even and odd step
    // counts alike.
    let mut current_is_a = true;
    for i in 0..steps {
        let (src, dst) = if current_is_a { (&a, &b) } else { (&b, &a) };
        let mut bound: Vec<&O::Buffer> = Vec::with_capacity(3 + params.len());
        bound.push(src);
        bound.push(dst);
        bound.push(&dims);
        bound.extend_from_slice(params);
        match (step_base, step_buffer.as_mut()) {
            (Some(base), Some(buffer)) => {
                let step = base + u64::from(i);
                let words = [step as u32, (step >> 32) as u32];
                ops.dispatch_with_update(
                    kernel,
                    &bound,
                    buffer,
                    bytemuck::cast_slice(&words),
                    groups,
                )?;
            }
            _ => ops.dispatch(kernel, &bound, groups)?,
        }
        current_is_a = !current_is_a;
    }

    let (current, previous) = if current_is_a { (a, b) } else { (b, a) };
    Ok(Trajectory {
        ops,
        current,
        previous,
        width,
        height,
        channels,
        cell_count,
    })
}

/// The one-dimensional group count covering `grid` at `block_width`, refused
/// when it exceeds what the device launches.
///
/// The dispatch is one-dimensional, so every cell rides the x axis and a large
/// grid searches into the axis limit long before it searches out of memory: Vulkan
/// guarantees only 65535 groups there, which a 2048x2048 grid already exceeds.
/// Spreading the excess onto y would change how a kernel derives a cell index
/// from its invocation, and with it every task key, so the limit is reported
/// rather than worked around.
fn group_count(grid: &Grid, block_width: u32, limit: u32) -> Result<[u32; 3]> {
    let groups = grid.cell_count().div_ceil(block_width);
    if groups > limit {
        return Err(Error::Validation(format!(
            "a {}x{} grid needs {groups} groups of {block_width}; this device launches {limit}",
            grid.width(),
            grid.height()
        )));
    }
    Ok([groups, 1, 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_grid_is_covered_by_groups_rounded_up() {
        // A cell count that is not a multiple of the width takes one more
        // group, whose surplus invocations fall out on the kernel's own bounds
        // guard.
        let grid = |w, h| Grid::new(w, h, 1, vec![0.0; (w * h) as usize]).expect("grid");
        assert_eq!(
            group_count(&grid(64, 1), 64, 65535).expect("exactly one group"),
            [1, 1, 1]
        );
        assert_eq!(
            group_count(&grid(65, 1), 64, 65535).expect("one group and a remainder"),
            [2, 1, 1]
        );
    }

    #[test]
    fn a_grid_past_the_devices_group_limit_is_refused() {
        // The live case: at 64 cells per group a 2048x2048 grid needs 65536,
        // one past what Vulkan guarantees. Unchecked, the driver refuses the
        // dispatch and the failure names nothing a caller can act on.
        let grid = Grid::new(2048, 2048, 1, vec![0.0; 2048 * 2048]).expect("grid");
        let error = group_count(&grid, 64, 65535).expect_err("past the limit");
        let Error::Validation(message) = error else {
            panic!("expected a validation error");
        };
        assert!(message.contains("2048x2048"), "names the grid: {message}");
        assert!(message.contains("65536"), "names the count: {message}");
        assert!(message.contains("65535"), "names the limit: {message}");
    }

    #[test]
    fn the_limit_is_the_devices_own() {
        // A device reporting more launches more: the check reads the figure
        // rather than assuming the guaranteed minimum, so a card that can cover
        // a large grid is not refused one.
        let grid = Grid::new(2048, 2048, 1, vec![0.0; 2048 * 2048]).expect("grid");
        assert_eq!(
            group_count(&grid, 64, 1 << 31).expect("a device that launches more"),
            [65536, 1, 1]
        );
    }

    /// Stepping a grid dispatches a kernel, which needs a real device. The
    /// harness is one implementation, so each case is written once over the ops
    /// boundary and search against both backends.
    mod on_device {
        use super::*;
        use crate::substrates::cellular::cuda::CudaOps;
        use crate::substrates::cellular::reference::{
            SMOKE_PTX, SMOKE_WGSL, STEP_PROBE_PTX, STEP_PROBE_WGSL, SmokeMax, advance,
        };
        use crate::substrates::cellular::wgsl::WgslOps;

        /// Runs `case` against every backend, with that backend's smoke kernel
        /// already built.
        fn on_each_backend(case: impl Fn(&dyn Fn(&Grid, u32) -> Grid)) {
            let wgsl = WgslOps::open(None).expect("open a Vulkan device");
            let wgsl_kernel = wgsl
                .kernel(SMOKE_WGSL, WgslOps::ENTRY, BLOCK_WIDTH)
                .expect("build the smoke shader");
            case(&|initial, steps| {
                search(&wgsl, &wgsl_kernel, initial, steps, &[], None)
                    .expect("WGSL search")
                    .grid()
                    .expect("WGSL grid")
            });

            let cuda = CudaOps::open(None).expect("open a CUDA device");
            let cuda_kernel = cuda
                .kernel(SMOKE_PTX, CudaOps::ENTRY, BLOCK_WIDTH)
                .expect("load the smoke PTX");
            case(&|initial, steps| {
                search(&cuda, &cuda_kernel, initial, steps, &[], None)
                    .expect("CUDA search")
                    .grid()
                    .expect("CUDA grid")
            });
        }

        #[test]
        fn run_advances_one_step() {
            on_each_backend(|advance_on_device| {
                // 5x1x1: with height 1 the up/down neighbors alias the cell, so
                // a step is a toroidal 3-point max along x over [1, 4, 2, 5, 3]:
                //   x0=max(1,3,4)=4, x1=max(4,1,2)=4, x2=max(2,4,5)=5,
                //   x3=max(5,2,3)=5, x4=max(3,5,1)=5.
                let initial = Grid::new(5, 1, 1, vec![1.0, 4.0, 2.0, 5.0, 3.0]).expect("grid");
                assert_eq!(
                    advance_on_device(&initial, 1).data(),
                    &[4.0, 4.0, 5.0, 5.0, 5.0]
                );
            });
        }

        #[test]
        fn run_reduces_each_channel_independently() {
            on_each_backend(|advance_on_device| {
                // 3x1x2, cell-major interleaved. Channel 0 along x is [1, 4, 7],
                // so every cell's toroidal 3-point max is 7. Channel 1 is
                // [9, 2, 5], so every cell's max is 9. The two never mix.
                let initial = Grid::new(3, 1, 2, vec![1.0, 9.0, 4.0, 2.0, 7.0, 5.0]).expect("grid");
                assert_eq!(
                    advance_on_device(&initial, 1).data(),
                    &[7.0, 9.0, 7.0, 9.0, 7.0, 9.0]
                );
            });
        }

        #[test]
        fn run_composes_across_steps() {
            on_each_backend(|advance_on_device| {
                // Advancing k steps must equal advancing one step k times, for
                // even and odd k alike. A ping-pong that returns the wrong
                // buffer for even step counts would break this self-consistency.
                let initial =
                    Grid::new(4, 3, 2, (0..24).map(|i| (i % 7) as f32).collect()).expect("grid");
                let one = advance_on_device(&initial, 1);
                let two = advance_on_device(&initial, 2);
                assert_eq!(two.to_bytes(), advance_on_device(&one, 1).to_bytes());
                let three = advance_on_device(&initial, 3);
                assert_eq!(three.to_bytes(), advance_on_device(&two, 1).to_bytes());
            });
        }

        #[test]
        fn run_zero_steps_returns_the_input() {
            on_each_backend(|advance_on_device| {
                let initial =
                    Grid::new(3, 2, 2, (0..12).map(|i| i as f32).collect()).expect("grid");
                assert_eq!(
                    advance_on_device(&initial, 0).to_bytes(),
                    initial.to_bytes()
                );
            });
        }

        #[test]
        fn the_harness_matches_an_independent_cpu_reference() {
            // The strongest check the harness has: an independent computation
            // of the same steps. A misordered ping-pong or a group count
            // covering the wrong cells produces plausible numbers, and only a
            // byte-exact comparison against a separate implementation catches
            // it. The neighborhood max needs no tolerance, so the comparison is
            // exact on either backend.
            on_each_backend(|advance_on_device| {
                let (width, height, channels) = (8u32, 6u32, 3u32);
                let count = (width * height * channels) as usize;
                let data: Vec<f32> = (0..count).map(|i| ((i * 37) % 101) as f32).collect();
                let initial = Grid::new(width, height, channels, data).expect("grid");
                assert_eq!(
                    advance_on_device(&initial, 5).to_bytes(),
                    advance(&SmokeMax, &initial, 5).to_bytes(),
                    "the harness and the CPU reference disagree after five steps"
                );
            });
        }

        /// The step probe's expectation and its check, over whichever backend
        /// advanced the grid.
        fn assert_step_indices_transported(advanced: &Grid, base: u64, steps: u32, backend: &str) {
            // Sum of base ..= base + steps - 1.
            let expected = (base..base + u64::from(steps)).sum::<u64>() as f32;
            assert!(
                advanced.data().iter().all(|&v| v == expected),
                "{backend}: every cell holds the summed step indices {expected}, got {:?}",
                advanced.data()
            );
        }

        #[test]
        fn run_transports_the_per_step_index() {
            // A kernel reading the step buffer observes `base + i` on dispatch
            // `i`. The probe accumulates, so a wrong index on any single
            // dispatch — not only the last — changes the total, which is what
            // pins the update riding inside each dispatch's own submission.
            // Both backends carry the update their own way, so both are asked.
            let initial = Grid::new(4, 4, 1, vec![0.0; 16]).expect("grid");
            let (base, steps) = (100u64, 5u32);

            let wgsl = WgslOps::open(None).expect("open a Vulkan device");
            let wgsl_kernel = wgsl
                .kernel(STEP_PROBE_WGSL, WgslOps::ENTRY, BLOCK_WIDTH)
                .expect("build the step probe shader");
            let advanced = search(&wgsl, &wgsl_kernel, &initial, steps, &[], Some(base))
                .expect("WGSL search")
                .grid()
                .expect("WGSL grid");
            assert_step_indices_transported(&advanced, base, steps, "WGSL");

            let cuda = CudaOps::open(None).expect("open a CUDA device");
            let cuda_kernel = cuda
                .kernel(STEP_PROBE_PTX, CudaOps::ENTRY, BLOCK_WIDTH)
                .expect("load the step probe PTX");
            let advanced = search(&cuda, &cuda_kernel, &initial, steps, &[], Some(base))
                .expect("CUDA search")
                .grid()
                .expect("CUDA grid");
            assert_step_indices_transported(&advanced, base, steps, "CUDA");
        }
    }
}
