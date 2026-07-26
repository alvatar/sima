//! The CUDA reduction of a final grid pair into the per-candidate stat scalars.
//!
//! The CUDA counterpart of [`cellular::reduce`](crate::shared::cellular::reduce), pass
//! for pass: the same four entry points in the same order over the same fixed
//! partition topology, so both substrates fold every sum identically. The
//! scalars are named by the shared
//! [`scalar_names`](crate::shared::cellular::scalar_names) — the naming authority is
//! one copy for both substrates.
//!
//! The kernel ships as [`REDUCE_PTX`], compiled once from `kernels/reduce.cu`
//! and committed beside it. Its digest, not the CUDA C source's, is what enters
//! the environment: the PTX is the artifact a worker loads.

use sima_core::{Error, Result};
use sima_toolkit_cuda::{Buffer, Context, Kernel};

use crate::shared::cellular::cuda::step::BLOCK_WIDTH;
use crate::shared::cellular::{MAX_CHANNELS, PARTITIONS, name_scalars};

/// The committed PTX of the reduction kernel. Its digest joins the environment
/// because the reduction's output gates committed bytes, so regenerating it
/// must change task keys exactly as editing a step kernel does.
pub(crate) const REDUCE_PTX: &str = include_str!("kernels/reduce.ptx");

/// The four compiled reduction passes, built once and held for the engine's
/// lifetime alongside the step kernel.
pub(crate) struct ReduceKernels {
    pass1: Kernel,
    combine1: Kernel,
    pass2: Kernel,
    combine2: Kernel,
}

impl ReduceKernels {
    /// Loads the four entry points from the one committed module.
    ///
    /// The two level-1 passes launch one thread per partition; the two combine
    /// passes are single-threaded folds, so they carry a block width of one.
    pub(crate) fn build(context: &Context) -> Result<ReduceKernels> {
        Ok(ReduceKernels {
            pass1: context.kernel(REDUCE_PTX, "pass1", BLOCK_WIDTH)?,
            combine1: context.kernel(REDUCE_PTX, "combine1", 1)?,
            pass2: context.kernel(REDUCE_PTX, "pass2", BLOCK_WIDTH)?,
            combine2: context.kernel(REDUCE_PTX, "combine2", 1)?,
        })
    }
}

/// One reduction's inputs: the resident grid pair, its shape, and the model's
/// liveness rule.
pub(crate) struct GridPair<'a> {
    /// The final grid $G_N$, resident on the device, cell-major interleaved.
    pub(crate) current: &'a Buffer,
    /// The grid one step before, $G_{N-1}$, resident on the device.
    pub(crate) previous: &'a Buffer,
    /// Channels per cell.
    pub(crate) channels: u32,
    /// Cells in the grid: `width * height`.
    pub(crate) cell_count: u32,
    /// The channel a cell's liveness reads, and the minimum value on it a live
    /// cell holds — the model's own rule.
    pub(crate) alive_channel: u32,
    pub(crate) alive_min: f32,
}

/// Reduces the final grid pair into the named stat scalars, in emission order.
pub(crate) fn reduce(
    context: &Context,
    kernels: &ReduceKernels,
    input: &GridPair<'_>,
) -> Result<Vec<(String, f64)>> {
    let channels = input.channels;
    if channels == 0 || channels > MAX_CHANNELS {
        return Err(Error::Validation(format!(
            "reduction handles 1..={MAX_CHANNELS} channels, got {channels}"
        )));
    }
    // The liveness channel indexes into the cell's channels; out of range, the
    // kernel would read past the cell. A model misdeclaring it is caught here.
    if input.alive_channel >= channels {
        return Err(Error::Validation(format!(
            "reduction alive_channel {} is out of range for {channels} channels",
            input.alive_channel
        )));
    }

    // The parameter buffer the kernel reads: the alive minimum travels as its
    // f32 bits, since the toolkit's buffers are untyped byte storage.
    let params = [
        channels,
        input.cell_count,
        input.alive_channel,
        input.alive_min.to_bits(),
        PARTITIONS,
    ];
    let mut params_buffer = context.buffer(std::mem::size_of_val(&params))?;
    context.upload(&mut params_buffer, bytemuck::cast_slice(&params))?;

    // Level-1 partials (per channel a sum, min, max, then the alive count and
    // the activity sum), the published means, the variance-pass partials, and
    // the final scalars — all f32.
    let stride = 3 * channels + 2;
    let partials = context.buffer((PARTITIONS * stride) as usize * 4)?;
    let means = context.buffer(channels as usize * 4)?;
    let partials2 = context.buffer((PARTITIONS * channels) as usize * 4)?;
    let out_len = (4 * channels + 2) as usize;
    let out = context.buffer(out_len * 4)?;

    // Every pass takes the same seven pointers, mirroring the WGSL module's one
    // descriptor set; a pass ignores the parameters it does not read.
    let bound: [&Buffer; 7] = [
        input.current,
        input.previous,
        &params_buffer,
        &partials,
        &means,
        &partials2,
        &out,
    ];
    let level1 = [PARTITIONS.div_ceil(BLOCK_WIDTH), 1, 1];
    let single = [1, 1, 1];
    // Each dispatch synchronizes the stream before returning, which is what
    // makes a pass's writes visible to the next.
    context.dispatch(&kernels.pass1, &bound, level1)?;
    context.dispatch(&kernels.combine1, &bound, single)?;
    context.dispatch(&kernels.pass2, &bound, level1)?;
    context.dispatch(&kernels.combine2, &bound, single)?;

    let bytes = context.download(&out)?;
    let values: Vec<f32> = bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect();
    Ok(name_scalars(channels, &values))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use sima_toolkit_cuda::{PTX_OPTIONS, compile};

    use super::*;

    /// The CUDA C the committed PTX is generated from.
    const REDUCE_CU: &str = include_str!("kernels/reduce.cu");

    #[test]
    fn the_committed_ptx_declares_the_four_entry_points() {
        // A module missing an entry point would fail only on a machine with a
        // device; the names are in the text, so this checks them anywhere.
        for entry in ["pass1", "combine1", "pass2", "combine2"] {
            assert!(
                REDUCE_PTX.contains(&format!(".entry {entry}(")),
                "the committed PTX declares {entry}"
            );
        }
    }

    #[test]
    fn the_committed_ptx_targets_the_architecture_the_options_name() {
        assert!(
            PTX_OPTIONS.contains(&"--gpu-architecture=compute_75"),
            "the committed PTX is generated for compute_75"
        );
        assert!(
            REDUCE_PTX.contains(".target sm_75"),
            "the committed PTX targets sm_75"
        );
    }

    /// Requires `libnvrtc`.
    #[test]
    fn the_committed_ptx_reproduces_from_its_source() {
        // The committed artifact is what executes, so it must be exactly what
        // the committed source compiles to. A mismatch means one of the two was
        // edited without the other, or that this NVRTC differs from the one
        // that produced the commit — the version is in the PTX header.
        assert_eq!(
            compile(REDUCE_CU).expect("compile the reduction"),
            REDUCE_PTX,
            "regenerate with the compile step in the crate's kernel documentation"
        );
    }

    /// Uploads `data` (cell-major interleaved f32) into a fresh device buffer.
    fn upload(context: &Context, data: &[f32]) -> Buffer {
        let mut buffer = context.buffer(std::mem::size_of_val(data)).expect("buffer");
        context
            .upload(&mut buffer, bytemuck::cast_slice(data))
            .expect("upload");
        buffer
    }

    /// The reduction's scalars as a name → value map. `alive` is the
    /// `(channel, minimum)` liveness rule.
    fn reduced(
        context: &Context,
        kernels: &ReduceKernels,
        current: &[f32],
        previous: &[f32],
        channels: u32,
        cell_count: u32,
        alive: (u32, f32),
    ) -> HashMap<String, f64> {
        let cur = upload(context, current);
        let prev = upload(context, previous);
        reduce(
            context,
            kernels,
            &GridPair {
                current: &cur,
                previous: &prev,
                channels,
                cell_count,
                alive_channel: alive.0,
                alive_min: alive.1,
            },
        )
        .expect("reduce")
        .into_iter()
        .collect()
    }

    /// Requires a CUDA device.
    #[test]
    fn a_single_channel_grid_reduces_to_known_scalars() {
        // The WGSL reduction's own known-answer case, asserted against the same
        // figures: four cells [1, 2, 3, 4] over an all-zero previous grid, every
        // figure exact in f32. mean 2.5, min 1, max 4, variance 1.25; the alive
        // threshold 3 counts {3, 4}, so population 0.5; activity is 10/4.
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let map = reduced(
            &context,
            &kernels,
            &[1.0, 2.0, 3.0, 4.0],
            &[0.0, 0.0, 0.0, 0.0],
            1,
            4,
            (0, 3.0),
        );
        assert_eq!(map["c0.mean"], 2.5);
        assert_eq!(map["c0.min"], 1.0);
        assert_eq!(map["c0.max"], 4.0);
        assert_eq!(map["c0.var"], 1.25);
        assert_eq!(map["population"], 0.5);
        assert_eq!(map["activity"], 2.5);
    }

    /// Requires a CUDA device.
    #[test]
    fn each_channel_reduces_independently() {
        // Two cells, two channels, cell-major: cell0 = (1, 2), cell1 = (3, 4).
        // Channel 0 is [1, 3] (mean 2, var 1), channel 1 is [2, 4] (mean 3,
        // var 1). Alive on channel 1 at threshold 3 counts cell1 alone, so
        // population 0.5; activity against a zero previous is 10/(2·2).
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let map = reduced(
            &context,
            &kernels,
            &[1.0, 2.0, 3.0, 4.0],
            &[0.0, 0.0, 0.0, 0.0],
            2,
            2,
            (1, 3.0),
        );
        assert_eq!(map["c0.mean"], 2.0);
        assert_eq!(map["c0.var"], 1.0);
        assert_eq!(map["c0.min"], 1.0);
        assert_eq!(map["c0.max"], 3.0);
        assert_eq!(map["c1.mean"], 3.0);
        assert_eq!(map["c1.var"], 1.0);
        assert_eq!(map["c1.min"], 2.0);
        assert_eq!(map["c1.max"], 4.0);
        assert_eq!(map["population"], 0.5);
        assert_eq!(map["activity"], 2.5);
    }

    /// Requires a CUDA device.
    #[test]
    fn activity_is_the_mean_absolute_change() {
        // Current [4, 1, 3, 2], previous [1, 3, 3, 6]: |Δ| = 3 + 2 + 0 + 4 = 9
        // over four cells and one channel, so activity is 9/4 = 2.25.
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let map = reduced(
            &context,
            &kernels,
            &[4.0, 1.0, 3.0, 2.0],
            &[1.0, 3.0, 3.0, 6.0],
            1,
            4,
            (0, 0.0),
        );
        assert_eq!(map["activity"], 2.25);
    }

    /// Requires a CUDA device.
    #[test]
    fn population_spans_none_and_all_alive() {
        // The same grid: a threshold above every value counts none alive, one
        // below every value counts all.
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let grid = [1.0, 2.0, 3.0, 4.0];
        let zeros = [0.0; 4];
        let none = reduced(&context, &kernels, &grid, &zeros, 1, 4, (0, 100.0));
        assert_eq!(none["population"], 0.0);
        let all = reduced(&context, &kernels, &grid, &zeros, 1, 4, (0, 0.0));
        assert_eq!(all["population"], 1.0);
    }

    /// Requires a CUDA device.
    #[test]
    fn the_reduction_is_deterministic() {
        // The fixed topology folds every sum in the same order, so reducing the
        // same grid twice yields byte-identical scalars.
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let cur = upload(&context, &[1.0, 2.0, 3.0, 4.0]);
        let prev = upload(&context, &[0.5, 1.5, 2.5, 3.5]);
        let pair = GridPair {
            current: &cur,
            previous: &prev,
            channels: 1,
            cell_count: 4,
            alive_channel: 0,
            alive_min: 2.0,
        };
        let first = reduce(&context, &kernels, &pair).expect("first");
        let second = reduce(&context, &kernels, &pair).expect("second");
        let first_bits: Vec<u64> = first.iter().map(|(_, v)| v.to_bits()).collect();
        let second_bits: Vec<u64> = second.iter().map(|(_, v)| v.to_bits()).collect();
        assert_eq!(first_bits, second_bits);
    }

    /// Requires a CUDA device.
    #[test]
    fn the_reduction_reads_the_harness_resident_pair() {
        // The reduction runs over the two ping-pong buffers `run` leaves
        // resident, not synthetic uploads, so `Trajectory::previous` (G_{N-1})
        // is exercised end to end. The smoke kernel over a grid that is already
        // a neighborhood maximum is a fixed point, so the pair is equal and the
        // activity is zero; the mean is the grid's own, which catches a
        // reduction reading the pair swapped only through the resident buffers.
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let kernel = context
            .kernel(
                include_str!("../../../../kernels/smoke.ptx"),
                "main_kernel",
                BLOCK_WIDTH,
            )
            .expect("smoke kernel");
        let initial = crate::shared::cellular::Grid::new(4, 4, 1, vec![2.0; 16]).expect("grid");
        let trajectory =
            crate::shared::cellular::cuda::step::run(&context, &kernel, &initial, 3, &[], None)
                .expect("run");
        let map: HashMap<String, f64> = reduce(
            &context,
            &kernels,
            &GridPair {
                current: trajectory.current(),
                previous: trajectory.previous(),
                channels: trajectory.channels(),
                cell_count: trajectory.cell_count(),
                alive_channel: 0,
                alive_min: 0.0,
            },
        )
        .expect("reduce")
        .into_iter()
        .collect();
        assert_eq!(map["c0.mean"], 2.0);
        assert_eq!(map["activity"], 0.0);
        assert_eq!(map["population"], 1.0);
    }

    /// Requires a CUDA device.
    #[test]
    fn a_shape_the_reduction_cannot_handle_is_rejected() {
        // Both guards are validation faults caught before any dispatch: a
        // channel count past the scratch-array bound, and a liveness channel
        // that indexes past the cell.
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let grid = upload(&context, &[0.0; 4]);
        for (channels, cell_count, alive_channel) in [(MAX_CHANNELS + 1, 4, 0), (2, 2, 2)] {
            assert!(matches!(
                reduce(
                    &context,
                    &kernels,
                    &GridPair {
                        current: &grid,
                        previous: &grid,
                        channels,
                        cell_count,
                        alive_channel,
                        alive_min: 0.0,
                    },
                ),
                Err(Error::Validation(_))
            ));
        }
    }
}
