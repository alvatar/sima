//! The GPU reduction of a final grid pair into the per-candidate stat scalars.
//!
//! The kernel is [`REDUCE_WGSL`], four compute passes dispatched in order;
//! [`ReduceKernels`] compiles them once per engine and [`reduce`] runs them
//! over the two live ping-pong buffers a [`run`](super::run) left resident. The
//! scalars are named as the reporting layer expects: `c<i>.mean`, `c<i>.var`,
//! `c<i>.min`, `c<i>.max` per channel, then `population` and `activity`.
//!
//! A diverged candidate carries non-finite values into the scalars as-is. WGSL
//! permits fast-math relaxation, so whether an evaluation that would yield a NaN
//! actually produces one is a per-backend property. The defense lives at the
//! Rust layer, where IEEE semantics are reliable: the snapshot predicate's
//! all-finite check is what catches a partially diverged grid.

use sima_core::{Error, Result};
use sima_toolkit_wgsl::{Buffer, Context, Kernel};

/// The reduction kernel source. Its digest joins the environment because the
/// reduction's output gates committed bytes, so editing it must change task
/// keys exactly as editing a step kernel does.
pub const REDUCE_WGSL: &str = include_str!("shaders/reduce.wgsl");

/// The upper bound on channels the reduction handles; the shader's per-channel
/// scratch arrays are sized to it, so a model exceeding it is rejected before
/// dispatch.
pub(crate) const MAX_CHANNELS: u32 = 16;

/// The fixed number of level-1 partitions. The topology is fixed so the
/// reduction is deterministic per backend: every sum folds in the same order.
const PARTITIONS: u32 = 64;

/// The four compiled reduction passes, built once and held for the engine's
/// lifetime alongside the step kernel.
pub struct ReduceKernels {
    pass1: Kernel,
    combine1: Kernel,
    pass2: Kernel,
    combine2: Kernel,
}

impl ReduceKernels {
    /// Compiles the four passes from the one shared source.
    pub fn build(context: &Context) -> Result<ReduceKernels> {
        Ok(ReduceKernels {
            pass1: context.kernel(REDUCE_WGSL, "pass1")?,
            combine1: context.kernel(REDUCE_WGSL, "combine1")?,
            pass2: context.kernel(REDUCE_WGSL, "pass2")?,
            combine2: context.kernel(REDUCE_WGSL, "combine2")?,
        })
    }
}

/// One reduction's inputs: the resident grid pair, its shape, and the model's
/// liveness rule.
pub struct GridPair<'a> {
    /// The final grid $G_N$, resident on the device, cell-major interleaved.
    pub current: &'a Buffer,
    /// The grid one step before, $G_{N-1}$, resident on the device.
    pub previous: &'a Buffer,
    /// Channels per cell.
    pub channels: u32,
    /// Cells in the grid: `width * height`.
    pub cell_count: u32,
    /// The channel a cell's liveness reads, and the minimum value on it a live
    /// cell holds — the model's own rule.
    pub alive_channel: u32,
    pub alive_min: f32,
}

/// Reduces the final grid pair into the named stat scalars, in emission order.
pub fn reduce(
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
    // shader would read past the cell. A model misdeclaring it is caught here.
    if input.alive_channel >= channels {
        return Err(Error::Validation(format!(
            "reduction alive_channel {} is out of range for {channels} channels",
            input.alive_channel
        )));
    }

    // The parameter buffer the shader reads: the alive minimum travels as its
    // f32 bits, since the toolkit's buffers are untyped byte storage.
    let params = [
        channels,
        input.cell_count,
        input.alive_channel,
        input.alive_min.to_bits(),
        PARTITIONS,
    ];
    let params_buffer = context.buffer(std::mem::size_of_val(&params))?;
    context.upload(&params_buffer, bytemuck::cast_slice(&params))?;

    // Level-1 partials (per channel a sum, min, max, then the alive count and
    // the activity sum), the published means, the variance-pass partials, and
    // the final scalars — all f32.
    let stride = 3 * channels + 2;
    let partials = context.buffer((PARTITIONS * stride) as usize * 4)?;
    let means = context.buffer(channels as usize * 4)?;
    let partials2 = context.buffer((PARTITIONS * channels) as usize * 4)?;
    let out_len = (4 * channels + 2) as usize;
    let out = context.buffer(out_len * 4)?;

    // Every pass binds the same seven buffers: the toolkit reflects one
    // descriptor set for the whole module, and a pass ignores what it does not
    // read.
    let bound: [&Buffer; 7] = [
        input.current,
        input.previous,
        &params_buffer,
        &partials,
        &means,
        &partials2,
        &out,
    ];
    let level1 = [PARTITIONS.div_ceil(64), 1, 1];
    let single = [1, 1, 1];
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

/// The scalar names the reduction emits for a `channels`-channel grid, in
/// emission order: four metrics per channel, then the two grid-level scalars.
/// This is the naming authority — the flat output is paired with it, and the
/// predicate layer validates a scalar name against it.
pub fn scalar_names(channels: u32) -> Vec<String> {
    let mut names = Vec::with_capacity((4 * channels + 2) as usize);
    for c in 0..channels {
        for metric in ["mean", "var", "min", "max"] {
            names.push(format!("c{c}.{metric}"));
        }
    }
    names.push("population".to_string());
    names.push("activity".to_string());
    names
}

/// Pairs the reduction's flat output with its names, widening each `f32` to
/// `f64`. The output layout matches [`scalar_names`] element for element.
fn name_scalars(channels: u32, values: &[f32]) -> Vec<(String, f64)> {
    scalar_names(channels)
        .into_iter()
        .zip(values.iter().map(|&v| f64::from(v)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_toolkit_wgsl::check;
    use std::collections::HashMap;

    #[test]
    fn every_pass_compiles_and_reflects_seven_bindings() -> Result<()> {
        // The four entry points share one module, so each reflects the module's
        // full seven-buffer descriptor set.
        for entry in ["pass1", "combine1", "pass2", "combine2"] {
            check(REDUCE_WGSL, entry)?;
        }
        Ok(())
    }

    /// Uploads `data` (cell-major interleaved f32) into a fresh device buffer.
    fn upload(context: &Context, data: &[f32]) -> Buffer {
        let buffer = context.buffer(std::mem::size_of_val(data)).expect("buffer");
        context
            .upload(&buffer, bytemuck::cast_slice(data))
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

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn a_single_channel_grid_reduces_to_known_scalars() {
        // Four cells [1, 2, 3, 4]; the previous grid is all zeros. Every figure
        // is exact in f32. mean 2.5, min 1, max 4, variance 1.25; alive threshold
        // 3 counts {3, 4}, so population 0.5; activity is the mean |Δ| = 10/4.
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

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
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

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
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

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
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

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
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

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn the_reduction_reads_the_harness_resident_pair() {
        // The reduction runs over the two ping-pong buffers `run` leaves
        // resident, not synthetic uploads, so `Trajectory::previous` (G_{N-1})
        // is exercised end to end. A kernel that adds one per step keeps every
        // sum exact: after k steps the final grid is `initial + k` and the step
        // before is `initial + (k - 1)`, so the absolute change is one in every
        // cell.
        const ADD_ONE_WGSL: &str = r#"
@group(0) @binding(0) var<storage, read> in_grid: array<f32>;
@group(0) @binding(1) var<storage, read_write> out_grid: array<f32>;
@group(0) @binding(2) var<storage, read> dims: array<u32>;
@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let cell = gid.x;
    if (cell >= dims[0] * dims[1]) { return; }
    out_grid[cell] = in_grid[cell] + 1.0;
}
"#;
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let kernel = context.kernel(ADD_ONE_WGSL, "main").expect("add-one kernel");

        let cells = 16u32;
        let initial =
            crate::cellular::Grid::new(4, 4, 1, vec![0.0; cells as usize]).expect("grid");
        let steps = 3u32;
        let trajectory =
            crate::cellular::run(&context, &kernel, &initial, steps, &[], None).expect("run");

        // The two resident buffers, downloaded as the reduction reads them.
        let read = |buffer: &Buffer| -> Vec<f32> {
            context
                .download(buffer)
                .expect("download")
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect()
        };
        let current = read(trajectory.current());
        let previous = read(trajectory.previous());

        // Activity is the mean absolute change over every cell and channel; the
        // mean is over the final grid alone. Both are computed here from the
        // downloaded pair, independent of the reduction.
        let activity: f64 = current
            .iter()
            .zip(&previous)
            .map(|(&c, &p)| f64::from((c - p).abs()))
            .sum::<f64>()
            / f64::from(cells);
        let mean: f64 = current.iter().map(|&v| f64::from(v)).sum::<f64>() / f64::from(cells);

        // The reduction reads the resident buffers in place, not the downloads.
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
        // Activity ties the reduction to the resident G_{N-1}; the mean is over
        // G_N alone, so it catches a reduction that read the pair swapped.
        assert_eq!(map["activity"], activity);
        assert_eq!(map["activity"], 1.0);
        assert_eq!(map["c0.mean"], mean);
        assert_eq!(map["c0.mean"], f64::from(steps));
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn too_many_channels_is_rejected() {
        // A channel count past the scratch-array bound is a validation fault,
        // caught before any dispatch.
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let grid = upload(&context, &[0.0; 4]);
        assert!(matches!(
            reduce(
                &context,
                &kernels,
                &GridPair {
                    current: &grid,
                    previous: &grid,
                    channels: MAX_CHANNELS + 1,
                    cell_count: 4,
                    alive_channel: 0,
                    alive_min: 0.0,
                },
            ),
            Err(Error::Validation(_))
        ));
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn an_out_of_range_alive_channel_is_rejected() {
        // A two-channel grid with the liveness channel at index 2 is out of
        // range: a validation fault, caught before any dispatch.
        let context = Context::new().expect("context");
        let kernels = ReduceKernels::build(&context).expect("kernels");
        let grid = upload(&context, &[0.0; 4]);
        assert!(matches!(
            reduce(
                &context,
                &kernels,
                &GridPair {
                    current: &grid,
                    previous: &grid,
                    channels: 2,
                    cell_count: 2,
                    alive_channel: 2,
                    alive_min: 0.0,
                },
            ),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn scalar_names_follow_the_channel_then_grid_order() {
        // Two channels: eight per-channel metrics, then population and activity.
        let values: Vec<f32> = (0..10).map(|i| i as f32).collect();
        let named = name_scalars(2, &values);
        let names: Vec<&str> = named.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "c0.mean",
                "c0.var",
                "c0.min",
                "c0.max",
                "c1.mean",
                "c1.var",
                "c1.min",
                "c1.max",
                "population",
                "activity",
            ]
        );
        assert_eq!(named[0].1, 0.0);
        assert_eq!(named[9].1, 9.0);
    }
}
