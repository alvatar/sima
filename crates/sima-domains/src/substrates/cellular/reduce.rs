//! The GPU reduction of a final grid pair into the per-candidate stat scalars,
//! written once over the [`CellularOps`] boundary and monomorphized per backend.
//!
//! Each backend ships its own transcription of the kernel — four compute passes
//! dispatched in order over the same fixed partition topology — and
//! [`CellularOps::REDUCE_SOURCE`] names it. [`ReduceKernels`] builds the four
//! passes once per engine and [`reduce`] runs them over the two live ping-pong
//! buffers a [`run`](super::run) left resident. The scalars are named as the
//! reporting layer expects: `c<i>.mean`, `c<i>.var`, `c<i>.min`, `c<i>.max` per
//! channel, then `population` and `activity`.
//!
//! A diverged candidate carries non-finite values into the scalars as-is. WGSL
//! permits fast-math relaxation, so whether an evaluation that would yield a NaN
//! actually produces one is a per-backend property. The defense lives at the
//! Rust layer, where IEEE semantics are reliable: the snapshot predicate's
//! all-finite check is what catches a partially diverged grid.

use sima_core::{Error, Result};

use crate::substrates::cellular::ops::CellularOps;

/// The upper bound on channels the reduction handles; each kernel's
/// per-channel scratch arrays are sized to it, so a model exceeding it is
/// rejected before dispatch.
pub(crate) const MAX_CHANNELS: u32 = 16;

/// The fixed number of level-1 partitions, shared by both backends'
/// reductions. The topology is fixed so the reduction is deterministic per
/// backend: every sum folds in the same order. Both kernels read it from their
/// parameter buffer, so one constant drives both and they accumulate alike.
pub(crate) const PARTITIONS: u32 = 64;

/// The threads per block every cellular kernel is launched with, on either
/// backend. Each kernel states the same width in its own source — WGSL with
/// `@workgroup_size`, CUDA with `__launch_bounds__` — and each toolkit checks
/// the two agree, so a grid sized by this constant covers exactly the
/// invocations the kernel launches.
pub(crate) const BLOCK_WIDTH: u32 = 64;

/// The four reduction passes, in dispatch order, with the block width each
/// launches at.
///
/// The two level-1 passes launch one invocation per partition; the two combine
/// passes are single-threaded folds, so they carry a width of one.
const PASSES: [(&str, u32); 4] = [
    ("pass1", BLOCK_WIDTH),
    ("combine1", 1),
    ("pass2", BLOCK_WIDTH),
    ("combine2", 1),
];

/// The four compiled reduction passes, built once and held for the engine's
/// lifetime alongside the step kernel.
pub(crate) struct ReduceKernels<O: CellularOps> {
    passes: Vec<O::Kernel>,
}

impl<O: CellularOps> ReduceKernels<O> {
    /// Builds the four passes from this backend's one reduction source.
    pub(crate) fn build(ops: &O) -> Result<ReduceKernels<O>> {
        let passes = PASSES
            .iter()
            .map(|&(entry, width)| ops.kernel(O::REDUCE_SOURCE, entry, width))
            .collect::<Result<Vec<_>>>()?;
        Ok(ReduceKernels { passes })
    }
}

/// One reduction's inputs: the resident grid pair, its shape, and the model's
/// liveness rule.
pub(crate) struct GridPair<'a, O: CellularOps> {
    /// The final grid $G_N$, resident on the device, cell-major interleaved.
    pub current: &'a O::Buffer,
    /// The grid one step before, $G_{N-1}$, resident on the device.
    pub previous: &'a O::Buffer,
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
pub(crate) fn reduce<O: CellularOps>(
    ops: &O,
    kernels: &ReduceKernels<O>,
    input: &GridPair<'_, O>,
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
    // f32 bits, since the toolkits' buffers are untyped byte storage.
    let params = [
        channels,
        input.cell_count,
        input.alive_channel,
        input.alive_min.to_bits(),
        PARTITIONS,
    ];
    let mut params_buffer = ops.buffer(std::mem::size_of_val(&params))?;
    ops.upload(&mut params_buffer, bytemuck::cast_slice(&params))?;

    // Level-1 partials (per channel a sum, min, max, then the alive count and
    // the activity sum), the published means, the variance-pass partials, and
    // the final scalars — all f32.
    let stride = 3 * channels + 2;
    let partials = ops.buffer((PARTITIONS * stride) as usize * 4)?;
    let means = ops.buffer(channels as usize * 4)?;
    let partials2 = ops.buffer((PARTITIONS * channels) as usize * 4)?;
    let out_len = (4 * channels + 2) as usize;
    let out = ops.buffer(out_len * 4)?;

    // Every pass binds the same seven buffers: the WGSL module reflects one
    // descriptor set for all four entry points and the CUDA transcription
    // declares the same seven pointers, so a pass ignores what it does not read.
    let bound: [&O::Buffer; 7] = [
        input.current,
        input.previous,
        &params_buffer,
        &partials,
        &means,
        &partials2,
        &out,
    ];
    // The level-1 passes cover one partition per invocation; the combines are
    // one invocation each. Every dispatch completes before the next begins, so
    // each pass reads what the one before it wrote.
    let level1 = [PARTITIONS.div_ceil(BLOCK_WIDTH), 1, 1];
    let single = [1, 1, 1];
    for (kernel, groups) in kernels.passes.iter().zip([level1, single, level1, single]) {
        ops.dispatch(kernel, &bound, groups)?;
    }

    // The output buffer was sized at four bytes per scalar, so the remainder is
    // empty and the chunks are the whole of it.
    let bytes = ops.download(&out)?;
    let values: Vec<f32> = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| f32::from_le_bytes(*chunk))
        .collect();
    name_scalars(channels, &values)
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
/// `f64`. The output layout matches [`scalar_names`] element for element, and
/// is the same for every backend.
///
/// A readback whose length disagrees with the names is a fault, not a shorter
/// answer: pairing them positionally would emit the metrics that happen to line
/// up and drop the rest, so a snapshot predicate naming a missing scalar would
/// read the run as having no such scalar rather than as having failed.
fn name_scalars(channels: u32, values: &[f32]) -> Result<Vec<(String, f64)>> {
    let names = scalar_names(channels);
    if names.len() != values.len() {
        return Err(Error::Backend(format!(
            "the reduction of a {channels}-channel grid emits {} scalars; {} values were read \
             back",
            names.len(),
            values.len()
        )));
    }
    Ok(names
        .into_iter()
        .zip(values.iter().map(|&v| f64::from(v)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two reduction sources, for the checks that read a bound out of every
    /// kernel that declares one.
    const REDUCE_WGSL: &str = include_str!("wgsl/shaders/reduce.wgsl");
    const REDUCE_CU: &str = include_str!("cuda/kernels/reduce.cu");

    #[test]
    fn every_kernel_sizes_its_scratch_to_the_bound_this_module_enforces() {
        // The bound exists three times: here, where a channel count is refused
        // before dispatch, and once inside each backend's reduction kernel,
        // where it sizes the per-channel scratch arrays. A kernel whose copy
        // fell below this one would write past its scratch on a grid this
        // module admits — out of bounds on the device, with no error anywhere.
        // Each kernel's copy is parsed back out of its own source so the three
        // cannot drift apart silently.
        assert_eq!(
            declared_bound(REDUCE_WGSL, "const MAX_CHANNELS: u32 = ", "u;"),
            Some(MAX_CHANNELS),
            "the WGSL reduction declares the bound this module enforces"
        );
        assert_eq!(
            declared_bound(REDUCE_CU, "#define MAX_CHANNELS ", "\n"),
            Some(MAX_CHANNELS),
            "the CUDA reduction declares the bound this module enforces"
        );
    }

    #[test]
    fn a_kernel_that_lowered_its_bound_is_caught() {
        // The agreement test above is only worth its place if it fails on a
        // drifted copy, so a mutated source stands in for one.
        let lowered = REDUCE_WGSL.replace(
            &format!("const MAX_CHANNELS: u32 = {MAX_CHANNELS}u;"),
            "const MAX_CHANNELS: u32 = 8u;",
        );
        assert_ne!(
            declared_bound(&lowered, "const MAX_CHANNELS: u32 = ", "u;"),
            Some(MAX_CHANNELS),
            "a kernel that lowered its bound reads back as disagreeing"
        );

        // And a source that declares nothing reads back as absent rather than
        // as agreeing, so a renamed constant fails the check too.
        assert_eq!(declared_bound("", "const MAX_CHANNELS: u32 = ", "u;"), None);
    }

    /// The integer a source declares after `prefix`, up to `terminator`.
    fn declared_bound(source: &str, prefix: &str, terminator: &str) -> Option<u32> {
        let at = source.find(prefix)? + prefix.len();
        let rest = &source[at..];
        rest[..rest.find(terminator)?].trim().parse().ok()
    }

    #[test]
    fn a_readback_of_the_wrong_length_is_rejected() {
        // The names and the values are paired positionally, so a readback that
        // is short emits fewer scalars than the reduction computed — and a
        // predicate looking for one of the missing names would read the run as
        // having no such scalar rather than as having failed. Two channels want
        // ten values.
        for values in [vec![0.0_f32; 9], vec![0.0_f32; 11]] {
            let count = values.len();
            let error = name_scalars(2, &values).expect_err("a mismatched readback");
            let Error::Backend(message) = error else {
                panic!("expected a backend error for {count} values");
            };
            assert!(message.contains("10"), "names what was wanted: {message}");
            assert!(
                message.contains(&count.to_string()),
                "names what arrived: {message}"
            );
        }
    }

    #[test]
    fn scalar_names_follow_the_channel_then_grid_order() {
        // Two channels: eight per-channel metrics, then population and activity.
        let values: Vec<f32> = (0..10).map(|i| i as f32).collect();
        let named = name_scalars(2, &values).expect("ten values for two channels");
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

    /// Reducing a grid dispatches the reduction passes, which needs a real
    /// device. The reduction is one implementation, so each case is written
    /// once over the ops boundary and run against both backends.
    mod on_device {
        use std::collections::HashMap;

        use super::*;
        use crate::substrates::cellular::cuda::CudaOps;
        use crate::substrates::cellular::harness::run;
        use crate::substrates::cellular::reference::{SMOKE_PTX, SMOKE_WGSL};
        use crate::substrates::cellular::wgsl::WgslOps;
        use crate::substrates::cellular::{Grid, ops::CellularOps};

        /// One reduction over uploaded grids, as a name → value map. `alive` is
        /// the `(channel, minimum)` liveness rule.
        fn reduced<O: CellularOps>(
            ops: &O,
            kernels: &ReduceKernels<O>,
            current: &[f32],
            previous: &[f32],
            channels: u32,
            cell_count: u32,
            alive: (u32, f32),
        ) -> HashMap<String, f64> {
            let upload = |data: &[f32]| {
                let mut buffer = ops.buffer(std::mem::size_of_val(data)).expect("buffer");
                ops.upload(&mut buffer, bytemuck::cast_slice(data))
                    .expect("upload");
                buffer
            };
            let (cur, prev) = (upload(current), upload(previous));
            reduce(
                ops,
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

        /// Runs `case` against every backend, with that backend's reduction
        /// passes already built.
        fn on_each_backend(
            case: impl Fn(&dyn Fn(&[f32], &[f32], u32, u32, (u32, f32)) -> HashMap<String, f64>),
        ) {
            let wgsl = WgslOps::open(None).expect("open a Vulkan device");
            let wgsl_kernels = ReduceKernels::build(&wgsl).expect("build the WGSL reduction");
            case(&|current, previous, channels, cells, alive| {
                reduced(
                    &wgsl,
                    &wgsl_kernels,
                    current,
                    previous,
                    channels,
                    cells,
                    alive,
                )
            });

            let cuda = CudaOps::open(None).expect("open a CUDA device");
            let cuda_kernels = ReduceKernels::build(&cuda).expect("build the CUDA reduction");
            case(&|current, previous, channels, cells, alive| {
                reduced(
                    &cuda,
                    &cuda_kernels,
                    current,
                    previous,
                    channels,
                    cells,
                    alive,
                )
            });
        }

        #[test]
        fn a_single_channel_grid_reduces_to_known_scalars() {
            // Four cells [1, 2, 3, 4]; the previous grid is all zeros. Every
            // figure is exact in f32. mean 2.5, min 1, max 4, variance 1.25; the
            // alive threshold 3 counts {3, 4}, so population 0.5; activity is
            // the mean |Δ| = 10/4.
            on_each_backend(|reduce_on_device| {
                let map = reduce_on_device(&[1.0, 2.0, 3.0, 4.0], &[0.0; 4], 1, 4, (0, 3.0));
                assert_eq!(map["c0.mean"], 2.5);
                assert_eq!(map["c0.min"], 1.0);
                assert_eq!(map["c0.max"], 4.0);
                assert_eq!(map["c0.var"], 1.25);
                assert_eq!(map["population"], 0.5);
                assert_eq!(map["activity"], 2.5);
            });
        }

        #[test]
        fn each_channel_reduces_independently() {
            // Two cells, two channels, cell-major: cell0 = (1, 2),
            // cell1 = (3, 4). Channel 0 is [1, 3] (mean 2, var 1), channel 1 is
            // [2, 4] (mean 3, var 1). Alive on channel 1 at threshold 3 counts
            // cell1 alone, so population 0.5; activity against a zero previous
            // is 10/(2·2).
            on_each_backend(|reduce_on_device| {
                let map = reduce_on_device(&[1.0, 2.0, 3.0, 4.0], &[0.0; 4], 2, 2, (1, 3.0));
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
            });
        }

        #[test]
        fn activity_is_the_mean_absolute_change() {
            // Current [4, 1, 3, 2], previous [1, 3, 3, 6]: |Δ| = 3+2+0+4 = 9
            // over four cells and one channel, so activity is 9/4 = 2.25.
            on_each_backend(|reduce_on_device| {
                let map =
                    reduce_on_device(&[4.0, 1.0, 3.0, 2.0], &[1.0, 3.0, 3.0, 6.0], 1, 4, (0, 0.0));
                assert_eq!(map["activity"], 2.25);
            });
        }

        #[test]
        fn population_spans_none_and_all_alive() {
            // The same grid: a threshold above every value counts none alive,
            // one below every value counts all.
            on_each_backend(|reduce_on_device| {
                let grid = [1.0, 2.0, 3.0, 4.0];
                assert_eq!(
                    reduce_on_device(&grid, &[0.0; 4], 1, 4, (0, 100.0))["population"],
                    0.0
                );
                assert_eq!(
                    reduce_on_device(&grid, &[0.0; 4], 1, 4, (0, 0.0))["population"],
                    1.0
                );
            });
        }

        #[test]
        fn the_reduction_is_deterministic() {
            // The fixed topology folds every sum in the same order, so reducing
            // the same grid twice yields byte-identical scalars.
            on_each_backend(|reduce_on_device| {
                let bits = |map: HashMap<String, f64>| {
                    let mut pairs: Vec<(String, u64)> =
                        map.into_iter().map(|(n, v)| (n, v.to_bits())).collect();
                    pairs.sort();
                    pairs
                };
                let once =
                    reduce_on_device(&[1.0, 2.0, 3.0, 4.0], &[0.5, 1.5, 2.5, 3.5], 1, 4, (0, 2.0));
                let twice =
                    reduce_on_device(&[1.0, 2.0, 3.0, 4.0], &[0.5, 1.5, 2.5, 3.5], 1, 4, (0, 2.0));
                assert_eq!(bits(once), bits(twice));
            });
        }

        #[test]
        fn a_shape_the_reduction_cannot_handle_is_rejected() {
            // Both guards are validation faults caught before any dispatch: a
            // channel count past the scratch-array bound, and a liveness channel
            // that indexes past the cell.
            let ops = WgslOps::open(None).expect("open a Vulkan device");
            let kernels = ReduceKernels::build(&ops).expect("build the reduction");
            let mut grid = ops.buffer(16).expect("buffer");
            ops.upload(&mut grid, &[0u8; 16]).expect("upload");
            for (channels, cell_count, alive_channel) in [(MAX_CHANNELS + 1, 4, 0), (2, 2, 2)] {
                assert!(matches!(
                    reduce(
                        &ops,
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

        #[test]
        fn the_reduction_reads_the_harness_resident_pair() {
            // The reduction runs over the two ping-pong buffers `run` leaves
            // resident, not synthetic uploads, so `Trajectory::previous`
            // ($G_{N-1}$) is exercised end to end. A kernel that raises every
            // cell to its neighborhood max over a grid that is already a
            // maximum is a fixed point, so the pair is equal and activity is
            // zero; the mean is the grid's own, which catches a reduction
            // reading the pair swapped.
            fn case<O: CellularOps>(ops: &O, source: &str) {
                let kernels = ReduceKernels::build(ops).expect("build the reduction");
                let kernel = ops
                    .kernel(source, O::ENTRY, BLOCK_WIDTH)
                    .expect("build the smoke kernel");
                let initial = Grid::new(4, 4, 1, vec![2.0; 16]).expect("grid");
                let trajectory = run(ops, &kernel, &initial, 3, &[], None).expect("run");
                let map: HashMap<String, f64> = reduce(
                    ops,
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
            case(
                &WgslOps::open(None).expect("open a Vulkan device"),
                SMOKE_WGSL,
            );
            case(&CudaOps::open(None).expect("open a CUDA device"), SMOKE_PTX);
        }
    }
}
