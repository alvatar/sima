//! [`CellularRule`], the CPU-reference contract the harness is validated
//! against, and the smoke kernels the substrate's own tests dispatch.
//!
//! The whole module is test scaffolding. A cellular family ships a kernel and a
//! genome, never a reference: what a reference buys is an independent
//! computation of the same step, so a harness that dispatched wrongly — a
//! misordered ping-pong, a group count covering the wrong cells — disagrees
//! byte for byte instead of producing plausible numbers.

use crate::substrates::cellular::Grid;

/// A synchronous cellular update: one step maps a whole input grid to a whole
/// output grid, each output cell a pure function of a neighborhood of the
/// input.
pub(crate) trait CellularRule {
    /// Computes one step from `input` into `output`. The two grids share
    /// `input`'s dimensions; the step overwrites every cell of `output`.
    fn step(&self, input: &Grid, output: &mut Grid);
}

/// The smoke kernel, transcribed per backend: a 5-point neighborhood max with
/// toroidal boundaries, each channel reduced independently.
///
/// It is the substrate's own kernel rather than a model's, so a harness test
/// exercises the dispatch convention without depending on any domain, and the
/// two transcriptions let one test case search on either backend.
pub(crate) const SMOKE_WGSL: &str = include_str!("wgsl/shaders/smoke.wgsl");
pub(crate) const SMOKE_PTX: &str = include_str!("cuda/kernels/smoke.ptx");

/// The step probe, transcribed per backend: every cell accumulates the low word
/// of the per-step index its dispatch was given.
///
/// It has no CPU reference and needs none — what it checks is not arithmetic but
/// that the value a step reads is the value that step was dispatched with, which
/// only a device can answer. Its two transcriptions let the one test case pin
/// that on either backend.
pub(crate) const STEP_PROBE_WGSL: &str = include_str!("wgsl/shaders/step_probe.wgsl");
pub(crate) const STEP_PROBE_PTX: &str = include_str!("cuda/kernels/step_probe.ptx");

/// The CPU reference the smoke kernels mirror.
pub(crate) struct SmokeMax;

impl CellularRule for SmokeMax {
    fn step(&self, input: &Grid, output: &mut Grid) {
        let width = input.width() as usize;
        let height = input.height() as usize;
        let channels = input.channels() as usize;
        let src = input.data();
        let dst = output.data_mut();
        for y in 0..height {
            for x in 0..width {
                let left = (x + width - 1) % width;
                let right = (x + 1) % width;
                let up = (y + height - 1) % height;
                let down = (y + 1) % height;
                for c in 0..channels {
                    let at = |cx: usize, cy: usize| (cy * width + cx) * channels + c;
                    let mut m = src[at(x, y)];
                    m = m.max(src[at(left, y)]);
                    m = m.max(src[at(right, y)]);
                    m = m.max(src[at(x, up)]);
                    m = m.max(src[at(x, down)]);
                    dst[at(x, y)] = m;
                }
            }
        }
    }
}

/// Advances `initial` by `steps` through `rule` alone, as the harness's own
/// double buffering does.
pub(crate) fn advance(rule: &impl CellularRule, initial: &Grid, steps: u32) -> Grid {
    let mut current = initial.clone();
    let mut next = initial.clone();
    for _ in 0..steps {
        rule.step(&current, &mut next);
        std::mem::swap(&mut current, &mut next);
    }
    current
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_toolkit_cuda::compile;

    /// The CUDA C the committed smoke PTX is generated from.
    const SMOKE_CU: &str = include_str!("cuda/kernels/smoke.cu");

    /// A minimal neighborhood rule: each output cell is the per-channel max of
    /// the cell and its right neighbor along x, wrapping toroidally. It reads a
    /// genuine neighbor, so it exercises the trait as a stencil rather than a
    /// cell-local map.
    struct RightMax;

    impl CellularRule for RightMax {
        fn step(&self, input: &Grid, output: &mut Grid) {
            let width = input.width() as usize;
            let height = input.height() as usize;
            let channels = input.channels() as usize;
            let src = input.data();
            let dst = output.data_mut();
            for y in 0..height {
                for x in 0..width {
                    let right = (x + 1) % width;
                    for c in 0..channels {
                        let here = src[(y * width + x) * channels + c];
                        let neighbor = src[(y * width + right) * channels + c];
                        dst[(y * width + x) * channels + c] = here.max(neighbor);
                    }
                }
            }
        }
    }

    #[test]
    fn step_computes_the_expected_neighborhood() {
        // A 3x1x1 grid: [1.0, 5.0, 2.0]. Right-neighbor max, wrapping:
        //   x0 = max(1, 5) = 5, x1 = max(5, 2) = 5, x2 = max(2, 1) = 2.
        let input = Grid::new(3, 1, 1, vec![1.0, 5.0, 2.0]).expect("input grid");
        let mut output = Grid::new(3, 1, 1, vec![0.0; 3]).expect("output grid");
        RightMax.step(&input, &mut output);
        assert_eq!(output.data(), &[5.0, 5.0, 2.0]);
    }

    #[test]
    fn advancing_composes_the_rule() {
        // Two steps of the right-neighbor max reach two cells to the right, so
        // the composition is checked rather than only one application.
        let input = Grid::new(4, 1, 1, vec![1.0, 5.0, 2.0, 3.0]).expect("input grid");
        assert_eq!(advance(&RightMax, &input, 2).data(), &[5.0, 5.0, 3.0, 5.0]);
    }

    #[test]
    fn the_wgsl_smoke_kernel_compiles_device_free() {
        sima_toolkit_wgsl::check(SMOKE_WGSL, "main").expect("the smoke shader compiles");
    }

    /// Requires `libnvrtc`.
    #[test]
    fn the_committed_smoke_ptx_reproduces_from_its_source() {
        // The two transcriptions are compared to each other on a device; this
        // holds the CUDA one to its own source anywhere.
        assert_eq!(
            compile(SMOKE_CU).expect("compile the smoke kernel"),
            SMOKE_PTX
        );
    }
}
