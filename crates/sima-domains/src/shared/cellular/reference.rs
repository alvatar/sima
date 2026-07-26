//! [`CellularRule`]: the CPU-reference contract the harness is validated against.

use crate::shared::cellular::Grid;

/// A synchronous cellular update: one step maps a whole input grid to a whole
/// output grid, each output cell a pure function of a neighborhood of the
/// input.
///
/// A test implements this as an independent CPU reference for a WGSL kernel, so
/// the dispatch harness can be cross-checked against it: the reference and the
/// kernel compute the same step, and their resulting grids must agree byte for
/// byte. It is substrate test scaffolding, not a per-model contract — models
/// supply a kernel and genome, never a reference.
pub trait CellularRule {
    /// Computes one step from `input` into `output`. The two grids share
    /// `input`'s dimensions; the step overwrites every cell of `output`.
    fn step(&self, input: &Grid, output: &mut Grid);
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
