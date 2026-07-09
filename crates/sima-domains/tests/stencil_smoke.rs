//! End-to-end check of the stencil path: the GPU harness advancing a grid with
//! the neighborhood-max kernel produces byte-identical results to the CPU
//! reference over several steps, on a real device.

mod common;

use sima_domains::stencil::{Grid, StencilRule};
use sima_toolkit_wgsl::Context;

/// The CPU reference matching `shaders/smoke.wgsl`: a 5-point neighborhood max
/// with toroidal boundaries, each channel reduced independently.
struct SmokeMax;

impl StencilRule for SmokeMax {
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

/// The kernel the reference mirrors.
const SMOKE_WGSL: &str = include_str!("../shaders/smoke.wgsl");

/// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
#[test]
#[ignore = "requires a Vulkan device"]
fn harness_matches_reference_over_k_steps() {
    let context = Context::new().expect("create compute context");
    let kernel = context
        .kernel(SMOKE_WGSL, "main")
        .expect("build smoke kernel");

    // Distinct per-cell values so the neighborhood max is non-trivial; several
    // steps exercise the ping-pong across dispatches. Byte-exact equality here
    // is what lets the smoke kernel need no tolerance policy.
    let width = 8u32;
    let height = 6u32;
    let channels = 3u32;
    let count = (width * height * channels) as usize;
    let data: Vec<f32> = (0..count).map(|i| ((i * 37) % 101) as f32).collect();
    let initial = Grid::new(width, height, channels, data).expect("grid");

    common::cross_check(&context, &kernel, &SmokeMax, &initial, 5);
}
