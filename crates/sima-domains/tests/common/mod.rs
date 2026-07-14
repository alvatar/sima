//! Shared support for the sima-domains integration tests.

use sima_domains::cellular::{CellularRule, Grid, run};
use sima_toolkit_wgsl::{Context, Kernel};

/// Advances `initial` by `steps` both ways — through the CPU `rule` and through
/// the GPU harness with `kernel` — and asserts the two grids are byte-identical.
///
/// A cellular family uses this to confirm its WGSL kernel matches its CPU
/// reference: the reference and the kernel compute the same step, so their
/// resulting grids must agree byte for byte.
pub fn cross_check(
    context: &Context,
    kernel: &Kernel,
    rule: &impl CellularRule,
    initial: &Grid,
    steps: u32,
) {
    // CPU reference: ping-pong two grids so `current` holds the newest state,
    // matching the harness's own double buffering.
    let mut current = initial.clone();
    let mut next = initial.clone();
    for _ in 0..steps {
        rule.step(&current, &mut next);
        std::mem::swap(&mut current, &mut next);
    }

    let gpu = run(context, kernel, initial, steps, &[], None).expect("harness run");
    assert_eq!(
        gpu.to_bytes(),
        current.to_bytes(),
        "GPU harness and CPU reference disagree after {steps} steps"
    );
}
