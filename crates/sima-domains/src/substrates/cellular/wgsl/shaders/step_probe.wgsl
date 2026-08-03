// Exercises the per-step index the harness rewrites inside each dispatch's own
// submission: every cell accumulates the low word of the step index it was
// dispatched with, so after `steps` dispatches from `base` each cell holds the
// sum of `base ..= base + steps - 1`. Accumulating is what makes every
// dispatch's update contribute, so a wrong intermediate index is caught, not
// only a wrong final one.
//
// Bindings follow the cellular-kind convention: 0 the input grid, 1 the output
// grid, 2 the dimensions, and — this probe declaring no family parameters —
// 3 the per-step index as two words.

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
