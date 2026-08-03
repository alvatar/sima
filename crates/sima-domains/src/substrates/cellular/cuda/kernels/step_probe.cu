// Exercises the per-step index the harness rewrites inside each dispatch's own
// submission: every cell accumulates the low word of the step index it was
// dispatched with, so after `steps` dispatches from `base` each cell holds the
// sum of `base ..= base + steps - 1`. Accumulating is what makes every
// dispatch's update contribute, so a wrong intermediate index is caught, not
// only a wrong final one.
//
// A transcription of `shaders/step_probe.wgsl` with the same arithmetic, so one
// test case runs on either backend.
//
// Parameters follow the cellular-kind convention: parameter 0 the input grid,
// parameter 1 the output grid, parameter 2 the dimensions [width, height,
// channels], and — this probe declaring no family parameters — parameter 3 the
// per-step index as two words. One thread per cell along x.

extern "C" __global__ void __launch_bounds__(64) main_kernel(
    const float* in_grid,
    float* out_grid,
    const unsigned int* dims,
    const unsigned int* step_words) {
    unsigned int cell = blockIdx.x * blockDim.x + threadIdx.x;
    if (cell >= dims[0] * dims[1]) {
        return;
    }
    // The test keeps the running sum well under 2^24, so the float holds it
    // exactly and the two backends agree bit for bit.
    out_grid[cell] = in_grid[cell] + (float)step_words[0];
}
