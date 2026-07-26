// Exercises the cellular compute path: a 5-point neighborhood max over a
// multi-channel grid with toroidal (wrap-around) boundaries. For each cell and
// each channel independently, the output is the maximum of that channel over
// the cell and its four axis neighbors, indices wrapping modulo width/height.
//
// A transcription of `shaders/smoke.wgsl` with the same arithmetic, so a grid
// advanced by either substrate lands on the same values and the two engines can
// be compared over it.
//
// Parameters follow the cellular-kind convention: parameter 0 the input grid,
// parameter 1 the output grid, parameter 2 the dimensions [width, height,
// channels]. One thread per cell along x; the bounds guard lets the launch
// round the block count up past the cell count. A cell loops its channels.

extern "C" __global__ void __launch_bounds__(64) main_kernel(
    const float* in_grid,
    float* out_grid,
    const unsigned int* dims) {
    unsigned int width = dims[0];
    unsigned int height = dims[1];
    unsigned int channels = dims[2];
    unsigned int cell = blockIdx.x * blockDim.x + threadIdx.x;
    if (cell >= width * height) {
        return;
    }

    unsigned int x = cell % width;
    unsigned int y = cell / width;
    // Toroidal neighbor coordinates; the `+ extent - 1` form keeps the
    // subtraction in unsigned range.
    unsigned int left = (x + width - 1u) % width;
    unsigned int right = (x + 1u) % width;
    unsigned int up = (y + height - 1u) % height;
    unsigned int down = (y + 1u) % height;

    unsigned int base = cell * channels;
    unsigned int left_base = (y * width + left) * channels;
    unsigned int right_base = (y * width + right) * channels;
    unsigned int up_base = (up * width + x) * channels;
    unsigned int down_base = (down * width + x) * channels;

    for (unsigned int c = 0u; c < channels; c = c + 1u) {
        float m = in_grid[base + c];
        m = fmaxf(m, in_grid[left_base + c]);
        m = fmaxf(m, in_grid[right_base + c]);
        m = fmaxf(m, in_grid[up_base + c]);
        m = fmaxf(m, in_grid[down_base + c]);
        out_grid[base + c] = m;
    }
}
