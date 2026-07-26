// Gray-Scott reaction-diffusion: one thread advances one cell one step.
//
//   u' = u + dt * (du * lap(u) - u*v*v + f * (1 - u))
//   v' = v + dt * (dv * lap(v) + u*v*v - (f + k) * v)
//
// lap is the 5-point Laplacian (N + S + E + W - 4*C) with toroidal
// boundaries on a unit lattice. Channel 0 is u, channel 1 is v, cell-major
// interleaved. No clamping: divergence is a phenotype the evaluation layer
// scores, not an error the kernel hides.
//
// A transcription of the WGSL model's `gray_scott.wgsl` with the same
// arithmetic written in the same order, so the two programs' trajectories agree
// to the tolerance their cross-program test holds them to. They are separate
// programs with separate identities: neither reuses the other's results.
//
// Parameters follow the cellular-kind convention: parameter 0 the input grid,
// parameter 1 the output grid, parameter 2 the dimensions [width, height,
// channels], parameter 3 the rates [f, k, du, dv, dt]. One thread per cell
// along x; the bounds guard lets the launch round the block count up past the
// cell count.

extern "C" __global__ void __launch_bounds__(64) main_kernel(
    const float* in_grid,
    float* out_grid,
    const unsigned int* dims,
    const float* rates) {
    unsigned int width = dims[0];
    unsigned int height = dims[1];
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

    // Two channels, cell-major: cell i holds [u, v] at [2i, 2i + 1].
    unsigned int base = cell * 2u;
    unsigned int left_base = (y * width + left) * 2u;
    unsigned int right_base = (y * width + right) * 2u;
    unsigned int up_base = (up * width + x) * 2u;
    unsigned int down_base = (down * width + x) * 2u;

    float u = in_grid[base];
    float v = in_grid[base + 1u];
    float lap_u = in_grid[left_base] + in_grid[right_base]
        + in_grid[up_base] + in_grid[down_base] - 4.0f * u;
    float lap_v = in_grid[left_base + 1u] + in_grid[right_base + 1u]
        + in_grid[up_base + 1u] + in_grid[down_base + 1u] - 4.0f * v;

    float f = rates[0];
    float k = rates[1];
    float du = rates[2];
    float dv = rates[3];
    float dt = rates[4];

    // The model is these three reaction terms; everything above is addressing.
    // uvv is the autocatalysis (v consumes u to make more v), f feeds u back
    // toward 1, and f + k drains v.
    float uvv = u * v * v;
    out_grid[base] = u + dt * (du * lap_u - uvv + f * (1.0f - u));
    out_grid[base + 1u] = v + dt * (dv * lap_v + uvv - (f + k) * v);
}
