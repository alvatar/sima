// Gray-Scott reaction-diffusion: one invocation advances one cell one step.
//
//   u' = u + dt * (du * lap(u) - u*v*v + f * (1 - u))
//   v' = v + dt * (dv * lap(v) + u*v*v - (f + k) * v)
//
// lap is the 5-point Laplacian (N + S + E + W - 4*C) with toroidal
// boundaries on a unit lattice. Channel 0 is u, channel 1 is v, cell-major
// interleaved. No clamping: divergence is a phenotype the evaluation layer
// scores, not an error the kernel hides.
//
// Bindings follow the cellular-kind convention: binding 0 the input grid,
// binding 1 the output grid, binding 2 the dimensions [width, height,
// channels], binding 3 the rates [f, k, du, dv, dt]. One invocation per
// cell along x; the bounds guard lets the launch round the workgroup count
// up past the cell count.

@group(0) @binding(0) var<storage, read> in_grid: array<f32>;
@group(0) @binding(1) var<storage, read_write> out_grid: array<f32>;
@group(0) @binding(2) var<storage, read> dims: array<u32>;
@group(0) @binding(3) var<storage, read> rates: array<f32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let width = dims[0];
    let height = dims[1];
    let cell = gid.x;
    if (cell >= width * height) {
        return;
    }

    let x = cell % width;
    let y = cell / width;
    // Toroidal neighbor coordinates; the `+ extent - 1` form keeps the
    // subtraction in unsigned range.
    let left = (x + width - 1u) % width;
    let right = (x + 1u) % width;
    let up = (y + height - 1u) % height;
    let down = (y + 1u) % height;

    // Two channels, cell-major: cell i holds [u, v] at [2i, 2i + 1].
    let base = cell * 2u;
    let left_base = (y * width + left) * 2u;
    let right_base = (y * width + right) * 2u;
    let up_base = (up * width + x) * 2u;
    let down_base = (down * width + x) * 2u;

    let u = in_grid[base];
    let v = in_grid[base + 1u];
    let lap_u = in_grid[left_base] + in_grid[right_base]
        + in_grid[up_base] + in_grid[down_base] - 4.0 * u;
    let lap_v = in_grid[left_base + 1u] + in_grid[right_base + 1u]
        + in_grid[up_base + 1u] + in_grid[down_base + 1u] - 4.0 * v;

    let f = rates[0];
    let k = rates[1];
    let du = rates[2];
    let dv = rates[3];
    let dt = rates[4];

    let uvv = u * v * v;
    out_grid[base] = u + dt * (du * lap_u - uvv + f * (1.0 - u));
    out_grid[base + 1u] = v + dt * (dv * lap_v + uvv - (f + k) * v);
}
