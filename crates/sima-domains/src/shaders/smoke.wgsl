// Exercises the stencil compute path: a 5-point neighborhood max over a
// multi-channel grid with toroidal (wrap-around) boundaries. For each cell and
// each channel independently, the output is the maximum of that channel over
// the cell and its four axis neighbors, indices wrapping modulo width/height.
//
// Bindings follow the stencil-kind convention: binding 0 the input grid,
// binding 1 the output grid, binding 2 the dimensions [width, height,
// channels]. One invocation per cell along x; the bounds guard lets the launch
// round the workgroup count up past the cell count. A cell loops its channels.

@group(0) @binding(0) var<storage, read> in_grid: array<f32>;
@group(0) @binding(1) var<storage, read_write> out_grid: array<f32>;
@group(0) @binding(2) var<storage, read> dims: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let width = dims[0];
    let height = dims[1];
    let channels = dims[2];
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

    let base = cell * channels;
    let left_base = (y * width + left) * channels;
    let right_base = (y * width + right) * channels;
    let up_base = (up * width + x) * channels;
    let down_base = (down * width + x) * channels;

    for (var c = 0u; c < channels; c = c + 1u) {
        var m = in_grid[base + c];
        m = max(m, in_grid[left_base + c]);
        m = max(m, in_grid[right_base + c]);
        m = max(m, in_grid[up_base + c]);
        m = max(m, in_grid[down_base + c]);
        out_grid[base + c] = m;
    }
}
