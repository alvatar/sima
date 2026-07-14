// Asynchronous Neural Cellular Automaton: one invocation advances one cell one
// step. Composed on top of the shared WGSL SplitMix64 PRNG (prepended at compile
// time), which supplies the U64 type and prng_next / prng_derive.
//
// Per cell, per step:
//   1. perceive: P depthwise 3x3 filters over the 8 state channels of the 3x3
//      toroidal neighborhood -> a perception vector of length C_state*P = 24.
//   2. update net: dense 24->32 with ReLU, then dense 32->8 -> delta state.
//   3. mask: a per-(cell, step) stochastic bit at fire rate 1/2; state channels
//      move by delta only where the mask fires.
//
// Bindings follow the cellular-kind convention: binding 0 the input grid,
// binding 1 the output grid, binding 2 the dimensions [width, height, channels],
// binding 3 the params [dt, then the 1091 genome weights], binding 4 the
// candidate seed as two u32 words [lo, hi], binding 5 the absolute step as two
// u32 words [lo, hi]. The step is supplied by the harness, not carried in the
// grid, so the committed grid is exactly the 8 state channels.

@group(0) @binding(0) var<storage, read> in_grid: array<f32>;
@group(0) @binding(1) var<storage, read_write> out_grid: array<f32>;
@group(0) @binding(2) var<storage, read> dims: array<u32>;
@group(0) @binding(3) var<storage, read> params: array<f32>;
@group(0) @binding(4) var<storage, read> seed_words: array<u32>;
@group(0) @binding(5) var<storage, read> step_words: array<u32>;

const C_STATE: u32 = 8u;
const P: u32 = 3u;
const H: u32 = 32u;
const CHANNELS: u32 = C_STATE; // every grid channel is network state

// Block offsets into the params buffer: dt at 0, then the genome weights in
// their frozen order (perception, W1, b1, W2, b2).
const OFF_DT: u32 = 0u;
const OFF_PERC: u32 = 1u;
const OFF_W1: u32 = OFF_PERC + P * 3u * 3u;      // 1 + 27  = 28
const OFF_B1: u32 = OFF_W1 + (C_STATE * P) * H;  // 28 + 768 = 796
const OFF_W2: u32 = OFF_B1 + H;                  // 796 + 32 = 828
const OFF_B2: u32 = OFF_W2 + H * C_STATE;        // 828 + 256 = 1084

// The mask takes its own purpose stream under the cell's substream, so mask
// draws never coincide with the ignition-noise draws seeded_patch takes with
// counters 0..channels-1.
const MASK_TAG: u32 = 1u;

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
    let base = cell * CHANNELS;
    let dt = params[OFF_DT];

    // Perception: perc[c*P + p] is filter p's response on state channel c.
    var perc = array<f32, C_STATE * P>();
    // Accumulate over the 3x3 toroidal neighborhood; the `+ extent + d - 1` form
    // keeps the offset in unsigned range for d in {0, 1, 2} (taps -1, 0, +1).
    for (var dy = 0u; dy < 3u; dy = dy + 1u) {
        let ny = (y + height + dy - 1u) % height;
        for (var dx = 0u; dx < 3u; dx = dx + 1u) {
            let nx = (x + width + dx - 1u) % width;
            let nbase = (ny * width + nx) * CHANNELS;
            let tap = dy * 3u + dx; // 0..9 within a filter
            for (var p = 0u; p < P; p = p + 1u) {
                let w = params[OFF_PERC + p * 9u + tap];
                for (var c = 0u; c < C_STATE; c = c + 1u) {
                    perc[c * P + p] = perc[c * P + p] + w * in_grid[nbase + c];
                }
            }
        }
    }

    // Dense 24 -> 32 with ReLU. W1 is input-major: W1[i*H + h].
    var hidden: array<f32, H>;
    for (var h = 0u; h < H; h = h + 1u) {
        var acc = params[OFF_B1 + h];
        for (var i = 0u; i < C_STATE * P; i = i + 1u) {
            acc = acc + params[OFF_W1 + i * H + h] * perc[i];
        }
        hidden[h] = max(acc, 0.0);
    }

    // The mask fires when unit_f64(r) < 0.5, which at fire rate 1/2 is exactly
    // bit 63 of r being 0 — a single high-bit test, no float arithmetic. The
    // step is the absolute trajectory step the harness supplied, keyed as an
    // integer, so the mask sequence is exact for the whole trajectory.
    let seed = U64(seed_words[0], seed_words[1]);
    let step = U64(step_words[0], step_words[1]);
    let cell_stream = prng_derive(seed, U64(cell, 0u));
    let mask_stream = prng_derive(cell_stream, U64(MASK_TAG, 0u));
    let r = prng_next(mask_stream, step);
    let fires = (r.hi >> 31u) == 0u;
    let m = select(0.0, 1.0, fires);

    // Dense 32 -> 8. W2 is hidden-major: W2[h*C_state + c]. State channels move
    // by the residual only where the mask fires.
    for (var c = 0u; c < C_STATE; c = c + 1u) {
        var acc = params[OFF_B2 + c];
        for (var h = 0u; h < H; h = h + 1u) {
            acc = acc + params[OFF_W2 + h * C_STATE + c] * hidden[h];
        }
        out_grid[base + c] = in_grid[base + c] + m * dt * acc;
    }
}
