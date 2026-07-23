// The per-candidate stats reduction over a final grid pair.
//
// One standalone kernel with four compute entry points, dispatched in order;
// the toolkit's cross-dispatch barrier makes each pass's writes visible to the
// next. The topology is fixed — a constant partition count — so the result is
// deterministic per backend: every sum is accumulated in the same order.
//
// - pass1 (level 1): each of `partitions` invocations folds one contiguous
//   chunk of cells into a partials slot — per-channel sum, min, max, plus the
//   alive count and the activity sum.
// - combine1 (level 2): one invocation folds the partials in index order into
//   the per-channel mean, min, and max, the population, and the activity, and
//   publishes the means for the variance pass.
// - pass2 (level 1, second variance pass): each invocation folds squared
//   deviations from the published means over its chunk.
// - combine2 (level 2): one invocation folds those into the per-channel
//   variance.
//
// Every entry point binds the same seven group-0 storage buffers, since the
// toolkit reflects one descriptor set for the whole module; a pass ignores the
// buffers it does not read.

// The upper bound on channels a model may declare; the per-channel scratch
// arrays are sized to it.
const MAX_CHANNELS: u32 = 16u;

@group(0) @binding(0) var<storage, read> grid_n: array<f32>;
@group(0) @binding(1) var<storage, read> grid_prev: array<f32>;
// [channels, cell_count, alive_channel, alive_min bits, partitions].
@group(0) @binding(2) var<storage, read> params: array<u32>;
@group(0) @binding(3) var<storage, read_write> partials: array<f32>;
@group(0) @binding(4) var<storage, read_write> means: array<f32>;
@group(0) @binding(5) var<storage, read_write> partials2: array<f32>;
// [c0.mean, c0.var, c0.min, c0.max, ..., population, activity].
@group(0) @binding(6) var<storage, read_write> out: array<f32>;

fn channels() -> u32 { return params[0]; }
fn cell_count() -> u32 { return params[1]; }
fn alive_channel() -> u32 { return params[2]; }
fn alive_min() -> f32 { return bitcast<f32>(params[3]); }
fn partitions() -> u32 { return params[4]; }

// The half-open cell range partition `p` folds: contiguous chunks sized so the
// last partition absorbs the remainder.
fn chunk_range(p: u32) -> vec2<u32> {
    let n = cell_count();
    let chunk = (n + partitions() - 1u) / partitions();
    let start = p * chunk;
    var end = start + chunk;
    if (end > n) { end = n; }
    return vec2<u32>(start, end);
}

// The partials stride: per channel a sum, a min, and a max, then the alive
// count and the activity sum.
fn stride() -> u32 { return 3u * channels() + 2u; }

@compute @workgroup_size(64)
fn pass1(@builtin(global_invocation_id) gid: vec3<u32>) {
    let p = gid.x;
    if (p >= partitions()) { return; }
    let c_count = channels();
    let range = chunk_range(p);

    var sum: array<f32, 16>;
    var lo: array<f32, 16>;
    var hi: array<f32, 16>;
    for (var c = 0u; c < c_count; c = c + 1u) {
        sum[c] = 0.0;
        lo[c] = bitcast<f32>(0x7f800000u);
        hi[c] = bitcast<f32>(0xff800000u);
    }
    var alive = 0.0;
    var activity = 0.0;
    let ac = alive_channel();
    let amin = alive_min();
    for (var cell = range.x; cell < range.y; cell = cell + 1u) {
        let base = cell * c_count;
        for (var c = 0u; c < c_count; c = c + 1u) {
            let v = grid_n[base + c];
            sum[c] = sum[c] + v;
            lo[c] = min(lo[c], v);
            hi[c] = max(hi[c], v);
            activity = activity + abs(v - grid_prev[base + c]);
        }
        if (grid_n[base + ac] >= amin) {
            alive = alive + 1.0;
        }
    }

    let off = p * stride();
    for (var c = 0u; c < c_count; c = c + 1u) {
        partials[off + c] = sum[c];
        partials[off + c_count + c] = lo[c];
        partials[off + 2u * c_count + c] = hi[c];
    }
    partials[off + 3u * c_count] = alive;
    partials[off + 3u * c_count + 1u] = activity;
}

@compute @workgroup_size(1)
fn combine1(@builtin(global_invocation_id) gid: vec3<u32>) {
    if (gid.x != 0u) { return; }
    let c_count = channels();
    let n = f32(cell_count());
    let s = stride();

    for (var c = 0u; c < c_count; c = c + 1u) {
        var total = 0.0;
        var lo = bitcast<f32>(0x7f800000u);
        var hi = bitcast<f32>(0xff800000u);
        for (var p = 0u; p < partitions(); p = p + 1u) {
            let off = p * s;
            total = total + partials[off + c];
            lo = min(lo, partials[off + c_count + c]);
            hi = max(hi, partials[off + 2u * c_count + c]);
        }
        let mean = total / n;
        means[c] = mean;
        out[c * 4u] = mean;
        out[c * 4u + 2u] = lo;
        out[c * 4u + 3u] = hi;
    }

    var alive = 0.0;
    var activity = 0.0;
    for (var p = 0u; p < partitions(); p = p + 1u) {
        let off = p * s;
        alive = alive + partials[off + 3u * c_count];
        activity = activity + partials[off + 3u * c_count + 1u];
    }
    out[c_count * 4u] = alive / n;
    out[c_count * 4u + 1u] = activity / (n * f32(c_count));
}

@compute @workgroup_size(64)
fn pass2(@builtin(global_invocation_id) gid: vec3<u32>) {
    let p = gid.x;
    if (p >= partitions()) { return; }
    let c_count = channels();
    let range = chunk_range(p);

    var sq: array<f32, 16>;
    for (var c = 0u; c < c_count; c = c + 1u) { sq[c] = 0.0; }
    for (var cell = range.x; cell < range.y; cell = cell + 1u) {
        let base = cell * c_count;
        for (var c = 0u; c < c_count; c = c + 1u) {
            let d = grid_n[base + c] - means[c];
            sq[c] = sq[c] + d * d;
        }
    }
    for (var c = 0u; c < c_count; c = c + 1u) {
        partials2[p * c_count + c] = sq[c];
    }
}

@compute @workgroup_size(1)
fn combine2(@builtin(global_invocation_id) gid: vec3<u32>) {
    if (gid.x != 0u) { return; }
    let c_count = channels();
    let n = f32(cell_count());
    for (var c = 0u; c < c_count; c = c + 1u) {
        var total = 0.0;
        for (var p = 0u; p < partitions(); p = p + 1u) {
            total = total + partials2[p * c_count + c];
        }
        out[c * 4u + 1u] = total / n;
    }
}
