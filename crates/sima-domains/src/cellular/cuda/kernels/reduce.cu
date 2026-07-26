// The per-candidate stats reduction over a final grid pair.
//
// A transcription of `cellular/shaders/reduce.wgsl`: the same four entry
// points dispatched in order, the same fixed partition count, the same chunk
// boundaries, and the same output layout. Both substrates therefore fold every
// sum in the same order, and their scalars agree to the tolerance the
// cross-program test holds them to.
//
// - pass1 (level 1): each of `partitions` threads folds one contiguous chunk
//   of cells into a partials slot — per-channel sum, min, max, plus the alive
//   count and the activity sum.
// - combine1 (level 2): one thread folds the partials in index order into the
//   per-channel mean, min, and max, the population, and the activity, and
//   publishes the means for the variance pass.
// - pass2 (level 1, second variance pass): each thread folds squared
//   deviations from the published means over its chunk.
// - combine2 (level 2): one thread folds those into the per-channel variance.
//
// Every entry point takes the same seven pointers even where it reads only
// some of them, mirroring the WGSL module's single descriptor set: the Rust
// dispatch binds one buffer list for all four passes, and a pass ignores what
// it does not read. Each launch is separated by a stream synchronization, which
// is what makes a pass's writes visible to the next.

// The upper bound on channels a model may declare; the per-channel scratch
// arrays are sized to it.
#define MAX_CHANNELS 16

// The parameter block, [channels, cell_count, alive_channel, alive_min bits,
// partitions], read through named accessors as in the WGSL source.
__device__ __forceinline__ unsigned int channels(const unsigned int* params) { return params[0]; }
__device__ __forceinline__ unsigned int cell_count(const unsigned int* params) { return params[1]; }
__device__ __forceinline__ unsigned int alive_channel(const unsigned int* params) { return params[2]; }
__device__ __forceinline__ float alive_min(const unsigned int* params) { return __uint_as_float(params[3]); }
__device__ __forceinline__ unsigned int partitions(const unsigned int* params) { return params[4]; }

// The half-open cell range partition `p` folds: contiguous chunks sized so the
// last partition absorbs the remainder.
struct Range {
    unsigned int start;
    unsigned int end;
};

__device__ __forceinline__ Range chunk_range(const unsigned int* params, unsigned int p) {
    unsigned int n = cell_count(params);
    unsigned int chunk = (n + partitions(params) - 1u) / partitions(params);
    Range range;
    range.start = p * chunk;
    range.end = range.start + chunk;
    if (range.end > n) { range.end = n; }
    return range;
}

// The partials stride: per channel a sum, a min, and a max, then the alive
// count and the activity sum.
__device__ __forceinline__ unsigned int stride(const unsigned int* params) {
    return 3u * channels(params) + 2u;
}

extern "C" __global__ void __launch_bounds__(64) pass1(
    const float* grid_n,
    const float* grid_prev,
    const unsigned int* params,
    float* partials,
    float* means,
    float* partials2,
    float* out) {
    unsigned int p = blockIdx.x * blockDim.x + threadIdx.x;
    if (p >= partitions(params)) { return; }
    unsigned int c_count = channels(params);
    Range range = chunk_range(params, p);

    float sum[MAX_CHANNELS];
    float lo[MAX_CHANNELS];
    float hi[MAX_CHANNELS];
    for (unsigned int c = 0u; c < c_count; c = c + 1u) {
        sum[c] = 0.0f;
        lo[c] = __uint_as_float(0x7f800000u);
        hi[c] = __uint_as_float(0xff800000u);
    }
    float alive = 0.0f;
    float activity = 0.0f;
    unsigned int ac = alive_channel(params);
    float amin = alive_min(params);
    for (unsigned int cell = range.start; cell < range.end; cell = cell + 1u) {
        unsigned int base = cell * c_count;
        for (unsigned int c = 0u; c < c_count; c = c + 1u) {
            float v = grid_n[base + c];
            sum[c] = sum[c] + v;
            // fminf and fmaxf return the non-NaN operand, where WGSL's min and
            // max leave the choice to the backend. Neither substrate is relied
            // on for NaN handling: the snapshot predicate's all-finite check at
            // the Rust layer is what catches a diverged grid.
            lo[c] = fminf(lo[c], v);
            hi[c] = fmaxf(hi[c], v);
            activity = activity + fabsf(v - grid_prev[base + c]);
        }
        if (grid_n[base + ac] >= amin) {
            alive = alive + 1.0f;
        }
    }

    unsigned int off = p * stride(params);
    for (unsigned int c = 0u; c < c_count; c = c + 1u) {
        partials[off + c] = sum[c];
        partials[off + c_count + c] = lo[c];
        partials[off + 2u * c_count + c] = hi[c];
    }
    partials[off + 3u * c_count] = alive;
    partials[off + 3u * c_count + 1u] = activity;
}

extern "C" __global__ void __launch_bounds__(1) combine1(
    const float* grid_n,
    const float* grid_prev,
    const unsigned int* params,
    float* partials,
    float* means,
    float* partials2,
    float* out) {
    if (blockIdx.x * blockDim.x + threadIdx.x != 0u) { return; }
    unsigned int c_count = channels(params);
    float n = (float)cell_count(params);
    unsigned int s = stride(params);

    for (unsigned int c = 0u; c < c_count; c = c + 1u) {
        float total = 0.0f;
        float lo = __uint_as_float(0x7f800000u);
        float hi = __uint_as_float(0xff800000u);
        for (unsigned int p = 0u; p < partitions(params); p = p + 1u) {
            unsigned int off = p * s;
            total = total + partials[off + c];
            lo = fminf(lo, partials[off + c_count + c]);
            hi = fmaxf(hi, partials[off + 2u * c_count + c]);
        }
        float mean = total / n;
        means[c] = mean;
        out[c * 4u] = mean;
        out[c * 4u + 2u] = lo;
        out[c * 4u + 3u] = hi;
    }

    float alive = 0.0f;
    float activity = 0.0f;
    for (unsigned int p = 0u; p < partitions(params); p = p + 1u) {
        unsigned int off = p * s;
        alive = alive + partials[off + 3u * c_count];
        activity = activity + partials[off + 3u * c_count + 1u];
    }
    out[c_count * 4u] = alive / n;
    out[c_count * 4u + 1u] = activity / (n * (float)c_count);
}

extern "C" __global__ void __launch_bounds__(64) pass2(
    const float* grid_n,
    const float* grid_prev,
    const unsigned int* params,
    float* partials,
    float* means,
    float* partials2,
    float* out) {
    unsigned int p = blockIdx.x * blockDim.x + threadIdx.x;
    if (p >= partitions(params)) { return; }
    unsigned int c_count = channels(params);
    Range range = chunk_range(params, p);

    float sq[MAX_CHANNELS];
    for (unsigned int c = 0u; c < c_count; c = c + 1u) { sq[c] = 0.0f; }
    for (unsigned int cell = range.start; cell < range.end; cell = cell + 1u) {
        unsigned int base = cell * c_count;
        for (unsigned int c = 0u; c < c_count; c = c + 1u) {
            float d = grid_n[base + c] - means[c];
            sq[c] = sq[c] + d * d;
        }
    }
    for (unsigned int c = 0u; c < c_count; c = c + 1u) {
        partials2[p * c_count + c] = sq[c];
    }
}

extern "C" __global__ void __launch_bounds__(1) combine2(
    const float* grid_n,
    const float* grid_prev,
    const unsigned int* params,
    float* partials,
    float* means,
    float* partials2,
    float* out) {
    if (blockIdx.x * blockDim.x + threadIdx.x != 0u) { return; }
    unsigned int c_count = channels(params);
    float n = (float)cell_count(params);
    for (unsigned int c = 0u; c < c_count; c = c + 1u) {
        float total = 0.0f;
        for (unsigned int p = 0u; p < partitions(params); p = p + 1u) {
            total = total + partials2[p * c_count + c];
        }
        out[c * 4u + 1u] = total / n;
    }
}
