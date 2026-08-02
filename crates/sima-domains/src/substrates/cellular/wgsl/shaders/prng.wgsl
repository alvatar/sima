// The GPU counterpart of sima_core::prng: the counter-based SplitMix64
// reproduced in WGSL, bit-identical to the CPU implementation. Core WGSL has no
// 64-bit integer, so a u64 is emulated as a pair of u32 (lo, hi) and every
// operation keeps the low 64 bits, wrapping mod 2^64 exactly as the CPU does.
//
// This snippet declares no bindings and no entry point, so it composes into any
// cellular kernel that needs result-affecting randomness. It exposes prng_next
// and prng_derive; the u64 helpers below implement the arithmetic they need.

// A 64-bit unsigned integer as low and high 32-bit halves.
struct U64 {
    lo: u32,
    hi: u32,
}

// a ^ b.
fn u64_xor(a: U64, b: U64) -> U64 {
    return U64(a.lo ^ b.lo, a.hi ^ b.hi);
}

// a + b mod 2^64, carrying from the low half into the high half.
fn u64_add(a: U64, b: U64) -> U64 {
    let lo = a.lo + b.lo;
    // u32 addition wraps, so a smaller sum than an addend means it overflowed.
    let carry = select(0u, 1u, lo < a.lo);
    return U64(lo, a.hi + b.hi + carry);
}

// a >> n for 0 <= n < 64, shifting the pair as one 64-bit value.
fn u64_shr(a: U64, n: u32) -> U64 {
    if (n == 0u) {
        return a;
    }
    if (n < 32u) {
        // Bits shifted out of the high half fall into the top of the low half.
        return U64((a.lo >> n) | (a.hi << (32u - n)), a.hi >> n);
    }
    // 32 <= n < 64: the high half becomes the low half, then shifts further.
    return U64(a.hi >> (n - 32u), 0u);
}

// The exact 64-bit product of two u32, via 16-bit partial products so no
// intermediate exceeds 32 bits without its carry being tracked.
fn u64_mul_wide(a: u32, b: u32) -> U64 {
    let a0 = a & 0xFFFFu;
    let a1 = a >> 16u;
    let b0 = b & 0xFFFFu;
    let b1 = b >> 16u;

    let c0 = a0 * b0;
    let c2 = a1 * b1;
    // The two cross terms, summed with an explicit carry into bit 32.
    let t0 = a1 * b0;
    let t1 = a0 * b1;
    let cross_lo = t0 + t1;
    let cross_carry = select(0u, 1u, cross_lo < t0);

    // cross contributes cross_lo << 16 (low 16 into the low half's top, high 16
    // into the high half) and cross_carry into bit 48 (high half bit 16).
    let shifted = cross_lo << 16u;
    let lo = c0 + shifted;
    let carry1 = select(0u, 1u, lo < c0);
    let hi = c2 + (cross_lo >> 16u) + (cross_carry << 16u) + carry1;
    return U64(lo, hi);
}

// a * b mod 2^64. Terms of weight >= 2^64 vanish, so only a.lo*b.lo (full 64)
// and the low 32 bits of (a.lo*b.hi + a.hi*b.lo) shifted into the high half
// survive.
fn u64_mul(a: U64, b: U64) -> U64 {
    let ll = u64_mul_wide(a.lo, b.lo);
    let cross = a.lo * b.hi + a.hi * b.lo;
    return U64(ll.lo, ll.hi + cross);
}

// SplitMix64 finalizer: xor-shift and multiply avalanche, from the published
// algorithm, matching sima_core::prng::mix.
fn splitmix_mix(z0: U64) -> U64 {
    var z = z0;
    z = u64_xor(z, u64_shr(z, 30u));
    z = u64_mul(z, U64(0x1CE4E5B9u, 0xBF58476Du)); // 0xBF58476D1CE4E5B9
    z = u64_xor(z, u64_shr(z, 27u));
    z = u64_mul(z, U64(0x133111EBu, 0x94D049BBu)); // 0x94D049BB133111EB
    z = u64_xor(z, u64_shr(z, 31u));
    return z;
}

// next(seed, counter) = mix(seed + (counter + 1) * GOLDEN), the counter-th
// output of the SplitMix64 stream for seed. GOLDEN = 0x9E3779B97F4A7C15.
fn prng_next(seed: U64, counter: U64) -> U64 {
    let stepped = u64_mul(u64_add(counter, U64(1u, 0u)), U64(0x7F4A7C15u, 0x9E3779B9u));
    return splitmix_mix(u64_add(seed, stepped));
}

// derive(seed, tag) = mix(seed ^ mix(tag)), a decorrelated substream seed.
fn prng_derive(seed: U64, tag: U64) -> U64 {
    return splitmix_mix(u64_xor(seed, splitmix_mix(tag)));
}
