//! The GPU counterpart of [`sima_core::prng`]: the counter-based SplitMix64
//! reproduced in WGSL, bit-identical to the CPU implementation. Any cellular
//! kernel needing result-affecting randomness composes this source.
//!
//! The kernels compose the snippet with `include_str!("shaders/prng.wgsl")`
//! (a string literal, which `concat!` in a `const KERNEL_WGSL` requires); the
//! [`PRNG_WGSL`] handle names the same file as the substrate home and anchors
//! the parity test below.

/// The shared WGSL SplitMix64 snippet: the u64 emulation plus `prng_next` and
/// `prng_derive`, with no bindings and no entry point. Its only consumer is the
/// parity test, which composes it into a probe kernel; production kernels
/// `include_str!` the same file directly.
#[cfg(test)]
const PRNG_WGSL: &str = include_str!("shaders/prng.wgsl");

#[cfg(test)]
mod tests {
    use sima_core::prng;

    use super::PRNG_WGSL;

    // Probe operation codes for the u64 emulation helpers, dispatched by the
    // `op` field of a request row. The two composed PRNG entries take the
    // reserved codes 0 and 1, spelled as literals in the parity test.
    const OP_ADD: u32 = 2;
    const OP_SHR: u32 = 3;
    const OP_MUL: u32 = 4;
    const OP_MUL_WIDE: u32 = 5;

    /// A probe kernel over the shared snippet: one invocation per request, each
    /// request `(op, a, b)` five u32 wide (op, then the two u64 operands as
    /// low/high words), writing the 64-bit result as two u32. `op` selects the
    /// function: 0 and 1 the composed PRNG entries `prng_next` and `prng_derive`,
    /// [`OP_ADD`]/[`OP_SHR`]/[`OP_MUL`]/[`OP_MUL_WIDE`] the u64 helpers. `u64_shr`
    /// takes its shift amount from `b`'s low word; `u64_mul_wide` takes its two
    /// u32 operands from the low words of `a` and `b`.
    const PROBE_MAIN: &str = r#"
@group(0) @binding(0) var<storage, read> requests: array<u32>;
@group(0) @binding(1) var<storage, read_write> results: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    let n = arrayLength(&results) / 2u;
    if (i >= n) { return; }
    let base = i * 5u;
    let op = requests[base];
    let a = U64(requests[base + 1u], requests[base + 2u]);
    let b = U64(requests[base + 3u], requests[base + 4u]);
    var r: U64;
    if (op == 0u) {
        r = prng_next(a, b);
    } else if (op == 1u) {
        r = prng_derive(a, b);
    } else if (op == 2u) {
        r = u64_add(a, b);
    } else if (op == 3u) {
        r = u64_shr(a, b.lo);
    } else if (op == 4u) {
        r = u64_mul(a, b);
    } else {
        r = u64_mul_wide(a.lo, b.lo);
    }
    results[i * 2u] = r.lo;
    results[i * 2u + 1u] = r.hi;
}
"#;

    /// The composed probe kernel source: the shared snippet then the probe entry.
    fn probe_source() -> String {
        format!("{PRNG_WGSL}{PROBE_MAIN}")
    }

    /// One request row `(op, a, b)` packed as five little-endian u32 words.
    fn request(op: u32, a: u64, b: u64) -> [u32; 5] {
        [op, a as u32, (a >> 32) as u32, b as u32, (b >> 32) as u32]
    }

    /// Dispatches the probe kernel over `requests` (five u32 per row) and returns
    /// one u64 result per row, reassembled from the two u32 result words.
    fn dispatch_probe(requests: &[u32]) -> Vec<u64> {
        use sima_toolkit_wgsl::Context;

        assert_eq!(requests.len() % 5, 0, "each request row is five u32 wide");
        let rows = requests.len() / 5;
        let context = Context::new().expect("create compute context");
        let kernel = context
            .kernel(&probe_source(), "main")
            .expect("build probe kernel");
        let request_bytes: &[u8] = bytemuck::cast_slice(requests);
        let request_buffer = context.buffer(request_bytes.len()).expect("request buffer");
        context
            .upload(&request_buffer, request_bytes)
            .expect("upload requests");
        let result_buffer = context
            .buffer(rows * 2 * std::mem::size_of::<u32>())
            .expect("result buffer");
        let groups = [(rows as u32).div_ceil(64), 1, 1];
        context
            .dispatch(&kernel, &[&request_buffer, &result_buffer], groups)
            .expect("dispatch probe");
        let bytes = context.download(&result_buffer).expect("download results");
        let words: &[u32] = bytemuck::cast_slice(&bytes);
        (0..rows)
            .map(|i| words[i * 2] as u64 | (words[i * 2 + 1] as u64) << 32)
            .collect()
    }

    #[test]
    fn the_probe_kernel_compiles_device_free() {
        sima_toolkit_wgsl::check(&probe_source(), "main").expect("probe kernel compiles");
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    ///
    /// Proves the WGSL PRNG equals [`sima_core::prng`] bit-for-bit: it runs
    /// `prng_next`/`prng_derive` on the GPU for a spread of inputs, including the
    /// three published known answers, and compares every 64-bit result against
    /// the CPU implementation and the pinned values.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn the_wgsl_prng_matches_sima_core() {
        // op 0 = next(seed, counter); op 1 = derive(seed, tag). The first three
        // rows are the published known answers; the rest are a spread.
        let cases: [(u32, u64, u64); 12] = [
            (0, 0, 0),
            (1, 0, 1),
            (1, 1, 1),
            (0, 0, 1),
            (0, 1, 0),
            (0, 42, 7),
            (0, u64::MAX, 3),
            (0, 0x0123_4567_89AB_CDEF, 100),
            (1, 0, 2),
            (1, 42, 5),
            (1, u64::MAX, u64::MAX),
            (1, 0x0123_4567_89AB_CDEF, 0xFEDC_BA98),
        ];

        let mut requests: Vec<u32> = Vec::with_capacity(cases.len() * 5);
        for &(op, seed, arg) in &cases {
            requests.extend_from_slice(&request(op, seed, arg));
        }
        let results = dispatch_probe(&requests);

        for (i, &(op, seed, arg)) in cases.iter().enumerate() {
            let expected = if op == 0 {
                prng::next(seed, arg)
            } else {
                prng::derive(seed, arg)
            };
            assert_eq!(
                results[i], expected,
                "case {i} (op {op}, seed {seed}, arg {arg}) diverged from sima_core::prng"
            );
        }

        // The three published known answers, keyed to the first three rows.
        assert_eq!(results[0], 0xE220_A839_7B1D_CDAF, "next(0, 0)");
        assert_eq!(results[1], 0x7AB4_0E09_0F36_3A7D, "derive(0, 1)");
        assert_eq!(results[2], 0x83EC_686C_1600_460A, "derive(1, 1)");
    }

    /// Requires a real Vulkan device. Run with `cargo test -- --ignored`.
    ///
    /// Probes each u64 emulation helper at its engineered corners against Rust's
    /// own `u64` arithmetic — the language semantics the WGSL emulation
    /// reproduces. Every row carries its expected result computed natively, so a
    /// helper that mishandles a carry, a shift-amount mask, or a cross-term
    /// diverges from a bit-exact reference.
    #[test]
    #[ignore = "requires a Vulkan device"]
    fn the_wgsl_u64_helpers_match_native() {
        // Each row is (op, a, b, expected) with expected computed by Rust's own
        // wrapping/shift arithmetic. `shr` carries its shift amount in b; the two
        // `mul_wide` operands are u32, widened for transport.
        let add = |a: u64, b: u64| (OP_ADD, a, b, a.wrapping_add(b));
        let shr = |a: u64, n: u32| (OP_SHR, a, u64::from(n), a >> n);
        let mul = |a: u64, b: u64| (OP_MUL, a, b, a.wrapping_mul(b));
        let mul_wide = |a: u32, b: u32| {
            (
                OP_MUL_WIDE,
                u64::from(a),
                u64::from(b),
                u64::from(a) * u64::from(b),
            )
        };

        // A dense pattern with bits in both halves, for the shift corners.
        const DENSE: u64 = 0xF0F0_F0F0_0F0F_0F0F;
        let rows: Vec<(u32, u64, u64, u64)> = vec![
            // u64_add: low halves summing to exactly 2^32 (carry boundary),
            // carry into a saturated high half (u64::MAX + 1 wraps to 0), and
            // zero operands.
            add(0x8000_0000, 0x8000_0000),
            add(u64::MAX, 1),
            add(0, 0),
            // u64_shr on a dense pattern at n in {0, 1, 31, 32, 33, 63}. 0 and 32
            // sit on WGSL's shift-amount masking, which the snippet guards.
            shr(DENSE, 0),
            shr(DENSE, 1),
            shr(DENSE, 31),
            shr(DENSE, 32),
            shr(DENSE, 33),
            shr(DENSE, 63),
            // u64_mul_wide: 0xFFFFFFFF squared saturates every partial product;
            // 0xFFFFFFFF * 0xFFFFFFFE forces the cross term to overflow (its carry
            // into bit 48); then the 0 and 1 multipliers.
            mul_wide(0xFFFF_FFFF, 0xFFFF_FFFF),
            mul_wide(0xFFFF_FFFF, 0xFFFF_FFFE),
            mul_wide(0, 0x1234_5678),
            mul_wide(1, 0x1234_5678),
            // u64_mul mod 2^64: max squared, a product overflowing 2^64 (2^32 *
            // 2^32 wraps to 0), and a pair with both halves nonzero so both cross
            // terms feed the high half.
            mul(u64::MAX, u64::MAX),
            mul(0x0000_0001_0000_0000, 0x0000_0001_0000_0000),
            mul(0x0000_0001_0000_0002, 0x0000_0003_0000_0004),
        ];

        let mut requests: Vec<u32> = Vec::with_capacity(rows.len() * 5);
        for &(op, a, b, _) in &rows {
            requests.extend_from_slice(&request(op, a, b));
        }
        let results = dispatch_probe(&requests);

        for (i, &(op, a, b, expected)) in rows.iter().enumerate() {
            assert_eq!(
                results[i], expected,
                "row {i} (op {op}, a {a:#018x}, b {b:#018x}) diverged from native u64 arithmetic"
            );
        }
    }
}
