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

    /// A probe kernel over the shared snippet: one invocation per request, each
    /// request `(op, seed, arg)` five u32 wide (op, then seed and arg as low/high
    /// words), writing the 64-bit result as two u32. `op == 0` computes
    /// `prng_next`, otherwise `prng_derive`.
    const PROBE_MAIN: &str = r#"
@group(0) @binding(0) var<storage, read> requests: array<u32>;
@group(0) @binding(1) var<storage, read_write> results: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    let n = arrayLength(&results) / 2u;
    if (i >= n) { return; }
    let base = i * 5u;
    let seed = U64(requests[base + 1u], requests[base + 2u]);
    let arg = U64(requests[base + 3u], requests[base + 4u]);
    var r: U64;
    if (requests[base] == 0u) {
        r = prng_next(seed, arg);
    } else {
        r = prng_derive(seed, arg);
    }
    results[i * 2u] = r.lo;
    results[i * 2u + 1u] = r.hi;
}
"#;

    /// The composed probe kernel source: the shared snippet then the probe entry.
    fn probe_source() -> String {
        format!("{PRNG_WGSL}{PROBE_MAIN}")
    }

    /// One request row `(op, seed, arg)` packed as five little-endian u32 words.
    fn request(op: u32, seed: u64, arg: u64) -> [u32; 5] {
        [
            op,
            seed as u32,
            (seed >> 32) as u32,
            arg as u32,
            (arg >> 32) as u32,
        ]
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
        use sima_toolkit_wgsl::Context;

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

        let context = Context::new().expect("create compute context");
        let kernel = context
            .kernel(&probe_source(), "main")
            .expect("build probe kernel");
        let request_bytes: &[u8] = bytemuck::cast_slice(&requests);
        let request_buffer = context.buffer(request_bytes.len()).expect("request buffer");
        context
            .upload(&request_buffer, request_bytes)
            .expect("upload requests");
        let result_buffer = context
            .buffer(cases.len() * 2 * std::mem::size_of::<u32>())
            .expect("result buffer");

        let groups = [(cases.len() as u32).div_ceil(64), 1, 1];
        context
            .dispatch(&kernel, &[&request_buffer, &result_buffer], groups)
            .expect("dispatch probe");

        let bytes = context.download(&result_buffer).expect("download results");
        let words: &[u32] = bytemuck::cast_slice(&bytes);

        for (i, &(op, seed, arg)) in cases.iter().enumerate() {
            let got = words[i * 2] as u64 | (words[i * 2 + 1] as u64) << 32;
            let expected = if op == 0 {
                prng::next(seed, arg)
            } else {
                prng::derive(seed, arg)
            };
            assert_eq!(
                got, expected,
                "case {i} (op {op}, seed {seed}, arg {arg}) diverged from sima_core::prng"
            );
        }

        // The three published known answers, keyed to the first three rows.
        let known = |i: usize| words[i * 2] as u64 | (words[i * 2 + 1] as u64) << 32;
        assert_eq!(known(0), 0xE220_A839_7B1D_CDAF, "next(0, 0)");
        assert_eq!(known(1), 0x7AB4_0E09_0F36_3A7D, "derive(0, 1)");
        assert_eq!(known(2), 0x83EC_686C_1600_460A, "derive(1, 1)");
    }
}
