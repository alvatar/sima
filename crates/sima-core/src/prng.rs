//! Deterministic counter-based PRNG built on SplitMix64.
//!
//! [`next`] is a pure function of `(seed, counter)`, so any substrate that
//! reproduces the arithmetic — CPU here, GPU shaders in later phases —
//! produces the identical stream with no sequential state to replicate.
//! The counter form equals the published sequential SplitMix64: `next(seed,
//! n)` is the `(n+1)`-th output of the reference generator seeded with
//! `seed`, which is what pins this module against the literature's
//! known-answer values.
//!
//! Substream derivation: [`derive`] maps `(seed, tag)` to a new seed via
//! `mix(seed ^ mix(tag))`. The construction is structurally distinct from
//! [`next`] (xor of a mixed tag, no golden-ratio stepping), so a derived
//! seed colliding with a stream output is a chance event (~2^-64 per pair),
//! never a systematic identity; the tests spot-check small tag and counter
//! ranges.
//!
//! Why SplitMix64: its sequential form is already counter-shaped — state
//! advances by a fixed golden-ratio increment and every output is a pure
//! finalizer of that state — so random access by counter equals the
//! published sequential generator exactly, with no warm-up and no state to
//! carry between draws. The whole generator is two multiplies and three
//! xor-shifts over `u64`, small enough to port to a GPU kernel line by line
//! and to check against the literature's known-answer values.
//!
//! Why implemented here instead of a crate: result-affecting randomness
//! must stay bit-identical across substrates and releases forever. A
//! dependency's internals can change under semver and silently shift
//! streams; this file pins the exact arithmetic the GPU port must
//! reproduce, and the known-answer tests below (values from the published
//! algorithm, never from this code) prove it. The battle-testing lives in
//! the published algorithm, not in a wrapper crate.

/// Golden-ratio increment from the published SplitMix64 algorithm.
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// SplitMix64 finalizer: xor-shift and multiply avalanche, from the
/// published algorithm (Steele, Lea & Flood 2014).
fn mix(mut z: u64) -> u64 {
    z ^= z >> 30;
    z = z.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z ^= z >> 27;
    z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    z
}

/// The `counter`-th output of the SplitMix64 stream for `seed`, computed
/// directly: `mix(seed + (counter + 1) * GOLDEN)`.
pub fn next(seed: u64, counter: u64) -> u64 {
    mix(seed.wrapping_add(counter.wrapping_add(1).wrapping_mul(GOLDEN)))
}

/// Derives a decorrelated substream seed from `seed` and a caller-chosen
/// `tag` (e.g. a lane or purpose index).
pub fn derive(seed: u64, tag: u64) -> u64 {
    mix(seed ^ mix(tag))
}

/// Maps the top 53 bits of `x` to the unit interval `[0, 1)` — the only
/// float this PRNG ever produces.
pub fn unit_f64(x: u64) -> f64 {
    // 2^-53: one ulp below 1.0 stays representable, so the range is [0, 1).
    (x >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
}

/// Sequential convenience over the pure counter form: holds `(seed,
/// counter)` and advances. Every draw equals the corresponding [`next`]
/// call, proven by test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stream {
    seed: u64,
    counter: u64,
}

impl Stream {
    /// Starts a stream at counter 0.
    pub fn new(seed: u64) -> Self {
        Stream { seed, counter: 0 }
    }

    /// Draws the next `u64`, advancing the counter.
    pub fn next_u64(&mut self) -> u64 {
        let value = next(self.seed, self.counter);
        self.counter = self.counter.wrapping_add(1);
        value
    }

    /// Draws the next `u64` mapped to `[0, 1)` via [`unit_f64`].
    pub fn next_f64(&mut self) -> f64 {
        unit_f64(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Published SplitMix64 sequence for seed 0 (first four outputs, from
    /// the literature — e.g. the test values circulated with Vigna's
    /// reference C implementation).
    const SEED0: [u64; 4] = [
        0xE220_A839_7B1D_CDAF,
        0x6E78_9E6A_A1B9_65F4,
        0x06C4_5D18_8009_454F,
        0xF88B_B8A8_724C_81EC,
    ];

    /// Reference sequence for seed 1; the first value is the widely cited
    /// SplitMix64(1) output, the rest generated from an independent Python
    /// implementation of the published algorithm.
    const SEED1: [u64; 4] = [
        0x910A_2DEC_8902_5CC1,
        0xBEEB_8DA1_658E_EC67,
        0xF893_A2EE_FB32_555E,
        0x71C1_8690_EE42_C90B,
    ];

    #[test]
    fn next_matches_published_seed0_sequence() {
        for (counter, expected) in SEED0.iter().enumerate() {
            assert_eq!(next(0, counter as u64), *expected);
        }
    }

    #[test]
    fn next_matches_reference_seed1_sequence() {
        for (counter, expected) in SEED1.iter().enumerate() {
            assert_eq!(next(1, counter as u64), *expected);
        }
    }

    /// Pinned from the independent Python reference of `mix(seed ^ mix(tag))`.
    /// `derive(0, 0) == 0` documents that 0 is a fixed point of the finalizer.
    #[test]
    fn derive_matches_reference_values() {
        assert_eq!(derive(0, 0), 0);
        assert_eq!(derive(0, 1), 0x7AB4_0E09_0F36_3A7D);
        assert_eq!(derive(0, 2), 0x52EA_D7E3_6EA7_FEA8);
        assert_eq!(derive(1, 1), 0x83EC_686C_1600_460A);
    }

    #[test]
    fn stream_equals_pure_function_form() {
        let seed = 0xDEAD_BEEF;
        let mut stream = Stream::new(seed);
        for counter in 0..16 {
            assert_eq!(stream.next_u64(), next(seed, counter));
        }
        let mut float_stream = Stream::new(seed);
        for counter in 0..16 {
            assert_eq!(float_stream.next_f64(), unit_f64(next(seed, counter)));
        }
    }

    /// Substreams derived from distinct tags share no 8-draw prefix.
    #[test]
    fn derived_substreams_are_decorrelated() {
        let root = 42;
        let prefixes: Vec<Vec<u64>> = (1..=3)
            .map(|tag| {
                let seed = derive(root, tag);
                (0..8).map(|c| next(seed, c)).collect()
            })
            .collect();
        for i in 0..prefixes.len() {
            for j in (i + 1)..prefixes.len() {
                assert_ne!(prefixes[i], prefixes[j]);
            }
        }
    }

    /// Spot check over small tag/counter ranges: derived seeds do not appear
    /// among the root stream's outputs there. Collision in general is a
    /// ~2^-64 chance event per pair, per the module docs.
    #[test]
    fn derived_seeds_do_not_alias_stream_outputs() {
        for root in [0u64, 1, 42] {
            let outputs: Vec<u64> = (0..8).map(|c| next(root, c)).collect();
            for tag in 0..8 {
                assert!(!outputs.contains(&derive(root, tag)));
            }
        }
    }

    /// Bit-exact pins for the one float mapping: zero, the all-ones
    /// extreme (largest representable value below 1.0), and the mapping of
    /// the first seed-0 output (bits from the independent Python reference).
    #[test]
    fn unit_f64_is_pinned_and_in_range() {
        assert_eq!(unit_f64(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(unit_f64(u64::MAX).to_bits(), 0x3FEF_FFFF_FFFF_FFFF);
        assert_eq!(unit_f64(SEED0[0]).to_bits(), 0x3FEC_4415_072F_63B9);
        for x in [0, 1, u64::MAX, SEED0[0], SEED1[3]] {
            let v = unit_f64(x);
            assert!((0.0..1.0).contains(&v));
        }
    }
}
