//! The Gray-Scott generator: turns a seeded config into a run's candidate
//! specs.
//!
//! [`GrayScottGeneratorConfig`] is the generator's params blob — the
//! candidate count and one sampling range per genome parameter, with its
//! canonical byte codec. [`GrayScottGenerator`] reads it and draws `count`
//! genomes uniformly from the configured box, one decorrelated PRNG
//! substream per candidate.

use std::collections::HashMap;

use sima_contracts::Generator;
use sima_core::{Dec, Enc, Error, Result, prng};
use sima_model::{FormatId, GeneratorId, Spec};

use super::genome::GrayScottGenome;

/// The Gray-Scott generator's params: the candidate count and the sampled
/// box — one `[lo, hi]` range per genome parameter, in the frozen field
/// order `feed`, `kill`, `diffusion_u`, `diffusion_v`.
///
/// A degenerate range `[v, v]` fixes its parameter, so a Pearson-style run —
/// vary `feed` and `kill`, pin the classical diffusion pair — is a
/// configuration of this one generator, never a second generator.
///
/// The canonical form is the `u64` count, then the four ranges in field
/// order, each as `f32` lo then `f32` hi — 40 bytes, carrying no domain tag:
/// the config lives inside a params blob, which frames it.
// On constructed values the derived `PartialEq` is total and coincides with
// byte equality: the corner validation excludes NaN (which would make
// equality irreflexive) and -0.0 (the one value that equals another
// numerically while differing in bytes) from every bound, and `u64`
// equality is exact.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GrayScottGeneratorConfig {
    /// The number of candidates to draw.
    count: u64,
    /// Sampling range `[lo, hi]` of the feed rate `f`.
    feed: [f32; 2],
    /// Sampling range `[lo, hi]` of the kill rate `k`.
    kill: [f32; 2],
    /// Sampling range `[lo, hi]` of the `u` channel's diffusion coefficient.
    diffusion_u: [f32; 2],
    /// Sampling range `[lo, hi]` of the `v` channel's diffusion coefficient.
    diffusion_v: [f32; 2],
}

impl GrayScottGeneratorConfig {
    /// Builds a config, validating the count and the box: `count` must be at
    /// least 1, both box corners must construct as genomes, and each range
    /// must satisfy `lo ≤ hi`. Any violation is [`Error::Validation`].
    pub fn new(
        count: u64,
        feed: [f32; 2],
        kill: [f32; 2],
        diffusion_u: [f32; 2],
        diffusion_v: [f32; 2],
    ) -> Result<GrayScottGeneratorConfig> {
        // A zero-candidate run is meaningless; enforcing it here makes
        // decode, which funnels through this constructor, reject such
        // blobs too.
        if count == 0 {
            return Err(Error::Validation(format!(
                "gray-scott generator count must be at least 1, got {count}"
            )));
        }
        // Both box corners must construct as genomes — the genome's own
        // validation, reused verbatim, whose errors already name the
        // parameter and the value. Each parameter's valid set is an
        // interval, so valid corners imply every point of the box is valid.
        GrayScottGenome::new(feed[0], kill[0], diffusion_u[0], diffusion_v[0])?;
        GrayScottGenome::new(feed[1], kill[1], diffusion_u[1], diffusion_v[1])?;
        // Ordered bounds, checked after the corners so both bounds are known
        // non-NaN and the comparison is meaningful.
        for (name, [lo, hi]) in [
            ("feed", feed),
            ("kill", kill),
            ("diffusion_u", diffusion_u),
            ("diffusion_v", diffusion_v),
        ] {
            if lo > hi {
                return Err(Error::Validation(format!(
                    "gray-scott generator {name} range must satisfy lo <= hi, got [{lo}, {hi}]"
                )));
            }
        }
        Ok(GrayScottGeneratorConfig {
            count,
            feed,
            kill,
            diffusion_u,
            diffusion_v,
        })
    }

    /// The number of candidates to draw.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The sampling range `[lo, hi]` of the feed rate `f`.
    pub fn feed(&self) -> [f32; 2] {
        self.feed
    }

    /// The sampling range `[lo, hi]` of the kill rate `k`.
    pub fn kill(&self) -> [f32; 2] {
        self.kill
    }

    /// The sampling range `[lo, hi]` of the `u` channel's diffusion
    /// coefficient.
    pub fn diffusion_u(&self) -> [f32; 2] {
        self.diffusion_u
    }

    /// The sampling range `[lo, hi]` of the `v` channel's diffusion
    /// coefficient.
    pub fn diffusion_v(&self) -> [f32; 2] {
        self.diffusion_v
    }

    /// Appends the canonical form: the `u64` count, then the four ranges in
    /// the frozen field order, each bound via [`Enc::f32`].
    pub fn encode(&self, enc: &mut Enc) {
        enc.u64(self.count);
        for [lo, hi] in [self.feed, self.kill, self.diffusion_u, self.diffusion_v] {
            enc.f32(lo).f32(hi);
        }
    }

    /// Reads a canonical form written by [`GrayScottGeneratorConfig::encode`],
    /// funneling the values through [`GrayScottGeneratorConfig::new`] so
    /// decode and construction share one validation path.
    pub fn decode(dec: &mut Dec<'_>) -> Result<GrayScottGeneratorConfig> {
        let count = dec.u64()?;
        let mut range = || -> Result<[f32; 2]> { Ok([dec.f32()?, dec.f32()?]) };
        let feed = range()?;
        let kill = range()?;
        let diffusion_u = range()?;
        let diffusion_v = range()?;
        GrayScottGeneratorConfig::new(count, feed, kill, diffusion_u, diffusion_v)
    }

    /// The standalone canonical bytes of this config.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        self.encode(&mut enc);
        enc.finish()
    }

    /// Parses standalone canonical bytes, rejecting trailing input.
    pub fn from_bytes(bytes: &[u8]) -> Result<GrayScottGeneratorConfig> {
        let mut dec = Dec::new(bytes);
        let config = GrayScottGeneratorConfig::decode(&mut dec)?;
        dec.finish()?;
        Ok(config)
    }
}

/// Draws a run's candidate genomes uniformly from the box a
/// [`GrayScottGeneratorConfig`] defines. Candidate `i` owns the decorrelated
/// PRNG substream `derive(root_seed, i)` and takes one draw per genome
/// parameter, so a candidate depends only on `(root_seed, i, ranges)`:
/// raising the count appends candidates and never changes existing ones,
/// keeping their spec ids — and any cached evaluation results — valid.
#[derive(Debug, Clone)]
pub struct GrayScottGenerator {
    id: GeneratorId,
}

impl GrayScottGenerator {
    /// Constructs the generator, registered under id `gray-scott.v1`.
    pub fn new() -> Result<GrayScottGenerator> {
        Ok(GrayScottGenerator {
            id: GeneratorId::new("gray-scott.v1")?,
        })
    }
}

impl Generator for GrayScottGenerator {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    fn generate(&self, root_seed: u64, params: &[u8], format: &FormatId) -> Result<Vec<Spec>> {
        let config = GrayScottGeneratorConfig::from_bytes(params)?;
        let ranges = [
            config.feed(),
            config.kill(),
            config.diffusion_u(),
            config.diffusion_v(),
        ];
        let mut specs = Vec::with_capacity(config.count() as usize);
        for i in 0..config.count() {
            // Candidate i owns the substream derive(root_seed, i) and takes
            // one draw per parameter, counters in the frozen field order.
            let seed = prng::derive(root_seed, i);
            let draw = |counter: u64, [lo, hi]: [f32; 2]| -> f32 {
                // Frozen identity-bearing arithmetic: t ∈ [0, 1) in f64,
                // mapped affinely and rounded once to f32 (to nearest even).
                // The result lies in [lo, hi] up to that final rounding, and
                // every point of the box is a valid genome by the config's
                // corner validation.
                let t = prng::unit_f64(prng::next(seed, counter));
                (lo as f64 + t * (hi as f64 - lo as f64)) as f32
            };
            // Construction cannot fail inside the validated box, but the
            // result is propagated rather than unwrapped: the validating
            // constructor is the genome's only entry, and propagation covers
            // the extreme f32 edges (a corner at f32::MAX, where the final
            // rounding could reach infinity).
            let genome = GrayScottGenome::new(
                draw(0, ranges[0]),
                draw(1, ranges[1]),
                draw(2, ranges[2]),
                draw(3, ranges[3]),
            )?;
            specs.push(Spec {
                format: format.clone(),
                bytes: genome.to_bytes(),
            });
        }
        // Specs are content-addressed and the genome carries no nonce, so
        // identical draws would collapse to one identity; surfacing them as
        // an error exposes the config mistake (ranges admitting too few
        // distinct values) instead of silently shrinking the run. Payload
        // equality is spec-id equality within one call: every spec here
        // shares one format.
        let mut first_drawn_at: HashMap<&[u8], usize> = HashMap::with_capacity(specs.len());
        for (i, spec) in specs.iter().enumerate() {
            if let Some(&j) = first_drawn_at.get(spec.bytes.as_slice()) {
                return Err(Error::Validation(format!(
                    "gray-scott generator drew identical genomes at candidates {j} and {i}: \
                     the configured ranges admit too few distinct values"
                )));
            }
            first_drawn_at.insert(&spec.bytes, i);
        }
        Ok(specs)
    }
}

#[cfg(test)]
mod tests {
    use sima_contracts::Generator;
    use sima_core::to_hex;
    use sima_model::FormatId;

    use super::*;

    /// A config spanning the interesting region of parameter space
    /// (Pearson's map) with the classical diffusion pair pinned.
    fn sample() -> GrayScottGeneratorConfig {
        GrayScottGeneratorConfig::new(64, [0.01, 0.08], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08])
            .expect("valid sample config")
    }

    /// The sample's ranges with `range` substituted at `position`, in the
    /// frozen field order `feed`, `kill`, `diffusion_u`, `diffusion_v`.
    fn with_substituted(position: usize, range: [f32; 2]) -> Result<GrayScottGeneratorConfig> {
        let mut r = [[0.01, 0.08], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08]];
        r[position] = range;
        GrayScottGeneratorConfig::new(64, r[0], r[1], r[2], r[3])
    }

    /// The canonical bytes of [`sample`], derived by hand from the layout —
    /// the `u64` count little-endian, then each bound's `to_bits()` as
    /// little-endian hex in the frozen order — and independently reproduced
    /// with Python `struct`: `(64).to_bytes(8, 'little').hex() +
    /// ''.join(struct.pack('<f', v).hex() for v in (0.01, 0.08, 0.03, 0.07,
    /// 0.16, 0.16, 0.08, 0.08))`.
    const CONFIG_BYTES_HEX: &str =
        "40000000000000000ad7233c0ad7a33d8fc2f53c295c8f3d0ad7233e0ad7233e0ad7a33d0ad7a33d";

    #[test]
    fn config_is_byte_stable() {
        assert_eq!(to_hex(&sample().to_bytes()), CONFIG_BYTES_HEX);
    }

    #[test]
    fn config_round_trips_through_bytes() -> Result<()> {
        let minimal = GrayScottGeneratorConfig::new(
            1,
            [0.01, 0.01],
            [0.03, 0.03],
            [0.16, 0.16],
            [0.08, 0.08],
        )?;
        for config in [sample(), minimal] {
            assert_eq!(
                GrayScottGeneratorConfig::from_bytes(&config.to_bytes())?,
                config
            );
        }
        Ok(())
    }

    #[test]
    fn config_rejects_truncation_and_trailing() {
        let full = sample().to_bytes();
        assert_eq!(full.len(), 40);
        for cut in 0..full.len() {
            assert!(
                matches!(
                    GrayScottGeneratorConfig::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
        let mut trailing = full;
        trailing.push(0);
        assert!(matches!(
            GrayScottGeneratorConfig::from_bytes(&trailing),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn config_rejects_zero_count() {
        assert!(matches!(
            GrayScottGeneratorConfig::new(
                0,
                [0.01, 0.08],
                [0.03, 0.07],
                [0.16, 0.16],
                [0.08, 0.08]
            ),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn config_rejects_inverted_ranges() {
        let names = ["feed", "kill", "diffusion_u", "diffusion_v"];
        for (position, name) in names.iter().enumerate() {
            match with_substituted(position, [0.07, 0.03]) {
                Err(Error::Validation(message)) => {
                    assert!(
                        message.contains(name),
                        "message {message:?} must name {name}"
                    )
                }
                other => panic!("inverted {name} range must be a validation error, got {other:?}"),
            }
        }
    }

    #[test]
    fn config_rejects_invalid_bounds() {
        // Each invalid value substituted as the lo and as the hi bound of
        // every parameter; the corner construction surfaces the genome's
        // rule for each.
        let ranges = [[0.01f32, 0.08], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08]];
        for (position, range) in ranges.iter().enumerate() {
            for bad in [f32::NAN, -0.0, -0.01] {
                for slot in 0..2 {
                    let mut corrupted = *range;
                    corrupted[slot] = bad;
                    assert!(
                        matches!(
                            with_substituted(position, corrupted),
                            Err(Error::Validation(_))
                        ),
                        "position {position} slot {slot} value {bad} must be rejected"
                    );
                }
            }
        }
        // Zero as a diffusion bound: the genome's strict-positivity rule.
        assert!(matches!(
            with_substituted(2, [0.0, 0.16]),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            with_substituted(3, [0.0, 0.08]),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn config_accepts_degenerate_and_zero_bounds() -> Result<()> {
        // A degenerate range fixes its parameter; every position admits one.
        with_substituted(0, [0.055, 0.055])?;
        with_substituted(1, [0.062, 0.062])?;
        with_substituted(2, [0.16, 0.16])?;
        with_substituted(3, [0.08, 0.08])?;
        // The genome admits +0.0 feed and kill, so zero bounds pass the
        // corner check.
        with_substituted(0, [0.0, 0.0])?;
        with_substituted(1, [0.0, 0.07])?;
        Ok(())
    }

    /// A config with all four ranges degenerate at the classical
    /// pattern-forming point.
    fn degenerate(count: u64) -> GrayScottGeneratorConfig {
        GrayScottGeneratorConfig::new(
            count,
            [0.055, 0.055],
            [0.062, 0.062],
            [0.16, 0.16],
            [0.08, 0.08],
        )
        .expect("valid degenerate config")
    }

    /// The payload bytes of candidate 0 for `root_seed = 42` and [`sample`],
    /// derived from an independent Python implementation of the whole
    /// sampling path — SplitMix64 (`mix`, `next`, `derive`, `unit_f64`) from
    /// the published algorithm, validated against the pinned known-answer
    /// values in `sima-core`'s `prng` tests, then
    /// `f32(lo + unit_f64(next(derive(42, 0), c)) * (hi - lo))` per range in
    /// the frozen order, packed with `struct.pack('<f', v)` — never from
    /// this crate's output.
    const CANDIDATE0_BYTES_HEX: &str = "13e1533d7e27153d0ad7233e0ad7a33d";

    #[test]
    fn generate_is_deterministic() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        let format = FormatId::new("gray-scott.v1")?;
        let params = sample().to_bytes();
        assert_eq!(
            generator.generate(42, &params, &format)?,
            generator.generate(42, &params, &format)?
        );
        Ok(())
    }

    #[test]
    fn generate_stamps_the_requested_format() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        // The format is stamped as received (the trait's contract; pairing
        // generator and format is the pipeline's concern).
        let format = FormatId::new("domain-a.v1")?;
        let specs = generator.generate(1, &sample().to_bytes(), &format)?;
        assert!(!specs.is_empty());
        for spec in specs {
            assert_eq!(spec.format, format);
        }
        Ok(())
    }

    #[test]
    fn different_root_seed_changes_the_specs() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        let format = FormatId::new("gray-scott.v1")?;
        let params = sample().to_bytes();
        let a = generator.generate(1, &params, &format)?;
        let b = generator.generate(2, &params, &format)?;
        assert_ne!(a[0].id(), b[0].id());
        Ok(())
    }

    #[test]
    fn sampled_genomes_lie_within_the_configured_ranges() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        let config = sample();
        let format = FormatId::new("gray-scott.v1")?;
        let specs = generator.generate(7, &config.to_bytes(), &format)?;
        assert_eq!(specs.len(), 64);
        for spec in specs {
            let genome = GrayScottGenome::from_bytes(&spec.bytes)?;
            // `hi` is inclusive: `t < 1` in f64, but the f32 rounding of the
            // result may land on `hi` exactly.
            for (value, [lo, hi]) in [
                (genome.feed(), config.feed()),
                (genome.kill(), config.kill()),
                (genome.diffusion_u(), config.diffusion_u()),
                (genome.diffusion_v(), config.diffusion_v()),
            ] {
                assert!(lo <= value && value <= hi, "{value} outside [{lo}, {hi}]");
            }
        }
        Ok(())
    }

    #[test]
    fn raising_count_preserves_existing_candidates() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        let format = FormatId::new("gray-scott.v1")?;
        let three = GrayScottGeneratorConfig::new(
            3,
            [0.01, 0.08],
            [0.03, 0.07],
            [0.16, 0.16],
            [0.08, 0.08],
        )?;
        let five = GrayScottGeneratorConfig::new(
            5,
            [0.01, 0.08],
            [0.03, 0.07],
            [0.16, 0.16],
            [0.08, 0.08],
        )?;
        let a = generator.generate(9, &three.to_bytes(), &format)?;
        let b = generator.generate(9, &five.to_bytes(), &format)?;
        assert_eq!(a[..], b[..3]);
        Ok(())
    }

    #[test]
    fn degenerate_ranges_produce_the_fixed_genome() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        let format = FormatId::new("gray-scott.v1")?;
        let specs = generator.generate(3, &degenerate(1).to_bytes(), &format)?;
        // The mapping is exact on degenerate ranges: `t * 0 = 0`.
        assert_eq!(
            specs[0].bytes,
            GrayScottGenome::new(0.055, 0.062, 0.16, 0.08)?.to_bytes()
        );
        Ok(())
    }

    #[test]
    fn duplicate_draws_are_rejected() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        let format = FormatId::new("gray-scott.v1")?;
        match generator.generate(3, &degenerate(2).to_bytes(), &format) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("candidates 0 and 1"),
                    "the error names the colliding candidates: {message}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn candidate_zero_matches_independent_reference() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        let format = FormatId::new("gray-scott.v1")?;
        let specs = generator.generate(42, &sample().to_bytes(), &format)?;
        assert_eq!(to_hex(&specs[0].bytes), CANDIDATE0_BYTES_HEX);
        Ok(())
    }

    #[test]
    fn generate_rejects_malformed_params() -> Result<()> {
        let generator = GrayScottGenerator::new()?;
        let format = FormatId::new("gray-scott.v1")?;
        // A single byte cannot even hold the u64 count prefix.
        assert!(matches!(
            generator.generate(1, &[0xFF], &format),
            Err(Error::Encoding(_))
        ));
        Ok(())
    }
}
