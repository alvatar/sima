//! [`GrayScottGenConfig`]: the Gray-Scott generator's sampling box, and the
//! model's `[run.generator]` sampling keys.

use sima_core::{Codec, Dec, Enc, Error, Result, prng};

use super::genome::GrayScottGenome;
use crate::domains::translate::{self, TomlConfig};

/// The Gray-Scott generator's sampling box: one `[lo, hi]` range per genome
/// parameter, in the frozen field order `feed`, `kill`, `diffusion_u`,
/// `diffusion_v`. The candidate `count` is a shared generator key and lives
/// outside this config.
///
/// A degenerate range `[v, v]` fixes its parameter, so a Pearson-style run —
/// vary `feed` and `kill`, pin the classical diffusion pair — is a configuration
/// of this one model, never a second one.
///
/// The canonical form is the four ranges in field order, each as `f32` lo then
/// `f32` hi — 32 bytes, carrying no tag: the config lives inside a params blob,
/// which frames it.
// On constructed values the derived `PartialEq` is total and coincides with byte
// equality: the corner validation excludes NaN (which would make equality
// irreflexive) and -0.0 (the one value that equals another numerically while
// differing in bytes) from every bound.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct GrayScottGenConfig {
    /// Sampling range `[lo, hi]` of the feed rate `f`.
    feed: [f32; 2],
    /// Sampling range `[lo, hi]` of the kill rate `k`.
    kill: [f32; 2],
    /// Sampling range `[lo, hi]` of the `u` channel's diffusion coefficient.
    diffusion_u: [f32; 2],
    /// Sampling range `[lo, hi]` of the `v` channel's diffusion coefficient.
    diffusion_v: [f32; 2],
}

impl GrayScottGenConfig {
    /// Builds a config, validating the box: both corners must construct as
    /// genomes, and each range must satisfy `lo ≤ hi`. Any violation is
    /// [`Error::Validation`].
    pub(crate) fn new(
        feed: [f32; 2],
        kill: [f32; 2],
        diffusion_u: [f32; 2],
        diffusion_v: [f32; 2],
    ) -> Result<GrayScottGenConfig> {
        // Both box corners must construct as genomes — the genome's own
        // validation, reused verbatim, whose errors already name the parameter
        // and the value. Each parameter's valid set is an interval, so valid
        // corners imply every point of the box is valid.
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
                    "gray_scott generator {name} range must satisfy lo <= hi, got [{lo}, {hi}]"
                )));
            }
        }
        Ok(GrayScottGenConfig {
            feed,
            kill,
            diffusion_u,
            diffusion_v,
        })
    }

    /// Draws candidate `index`'s genome for chain seed `seed`. Candidate `index`
    /// owns the substream `derive(seed, index)` and takes one draw per parameter,
    /// counters in the frozen field order.
    pub(crate) fn sample(&self, seed: u64, index: u64) -> GrayScottGenome {
        let s = prng::derive(seed, index);
        let ranges = [self.feed, self.kill, self.diffusion_u, self.diffusion_v];
        // Every point of the validated box is a valid genome, so construction
        // cannot fail.
        let draw = |counter: u64, [lo, hi]: [f32; 2]| prng::uniform_f32(s, counter, lo, hi);
        GrayScottGenome::new(
            draw(0, ranges[0]),
            draw(1, ranges[1]),
            draw(2, ranges[2]),
            draw(3, ranges[3]),
        )
        .expect("a draw from the validated box is a valid genome")
    }
}

impl Codec for GrayScottGenConfig {
    /// Appends the four ranges in the frozen field order, each bound via
    /// [`Enc::f32`].
    fn encode(&self, enc: &mut Enc) {
        for [lo, hi] in [self.feed, self.kill, self.diffusion_u, self.diffusion_v] {
            enc.f32(lo).f32(hi);
        }
    }

    /// Reads the four ranges and funnels them through [`GrayScottGenConfig::new`]
    /// so decode and construction share one validation path.
    fn decode(dec: &mut Dec<'_>) -> Result<GrayScottGenConfig> {
        let mut range = || -> Result<[f32; 2]> { Ok([dec.f32()?, dec.f32()?]) };
        let feed = range()?;
        let kill = range()?;
        let diffusion_u = range()?;
        let diffusion_v = range()?;
        GrayScottGenConfig::new(feed, kill, diffusion_u, diffusion_v)
    }
}

impl TomlConfig for GrayScottGenConfig {
    /// Reads the sampling keys from the `[run.generator]` table (the shared
    /// `count` is already stripped), rejecting any key it does not define. Each
    /// range is required, with no defaults — every value that determines candidate
    /// identity is visible in the config file.
    fn parse(table: &toml::Table, id: &str, section: &str) -> Result<GrayScottGenConfig> {
        translate::reject_unknown_keys(
            id,
            table,
            &["feed", "kill", "diffusion_u", "diffusion_v"],
            section,
        )?;
        let feed = translate::range(table, id, section, "feed")?;
        let kill = translate::range(table, id, section, "kill")?;
        let diffusion_u = translate::range(table, id, section, "diffusion_u")?;
        let diffusion_v = translate::range(table, id, section, "diffusion_v")?;
        GrayScottGenConfig::new(feed, kill, diffusion_u, diffusion_v)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::to_hex;

    use super::*;

    /// A config spanning the interesting region of parameter space (Pearson's
    /// map) with the classical diffusion pair pinned.
    fn sample() -> GrayScottGenConfig {
        GrayScottGenConfig::new([0.01, 0.08], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08])
            .expect("valid sample config")
    }

    /// The sample's ranges with `range` substituted at `position`, in the frozen
    /// field order `feed`, `kill`, `diffusion_u`, `diffusion_v`.
    fn with_substituted(position: usize, range: [f32; 2]) -> Result<GrayScottGenConfig> {
        let mut r = [[0.01, 0.08], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08]];
        r[position] = range;
        GrayScottGenConfig::new(r[0], r[1], r[2], r[3])
    }

    /// The canonical bytes of [`sample`], derived by hand from the layout — each
    /// bound's `to_bits()` as little-endian hex in the frozen order — and
    /// independently reproduced with Python `struct`:
    /// `''.join(struct.pack('<f', v).hex() for v in (0.01, 0.08, 0.03, 0.07,
    /// 0.16, 0.16, 0.08, 0.08))`.
    const CONFIG_BYTES_HEX: &str =
        "0ad7233c0ad7a33d8fc2f53c295c8f3d0ad7233e0ad7233e0ad7a33d0ad7a33d";

    /// The payload bytes of candidate 0 for `seed = 42` and [`sample`], derived
    /// from an independent Python implementation of the whole sampling path —
    /// SplitMix64 (`mix`, `next`, `derive`, `unit_f64`) from the published
    /// algorithm, validated against the pinned known-answer values in
    /// `sima-core`'s `prng` tests, then
    /// `f32(lo + unit_f64(next(derive(42, 0), c)) * (hi - lo))` per range in the
    /// frozen order, packed with `struct.pack('<f', v)` — never from this crate's
    /// output.
    const CANDIDATE0_BYTES_HEX: &str = "13e1533d7e27153d0ad7233e0ad7a33d";

    #[test]
    fn config_is_byte_stable() {
        assert_eq!(to_hex(&sample().to_bytes()), CONFIG_BYTES_HEX);
    }

    #[test]
    fn config_round_trips_through_bytes() -> Result<()> {
        let minimal =
            GrayScottGenConfig::new([0.01, 0.01], [0.03, 0.03], [0.16, 0.16], [0.08, 0.08])?;
        for config in [sample(), minimal] {
            assert_eq!(GrayScottGenConfig::from_bytes(&config.to_bytes())?, config);
        }
        Ok(())
    }

    #[test]
    fn config_rejects_truncation_and_trailing() {
        let full = sample().to_bytes();
        assert_eq!(full.len(), 32);
        for cut in 0..full.len() {
            assert!(
                matches!(
                    GrayScottGenConfig::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
        let mut trailing = full;
        trailing.push(0);
        assert!(matches!(
            GrayScottGenConfig::from_bytes(&trailing),
            Err(Error::Encoding(_))
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
        // Each invalid value substituted as the lo and as the hi bound of every
        // parameter; the corner construction surfaces the genome's rule.
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
        // The genome admits +0.0 feed and kill, so zero bounds pass the corner
        // check.
        with_substituted(0, [0.0, 0.0])?;
        with_substituted(1, [0.0, 0.07])?;
        Ok(())
    }

    #[test]
    fn sample_is_deterministic_and_pinned() {
        // Candidate 0 reproduces the independently derived payload, and repeats.
        let genome = sample().sample(42, 0);
        assert_eq!(to_hex(&genome.to_bytes()), CANDIDATE0_BYTES_HEX);
        assert_eq!(sample().sample(42, 0), genome);
    }

    #[test]
    fn sampled_genomes_lie_within_the_configured_ranges() {
        let config = sample();
        for index in 0..64 {
            let genome = config.sample(7, index);
            // `hi` is inclusive: `t < 1` in f64, but the f32 rounding of the
            // result may land on `hi` exactly.
            for (value, [lo, hi]) in [
                (genome.feed(), [0.01, 0.08]),
                (genome.kill(), [0.03, 0.07]),
                (genome.diffusion_u(), [0.16, 0.16]),
                (genome.diffusion_v(), [0.08, 0.08]),
            ] {
                assert!(lo <= value && value <= hi, "{value} outside [{lo}, {hi}]");
            }
        }
    }

    #[test]
    fn degenerate_ranges_produce_the_fixed_genome() -> Result<()> {
        let config =
            GrayScottGenConfig::new([0.055, 0.055], [0.062, 0.062], [0.16, 0.16], [0.08, 0.08])?;
        // The mapping is exact on degenerate ranges: `t * 0 = 0`.
        assert_eq!(
            config.sample(3, 0),
            GrayScottGenome::new(0.055, 0.062, 0.16, 0.08)?
        );
        Ok(())
    }

    /// The model's `[run.generator]` keys (the shared `count` is stripped before
    /// the model sees the table).
    const KEYS: &str = r#"
        feed = [0.01, 0.08]
        kill = [0.03, 0.07]
        diffusion_u = [0.16, 0.16]
        diffusion_v = [0.08, 0.08]
    "#;

    fn table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    #[test]
    fn parse_reads_the_ranges() -> Result<()> {
        // Integer bounds read through the one number path: `feed = [0, 0]` is the
        // degenerate range [+0.0, +0.0].
        let integer = KEYS.replace("feed = [0.01, 0.08]", "feed = [0, 0]");
        assert_eq!(
            GrayScottGenConfig::parse(&table(&integer), "id", "generator")?,
            GrayScottGenConfig::new([0.0, 0.0], [0.03, 0.07], [0.16, 0.16], [0.08, 0.08])?
        );
        assert_eq!(
            GrayScottGenConfig::parse(&table(KEYS), "id", "generator")?,
            sample()
        );
        Ok(())
    }

    #[test]
    fn parse_rejects_a_missing_key_naming_it() {
        for key in ["feed", "kill", "diffusion_u", "diffusion_v"] {
            let mut incomplete = table(KEYS);
            incomplete.remove(key);
            match GrayScottGenConfig::parse(&incomplete, "id", "generator") {
                Err(Error::Validation(message)) => {
                    assert!(message.contains(key), "the error names {key}: {message}")
                }
                other => panic!("expected Validation for missing {key}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_rejects_an_unknown_key() {
        let mut extended = table(KEYS);
        extended.insert("surprise".to_string(), toml::Value::Integer(1));
        match GrayScottGenConfig::parse(&extended, "id", "generator") {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("surprise"),
                    "the error names the key: {message}"
                )
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_malformed_ranges() {
        // Wrong shapes and out-of-domain bounds surface through the config
        // constructor's box validation.
        for (original, bad) in [
            ("feed = [0.01, 0.08]", "feed = 0.16"),
            ("feed = [0.01, 0.08]", "feed = [0.01]"),
            ("feed = [0.01, 0.08]", r#"feed = [0.01, "hi"]"#),
            ("feed = [0.01, 0.08]", "feed = [0.08, 0.01]"),
            ("diffusion_u = [0.16, 0.16]", "diffusion_u = [0.0, 0.0]"),
        ] {
            let text = KEYS.replace(original, bad);
            assert!(
                matches!(
                    GrayScottGenConfig::parse(&table(&text), "id", "generator"),
                    Err(Error::Validation(_))
                ),
                "{bad} must be rejected"
            );
        }
    }
}
