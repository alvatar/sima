//! [`NcaGenConfig`]: the Neural CA generator's sampling box, and the model's
//! `[run.generator]` sampling key.

use sima_core::{Codec, Dec, Enc, Error, Result, prng};

use super::genome::{N, NcaGenome};
use crate::domains::translate::{self, TomlConfig};

/// The Neural CA generator's sampling box: the half-width of the symmetric
/// interval `[-weight_scale, +weight_scale]` each of the genome's weights is
/// drawn from uniformly. The candidate `count` is a shared generator key and
/// lives outside this config.
///
/// The canonical form is the one `weight_scale` as f32 bits little-endian — 4
/// bytes, carrying no tag: the config lives inside a params blob, which frames
/// it.
// On constructed values the derived `PartialEq` is total and coincides with byte
// equality: validation excludes NaN (which would make equality irreflexive) and
// the non-positive values, so -0.0 never enters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NcaGenConfig {
    /// Half-width of the symmetric weight-sampling interval.
    weight_scale: f32,
}

impl NcaGenConfig {
    /// Builds a config, validating `weight_scale` finite and strictly greater
    /// than zero. A zero scale collapses every weight to zero — every candidate
    /// identical, which the generator's dedup rejects — so it is excluded here.
    /// A violation is [`Error::Validation`].
    pub(crate) fn new(weight_scale: f32) -> Result<NcaGenConfig> {
        if !(weight_scale.is_finite() && weight_scale > 0.0) {
            return Err(Error::Validation(format!(
                "nca generator weight_scale must be a finite value greater than zero, got {weight_scale}"
            )));
        }
        Ok(NcaGenConfig { weight_scale })
    }

    /// Draws candidate `index`'s genome for chain seed `seed`. Candidate `index`
    /// owns the substream `derive(seed, index)`, and weight `j` is drawn with
    /// counter `j`, mapped uniformly into `[-weight_scale, +weight_scale]` by the
    /// frozen identity-bearing affine idiom: `t ∈ [0, 1)` in f64, mapped affinely
    /// and rounded once to f32.
    pub(crate) fn sample(&self, seed: u64, index: u64) -> NcaGenome {
        let s = prng::derive(seed, index);
        let weights: Box<[f32; N]> = Box::new(std::array::from_fn(|j| {
            prng::uniform_f32(s, j as u64, -self.weight_scale, self.weight_scale)
        }));
        NcaGenome::new(weights).expect("a draw from a finite positive scale is finite")
    }
}

impl Codec for NcaGenConfig {
    /// Appends `weight_scale` via [`Enc::f32`].
    fn encode(&self, enc: &mut Enc) {
        enc.f32(self.weight_scale);
    }

    /// Reads `weight_scale` and funnels it through [`NcaGenConfig::new`] so
    /// decode and construction share one validation path.
    fn decode(dec: &mut Dec<'_>) -> Result<NcaGenConfig> {
        NcaGenConfig::new(dec.f32()?)
    }
}

impl TomlConfig for NcaGenConfig {
    /// Reads the `weight_scale` key from the `[run.generator]` table (the shared
    /// `count` is already stripped), rejecting any key it does not define.
    fn parse(table: &toml::Table, id: &str, section: &str) -> Result<NcaGenConfig> {
        translate::reject_unknown_keys(id, table, &["weight_scale"], section)?;
        NcaGenConfig::new(translate::float(table, id, section, "weight_scale")?)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::{Hash, hash_bytes, to_hex};

    use super::*;

    /// The sampling scale the byte pins are stated against.
    fn sample() -> NcaGenConfig {
        NcaGenConfig::new(0.5).expect("valid sample config")
    }

    /// The canonical bytes of [`sample`]: `0.5 = 0x3F000000`, little-endian.
    /// Independently reproduced with Python `struct.pack('<f', 0.5).hex()`.
    const CONFIG_BYTES_HEX: &str = "0000003f";

    /// The blake3 content id of candidate 0's `to_bytes()` for `weight_scale =
    /// 0.5` and `seed = 42`, derived from an independent Python implementation of
    /// the whole sampling path — SplitMix64 (`mix`, `next`, `derive`,
    /// `unit_f64`) from the published algorithm, validated against the pinned
    /// known-answer values in `sima-core`'s `prng` tests, then
    /// `f32(-0.5 + unit_f64(next(derive(42, 0), j)) * 1.0)` for `j` in `0..1091`,
    /// packed with `struct.pack('<f', w)` and blake3'd — never from this crate's
    /// output. This locks the whole sampling path.
    const CANDIDATE0_CONTENT_ID_HEX: &str =
        "3380be5456071ee179968ebc304bb07f09bed66ce8c4e966045e99847d05d157";

    #[test]
    fn config_is_byte_stable() {
        assert_eq!(to_hex(&sample().to_bytes()), CONFIG_BYTES_HEX);
    }

    #[test]
    fn config_round_trips_through_bytes() -> Result<()> {
        for config in [sample(), NcaGenConfig::new(1.5)?] {
            assert_eq!(NcaGenConfig::from_bytes(&config.to_bytes())?, config);
        }
        Ok(())
    }

    #[test]
    fn config_rejects_truncation_and_trailing() {
        let full = sample().to_bytes();
        assert_eq!(full.len(), 4);
        for cut in 0..full.len() {
            assert!(
                matches!(
                    NcaGenConfig::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
        let mut trailing = full;
        trailing.push(0);
        assert!(matches!(
            NcaGenConfig::from_bytes(&trailing),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn new_rejects_non_positive_and_non_finite() {
        for bad in [
            0.0f32,
            -0.0,
            -0.5,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ] {
            assert!(
                matches!(NcaGenConfig::new(bad), Err(Error::Validation(_))),
                "weight_scale = {bad} must be rejected"
            );
        }
    }

    #[test]
    fn sample_is_deterministic_and_pinned() -> Result<()> {
        // Candidate 0 reproduces the independently derived payload, and repeats.
        let genome = sample().sample(42, 0);
        assert_eq!(
            hash_bytes(&genome.to_bytes()),
            Hash::from_hex(CANDIDATE0_CONTENT_ID_HEX)?
        );
        assert_eq!(sample().sample(42, 0), genome);
        Ok(())
    }

    #[test]
    fn sampled_weights_lie_within_the_scale() -> Result<()> {
        // Every weight lands in the symmetric interval: `t < 1` in f64, but the
        // f32 rounding of the result may land on `weight_scale` exactly.
        let config = NcaGenConfig::new(0.25)?;
        for index in 0..16 {
            for &weight in config.sample(7, index).weights() {
                assert!(weight.abs() <= 0.25, "{weight} outside [-0.25, 0.25]");
            }
        }
        Ok(())
    }

    /// The model's `[run.generator]` keys (the shared `count` is stripped before
    /// the model sees the table).
    const KEYS: &str = "weight_scale = 0.5";

    fn table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    #[test]
    fn parse_reads_the_scale() -> Result<()> {
        // An integer bound reads through the one number path: `weight_scale = 2`.
        assert_eq!(
            NcaGenConfig::parse(&table("weight_scale = 2"), "id", "generator")?,
            NcaGenConfig::new(2.0)?
        );
        assert_eq!(
            NcaGenConfig::parse(&table(KEYS), "id", "generator")?,
            sample()
        );
        Ok(())
    }

    #[test]
    fn parse_rejects_a_missing_key_naming_it() {
        match NcaGenConfig::parse(&table(""), "id", "generator") {
            Err(Error::Validation(message)) => assert!(
                message.contains("weight_scale"),
                "the error names weight_scale: {message}"
            ),
            other => panic!("expected Validation for the missing key, got {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_an_unknown_key() {
        let mut extended = table(KEYS);
        extended.insert("surprise".to_string(), toml::Value::Integer(1));
        match NcaGenConfig::parse(&extended, "id", "generator") {
            Err(Error::Validation(message)) => assert!(
                message.contains("surprise"),
                "the error names the key: {message}"
            ),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_a_non_positive_scale() {
        for bad in ["weight_scale = 0.0", "weight_scale = -0.5"] {
            assert!(
                matches!(
                    NcaGenConfig::parse(&table(bad), "id", "generator"),
                    Err(Error::Validation(_))
                ),
                "{bad} must be rejected"
            );
        }
    }
}
