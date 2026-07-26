//! [`NcaIgnition`]: the ignition configuration of the Neural CA's initial grid,
//! and the model's `[run.params]` ignition keys.

use sima_core::{Codec, Dec, Enc, Error, Result};

use super::super::super::ignition::{PatchSpec, seeded_patch};
use super::CHANNELS;
use crate::domains::translate::{self, TomlConfig};
use crate::substrates::cellular::Grid;

/// The ignition configuration of the Neural CA's initial grid: the amplitude
/// seeded into every state channel of a centered square patch, the divisor of
/// the shorter grid extent giving the patch side, and the full relative width of
/// the noise band around the seeded amplitude.
///
/// The values determine candidate identity, so there is no `Default` — they are
/// always spelled by the caller.
///
/// The canonical form is `seed_value` as f32 bits little-endian, `side_divisor`
/// as a little-endian `u32`, `noise_width` as f32 bits little-endian: exactly 12
/// bytes. Every scalar is `f32` — the width of the grid state these values seed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NcaIgnition {
    /// Amplitude seeded into every state channel inside the patch. An arbitrary
    /// finite seed amplitude, with no sign rule.
    seed_value: f32,
    /// Divisor of the shorter grid extent giving the patch side:
    /// `side = max(min(width, height) / side_divisor, 1)`.
    side_divisor: u32,
    /// Full relative width of the noise band around the seeded amplitude.
    noise_width: f32,
}

impl NcaIgnition {
    /// Builds an ignition configuration, validating each field: `seed_value`
    /// finite; `side_divisor` at least 1, since 0 divides by zero in the side
    /// computation; `noise_width` finite and at least zero. A violation is
    /// [`Error::Validation`] naming the field.
    pub(crate) fn new(seed_value: f32, side_divisor: u32, noise_width: f32) -> Result<NcaIgnition> {
        if !seed_value.is_finite() {
            return Err(Error::Validation(format!(
                "nca ignition seed_value must be a finite value, got {seed_value}"
            )));
        }
        if side_divisor == 0 {
            return Err(Error::Validation(
                "nca ignition side_divisor must be at least 1, got 0".to_string(),
            ));
        }
        if !(noise_width.is_finite() && noise_width >= 0.0) {
            return Err(Error::Validation(format!(
                "nca ignition noise_width must be a finite value at least zero, got {noise_width}"
            )));
        }
        Ok(NcaIgnition {
            seed_value,
            side_divisor,
            noise_width,
        })
    }

    /// Builds the seeded initial grid over [`CHANNELS`] channels: an all-zero
    /// background and a centered square whose every state channel carries
    /// `seed_value`. The trajectory's step lives in the harness index, not the
    /// grid, so ignition sets no phase.
    pub(crate) fn ignite(&self, width: u32, height: u32, seed: u64) -> Result<Grid> {
        seeded_patch(
            width,
            height,
            CHANNELS,
            PatchSpec {
                background: &[0.0; CHANNELS as usize],
                patch: &[self.seed_value; CHANNELS as usize],
                side_divisor: self.side_divisor,
                noise: self.noise_width,
            },
            seed,
        )
    }
}

impl Codec for NcaIgnition {
    /// Appends `seed_value` via [`Enc::f32`], `side_divisor` via [`Enc::u32`],
    /// `noise_width` via [`Enc::f32`], in the frozen field order.
    fn encode(&self, enc: &mut Enc) {
        enc.f32(self.seed_value)
            .u32(self.side_divisor)
            .f32(self.noise_width);
    }

    /// Reads the fields and funnels them through [`NcaIgnition::new`] so decode
    /// and construction share one validation path.
    fn decode(dec: &mut Dec<'_>) -> Result<NcaIgnition> {
        let seed_value = dec.f32()?;
        let side_divisor = dec.u32()?;
        let noise_width = dec.f32()?;
        NcaIgnition::new(seed_value, side_divisor, noise_width)
    }
}

impl TomlConfig for NcaIgnition {
    /// Reads the ignition keys from the `[run.params]` table (the shared keys are
    /// already stripped), rejecting any key it does not define. All three keys
    /// are required, with no defaults.
    fn parse(table: &toml::Table, id: &str, section: &str) -> Result<NcaIgnition> {
        translate::reject_unknown_keys(
            id,
            table,
            &["seed_value", "side_divisor", "noise_width"],
            section,
        )?;
        let seed_value = translate::float(table, id, section, "seed_value")?;
        let side_divisor = translate::integer(table, id, section, "side_divisor")?;
        let noise_width = translate::float(table, id, section, "noise_width")?;
        NcaIgnition::new(seed_value, side_divisor, noise_width)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::to_hex;

    use super::*;

    /// The ignition configuration the byte pin is stated against.
    fn sample() -> NcaIgnition {
        NcaIgnition::new(1.0, 8, 0.02).expect("valid sample ignition")
    }

    /// The canonical bytes of [`sample`]: `seed_value 1.0 = 0x3F800000`,
    /// `side_divisor 8` as u32, `noise_width 0.02 = 0x3CA3D70A`, each
    /// little-endian. Independently reproduced with Python `struct`.
    const SAMPLE_BYTES_HEX: &str = "0000803f080000000ad7a33c";

    #[test]
    fn encoding_is_byte_stable() {
        assert_eq!(to_hex(&sample().to_bytes()), SAMPLE_BYTES_HEX);
    }

    #[test]
    fn codec_round_trips() -> Result<()> {
        // A negative seed amplitude and the noiseless boundary are both covered.
        for ignition in [sample(), NcaIgnition::new(-0.5, 4, 0.0)?] {
            assert_eq!(NcaIgnition::from_bytes(&ignition.to_bytes())?, ignition);
        }
        Ok(())
    }

    #[test]
    fn codec_rejects_truncation_and_trailing() {
        let full = sample().to_bytes();
        assert_eq!(full.len(), 12);
        for cut in 0..full.len() {
            assert!(
                matches!(
                    NcaIgnition::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
        let mut trailing = full;
        trailing.push(0);
        assert!(matches!(
            NcaIgnition::from_bytes(&trailing),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn decode_rejects_invalid_values() {
        // Well-formed 12 bytes whose seed_value bits encode a NaN: the structure
        // decodes, the value fails — decode funnels through `new`.
        let mut enc = Enc::new();
        enc.f32(f32::NAN).u32(8).f32(0.02);
        assert!(matches!(
            NcaIgnition::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn new_rejects_invalid_values() -> Result<()> {
        // seed_value must be finite, with no sign rule; noise_width must be
        // finite and non-negative; a zero divisor divides by zero.
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            match NcaIgnition::new(bad, 8, 0.02) {
                Err(Error::Validation(message)) => assert!(
                    message.contains("seed_value"),
                    "the error names seed_value: {message}"
                ),
                other => panic!("seed_value = {bad} must be a validation error, got {other:?}"),
            }
        }
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.01] {
            match NcaIgnition::new(1.0, 8, bad) {
                Err(Error::Validation(message)) => assert!(
                    message.contains("noise_width"),
                    "the error names noise_width: {message}"
                ),
                other => panic!("noise_width = {bad} must be a validation error, got {other:?}"),
            }
        }
        assert!(matches!(
            NcaIgnition::new(1.0, 0, 0.02),
            Err(Error::Validation(_))
        ));
        // A negative seed amplitude is admitted (no sign rule); +0.0 noise sits
        // on the admitted boundary.
        NcaIgnition::new(-1.0, 8, 0.0)?;
        Ok(())
    }

    #[test]
    fn ignite_builds_an_eight_channel_seeded_grid() -> Result<()> {
        // Noiseless so the patch state channels carry seed_value exactly. On a
        // 32x32 grid: side = max(32 / 8, 1) = 4, x0 = y0 = (32 - 4) / 2 = 14, so
        // the patch spans [14, 18) x [14, 18).
        let stride = CHANNELS as usize;
        let ignition = NcaIgnition::new(1.0, 8, 0.0)?;
        let grid = ignition.ignite(32, 32, 42)?;
        assert_eq!((grid.width(), grid.height(), grid.channels()), (32, 32, 8));
        let data = grid.data();
        // A background cell (0, 0): all eight channels zero.
        for (c, value) in data[..stride].iter().enumerate() {
            assert_eq!(value.to_bits(), 0.0f32.to_bits(), "background channel {c}");
        }
        // A patch cell (14, 14): every state channel carries seed_value.
        let patch = (14 * 32 + 14) * stride;
        for (c, value) in data[patch..patch + stride].iter().enumerate() {
            assert_eq!(value.to_bits(), 1.0f32.to_bits(), "patch channel {c}");
        }
        Ok(())
    }

    /// The model's `[run.params]` keys (the shared width/height/steps/dt are
    /// stripped before the model sees the table).
    const KEYS: &str = r#"
        seed_value = 1.0
        side_divisor = 8
        noise_width = 0.02
    "#;

    fn table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    #[test]
    fn parse_reads_the_keys() -> Result<()> {
        assert_eq!(
            NcaIgnition::parse(&table(KEYS), "id", "params")?,
            NcaIgnition::new(1.0, 8, 0.02)?
        );
        Ok(())
    }

    #[test]
    fn parse_rejects_a_missing_key_naming_it() {
        for key in ["seed_value", "side_divisor", "noise_width"] {
            let mut incomplete = table(KEYS);
            incomplete.remove(key);
            match NcaIgnition::parse(&incomplete, "id", "params") {
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
        match NcaIgnition::parse(&extended, "id", "params") {
            Err(Error::Validation(message)) => assert!(
                message.contains("surprise"),
                "the error names the key: {message}"
            ),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn parse_surfaces_value_rules() {
        // The constructor's rules reach the file surface: a zero side_divisor and
        // a negative noise_width.
        for (original, bad) in [
            ("side_divisor = 8", "side_divisor = 0"),
            ("noise_width = 0.02", "noise_width = -0.02"),
        ] {
            let text = KEYS.replace(original, bad);
            assert!(
                matches!(
                    NcaIgnition::parse(&table(&text), "id", "params"),
                    Err(Error::Validation(_))
                ),
                "{bad} must be rejected"
            );
        }
    }
}
