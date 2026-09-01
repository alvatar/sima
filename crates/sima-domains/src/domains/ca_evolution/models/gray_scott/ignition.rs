//! [`GrayScottIgnition`]: the ignition configuration of the Gray-Scott model's
//! initial grid, and the model's `[search.params]` ignition keys.

use sima_core::{Codec, Dec, Enc, Error, Result};

use crate::domains::ca_evolution::values::finite_sign_positive;

/// What a refusal names these values as belonging to.
const SUBJECT: &str = "gray_scott ignition";

use crate::domains::translate::{self, TomlConfig};

/// The ignition configuration of the Gray-Scott model's initial grid: the base
/// values dropped into the centered square patch, the divisor of the shorter
/// grid extent giving the patch side, and the full relative width of the noise
/// band around each base value.
///
/// Pearson's classical configuration is `(0.5, 0.25, 8, 0.02)`, spelled by the
/// caller: the values determine candidate identity, so there is no `Default` —
/// identity-determining values are always explicit.
///
/// The canonical form is `base_u` and `base_v` each as f32 bits little-endian,
/// `side_divisor` as a little-endian `u32`, `noise_width` as f32 bits
/// little-endian: exactly 16 bytes. Every scalar is `f32` — the width of the
/// grid state these values seed — so the ignition's content address reflects
/// exactly the precision the grid carries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct GrayScottIgnition {
    /// Base value of chemical `u` inside the patch.
    base_u: f32,
    /// Base value of chemical `v` inside the patch.
    base_v: f32,
    /// Divisor of the shorter grid extent giving the patch side:
    /// `side = max(min(width, height) / side_divisor, 1)`.
    side_divisor: u32,
    /// Full relative width of the noise band around each base value:
    /// `(t - 0.5) * noise_width` spans ±`noise_width / 2` for `t ∈ [0, 1]` — the
    /// unit draw's `[0, 1)` closes at the top when `unit_f64(..) as f32` rounds a
    /// large draw up to `1.0`.
    noise_width: f32,
}

impl GrayScottIgnition {
    /// Builds an ignition configuration, validating each field: `base_u`,
    /// `base_v`, and `noise_width` must be finite with positive sign (`+0.0`
    /// admitted); `side_divisor` must be at least 1, since 0 would divide by zero
    /// in the side computation. Any violation is [`Error::Validation`] naming the
    /// field.
    pub(crate) fn new(
        base_u: f32,
        base_v: f32,
        side_divisor: u32,
        noise_width: f32,
    ) -> Result<GrayScottIgnition> {
        if side_divisor == 0 {
            return Err(Error::Validation(
                "gray_scott ignition side_divisor must be at least 1, got 0".to_string(),
            ));
        }
        Ok(GrayScottIgnition {
            base_u: finite_sign_positive(SUBJECT, "base_u", base_u)?,
            base_v: finite_sign_positive(SUBJECT, "base_v", base_v)?,
            side_divisor,
            noise_width: finite_sign_positive(SUBJECT, "noise_width", noise_width)?,
        })
    }

    /// The base value of chemical `u` inside the patch.
    pub(crate) fn base_u(&self) -> f32 {
        self.base_u
    }

    /// The base value of chemical `v` inside the patch.
    pub(crate) fn base_v(&self) -> f32 {
        self.base_v
    }

    /// The divisor of the shorter grid extent giving the patch side.
    pub(crate) fn side_divisor(&self) -> u32 {
        self.side_divisor
    }

    /// The full relative width of the noise band around each base value.
    pub(crate) fn noise_width(&self) -> f32 {
        self.noise_width
    }
}

impl Codec for GrayScottIgnition {
    /// Appends `base_u` and `base_v` via [`Enc::f32`], `side_divisor` via
    /// [`Enc::u32`], `noise_width` via [`Enc::f32`], in the frozen field order.
    fn encode(&self, enc: &mut Enc) {
        enc.f32(self.base_u)
            .f32(self.base_v)
            .u32(self.side_divisor)
            .f32(self.noise_width);
    }

    /// Reads the fields and funnels them through [`GrayScottIgnition::new`] so
    /// decode and construction share one validation path.
    fn decode(dec: &mut Dec<'_>) -> Result<GrayScottIgnition> {
        let base_u = dec.f32()?;
        let base_v = dec.f32()?;
        let side_divisor = dec.u32()?;
        let noise_width = dec.f32()?;
        GrayScottIgnition::new(base_u, base_v, side_divisor, noise_width)
    }
}

impl TomlConfig for GrayScottIgnition {
    /// Reads the ignition keys from the `[search.params]` table (the shared keys are
    /// already stripped), rejecting any key it does not define. All four keys are
    /// required, with no defaults — every value that determines candidate identity
    /// is visible in the config file.
    fn parse(table: &toml::Table, id: &str, section: &str) -> Result<GrayScottIgnition> {
        translate::reject_unknown_keys(
            id,
            table,
            &["base_u", "base_v", "side_divisor", "noise_width"],
            section,
        )?;
        let base_u = translate::float(table, id, section, "base_u")?;
        let base_v = translate::float(table, id, section, "base_v")?;
        let side_divisor = translate::integer(table, id, section, "side_divisor")?;
        let noise_width = translate::float(table, id, section, "noise_width")?;
        GrayScottIgnition::new(base_u, base_v, side_divisor, noise_width)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::{Dec, Enc, to_hex};

    use super::*;

    /// Pearson's classical ignition configuration.
    fn pearson() -> GrayScottIgnition {
        GrayScottIgnition::new(0.5, 0.25, 8, 0.02).expect("valid pearson ignition")
    }

    /// The canonical bytes of [`pearson`], derived by hand from the layout —
    /// `base_u` and `base_v` as f32 bits little-endian, `side_divisor` as u32
    /// little-endian, `noise_width` as f32 bits little-endian: `0.5 =
    /// 0x3F000000`, `0.25 = 0x3E800000`, `8`, `0.02 = 0x3CA3D70A` — and
    /// independently reproduced with Python `struct`.
    const PEARSON_BYTES_HEX: &str = "0000003f0000803e080000000ad7a33c";

    #[test]
    fn codec_round_trips() -> Result<()> {
        // The noiseless configuration sits on the admitted boundary of the sign
        // rule, so the round trip covers +0.0 explicitly.
        for ignition in [pearson(), GrayScottIgnition::new(0.5, 0.25, 8, 0.0)?] {
            let mut enc = Enc::new();
            ignition.encode(&mut enc);
            let buf = enc.finish();
            let mut dec = Dec::new(&buf);
            assert_eq!(GrayScottIgnition::decode(&mut dec)?, ignition);
            dec.finish()?;
        }
        Ok(())
    }

    #[test]
    fn encoding_is_byte_stable() {
        let mut enc = Enc::new();
        pearson().encode(&mut enc);
        assert_eq!(to_hex(&enc.finish()), PEARSON_BYTES_HEX);
    }

    #[test]
    fn decode_rejects_invalid_values() {
        // Well-formed 16 bytes whose base_u bits encode a NaN: the structure
        // decodes, the value fails — decode funnels through `new`.
        let mut enc = Enc::new();
        enc.f32(f32::NAN).f32(0.25).u32(8).f32(0.02);
        let buf = enc.finish();
        let mut dec = Dec::new(&buf);
        assert!(matches!(
            GrayScottIgnition::decode(&mut dec),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn new_rejects_invalid_values() -> Result<()> {
        // Each invalid value substituted into each f32 field; the error names the
        // field. -0.0 is rejected like the genome's rule: one value, one byte
        // image.
        let names = ["base_u", "base_v", "noise_width"];
        for (position, name) in names.iter().enumerate() {
            for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.5, -0.0] {
                let mut p = [0.5, 0.25, 0.02];
                p[position] = bad;
                match GrayScottIgnition::new(p[0], p[1], 8, p[2]) {
                    Err(Error::Validation(message)) => assert!(
                        message.contains(name),
                        "message {message:?} must name {name}"
                    ),
                    other => panic!("{name} = {bad} must be a validation error, got {other:?}"),
                }
            }
        }
        // A zero divisor would divide by zero in the side computation.
        assert!(matches!(
            GrayScottIgnition::new(0.5, 0.25, 0, 0.02),
            Err(Error::Validation(_))
        ));
        // A valid construction round-trips through the accessors.
        let ignition = GrayScottIgnition::new(0.5, 0.25, 8, 0.02)?;
        assert_eq!(ignition.base_u(), 0.5);
        assert_eq!(ignition.base_v(), 0.25);
        assert_eq!(ignition.side_divisor(), 8);
        assert_eq!(ignition.noise_width(), 0.02);
        Ok(())
    }

    /// The model's `[search.params]` keys (the shared width/height/steps/dt are
    /// stripped before the model sees the table).
    const KEYS: &str = r#"
        base_u = 0.5
        base_v = 0.25
        side_divisor = 8
        noise_width = 0.02
    "#;

    fn table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    #[test]
    fn parse_reads_the_keys() -> Result<()> {
        // `noise_width = 0` (integer) reads through the one number path as +0.0.
        let integer = KEYS.replace("noise_width = 0.02", "noise_width = 0");
        assert_eq!(
            GrayScottIgnition::parse(&table(&integer), "id", "params")?,
            GrayScottIgnition::new(0.5, 0.25, 8, 0.0)?
        );
        assert_eq!(
            GrayScottIgnition::parse(&table(KEYS), "id", "params")?,
            GrayScottIgnition::new(0.5, 0.25, 8, 0.02)?
        );
        Ok(())
    }

    #[test]
    fn parse_rejects_a_missing_key_naming_it() {
        for key in ["base_u", "base_v", "side_divisor", "noise_width"] {
            let mut incomplete = table(KEYS);
            incomplete.remove(key);
            match GrayScottIgnition::parse(&incomplete, "id", "params") {
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
        match GrayScottIgnition::parse(&extended, "id", "params") {
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
    fn parse_surfaces_value_rules() {
        // The constructor's rules reach the file surface: a negative base and a
        // zero side_divisor.
        for (original, bad) in [
            ("base_u = 0.5", "base_u = -0.5"),
            ("side_divisor = 8", "side_divisor = 0"),
        ] {
            let text = KEYS.replace(original, bad);
            assert!(
                matches!(
                    GrayScottIgnition::parse(&table(&text), "id", "params"),
                    Err(Error::Validation(_))
                ),
                "{bad} must be rejected"
            );
        }
    }
}
