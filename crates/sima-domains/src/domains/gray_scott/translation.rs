//! The Gray-Scott config translations: the `[run.generator]` section into
//! canonical [`GrayScottGeneratorConfig`] bytes, the `[run.params]` section
//! into canonical [`GrayScottParams`] bytes, and the binding of the
//! domain's id.

use sima_core::{Error, Result};
use sima_model::Params;

use super::{GrayScottGeneratorConfig, GrayScottParams, GrayScottPatch};
use crate::domains::translate::reject_unknown_keys;

/// The Gray-Scott format id, doubling as its generator id.
pub(crate) const ID: &str = "gray-scott.v1";

/// Translates the `[run.generator]` table (minus `id`): a required `count`
/// and one required range per genome parameter, encoded through
/// [`GrayScottGeneratorConfig`]'s own codec, so the box validation surfaces
/// here. All five keys are required, with no defaults — every value that
/// determines candidate identity is visible in the config file. Unknown
/// keys are rejected.
pub(crate) fn generator_params(table: &toml::Table) -> Result<Vec<u8>> {
    reject_unknown_keys(
        ID,
        table,
        &["count", "feed", "kill", "diffusion_u", "diffusion_v"],
        "generator",
    )?;
    let count = count(table)?;
    let feed = range(table, "feed")?;
    let kill = range(table, "kill")?;
    let diffusion_u = range(table, "diffusion_u")?;
    let diffusion_v = range(table, "diffusion_v")?;
    Ok(GrayScottGeneratorConfig::new(count, feed, kill, diffusion_u, diffusion_v)?.to_bytes())
}

/// Translates the `[run.params]` table: eight required keys — the grid
/// extents, the per-task step count, the integration step, and the four
/// ignition patch values — encoded through [`GrayScottParams`]'s own codec,
/// so the params and patch validation surfaces here. All eight keys are
/// required, with no defaults — every value that determines candidate
/// identity is visible in the config file. Unknown keys are rejected.
#[allow(dead_code)]
pub(crate) fn params(table: &toml::Table) -> Result<Params> {
    reject_unknown_keys(
        ID,
        table,
        &[
            "width",
            "height",
            "steps",
            "dt",
            "base_u",
            "base_v",
            "side_divisor",
            "noise_width",
        ],
        "params",
    )?;
    let width = integer(table, "width")?;
    let height = integer(table, "height")?;
    let steps = integer(table, "steps")?;
    // dt is the one f32 field; it narrows from the shared f64 number path.
    let dt = float(table, "dt")? as f32;
    let base_u = float(table, "base_u")?;
    let base_v = float(table, "base_v")?;
    let side_divisor = integer(table, "side_divisor")?;
    let noise_width = float(table, "noise_width")?;
    let patch = GrayScottPatch::new(base_u, base_v, side_divisor, noise_width)?;
    Ok(Params {
        bytes: GrayScottParams::new(width, height, steps, dt, patch)?.to_bytes(),
    })
}

/// The required unsigned integer at `key`: a TOML integer within `u32`
/// range. Zero passes here — the ≥ 1 rules live in the params and patch
/// constructors, which name the field.
fn integer(table: &toml::Table, key: &str) -> Result<u32> {
    match required(table, "params", key)? {
        toml::Value::Integer(n) => u32::try_from(*n).map_err(|_| {
            Error::Validation(format!(
                "params gray-scott.v1 {key} must be an unsigned 32-bit integer, got {n}"
            ))
        }),
        other => Err(Error::Validation(format!(
            "params gray-scott.v1 {key} must be an integer, got {}",
            other.type_str()
        ))),
    }
}

/// The required number at `key`: integer and float are both accepted
/// (`0` means `+0.0`), read at f64 — the patch's stored precision — as the
/// config file's one number path for the params section.
fn float(table: &toml::Table, key: &str) -> Result<f64> {
    match required(table, "params", key)? {
        toml::Value::Integer(n) => Ok(*n as f64),
        toml::Value::Float(f) => Ok(*f),
        other => Err(Error::Validation(format!(
            "params gray-scott.v1 {key} must be a number, got {}",
            other.type_str()
        ))),
    }
}

/// The required `count` key: a TOML integer with value at least 1.
fn count(table: &toml::Table) -> Result<u64> {
    match required(table, "generator", "count")? {
        toml::Value::Integer(n) if *n >= 1 => Ok(*n as u64),
        toml::Value::Integer(n) => Err(Error::Validation(format!(
            "generator gray-scott.v1 count must be at least 1, got {n}"
        ))),
        other => Err(Error::Validation(format!(
            "generator gray-scott.v1 count must be an integer, got {}",
            other.type_str()
        ))),
    }
}

/// The required range at `key`: a two-element `[lo, hi]` array of numbers.
/// A fixed parameter is spelled as the degenerate range `[v, v]`.
fn range(table: &toml::Table, key: &str) -> Result<[f32; 2]> {
    let value = required(table, "generator", key)?;
    if let Some([lo, hi]) = value.as_array().map(Vec::as_slice)
        && let (Some(lo), Some(hi)) = (number(lo), number(hi))
    {
        return Ok([lo, hi]);
    }
    Err(Error::Validation(format!(
        "generator gray-scott.v1 {key} must be a two-element [lo, hi] array of numbers, got {}",
        value.type_str()
    )))
}

/// The value at `key`; a missing key is [`Error::Validation`] naming it and
/// the `section` it belongs to.
fn required<'t>(table: &'t toml::Table, section: &str, key: &str) -> Result<&'t toml::Value> {
    table.get(key).ok_or_else(|| {
        Error::Validation(format!("{section} gray-scott.v1 requires the key {key:?}"))
    })
}

/// The numeric reading of a TOML value: integer and float are both accepted
/// (a `0` range element means `+0.0`), converted as f64 then f32 — the
/// config file's one number path.
fn number(value: &toml::Value) -> Option<f32> {
    match value {
        toml::Value::Integer(n) => Some(*n as f64 as f32),
        toml::Value::Float(f) => Some(*f as f32),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses `text` as a TOML table.
    fn table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    /// The full grammar: one `[lo, hi]` range per parameter, the classical
    /// diffusion pair pinned by degenerate ranges.
    const FULL: &str = r#"
        count = 64
        feed = [0.01, 0.08]
        kill = [0.03, 0.07]
        diffusion_u = [0.16, 0.16]
        diffusion_v = [0.08, 0.08]
    "#;

    #[test]
    fn a_full_table_encodes_through_the_config_codec() -> Result<()> {
        let expected = GrayScottGeneratorConfig::new(
            64,
            [0.01, 0.08],
            [0.03, 0.07],
            [0.16, 0.16],
            [0.08, 0.08],
        )?;
        assert_eq!(generator_params(&table(FULL))?, expected.to_bytes());
        Ok(())
    }

    #[test]
    fn integer_values_are_accepted_as_numbers() -> Result<()> {
        // `feed = [0, 0]` means the degenerate range [+0.0, +0.0].
        let integer = FULL.replace("feed = [0.01, 0.08]", "feed = [0, 0]");
        let float = FULL.replace("feed = [0.01, 0.08]", "feed = [0.0, 0.0]");
        assert_eq!(
            generator_params(&table(&integer))?,
            generator_params(&table(&float))?
        );
        Ok(())
    }

    #[test]
    fn missing_keys_are_rejected_naming_the_key() {
        for key in ["count", "feed", "kill", "diffusion_u", "diffusion_v"] {
            let mut incomplete = table(FULL);
            incomplete.remove(key);
            match generator_params(&incomplete) {
                Err(Error::Validation(message)) => {
                    assert!(message.contains(key), "the error names {key}: {message}");
                }
                other => panic!("expected Validation for missing {key}, got {other:?}"),
            }
        }
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let mut extended = table(FULL);
        extended.insert("surprise".to_string(), toml::Value::Integer(1));
        match generator_params(&extended) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("surprise"),
                    "the error names the key: {message}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn malformed_values_are_rejected() {
        // Parameter keys: scalars, wrong types, and wrong array shapes.
        // `count`: zero, negative, float, string.
        let cases = [
            ("feed = [0.01, 0.08]", "feed = 0.16"),
            ("feed = [0.01, 0.08]", "feed = 0"),
            ("feed = [0.01, 0.08]", r#"feed = "fast""#),
            ("feed = [0.01, 0.08]", "feed = true"),
            ("feed = [0.01, 0.08]", "feed = [0.01]"),
            ("feed = [0.01, 0.08]", "feed = [0.01, 0.05, 0.08]"),
            ("feed = [0.01, 0.08]", r#"feed = [0.01, "hi"]"#),
            ("count = 64", "count = 0"),
            ("count = 64", "count = -1"),
            ("count = 64", "count = 1.5"),
            ("count = 64", r#"count = "many""#),
        ];
        for (original, bad) in cases {
            let text = FULL.replace(original, bad);
            assert!(
                matches!(generator_params(&table(&text)), Err(Error::Validation(_))),
                "{bad} must be rejected"
            );
        }
    }

    /// The full `[run.params]` grammar: all eight keys.
    const FULL_PARAMS: &str = r#"
        width = 64
        height = 48
        steps = 100
        dt = 1.0
        base_u = 0.5
        base_v = 0.25
        side_divisor = 8
        noise_width = 0.02
    "#;

    #[test]
    fn a_full_params_table_encodes_through_the_params_codec() -> Result<()> {
        let patch = GrayScottPatch::new(0.5, 0.25, 8, 0.02)?;
        let expected = GrayScottParams::new(64, 48, 100, 1.0, patch)?;
        assert_eq!(params(&table(FULL_PARAMS))?.bytes, expected.to_bytes());
        Ok(())
    }

    #[test]
    fn missing_params_keys_are_rejected_naming_the_key() {
        let keys = [
            "width",
            "height",
            "steps",
            "dt",
            "base_u",
            "base_v",
            "side_divisor",
            "noise_width",
        ];
        for key in keys {
            let mut incomplete = table(FULL_PARAMS);
            incomplete.remove(key);
            match params(&incomplete) {
                Err(Error::Validation(message)) => {
                    assert!(message.contains(key), "the error names {key}: {message}");
                }
                other => panic!("expected Validation for missing {key}, got {other:?}"),
            }
        }
    }

    #[test]
    fn unknown_params_keys_are_rejected() {
        let mut extended = table(FULL_PARAMS);
        extended.insert("surprise".to_string(), toml::Value::Integer(1));
        match params(&extended) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("surprise"),
                    "the error names the key: {message}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn malformed_params_values_are_rejected() {
        // Translation-level shape errors first, then value rules surfacing
        // through the GrayScottParams and GrayScottPatch validation.
        let cases = [
            ("width = 64", r#"width = "wide""#),
            ("width = 64", "width = -1"),
            ("height = 48", "height = 0"),
            ("steps = 100", "steps = 1.5"),
            ("dt = 1.0", "dt = 0.0"),
            ("base_u = 0.5", "base_u = -0.5"),
            ("side_divisor = 8", "side_divisor = 0"),
        ];
        for (original, bad) in cases {
            let text = FULL_PARAMS.replace(original, bad);
            assert!(
                matches!(params(&table(&text)), Err(Error::Validation(_))),
                "{bad} must be rejected"
            );
        }
    }

    #[test]
    fn integer_params_values_are_accepted_as_numbers() -> Result<()> {
        // `noise_width = 0` means the noiseless +0.0 configuration.
        let integer = FULL_PARAMS.replace("noise_width = 0.02", "noise_width = 0");
        let float = FULL_PARAMS.replace("noise_width = 0.02", "noise_width = 0.0");
        assert_eq!(
            params(&table(&integer))?.bytes,
            params(&table(&float))?.bytes
        );
        // `dt` reads through the same one number path.
        let dt_integer = FULL_PARAMS.replace("dt = 1.0", "dt = 1");
        assert_eq!(
            params(&table(&dt_integer))?.bytes,
            params(&table(FULL_PARAMS))?.bytes
        );
        Ok(())
    }

    #[test]
    fn box_validation_surfaces_through_translation() {
        // The config constructor's rules reach the file surface: the
        // genome's strict-positivity rule via the corner check, and the
        // ordered-bounds rule.
        for (original, bad) in [
            ("diffusion_u = [0.16, 0.16]", "diffusion_u = [0.0, 0.0]"),
            ("feed = [0.01, 0.08]", "feed = [0.08, 0.01]"),
        ] {
            let text = FULL.replace(original, bad);
            assert!(
                matches!(generator_params(&table(&text)), Err(Error::Validation(_))),
                "{bad} must be rejected"
            );
        }
    }
}
