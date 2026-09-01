//! The human-readable config world: the [`TomlConfig`] trait and the coercion
//! helpers its hand-written impls call.
//!
//! Each config struct implements [`TomlConfig`] by hand: [`reject_unknown_keys`]
//! guards the field set, then one helper per field shape reads and coerces the
//! value — [`integer`] for `u32`, [`float`] for `f32`, [`range`] for `[f32; 2]`
//! — and the values route through the struct's validating constructor. Every
//! error names the domain `id`, the config `section`, and the key.

use sima_core::{Error, Result};

/// A struct parsable from a `[section]` TOML table: each field read from its
/// same-named key, coerced by field type, unknown keys rejected, and the values
/// routed through the type's validating constructor. Implemented by hand per
/// config struct.
pub(crate) trait TomlConfig: Sized {
    /// Parses `table` as the `section` config for domain `id`.
    fn parse(table: &toml::Table, id: &str, section: &str) -> Result<Self>;
}

/// Rejects any key of `table` outside `known`, naming the key, the domain `id`,
/// and the `section` it appeared in.
pub(crate) fn reject_unknown_keys(
    id: &str,
    table: &toml::Table,
    known: &[&str],
    section: &str,
) -> Result<()> {
    for key in table.keys() {
        if !known.contains(&key.as_str()) {
            return Err(Error::Validation(format!(
                "{id} {section} config does not define the key {key:?}"
            )));
        }
    }
    Ok(())
}

/// The value at `key`; a missing key is [`Error::Validation`] naming it, the
/// domain `id`, and the `section` it belongs to.
pub(crate) fn required<'t>(
    table: &'t toml::Table,
    id: &str,
    section: &str,
    key: &str,
) -> Result<&'t toml::Value> {
    table
        .get(key)
        .ok_or_else(|| Error::Validation(format!("{section} {id} requires the key {key:?}")))
}

/// The required unsigned integer at `key`: a TOML integer within `u32` range.
/// Zero passes here — the `>= 1` rules live in the constructors that name the
/// field.
pub(crate) fn integer(table: &toml::Table, id: &str, section: &str, key: &str) -> Result<u32> {
    match required(table, id, section, key)? {
        toml::Value::Integer(n) => u32::try_from(*n).map_err(|_| {
            Error::Validation(format!(
                "{section} {id} {key} must be an unsigned 32-bit integer, got {n}"
            ))
        }),
        other => Err(Error::Validation(format!(
            "{section} {id} {key} must be an integer, got {}",
            other.type_str()
        ))),
    }
}

/// The required number at `key`: integer and float are both accepted (`0` means
/// `+0.0`), read as `f32` — the config file's one number path via [`number`].
pub(crate) fn float(table: &toml::Table, id: &str, section: &str, key: &str) -> Result<f32> {
    let value = required(table, id, section, key)?;
    number(value).ok_or_else(|| {
        Error::Validation(format!(
            "{section} {id} {key} must be a number, got {}",
            value.type_str()
        ))
    })
}

/// The numeric reading of a TOML value: integer and float are both accepted (a
/// `0` means `+0.0`), converted as f64 then f32 — the config file's one number
/// path.
fn number(value: &toml::Value) -> Option<f32> {
    match value {
        toml::Value::Integer(n) => Some(*n as f64 as f32),
        toml::Value::Float(f) => Some(*f as f32),
        _ => None,
    }
}

/// The required range at `key`: a two-element `[lo, hi]` array of numbers. A
/// fixed parameter is spelled as the degenerate range `[v, v]`.
pub(crate) fn range(table: &toml::Table, id: &str, section: &str, key: &str) -> Result<[f32; 2]> {
    let value = required(table, id, section, key)?;
    if let Some([lo, hi]) = value.as_array().map(Vec::as_slice)
        && let (Some(lo), Some(hi)) = (number(lo), number(hi))
    {
        return Ok([lo, hi]);
    }
    Err(Error::Validation(format!(
        "{section} {id} {key} must be a two-element [lo, hi] array of numbers, got {}",
        value.type_str()
    )))
}

/// Parses a configuration section's text into the table its translation reads.
/// Empty text is the table with no keys: a search that states no section.
pub(crate) fn table(toml: &str) -> Result<toml::Table> {
    toml.parse()
        .map_err(|e| Error::Validation(format!("the configuration section is not valid TOML: {e}")))
}
