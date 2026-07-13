//! Unit tests for the `Codec` and `TomlConfig` derives, on model-agnostic
//! sample structs. They pin the byte layout the derives emit, the routing of
//! decode/parse through a validating constructor, and the unknown/missing-key
//! and key-override behavior of the TOML parser.

use sima_core::{Codec, Error, Result, TomlConfig, to_hex};

/// A sample carrying one of each accepted field shape: a single `f32`, a single
/// `u32`, and a `[f32; 2]` range. `new` orders the range, so a decode or parse
/// of an inverted range fails through validation.
#[derive(Debug, PartialEq, Codec, TomlConfig)]
#[codec(validate = new)]
#[toml(validate = new)]
struct Sample {
    scale: f32,
    count: u32,
    span: [f32; 2],
}

impl Sample {
    fn new(scale: f32, count: u32, span: [f32; 2]) -> Result<Sample> {
        if span[0] > span[1] {
            return Err(Error::Validation(format!(
                "span must satisfy lo <= hi, got {span:?}"
            )));
        }
        Ok(Sample { scale, count, span })
    }
}

/// A sample without a validating constructor: `decode` builds the struct
/// directly. Proves the no-`validate` codec path.
#[derive(Debug, PartialEq, Codec)]
struct Raw {
    a: f32,
    b: u32,
}

/// A sample whose TOML key differs from its field name via `#[toml(key = "…")]`.
#[derive(Debug, PartialEq, Codec, TomlConfig)]
#[codec(validate = new)]
#[toml(validate = new)]
struct Renamed {
    #[toml(key = "rate")]
    value: f32,
}

impl Renamed {
    fn new(value: f32) -> Result<Renamed> {
        Ok(Renamed { value })
    }
}

fn table(text: &str) -> toml::Table {
    text.parse().expect("parse test table")
}

/// The canonical bytes of `Sample { 1.5, 7, [0.25, 0.5] }` — scale as f32 bits
/// LE, count as u32 LE, then the two range bounds as f32 bits LE — reproduced
/// independently with Python `struct`:
/// `struct.pack('<f',1.5)+struct.pack('<I',7)+struct.pack('<f',0.25)+struct.pack('<f',0.5)`.
const SAMPLE_BYTES_HEX: &str = "0000c03f070000000000803e0000003f";

#[test]
fn codec_layout_is_field_order_by_type() -> Result<()> {
    let sample = Sample::new(1.5, 7, [0.25, 0.5])?;
    assert_eq!(to_hex(&sample.to_bytes()), SAMPLE_BYTES_HEX);
    Ok(())
}

#[test]
fn codec_round_trips_through_bytes() -> Result<()> {
    let sample = Sample::new(1.5, 7, [0.25, 0.5])?;
    assert_eq!(Sample::from_bytes(&sample.to_bytes())?, sample);
    Ok(())
}

#[test]
fn codec_decode_routes_through_new() {
    // Well-formed bytes whose range decodes inverted: the structure decodes, the
    // value fails through the validating constructor.
    let mut enc = sima_core::Enc::new();
    enc.f32(1.0).u32(1).f32(0.5).f32(0.25);
    assert!(matches!(
        Sample::from_bytes(&enc.finish()),
        Err(Error::Validation(_))
    ));
}

#[test]
fn codec_rejects_truncation_and_trailing() -> Result<()> {
    let full = Sample::new(1.5, 7, [0.25, 0.5])?.to_bytes();
    for cut in 0..full.len() {
        assert!(
            matches!(Sample::from_bytes(&full[..cut]), Err(Error::Encoding(_))),
            "prefix of {cut} bytes must be rejected"
        );
    }
    let mut trailing = full;
    trailing.push(0);
    assert!(matches!(
        Sample::from_bytes(&trailing),
        Err(Error::Encoding(_))
    ));
    Ok(())
}

#[test]
fn codec_without_validate_builds_directly() -> Result<()> {
    let raw = Raw { a: 2.5, b: 9 };
    assert_eq!(Raw::from_bytes(&raw.to_bytes())?, raw);
    Ok(())
}

#[test]
fn toml_parse_reads_each_field_by_type() -> Result<()> {
    let parsed = Sample::parse(
        &table("scale = 1.5\ncount = 7\nspan = [0.25, 0.5]"),
        "sample.v1",
        "params",
    )?;
    assert_eq!(parsed, Sample::new(1.5, 7, [0.25, 0.5])?);
    // An integer coerces through the one number path.
    let integer = Sample::parse(
        &table("scale = 2\ncount = 1\nspan = [0, 1]"),
        "sample.v1",
        "params",
    )?;
    assert_eq!(integer, Sample::new(2.0, 1, [0.0, 1.0])?);
    Ok(())
}

#[test]
fn toml_parse_routes_through_new() {
    // An inverted range surfaces the constructor's rule at the file surface.
    match Sample::parse(
        &table("scale = 1.0\ncount = 1\nspan = [0.5, 0.25]"),
        "sample.v1",
        "params",
    ) {
        Err(Error::Validation(message)) => assert!(message.contains("span")),
        other => panic!("expected Validation, got {other:?}"),
    }
}

#[test]
fn toml_parse_rejects_a_missing_key_naming_it() {
    for key in ["scale", "count", "span"] {
        let mut incomplete = table("scale = 1.5\ncount = 7\nspan = [0.25, 0.5]");
        incomplete.remove(key);
        match Sample::parse(&incomplete, "sample.v1", "params") {
            Err(Error::Validation(message)) => {
                assert!(message.contains(key), "the error names {key}: {message}")
            }
            other => panic!("expected Validation for missing {key}, got {other:?}"),
        }
    }
}

#[test]
fn toml_parse_rejects_an_unknown_key_naming_it() {
    let mut extended = table("scale = 1.5\ncount = 7\nspan = [0.25, 0.5]");
    extended.insert("surprise".to_string(), toml::Value::Integer(1));
    match Sample::parse(&extended, "sample.v1", "params") {
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
fn toml_key_override_renames_the_key() -> Result<()> {
    // The field is read from `rate`, not `value`.
    assert_eq!(
        Renamed::parse(&table("rate = 2.0"), "renamed.v1", "params")?,
        Renamed::new(2.0)?
    );
    // The field name is not a valid key: it is rejected as unknown.
    match Renamed::parse(&table("value = 2.0"), "renamed.v1", "params") {
        Err(Error::Validation(message)) => assert!(message.contains("value")),
        other => panic!("expected Validation, got {other:?}"),
    }
    Ok(())
}

/// A struct whose field `id` shares a name with a parameter the generated
/// `parse` uses. The derives must be hygienic against field names, so `id` — a
/// plausible config field — compiles and round-trips like any other field.
#[derive(Debug, PartialEq, Codec, TomlConfig)]
#[codec(validate = new)]
#[toml(validate = new)]
struct Collision {
    id: u32,
    scale: f32,
}

impl Collision {
    fn new(id: u32, scale: f32) -> Result<Collision> {
        Ok(Collision { id, scale })
    }
}

#[test]
fn a_field_named_like_a_generated_parameter_round_trips() -> Result<()> {
    let value = Collision::new(7, 1.5)?;
    assert_eq!(Collision::from_bytes(&value.to_bytes())?, value);
    let parsed = Collision::parse(&table("id = 7\nscale = 1.5"), "collision.v1", "params")?;
    assert_eq!(parsed, value);
    Ok(())
}
