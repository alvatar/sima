//! [`CaParams`]: the run knobs every CA model shares, and the shared half of the
//! `[run.params]` translation.

use sima_core::{Codec, Dec, Enc, Error, Result};
use sima_model::Params;

use super::model::CaModel;
use crate::domains::translate::{TomlConfig, float, integer, reject_unknown_keys};

/// The run parameters shared by every CA model: the grid extents, the steps one
/// segment advances, and the integration step size. A model's own ignition
/// configuration follows these in the canonical params blob.
///
/// The canonical form is `width`, `height`, `steps` as little-endian `u32`, `dt`
/// as its IEEE-754 bits in a little-endian `u32` — exactly 16 bytes — then the
/// model's ignition bytes.
///
/// `steps` counts one segment's steps. A candidate's trajectory is a chain of
/// `segments` tasks, one task per segment, so the full trajectory spans
/// `segments * steps` steps. A total-steps-across-segments reading would need a
/// segment index threaded into the task input; the design keeps `steps`
/// per-segment, which needs no such index.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CaParams {
    width: u32,
    height: u32,
    steps: u32,
    dt: f32,
}

/// The shared `[run.params]` keys; the model owns any key beyond these.
pub(crate) const SHARED_KEYS: [&str; 4] = ["width", "height", "steps", "dt"];

/// Validates a count parameter: at least 1. Zero cells make no grid, and a
/// zero-step segment would commit its input unchanged.
fn at_least_one(name: &str, value: u32) -> Result<u32> {
    if value >= 1 {
        Ok(value)
    } else {
        Err(Error::Validation(format!(
            "ca_evolution params {name} must be at least 1, got {value}"
        )))
    }
}

impl CaParams {
    /// Builds shared run parameters, validating each field: `width`, `height`,
    /// and `steps` must be at least 1; `dt` must be finite and strictly greater
    /// than zero. Any violation is [`Error::Validation`] naming the field.
    pub(crate) fn new(width: u32, height: u32, steps: u32, dt: f32) -> Result<CaParams> {
        if !(dt.is_finite() && dt > 0.0) {
            return Err(Error::Validation(format!(
                "ca_evolution params dt must be a finite value greater than zero, got {dt}"
            )));
        }
        Ok(CaParams {
            width: at_least_one("width", width)?,
            height: at_least_one("height", height)?,
            steps: at_least_one("steps", steps)?,
            dt,
        })
    }

    /// The grid width in cells.
    pub(crate) fn width(&self) -> u32 {
        self.width
    }

    /// The grid height in cells.
    pub(crate) fn height(&self) -> u32 {
        self.height
    }

    /// The simulation steps one segment advances.
    pub(crate) fn steps(&self) -> u32 {
        self.steps
    }

    /// The integration step size.
    pub(crate) fn dt(&self) -> f32 {
        self.dt
    }
}

impl Codec for CaParams {
    /// Appends `width`, `height`, `steps` via [`Enc::u32`], `dt` via [`Enc::f32`],
    /// in the frozen field order.
    fn encode(&self, enc: &mut Enc) {
        enc.u32(self.width)
            .u32(self.height)
            .u32(self.steps)
            .f32(self.dt);
    }

    /// Reads the fields and funnels them through [`CaParams::new`] so decode and
    /// construction share one validation path.
    fn decode(dec: &mut Dec<'_>) -> Result<CaParams> {
        let width = dec.u32()?;
        let height = dec.u32()?;
        let steps = dec.u32()?;
        let dt = dec.f32()?;
        CaParams::new(width, height, steps, dt)
    }
}

impl TomlConfig for CaParams {
    /// Reads the shared `[run.params]` keys, rejecting any key outside the set,
    /// and funnels them through [`CaParams::new`].
    fn parse(table: &toml::Table, id: &str, section: &str) -> Result<CaParams> {
        reject_unknown_keys(id, table, &SHARED_KEYS, section)?;
        let width = integer(table, id, section, "width")?;
        let height = integer(table, id, section, "height")?;
        let steps = integer(table, id, section, "steps")?;
        let dt = float(table, id, section, "dt")?;
        CaParams::new(width, height, steps, dt)
    }
}

/// The canonical run-params blob: the shared fields, then the model's ignition.
pub(crate) fn encode_params<M: CaModel>(shared: &CaParams, ignition: &M::Ignition) -> Vec<u8> {
    let mut enc = Enc::new();
    shared.encode(&mut enc);
    ignition.encode(&mut enc);
    enc.finish()
}

/// Parses the run-params blob: the shared fields, then the model's ignition
/// codec consumes the remainder. Trailing bytes are a decode error.
pub(crate) fn decode_params<M: CaModel>(bytes: &[u8]) -> Result<(CaParams, M::Ignition)> {
    let mut dec = Dec::new(bytes);
    let shared = CaParams::decode(&mut dec)?;
    let ignition = M::Ignition::decode(&mut dec)?;
    dec.finish()?;
    Ok((shared, ignition))
}

/// Translates the `[run.params]` table into the canonical params blob: the
/// shared keys here, the model's ignition keys via its ignition's
/// [`TomlConfig`] parser. The table is split so each derived parser rejects only
/// the unknown keys in its own set — the shared keys go to [`CaParams`], the
/// rest to the model. All keys are required, with no defaults — every value that
/// determines candidate identity is visible in the config file.
pub(crate) fn translate<M: CaModel>(table: &toml::Table) -> Result<Params> {
    let mut shared_table = toml::Table::new();
    let mut model_keys = table.clone();
    for key in SHARED_KEYS {
        if let Some(value) = model_keys.remove(key) {
            shared_table.insert(key.to_string(), value);
        }
    }
    let shared = CaParams::parse(&shared_table, M::FORMAT_ID, "params")?;
    let ignition = M::Ignition::parse(&model_keys, M::FORMAT_ID, "params")?;
    Ok(Params {
        bytes: encode_params::<M>(&shared, &ignition),
    })
}

#[cfg(test)]
mod tests {
    use sima_core::to_hex;

    use super::super::toy_model::Toy;
    use super::*;

    fn sample() -> CaParams {
        CaParams::new(64, 48, 100, 1.0).expect("valid sample params")
    }

    /// The canonical bytes of [`sample`]: width 64, height 48, steps 100 as
    /// little-endian `u32`, dt 1.0 as its f32 bits (`0x3F800000`) little-endian.
    const SAMPLE_BYTES_HEX: &str = "4000000030000000640000000000803f";

    #[test]
    fn new_rejects_zero_counts() {
        let names = ["width", "height", "steps"];
        for (position, name) in names.iter().enumerate() {
            let mut p = [64u32, 48, 100];
            p[position] = 0;
            match CaParams::new(p[0], p[1], p[2], 1.0) {
                Err(Error::Validation(message)) => {
                    assert!(
                        message.contains(name),
                        "message {message:?} must name {name}"
                    )
                }
                other => panic!("{name} = 0 must be a validation error, got {other:?}"),
            }
        }
    }

    #[test]
    fn new_rejects_invalid_dt() {
        for bad in [0.0f32, -1.0, f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            match CaParams::new(64, 48, 100, bad) {
                Err(Error::Validation(message)) => {
                    assert!(message.contains("dt"), "the error names dt: {message}")
                }
                other => panic!("dt = {bad} must be a validation error, got {other:?}"),
            }
        }
    }

    #[test]
    fn accessors_round_trip() {
        let params = sample();
        assert_eq!(params.width(), 64);
        assert_eq!(params.height(), 48);
        assert_eq!(params.steps(), 100);
        assert_eq!(params.dt(), 1.0);
    }

    #[test]
    fn shared_fields_are_byte_stable() {
        let mut enc = Enc::new();
        sample().encode(&mut enc);
        assert_eq!(to_hex(&enc.finish()), SAMPLE_BYTES_HEX);
    }

    #[test]
    fn round_trips_through_bytes() -> Result<()> {
        let mut enc = Enc::new();
        sample().encode(&mut enc);
        let buf = enc.finish();
        let mut dec = Dec::new(&buf);
        assert_eq!(CaParams::decode(&mut dec)?, sample());
        dec.finish()?;
        Ok(())
    }

    #[test]
    fn params_blob_splits_shared_from_ignition() -> Result<()> {
        // A toy model with a one-scalar ignition: encode_params writes the four
        // shared fields then the model's ignition, and decode_params reads the
        // shared fields back and hands the remainder to the model.
        let shared = sample();
        let ignition = Toy::ignition(7.5);
        let blob = encode_params::<Toy>(&shared, &ignition);
        let (decoded_shared, decoded_ignition) = decode_params::<Toy>(&blob)?;
        assert_eq!(decoded_shared, shared);
        assert_eq!(decoded_ignition, ignition);
        Ok(())
    }

    #[test]
    fn params_blob_rejects_trailing_bytes() {
        let mut blob = encode_params::<Toy>(&sample(), &Toy::ignition(1.0));
        blob.push(0);
        assert!(matches!(
            decode_params::<Toy>(&blob),
            Err(Error::Encoding(_))
        ));
    }

    /// The toy model's full `[run.params]` grammar: the shared keys plus the toy
    /// model's single `base` key.
    fn params_table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    const FULL_PARAMS: &str = r#"
        width = 64
        height = 48
        steps = 100
        dt = 1.0
        base = 0.5
    "#;

    #[test]
    fn translate_encodes_a_full_table_through_the_blob() -> Result<()> {
        // The shared keys here, the model's `base` via its ignition parser; the
        // result decodes back to the same shared params and ignition.
        let blob = translate::<Toy>(&params_table(FULL_PARAMS))?.bytes;
        let (shared, ignition) = decode_params::<Toy>(&blob)?;
        assert_eq!(shared, CaParams::new(64, 48, 100, 1.0)?);
        assert_eq!(ignition, Toy::ignition(0.5));
        Ok(())
    }

    #[test]
    fn translate_rejects_a_missing_shared_key_naming_it() {
        // A missing shared key surfaces before the model runs, naming the key.
        for key in SHARED_KEYS {
            let mut table = params_table(FULL_PARAMS);
            table.remove(key);
            match translate::<Toy>(&table) {
                Err(Error::Validation(message)) => {
                    assert!(message.contains(key), "the error names {key}: {message}")
                }
                other => panic!("expected Validation for missing {key}, got {other:?}"),
            }
        }
    }

    #[test]
    fn translate_rejects_a_missing_model_key_naming_it() {
        // A missing model key surfaces from the model's ignition parser, naming
        // the key.
        let mut table = params_table(FULL_PARAMS);
        table.remove("base");
        match translate::<Toy>(&table) {
            Err(Error::Validation(message)) => {
                assert!(message.contains("base"), "the error names base: {message}")
            }
            other => panic!("expected Validation for missing base, got {other:?}"),
        }
    }

    #[test]
    fn translate_rejects_an_unknown_key() {
        // An unknown key is neither shared nor a model key, so the model rejects
        // it once the shared keys are stripped.
        let mut table = params_table(FULL_PARAMS);
        table.insert("surprise".to_string(), toml::Value::Integer(1));
        match translate::<Toy>(&table) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("surprise"),
                    "the error names the key: {message}"
                )
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }
}
