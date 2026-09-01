//! [`CaParams`]: the search knobs every CA model shares, and the shared half of the
//! `[search.params]` translation.

use sima_core::{Codec, Dec, Enc, Error, Result};
use sima_model::Params;

use super::model::CaModel;
use crate::domains::translate::{TomlConfig, float, integer, reject_unknown_keys};

/// The search parameters shared by every CA model: the grid extents, the steps one
/// segment advances, and the integration step size. A model's own ignition
/// configuration follows these in the canonical params blob.
///
/// The canonical form is `width`, `height`, `steps` as little-endian `u32`, `dt`
/// as its IEEE-754 bits in a little-endian `u32` — 16 bytes — then the snapshot
/// predicate: a presence flag `u8` (0 or 1), and when present the scalar name as
/// a length-prefixed UTF-8 string and the minimum's IEEE-754 bits as a
/// little-endian `u64`. The model's ignition bytes follow.
///
/// `steps` counts one segment's steps. A candidate's trajectory is a chain of
/// `segments` tasks, one task per segment, so the full trajectory spans
/// `segments * steps` steps. A total-steps-across-segments reading would need a
/// segment index threaded into the task input; the design keeps `steps`
/// per-segment, which needs no such index.
///
/// `snapshot_when` is the domain-owned predicate that gates the committed state
/// artifact: `Some((scalar, min))` commits the snapshot only when the named stat
/// reaches `min`, `None` commits always. It rides in the identity-bearing params
/// blob so it cannot influence the record from outside the task key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CaParams {
    width: u32,
    height: u32,
    steps: u32,
    dt: f32,
    snapshot_when: Option<(String, f64)>,
}

/// The shared `[search.params]` keys; the model owns any key beyond these.
pub(crate) const SHARED_KEYS: [&str; 5] = ["width", "height", "steps", "dt", "snapshot_when"];

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
    /// Builds shared search parameters, validating each field: `width`, `height`,
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
            snapshot_when: None,
        })
    }

    /// Returns the params carrying `predicate` as the snapshot gate. The scalar
    /// name is validated by the translation layer before it reaches here.
    pub(crate) fn with_snapshot_when(mut self, predicate: Option<(String, f64)>) -> CaParams {
        self.snapshot_when = predicate;
        self
    }

    /// The snapshot predicate: the scalar name and the minimum the stat must
    /// reach for the state artifact to be committed. `None` commits always.
    pub(crate) fn snapshot_when(&self) -> Option<&(String, f64)> {
        self.snapshot_when.as_ref()
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
    /// then the snapshot predicate: a presence flag, and when present the scalar
    /// name and the minimum's `f64` bits.
    fn encode(&self, enc: &mut Enc) {
        enc.u32(self.width)
            .u32(self.height)
            .u32(self.steps)
            .f32(self.dt);
        match &self.snapshot_when {
            None => {
                enc.u8(0);
            }
            Some((scalar, min)) => {
                enc.u8(1).str(scalar).f64(*min);
            }
        }
    }

    /// Reads the fields, funneling the numeric ones through [`CaParams::new`] so
    /// decode and construction share one validation path, then the snapshot
    /// predicate. The scalar name is trusted here — it was validated when the
    /// params were translated.
    fn decode(dec: &mut Dec<'_>) -> Result<CaParams> {
        let width = dec.u32()?;
        let height = dec.u32()?;
        let steps = dec.u32()?;
        let dt = dec.f32()?;
        let params = CaParams::new(width, height, steps, dt)?;
        let snapshot_when = match dec.u8()? {
            0 => None,
            1 => Some((dec.str()?.to_string(), dec.f64()?)),
            flag => {
                return Err(Error::Encoding(format!(
                    "invalid snapshot_when flag byte {flag}, expected 0 or 1"
                )));
            }
        };
        Ok(params.with_snapshot_when(snapshot_when))
    }
}

/// Reads every shared `[search.params]` key for model `M`, rejecting any key
/// outside the set, and funnels them through [`CaParams::new`].
///
/// The snapshot predicate is read here rather than patched in afterwards: it is
/// one of the shared keys, and a parse that accepted the key while dropping its
/// value would hand back a `CaParams` missing a predicate the config states.
/// Reading it needs the model — the scalar is validated against the names its
/// reduction emits — and whether the search is segmented, which is why this is a
/// model-aware function rather than a [`TomlConfig`] impl.
fn parse_shared<M: CaModel>(table: &toml::Table, segmented: bool) -> Result<CaParams> {
    let (id, section) = (M::FORMAT_ID, "params");
    reject_unknown_keys(id, table, &SHARED_KEYS, section)?;
    let width = integer(table, id, section, "width")?;
    let height = integer(table, id, section, "height")?;
    let steps = integer(table, id, section, "steps")?;
    let dt = float(table, id, section, "dt")?;
    let params = CaParams::new(width, height, steps, dt)?;
    Ok(match table.get("snapshot_when") {
        Some(value) => params.with_snapshot_when(Some(parse_snapshot_when::<M>(value, segmented)?)),
        None => params,
    })
}

/// The canonical search-params blob: the shared fields, then the model's ignition.
pub(crate) fn encode_params<M: CaModel>(shared: &CaParams, ignition: &M::Ignition) -> Vec<u8> {
    let mut enc = Enc::new();
    shared.encode(&mut enc);
    ignition.encode(&mut enc);
    enc.finish()
}

/// Parses the search-params blob: the shared fields, then the model's ignition
/// codec consumes the remainder. Trailing bytes are a decode error.
pub(crate) fn decode_params<M: CaModel>(bytes: &[u8]) -> Result<(CaParams, M::Ignition)> {
    let mut dec = Dec::new(bytes);
    let shared = CaParams::decode(&mut dec)?;
    let ignition = M::Ignition::decode(&mut dec)?;
    dec.finish()?;
    Ok((shared, ignition))
}

/// Translates the `[search.params]` table into the canonical params blob: the
/// shared keys here, the model's ignition keys via its ignition's
/// [`TomlConfig`] parser. The table is split so each derived parser rejects only
/// the unknown keys in its own set — the shared keys go to [`CaParams`], the
/// rest to the model. All keys are required, with no defaults — every value that
/// determines candidate identity is visible in the config file.
///
/// `segmented` is whether the search divides candidates into segments; a
/// `snapshot_when` predicate on a segmented search is a validation error, because
/// one params-carried predicate would gate every segment identically and a
/// chain successor faults on its predecessor's dropped state.
pub(crate) fn translate<M: CaModel>(table: &toml::Table, segmented: bool) -> Result<Params> {
    let mut shared_table = toml::Table::new();
    let mut model_keys = table.clone();
    for key in SHARED_KEYS {
        if let Some(value) = model_keys.remove(key) {
            shared_table.insert(key.to_string(), value);
        }
    }
    let shared = parse_shared::<M>(&shared_table, segmented)?;
    let ignition = M::Ignition::parse(&model_keys, M::FORMAT_ID, "params")?;
    Ok(Params {
        bytes: encode_params::<M>(&shared, &ignition),
    })
}

/// Parses and validates the `snapshot_when` inline table `{ scalar, min }`
/// against model `M`: the search must be unsegmented, the keys exactly `scalar` and
/// `min`, and the scalar name one the model's reduction emits.
fn parse_snapshot_when<M: CaModel>(value: &toml::Value, segmented: bool) -> Result<(String, f64)> {
    if segmented {
        return Err(Error::Validation(format!(
            "{} params snapshot_when requires an unsegmented search: one params-carried \
             predicate would gate every segment identically and break the chain",
            M::FORMAT_ID
        )));
    }
    let table = value.as_table().ok_or_else(|| {
        Error::Validation(format!(
            "{} params snapshot_when must be a table {{ scalar = \"...\", min = ... }}",
            M::FORMAT_ID
        ))
    })?;
    reject_unknown_keys(
        M::FORMAT_ID,
        table,
        &["scalar", "min"],
        "params.snapshot_when",
    )?;
    let scalar = table
        .get("scalar")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| {
            Error::Validation(format!(
                "{} params snapshot_when.scalar must be a string",
                M::FORMAT_ID
            ))
        })?
        .to_string();
    let min = table.get("min").and_then(toml_number).ok_or_else(|| {
        Error::Validation(format!(
            "{} params snapshot_when.min must be a number",
            M::FORMAT_ID
        ))
    })?;
    let valid = crate::substrates::cellular::scalar_names(M::CHANNELS);
    if !valid.contains(&scalar) {
        return Err(Error::Validation(format!(
            "{} params snapshot_when.scalar {scalar:?} is not a stat this model emits; \
             valid names: {}",
            M::FORMAT_ID,
            valid.join(", ")
        )));
    }
    Ok((scalar, min))
}

/// Reads a TOML number as `f64`, accepting both a float and an integer literal.
fn toml_number(value: &toml::Value) -> Option<f64> {
    match value {
        toml::Value::Float(f) => Some(*f),
        toml::Value::Integer(i) => Some(*i as f64),
        _ => None,
    }
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
    /// little-endian `u32`, dt 1.0 as its f32 bits (`0x3F800000`) little-endian,
    /// then the snapshot-predicate presence flag `00` (absent).
    const SAMPLE_BYTES_HEX: &str = "4000000030000000640000000000803f00";

    #[test]
    fn the_shared_parse_keeps_the_predicate_the_config_states() -> Result<()> {
        // The key sits in the shared set, so a parse that accepted it and
        // dropped its value would hand back params missing a predicate the
        // config asked for — and the caller that patched it back in was the
        // only thing keeping that from happening.
        let table: toml::Table = r#"
            width = 8
            height = 8
            steps = 4
            dt = 1.0
            snapshot_when = { scalar = "activity", min = 0.5 }
        "#
        .parse()
        .expect("a table");
        let shared = parse_shared::<Toy>(&table, false)?;
        assert_eq!(shared.snapshot_when(), Some(&("activity".to_string(), 0.5)));
        Ok(())
    }

    #[test]
    fn the_shared_parse_refuses_a_predicate_on_a_segmented_search() -> Result<()> {
        // The rule the predicate carries, applied where the predicate is read
        // rather than at one caller: one params-carried predicate would gate
        // every segment identically and break the chain.
        let table: toml::Table = r#"
            width = 8
            height = 8
            steps = 4
            dt = 1.0
            snapshot_when = { scalar = "activity", min = 0.5 }
        "#
        .parse()
        .expect("a table");
        assert!(matches!(
            parse_shared::<Toy>(&table, true),
            Err(Error::Validation(_))
        ));
        Ok(())
    }

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
    fn a_present_predicate_is_byte_stable() {
        // The 16 shared bytes, then the presence flag `01`, the scalar name as a
        // u64-framed UTF-8 string, and the minimum's f64 bits. `population` is
        // `0a…` + its ASCII; the minimum 0.5 is `0x3FE0…` little-endian.
        let params = sample().with_snapshot_when(Some(("population".to_string(), 0.5)));
        let mut enc = Enc::new();
        params.encode(&mut enc);
        let expected = format!(
            "{}01{}{}{}",
            "4000000030000000640000000000803f",
            "0a00000000000000",
            "706f70756c6174696f6e",
            "000000000000e03f",
        );
        assert_eq!(to_hex(&enc.finish()), expected);
    }

    #[test]
    fn a_present_predicate_round_trips_through_bytes() -> Result<()> {
        let params = sample().with_snapshot_when(Some(("activity".to_string(), 1.0e-4)));
        let mut enc = Enc::new();
        params.encode(&mut enc);
        let buf = enc.finish();
        let mut dec = Dec::new(&buf);
        assert_eq!(CaParams::decode(&mut dec)?, params);
        dec.finish()?;
        Ok(())
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

    /// The toy model's full `[search.params]` grammar: the shared keys plus the toy
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
        let blob = translate::<Toy>(&params_table(FULL_PARAMS), false)?.bytes;
        let (shared, ignition) = decode_params::<Toy>(&blob)?;
        assert_eq!(shared, CaParams::new(64, 48, 100, 1.0)?);
        assert_eq!(ignition, Toy::ignition(0.5));
        Ok(())
    }

    #[test]
    fn translate_rejects_a_missing_shared_key_naming_it() {
        // A missing required shared key surfaces before the model runs, naming
        // the key. `snapshot_when` is optional, so its absence is not a fault.
        for key in ["width", "height", "steps", "dt"] {
            let mut table = params_table(FULL_PARAMS);
            table.remove(key);
            match translate::<Toy>(&table, false) {
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
        match translate::<Toy>(&table, false) {
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
        match translate::<Toy>(&table, false) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("surprise"),
                    "the error names the key: {message}"
                )
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// A `[search.params]` table with a `snapshot_when` inline predicate over the
    /// full toy grammar.
    fn params_with_predicate(scalar: &str, min: &str) -> toml::Table {
        params_table(&format!(
            "{FULL_PARAMS}\nsnapshot_when = {{ scalar = \"{scalar}\", min = {min} }}"
        ))
    }

    #[test]
    fn translate_reads_the_snapshot_predicate() -> Result<()> {
        // The inline table becomes the params' snapshot gate, round-tripping
        // through the blob.
        let blob = translate::<Toy>(&params_with_predicate("population", "0.5"), false)?.bytes;
        let (shared, _) = decode_params::<Toy>(&blob)?;
        assert_eq!(
            shared.snapshot_when(),
            Some(&("population".to_string(), 0.5))
        );
        Ok(())
    }

    #[test]
    fn absent_snapshot_when_leaves_no_predicate() -> Result<()> {
        // No predicate key means the snapshot always commits, and the params
        // carry no gate.
        let blob = translate::<Toy>(&params_table(FULL_PARAMS), false)?.bytes;
        let (shared, _) = decode_params::<Toy>(&blob)?;
        assert_eq!(shared.snapshot_when(), None);
        Ok(())
    }

    #[test]
    fn a_predicate_changes_the_params_bytes() -> Result<()> {
        // A predicate-bearing config and a predicate-free one produce different
        // params blobs, so their search ids differ.
        let without = translate::<Toy>(&params_table(FULL_PARAMS), false)?.bytes;
        let with = translate::<Toy>(&params_with_predicate("population", "0.5"), false)?.bytes;
        assert_ne!(without, with);
        Ok(())
    }

    #[test]
    fn translate_rejects_an_unknown_scalar_naming_the_valid_set() {
        // The toy has one channel, so `c1.mean` is out of range. The error names
        // the valid names, which include `population` and `activity`.
        match translate::<Toy>(&params_with_predicate("c1.mean", "0.5"), false) {
            Err(Error::Validation(message)) => {
                assert!(message.contains("c1.mean"), "names the scalar: {message}");
                assert!(
                    message.contains("population") && message.contains("activity"),
                    "names the valid set: {message}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn translate_rejects_a_predicate_on_a_segmented_search() {
        // A params-carried predicate would gate every segment identically; the
        // error states the constraint.
        match translate::<Toy>(&params_with_predicate("population", "0.5"), true) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("unsegmented"),
                    "states the constraint: {message}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn translate_rejects_an_unknown_key_inside_the_predicate() {
        match translate::<Toy>(
            &params_table(&format!(
                "{FULL_PARAMS}\nsnapshot_when = {{ scalar = \"population\", min = 0.5, extra = 1 }}"
            )),
            false,
        ) {
            Err(Error::Validation(message)) => {
                assert!(message.contains("extra"), "names the key: {message}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }
}
