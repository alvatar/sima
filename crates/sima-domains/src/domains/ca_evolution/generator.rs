//! [`CaGenerator<M>`]: draws a run's candidate specs for the model `M`, and the
//! shared half of the `[run.generator]` translation.

use std::collections::HashMap;
use std::marker::PhantomData;

use sima_contracts::Generator;
use sima_core::{Dec, Enc, Error, Result};
use sima_model::{FormatId, GeneratorId, Spec};

use super::model::CaModel;
use crate::domains::translate;

/// Draws a run's candidate genomes for the model `M`. Candidate `i` is drawn by
/// `M::sample(cfg, root_seed, i)`, so a candidate depends only on
/// `(root_seed, i, cfg)`: raising the count appends candidates and never changes
/// existing ones, keeping their spec ids — and any cached results — valid.
pub(crate) struct CaGenerator<M: CaModel> {
    id: GeneratorId,
    /// `M` is used only through its associated items in [`Generator::generate`],
    /// never stored; `fn() -> M` keeps the generator `Send + Sync`.
    model: PhantomData<fn() -> M>,
}

impl<M: CaModel> CaGenerator<M> {
    /// Constructs the generator, registered under `M::FORMAT_ID`.
    pub(crate) fn new() -> Result<CaGenerator<M>> {
        Ok(CaGenerator {
            id: GeneratorId::new(M::FORMAT_ID)?,
            model: PhantomData,
        })
    }
}

impl<M: CaModel> Generator for CaGenerator<M> {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    fn generate(&self, root_seed: u64, params: &[u8], format: &FormatId) -> Result<Vec<Spec>> {
        let (count, cfg) = decode_gen_params::<M>(params)?;
        let mut specs = Vec::with_capacity(count as usize);
        // Content addressing: identical draws would collapse to one identity, so
        // a collision is surfaced as an error exposing the config mistake
        // (ranges admitting too few distinct values) instead of silently
        // shrinking the run. Dedup is by the model's genome key.
        let mut first_drawn_at: HashMap<Vec<u8>, u64> = HashMap::with_capacity(count as usize);
        for i in 0..count {
            // Candidate i owns the substream the model derives from
            // (root_seed, i), so a candidate depends only on (root_seed, i, cfg).
            let genome = M::sample(&cfg, root_seed, i);
            let key = M::genome_key(&genome);
            if let Some(&j) = first_drawn_at.get(&key) {
                return Err(Error::Validation(format!(
                    "{} generator drew identical genomes at candidates {j} and {i}: \
                     the configured ranges admit too few distinct values",
                    M::NAME
                )));
            }
            first_drawn_at.insert(key, i);
            specs.push(Spec {
                format: format.clone(),
                bytes: M::encode_genome(&genome),
            });
        }
        Ok(specs)
    }
}

/// Translates the `[run.generator]` table into the generator params blob: the
/// shared `count` here, the model's sampling keys via
/// [`CaModel::parse_gen_config`]. The model receives the table with `count`
/// stripped, so it rejects only keys outside its own set.
pub(crate) fn translate<M: CaModel>(table: &toml::Table) -> Result<Vec<u8>> {
    let count = parse_count::<M>(table)?;
    let mut model_keys = table.clone();
    model_keys.remove("count");
    let cfg = M::parse_gen_config(&model_keys)?;
    Ok(encode_gen_params::<M>(count, &cfg))
}

/// The generator params blob: the shared `count` as a `u64`, then the model's
/// standalone config bytes.
fn encode_gen_params<M: CaModel>(count: u64, cfg: &M::GenConfig) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.u64(count);
    let mut bytes = enc.finish();
    bytes.extend_from_slice(&M::encode_gen_config(cfg));
    bytes
}

/// Parses the generator params blob: the shared `count` from its fixed-width
/// `u64` prefix, then the model's standalone codec consumes the remainder and
/// rejects trailing bytes.
fn decode_gen_params<M: CaModel>(bytes: &[u8]) -> Result<(u64, M::GenConfig)> {
    let mut dec = Dec::new(bytes);
    let count = validated_count(dec.u64()?)?;
    let cfg = M::decode_gen_config(&bytes[std::mem::size_of::<u64>()..])?;
    Ok((count, cfg))
}

/// The shared `count` key: a TOML integer at least 1.
fn parse_count<M: CaModel>(table: &toml::Table) -> Result<u64> {
    match translate::required(table, M::FORMAT_ID, "generator", "count")? {
        toml::Value::Integer(n) if *n >= 1 => Ok(*n as u64),
        toml::Value::Integer(n) => Err(Error::Validation(format!(
            "generator {} count must be at least 1, got {n}",
            M::FORMAT_ID
        ))),
        other => Err(Error::Validation(format!(
            "generator {} count must be an integer, got {}",
            M::FORMAT_ID,
            other.type_str()
        ))),
    }
}

/// Validates the candidate count: at least 1. A zero-candidate run is
/// meaningless, so a params blob encoding zero fails to decode.
fn validated_count(count: u64) -> Result<u64> {
    if count >= 1 {
        Ok(count)
    } else {
        Err(Error::Validation(format!(
            "generator count must be at least 1, got {count}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use sima_contracts::Generator;

    use super::super::toy_model::Toy;
    use super::*;

    fn format() -> FormatId {
        FormatId::new(Toy::FORMAT_ID).expect("valid format id")
    }

    /// A generator params blob for the toy model: `count` candidates over the
    /// range `[lo, hi]`.
    fn params(count: u64, lo: f32, hi: f32) -> Vec<u8> {
        encode_gen_params::<Toy>(count, &Toy::gen_config([lo, hi]))
    }

    #[test]
    fn generate_is_deterministic() -> Result<()> {
        let generator = CaGenerator::<Toy>::new()?;
        let params = params(8, 0.01, 0.08);
        assert_eq!(
            generator.generate(42, &params, &format())?,
            generator.generate(42, &params, &format())?
        );
        Ok(())
    }

    #[test]
    fn generate_stamps_the_requested_format() -> Result<()> {
        // The format is stamped as received (the trait's contract).
        let generator = CaGenerator::<Toy>::new()?;
        let other = FormatId::new("domain-a.v1")?;
        let specs = generator.generate(1, &params(4, 0.01, 0.08), &other)?;
        assert_eq!(specs.len(), 4);
        for spec in specs {
            assert_eq!(spec.format, other);
        }
        Ok(())
    }

    #[test]
    fn raising_count_preserves_existing_candidates() -> Result<()> {
        let generator = CaGenerator::<Toy>::new()?;
        let three = generator.generate(9, &params(3, 0.01, 0.08), &format())?;
        let five = generator.generate(9, &params(5, 0.01, 0.08), &format())?;
        assert_eq!(three[..], five[..3]);
        Ok(())
    }

    #[test]
    fn duplicate_draws_are_rejected() -> Result<()> {
        // A degenerate range fixes every draw, so the second candidate collides
        // with the first.
        let generator = CaGenerator::<Toy>::new()?;
        match generator.generate(3, &params(2, 0.05, 0.05), &format()) {
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
    fn a_zero_count_blob_fails_to_decode() {
        let generator = CaGenerator::<Toy>::new().expect("generator");
        assert!(matches!(
            generator.generate(1, &params(0, 0.01, 0.08), &format()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn a_malformed_params_blob_is_rejected() -> Result<()> {
        let generator = CaGenerator::<Toy>::new()?;
        // A single byte cannot hold the u64 count prefix.
        assert!(matches!(
            generator.generate(1, &[0xFF], &format()),
            Err(Error::Encoding(_))
        ));
        Ok(())
    }

    /// The toy model's full `[run.generator]` grammar: the shared `count` plus
    /// the toy model's single `rate` range.
    fn gen_table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    const FULL_GEN: &str = r#"
        count = 8
        rate = [0.01, 0.08]
    "#;

    #[test]
    fn translate_encodes_a_full_table_then_decodes_back() -> Result<()> {
        // The shared count here, the model's range via parse_gen_config; the blob
        // decodes back to the same count and config.
        let blob = translate::<Toy>(&gen_table(FULL_GEN))?;
        let (count, cfg) = decode_gen_params::<Toy>(&blob)?;
        assert_eq!(count, 8);
        assert_eq!(cfg, Toy::gen_config([0.01, 0.08]));
        Ok(())
    }

    #[test]
    fn translate_rejects_a_missing_count_naming_it() {
        let mut table = gen_table(FULL_GEN);
        table.remove("count");
        match translate::<Toy>(&table) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("count"),
                    "the error names count: {message}"
                )
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn translate_rejects_a_zero_count() {
        let table = gen_table("count = 0\n        rate = [0.01, 0.08]");
        assert!(matches!(
            translate::<Toy>(&table),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn translate_rejects_a_missing_model_key_naming_it() {
        let mut table = gen_table(FULL_GEN);
        table.remove("rate");
        match translate::<Toy>(&table) {
            Err(Error::Validation(message)) => {
                assert!(message.contains("rate"), "the error names rate: {message}")
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn translate_rejects_an_unknown_key() {
        let mut table = gen_table(FULL_GEN);
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
