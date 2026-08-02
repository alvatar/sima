//! [`CaGenerator<M>`]: draws a run's candidate specs for the model `M`, and the
//! shared half of the `[run.generator]` translation.

use std::collections::HashMap;
use std::marker::PhantomData;

use sima_contracts::Generator;
use sima_core::{Codec, Dec, Enc, Error, Result};
use sima_model::{FormatId, GeneratorId, Spec};

use super::model::CaModel;
use crate::domains::translate::{TomlConfig, required};

/// Draws a run's candidate genomes for the model `M`. Candidate `i` is drawn by
/// `M::sample(cfg, root_seed, i)`, so a candidate depends only on
/// `(root_seed, i, cfg)`: raising the count appends candidates and never changes
/// existing ones, keeping their spec ids — and any cached results — valid.
pub(crate) struct CaGenerator<M: CaModel> {
    id: GeneratorId,
    format: FormatId,
    /// `M` is used only through its associated items in [`Generator::generate`],
    /// never stored; `fn() -> M` keeps the generator `Send + Sync`.
    model: PhantomData<fn() -> M>,
}

impl<M: CaModel> CaGenerator<M> {
    /// Constructs the generator, registered under `M::FORMAT_ID`.
    pub(crate) fn new() -> Result<CaGenerator<M>> {
        Ok(CaGenerator {
            id: GeneratorId::new(M::FORMAT_ID)?,
            format: FormatId::new(M::FORMAT_ID)?,
            model: PhantomData,
        })
    }
}

impl<M: CaModel> Generator for CaGenerator<M> {
    fn id(&self) -> &GeneratorId {
        &self.id
    }

    fn format(&self) -> &FormatId {
        &self.format
    }

    fn translate_config(&self, toml: &str) -> Result<Vec<u8>> {
        translate::<M>(&crate::domains::translate::table(toml)?)
    }

    fn generate(&self, root_seed: u64, params: &[u8]) -> Result<Vec<Spec>> {
        let format = &self.format;
        let (count, cfg) = decode_gen_params::<M>(params)?;
        let mut specs = Vec::with_capacity(count as usize);
        // Content addressing: identical draws would collapse to one identity, so
        // a collision is surfaced as an error exposing the config mistake
        // (ranges admitting too few distinct values) instead of silently
        // shrinking the run. Dedup is by the genome's canonical bytes, which are
        // exactly the bytes the spec carries.
        let mut first_drawn_at: HashMap<Vec<u8>, u64> = HashMap::with_capacity(count as usize);
        for i in 0..count {
            // Candidate i owns the substream the model derives from
            // (root_seed, i), so a candidate depends only on (root_seed, i, cfg).
            let genome = M::sample(&cfg, root_seed, i);
            let bytes = genome.to_bytes();
            if let Some(&j) = first_drawn_at.get(&bytes) {
                return Err(Error::Validation(format!(
                    "{} generator drew identical genomes at candidates {j} and {i}: \
                     the configured ranges admit too few distinct values",
                    M::NAME
                )));
            }
            first_drawn_at.insert(bytes.clone(), i);
            specs.push(Spec {
                format: format.clone(),
                bytes,
            });
        }
        Ok(specs)
    }
}

/// Translates the `[run.generator]` table into the generator params blob: the
/// shared `count` here, the model's sampling keys via its generator config's
/// [`TomlConfig`] parser. The model receives the table with `count` stripped,
/// so it rejects only keys outside its own set.
pub(crate) fn translate<M: CaModel>(table: &toml::Table) -> Result<Vec<u8>> {
    let count = parse_count::<M>(table)?;
    let mut model_keys = table.clone();
    model_keys.remove("count");
    let cfg = M::GenConfig::parse(&model_keys, M::FORMAT_ID, "generator")?;
    Ok(encode_gen_params::<M>(count, &cfg))
}

/// The generator params blob: the shared `count` as a `u64`, then the model's
/// standalone config bytes.
fn encode_gen_params<M: CaModel>(count: u64, cfg: &M::GenConfig) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.u64(count);
    let mut bytes = enc.finish();
    bytes.extend_from_slice(&cfg.to_bytes());
    bytes
}

/// Parses the generator params blob: the shared `count` from its fixed-width
/// `u64` prefix, then the model's standalone codec consumes the remainder and
/// rejects trailing bytes.
fn decode_gen_params<M: CaModel>(bytes: &[u8]) -> Result<(u64, M::GenConfig)> {
    let mut dec = Dec::new(bytes);
    let count = validated_count(dec.u64()?)?;
    let cfg = M::GenConfig::from_bytes(&bytes[std::mem::size_of::<u64>()..])?;
    Ok((count, cfg))
}

/// The shared `count` key: a TOML integer at least 1.
fn parse_count<M: CaModel>(table: &toml::Table) -> Result<u64> {
    match required(table, M::FORMAT_ID, "generator", "count")? {
        // The same bound the decode applies, stated where a person can act on
        // it: a config naming too many candidates fails at load rather than at
        // the first draw.
        toml::Value::Integer(n) if *n >= 1 => validated_count(*n as u64).map_err(|_| {
            Error::Validation(format!(
                "generator {} count is {n}; a run draws at most {MAX_CANDIDATES} candidates",
                M::FORMAT_ID
            ))
        }),
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

/// The most candidates one run draws.
///
/// The bound exists because the count arrives as decoded bytes and is what
/// sizes the draw's allocations: a blob claiming 2^60 candidates would ask for
/// that much memory before a single genome is sampled. A million candidates is
/// far past any search this substrate runs and small enough that reserving for
/// it is ordinary.
pub(crate) const MAX_CANDIDATES: u64 = 1_000_000;

/// Validates the candidate count: at least 1, at most [`MAX_CANDIDATES`].
///
/// A zero-candidate run is meaningless, so a params blob encoding zero fails to
/// decode; a count past the cap is refused before it sizes an allocation,
/// since the blob is decoded input and nothing upstream of here has to be
/// trusted to have written a sane one.
fn validated_count(count: u64) -> Result<u64> {
    match count {
        0 => Err(Error::Validation(
            "generator count must be at least 1, got 0".to_string(),
        )),
        n if n > MAX_CANDIDATES => Err(Error::Validation(format!(
            "generator count is {n}; a run draws at most {MAX_CANDIDATES} candidates"
        ))),
        n => Ok(n),
    }
}

#[cfg(test)]
mod tests {
    use sima_contracts::Generator;

    use super::super::toy_model::Toy;
    use super::*;

    /// A generator params blob for the toy model: `count` candidates over the
    /// range `[lo, hi]`.
    fn params(count: u64, lo: f32, hi: f32) -> Vec<u8> {
        encode_gen_params::<Toy>(count, &Toy::gen_config([lo, hi]))
    }

    #[test]
    fn a_count_past_the_cap_is_refused_before_it_sizes_an_allocation() {
        // The blob is decoded input: a hostile or corrupt one claiming 2^60
        // candidates would reserve that much before a genome is drawn. The
        // refusal names the count and the cap.
        let blob = params(1 << 60, 0.0, 1.0);
        let Err(Error::Validation(message)) = decode_gen_params::<Toy>(&blob) else {
            panic!("expected a count past the cap to be refused");
        };
        assert!(message.contains(&(1u64 << 60).to_string()), "{message}");
        assert!(message.contains(&MAX_CANDIDATES.to_string()), "{message}");
    }

    #[test]
    fn the_cap_is_stated_at_load_as_well_as_at_decode() {
        // A config naming too many candidates fails where a person can act on
        // it, rather than at the first draw of a run that already has a store.
        let table: toml::Table = format!("count = {}\nlo = 0.0\nhi = 1.0", MAX_CANDIDATES + 1)
            .parse()
            .expect("a table");
        let Err(Error::Validation(message)) = translate::<Toy>(&table) else {
            panic!("expected a count past the cap to be refused at load");
        };
        assert!(message.contains(&MAX_CANDIDATES.to_string()), "{message}");
    }

    #[test]
    fn the_cap_itself_still_draws() {
        // The bound is inclusive: a run at exactly the cap is a run, not an
        // off-by-one refusal.
        assert_eq!(
            validated_count(MAX_CANDIDATES).expect("the cap"),
            MAX_CANDIDATES
        );
    }

    #[test]
    fn generate_is_deterministic() -> Result<()> {
        let generator = CaGenerator::<Toy>::new()?;
        let params = params(8, 0.01, 0.08);
        assert_eq!(
            generator.generate(42, &params)?,
            generator.generate(42, &params)?
        );
        Ok(())
    }

    #[test]
    fn generate_stamps_the_generators_own_format() -> Result<()> {
        // The generator knows the format its specs are of, so every spec
        // carries it without a caller supplying one.
        let generator = CaGenerator::<Toy>::new()?;
        let specs = generator.generate(1, &params(4, 0.01, 0.08))?;
        assert_eq!(specs.len(), 4);
        for spec in specs {
            assert_eq!(&spec.format, generator.format());
        }
        Ok(())
    }

    #[test]
    fn raising_count_preserves_existing_candidates() -> Result<()> {
        let generator = CaGenerator::<Toy>::new()?;
        let three = generator.generate(9, &params(3, 0.01, 0.08))?;
        let five = generator.generate(9, &params(5, 0.01, 0.08))?;
        assert_eq!(three[..], five[..3]);
        Ok(())
    }

    #[test]
    fn duplicate_draws_are_rejected() -> Result<()> {
        // A degenerate range fixes every draw, so the second candidate collides
        // with the first.
        let generator = CaGenerator::<Toy>::new()?;
        match generator.generate(3, &params(2, 0.05, 0.05)) {
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
            generator.generate(1, &params(0, 0.01, 0.08)),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn a_malformed_params_blob_is_rejected() -> Result<()> {
        let generator = CaGenerator::<Toy>::new()?;
        // A single byte cannot hold the u64 count prefix.
        assert!(matches!(
            generator.generate(1, &[0xFF]),
            Err(Error::Encoding(_))
        ));
        Ok(())
    }

    /// The toy model's full `[run.generator]` grammar: the shared `count` plus
    /// the toy model's single `value` range.
    fn gen_table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    const FULL_GEN: &str = r#"
        count = 8
        value = [0.01, 0.08]
    "#;

    #[test]
    fn translate_encodes_a_full_table_then_decodes_back() -> Result<()> {
        // The shared count here, the model's range via its generator config
        // parser; the blob decodes back to the same count and config.
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
        let table = gen_table("count = 0\n        value = [0.01, 0.08]");
        assert!(matches!(
            translate::<Toy>(&table),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn translate_rejects_a_missing_model_key_naming_it() {
        let mut table = gen_table(FULL_GEN);
        table.remove("value");
        match translate::<Toy>(&table) {
            Err(Error::Validation(message)) => {
                assert!(
                    message.contains("value"),
                    "the error names value: {message}"
                )
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
