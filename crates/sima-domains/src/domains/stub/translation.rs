//! The stub domain's config translations, and the binding of its format id.
//!
//! Both translations turn human-facing TOML into the opaque canonical bytes
//! the model carries, through the stub's own codecs — the crate never
//! hand-rolls an identity-bearing encoding:
//!
//! - the generator section's `behaviors` list — words like `"succeed"` and
//!   `"flaky:2"` — becomes a [`StubGeneratorConfig`] blob;
//! - the params section's optional `hex` string becomes the raw run-params
//!   bytes, since stub params carry no meaning of their own.

use sima_core::{Error, Result};
use sima_model::{Environment, EnvironmentComponent, EnvironmentValue, FormatId, Params};

use super::{StubBehavior, StubExecutor, StubGeneratorConfig};
use crate::domain::Domain;

/// The stub format id, doubling as the stub generator id.
pub(crate) const ID: &str = "stub.v1";

/// The stub domain: the stub executor and a one-component environment
/// carrying its version.
pub(crate) fn domain() -> Result<Domain> {
    Ok(Domain {
        format: FormatId::new(ID)?,
        executor: Box::new(StubExecutor::new()?),
        environment: Environment::new(vec![EnvironmentComponent::new(
            "stub.executor",
            EnvironmentValue::Version("v1".to_string()),
        )?])?,
    })
}

/// Translates the `[run.params]` table: an optional `hex` string, decoded
/// to the raw params bytes, defaulting to empty. Stub params carry no
/// meaning, so hex is the transparent spelling of arbitrary bytes. Unknown
/// keys are rejected.
pub(crate) fn params(table: &toml::Table) -> Result<Params> {
    reject_unknown_keys(table, &["hex"], "params")?;
    let bytes = match table.get("hex") {
        None => Vec::new(),
        Some(toml::Value::String(hex)) => decode_hex(hex)?,
        Some(other) => {
            return Err(Error::Validation(format!(
                "stub.v1 params hex must be a string, got {}",
                other.type_str()
            )));
        }
    };
    Ok(Params { bytes })
}

/// Translates the `[run.generator]` table (minus `id`): a required
/// `behaviors` list of behavior words, encoded through the stub generator's
/// own codec. Unknown keys are rejected.
pub(crate) fn generator_params(table: &toml::Table) -> Result<Vec<u8>> {
    reject_unknown_keys(table, &["behaviors"], "generator")?;
    let Some(value) = table.get("behaviors") else {
        return Err(Error::Validation(
            "generator stub.v1 requires a behaviors list".to_string(),
        ));
    };
    let Some(entries) = value.as_array() else {
        return Err(Error::Validation(format!(
            "generator stub.v1 behaviors must be a list of strings, got {}",
            value.type_str()
        )));
    };
    let behaviors = entries
        .iter()
        .map(|entry| match entry.as_str() {
            Some(word) => parse_behavior(word),
            None => Err(Error::Validation(format!(
                "generator stub.v1 behaviors must be strings, got {}",
                entry.type_str()
            ))),
        })
        .collect::<Result<Vec<StubBehavior>>>()?;
    Ok(StubGeneratorConfig { behaviors }.to_bytes())
}

/// Rejects any key of `table` outside `known`, naming the key and the
/// `section` it appeared in.
fn reject_unknown_keys(table: &toml::Table, known: &[&str], section: &str) -> Result<()> {
    for key in table.keys() {
        if !known.contains(&key.as_str()) {
            return Err(Error::Validation(format!(
                "stub.v1 {section} config does not define the key {key:?}"
            )));
        }
    }
    Ok(())
}

/// Parses one behavior word: `succeed`, `flaky:N`, `sleep:MS`, `reject`,
/// `panic`, or `accumulate:K` with K ≥ 1. Anything else is
/// [`Error::Validation`] naming the entry.
fn parse_behavior(entry: &str) -> Result<StubBehavior> {
    let (word, argument) = match entry.split_once(':') {
        Some((word, argument)) => (word, Some(argument)),
        None => (entry, None),
    };
    let bad = || {
        Error::Validation(format!(
            "unknown stub behavior {entry:?}: expected succeed, flaky:N, sleep:MS, reject, \
             panic, or accumulate:K"
        ))
    };
    match (word, argument) {
        ("succeed", None) => Ok(StubBehavior::Succeed),
        ("reject", None) => Ok(StubBehavior::Reject),
        ("panic", None) => Ok(StubBehavior::Panic),
        ("flaky", Some(n)) => n.parse().map(StubBehavior::Flaky).map_err(|_| bad()),
        ("sleep", Some(millis)) => millis.parse().map(StubBehavior::Sleep).map_err(|_| bad()),
        ("accumulate", Some(k)) => match k.parse() {
            // A zero-step segment does no work and commits its input
            // unchanged; requiring K ≥ 1 keeps every task meaningful.
            Ok(0) | Err(_) => Err(bad()),
            Ok(k) => Ok(StubBehavior::Accumulate(k)),
        },
        _ => Err(bad()),
    }
}

/// Decodes a hex string into bytes: two case-insensitive hex digits per
/// byte, empty for empty.
fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return Err(Error::Validation(format!(
            "params hex has odd length {}",
            hex.len()
        )));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok(hex_digit(pair[0])? << 4 | hex_digit(pair[1])?))
        .collect()
}

/// The value of one hex digit; anything else is [`Error::Validation`].
fn hex_digit(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        other => Err(Error::Validation(format!(
            "params hex holds a non-hex character {:?}",
            other as char
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses `text` as a TOML table.
    fn table(text: &str) -> toml::Table {
        text.parse().expect("parse test table")
    }

    #[test]
    fn the_behavior_grammar_accepts_all_five_forms() -> Result<()> {
        for (word, expected) in [
            ("succeed", StubBehavior::Succeed),
            ("flaky:2", StubBehavior::Flaky(2)),
            ("sleep:50", StubBehavior::Sleep(50)),
            ("reject", StubBehavior::Reject),
            ("panic", StubBehavior::Panic),
            ("accumulate:100", StubBehavior::Accumulate(100)),
        ] {
            assert_eq!(parse_behavior(word)?, expected, "{word}");
        }
        Ok(())
    }

    #[test]
    fn malformed_behaviors_are_rejected_naming_the_entry() {
        for bad in [
            "flaky",
            "flaky:x",
            "sleep:-1",
            "explode",
            "succeed:1",
            "",
            "accumulate",
            "accumulate:x",
            "accumulate:0",
        ] {
            match parse_behavior(bad) {
                Err(Error::Validation(msg)) => {
                    assert!(
                        msg.contains(&format!("{bad:?}")),
                        "the error names the entry {bad:?}: {msg}"
                    );
                }
                other => panic!("expected Validation for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn the_behaviors_list_encodes_through_the_stub_codec() -> Result<()> {
        let blob = generator_params(&table(
            r#"behaviors = ["succeed", "flaky:2", "sleep:50", "reject", "panic"]"#,
        ))?;
        let expected = StubGeneratorConfig {
            behaviors: vec![
                StubBehavior::Succeed,
                StubBehavior::Flaky(2),
                StubBehavior::Sleep(50),
                StubBehavior::Reject,
                StubBehavior::Panic,
            ],
        };
        assert_eq!(blob, expected.to_bytes());
        Ok(())
    }

    #[test]
    fn a_generator_table_without_behaviors_is_rejected() {
        assert!(matches!(
            generator_params(&toml::Table::new()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn a_non_string_behaviors_list_is_rejected() {
        for text in ["behaviors = 3", "behaviors = [1, 2]"] {
            assert!(
                matches!(generator_params(&table(text)), Err(Error::Validation(_))),
                "{text} must be rejected"
            );
        }
    }

    #[test]
    fn unknown_generator_keys_are_rejected() {
        let text = r#"
            behaviors = ["succeed"]
            surprise = 1
        "#;
        match generator_params(&table(text)) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains("surprise"), "the error names the key: {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn params_hex_decodes_to_the_raw_bytes() -> Result<()> {
        assert_eq!(
            params(&table(r#"hex = "00ff10""#))?.bytes,
            vec![0x00, 0xff, 0x10]
        );
        Ok(())
    }

    #[test]
    fn absent_and_empty_hex_both_mean_empty_params() -> Result<()> {
        assert!(params(&toml::Table::new())?.bytes.is_empty());
        assert!(params(&table(r#"hex = """#))?.bytes.is_empty());
        Ok(())
    }

    #[test]
    fn odd_length_hex_is_rejected() {
        assert!(matches!(
            params(&table(r#"hex = "abc""#)),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn non_hex_content_is_rejected() {
        for text in [r#"hex = "zz""#, r#"hex = 3"#] {
            assert!(
                matches!(params(&table(text)), Err(Error::Validation(_))),
                "{text} must be rejected"
            );
        }
    }

    #[test]
    fn unknown_params_keys_are_rejected() {
        match params(&table("surprise = 1")) {
            Err(Error::Validation(msg)) => {
                assert!(msg.contains("surprise"), "the error names the key: {msg}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn the_domain_binds_the_stub_pieces() -> Result<()> {
        let domain = domain()?;
        assert_eq!(domain.format.as_str(), ID);
        assert_eq!(domain.executor.format().as_str(), ID);
        let components = domain.environment.components();
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].name(), "stub.executor");
        assert_eq!(
            *components[0].value(),
            EnvironmentValue::Version("v1".to_string())
        );
        Ok(())
    }
}
