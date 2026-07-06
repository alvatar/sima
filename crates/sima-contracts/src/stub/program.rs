//! The stub program: a behavior selector plus a per-candidate nonce, carried
//! in a `Spec`'s opaque bytes.
//!
//! A [`StubProgram`] is what the stub generator writes into each spec and the
//! stub executor reads back. Its canonical form carries no domain tag: the
//! `Spec` that holds it already frames the outer object, so this is the
//! inner payload only. Encoding goes through [`Enc`]/[`Dec`], and `from_bytes`
//! rejects trailing input so every program has exactly one byte image.

use sima_core::{Dec, Enc, Error, Result};

/// What the stub executor does when it evaluates a candidate carrying this
/// program. The canonical form is a tag byte, followed by a `u64` argument
/// for the arms that carry one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StubBehavior {
    /// Evaluate successfully on every attempt.
    Succeed,
    /// A flaky candidate: fail while the attempt number is below `n`, then
    /// succeed. The deterministic model of a transient failure.
    Flaky(u64),
    /// Panic, so workers can prove panic isolation (M1.5).
    Panic,
    /// Sleep for the given milliseconds, then succeed.
    Sleep(u64),
}

impl StubBehavior {
    /// Appends the canonical form: tag byte, then the `u64` argument the arm
    /// carries, if any.
    pub fn encode(&self, enc: &mut Enc) {
        match self {
            StubBehavior::Succeed => {
                enc.u8(0);
            }
            StubBehavior::Flaky(n) => {
                enc.u8(1).u64(*n);
            }
            StubBehavior::Panic => {
                enc.u8(2);
            }
            StubBehavior::Sleep(millis) => {
                enc.u8(3).u64(*millis);
            }
        }
    }

    /// Reads a canonical form written by [`StubBehavior::encode`]. An
    /// unrecognized tag byte is an encoding error, never a panic.
    pub fn decode(dec: &mut Dec<'_>) -> Result<StubBehavior> {
        match dec.u8()? {
            0 => Ok(StubBehavior::Succeed),
            1 => Ok(StubBehavior::Flaky(dec.u64()?)),
            2 => Ok(StubBehavior::Panic),
            3 => Ok(StubBehavior::Sleep(dec.u64()?)),
            tag => Err(Error::Encoding(format!("unknown stub behavior tag {tag}"))),
        }
    }
}

/// A stub candidate: the programmed behavior plus a nonce that makes each
/// generated spec distinct. Lives in `Spec.bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StubProgram {
    /// What the executor does with this candidate.
    pub behavior: StubBehavior,
    /// A per-candidate value keeping specs distinct even when two candidates
    /// program the same behavior. The generator seeds it from the run seed.
    pub nonce: u64,
}

impl StubProgram {
    /// Appends the canonical form: the behavior, then the `u64` nonce.
    pub fn encode(&self, enc: &mut Enc) {
        self.behavior.encode(enc);
        enc.u64(self.nonce);
    }

    /// Reads a canonical form written by [`StubProgram::encode`].
    pub fn decode(dec: &mut Dec<'_>) -> Result<StubProgram> {
        let behavior = StubBehavior::decode(dec)?;
        let nonce = dec.u64()?;
        Ok(StubProgram { behavior, nonce })
    }

    /// The standalone canonical bytes — exactly the bytes a spec carries.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        self.encode(&mut enc);
        enc.finish()
    }

    /// Parses standalone canonical bytes, rejecting trailing input.
    pub fn from_bytes(bytes: &[u8]) -> Result<StubProgram> {
        let mut dec = Dec::new(bytes);
        let program = StubProgram::decode(&mut dec)?;
        dec.finish()?;
        Ok(program)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn program_encoding_matches_pinned_hex() {
        // Hand-derived per §6, nonce 0: behavior bytes then u64 nonce LE.
        let cases = [
            (StubBehavior::Succeed, "00", "0000000000000000"),
            (
                StubBehavior::Flaky(3),
                "010300000000000000",
                "0000000000000000",
            ),
            (StubBehavior::Panic, "02", "0000000000000000"),
            (
                StubBehavior::Sleep(0),
                "030000000000000000",
                "0000000000000000",
            ),
        ];
        for (behavior, behavior_hex, nonce_hex) in cases {
            let program = StubProgram { behavior, nonce: 0 };
            assert_eq!(
                to_hex(&program.to_bytes()),
                format!("{behavior_hex}{nonce_hex}")
            );
        }
    }

    #[test]
    fn program_round_trips_every_behavior() -> Result<()> {
        let behaviors = [
            StubBehavior::Succeed,
            StubBehavior::Flaky(3),
            StubBehavior::Panic,
            StubBehavior::Sleep(7),
        ];
        for behavior in behaviors {
            for nonce in [0u64, 1, 0x0102_0304_0506_0708] {
                let program = StubProgram { behavior, nonce };
                assert_eq!(StubProgram::from_bytes(&program.to_bytes())?, program);
            }
        }
        Ok(())
    }

    #[test]
    fn decode_rejects_unknown_behavior_tag() {
        // Tag 0xFF is undefined; a full u64 follows so only the tag is at
        // fault, and decode reports it instead of panicking.
        let mut buf = vec![0xFF];
        buf.extend_from_slice(&0u64.to_le_bytes());
        assert!(matches!(
            StubProgram::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn from_bytes_rejects_truncation() {
        let full = StubProgram {
            behavior: StubBehavior::Flaky(3),
            nonce: 0,
        }
        .to_bytes();
        for cut in 0..full.len() {
            assert!(
                matches!(
                    StubProgram::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = StubProgram {
            behavior: StubBehavior::Succeed,
            nonce: 0,
        }
        .to_bytes();
        buf.push(0);
        assert!(matches!(
            StubProgram::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }
}
