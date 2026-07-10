//! The stub's continuation state: the object an `Accumulate` segment commits
//! and the next segment resumes from.
//!
//! [`StubState`] carries the absolute step index and the accumulator value.
//! Unlike [`StubProgram`](super::StubProgram), whose outer `Spec` frames it,
//! this object travels standalone — as the `state` artifact's bytes and as
//! checkpoint payloads — so its canonical form carries its own tag.

use sima_core::{Dec, Enc, Error, Result};

/// Frame tag identifying stub continuation state.
const TAG_STATE: &str = "stub.state.v1";

/// The stub's continuation state at a step boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StubState {
    /// The absolute step index across the whole chain: the number of steps
    /// applied so far, and the key of the next step's derivation.
    pub step: u64,
    /// The accumulator the steps fold into.
    pub acc: u64,
}

impl StubState {
    /// The standalone canonical bytes: tag, step, accumulator.
    pub fn to_bytes(self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.str(TAG_STATE).u64(self.step).u64(self.acc);
        enc.finish()
    }

    /// Parses standalone canonical bytes, rejecting a wrong tag, truncation,
    /// and trailing input.
    pub fn from_bytes(bytes: &[u8]) -> Result<StubState> {
        let mut dec = Dec::new(bytes);
        let tag = dec.str()?;
        if tag != TAG_STATE {
            return Err(Error::Encoding(format!(
                "expected stub state tag {TAG_STATE:?}, found {tag:?}"
            )));
        }
        let state = StubState {
            step: dec.u64()?,
            acc: dec.u64()?,
        };
        dec.finish()?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::to_hex;

    #[test]
    fn state_encoding_matches_pinned_hex() {
        // Hand-derived: str tag "stub.state.v1" (u64 len 13 LE ‖ UTF-8),
        // step u64 LE, acc u64 LE.
        let state = StubState {
            step: 2,
            acc: 0x0102_0304_0506_0708,
        };
        let expected = concat!(
            "0d00000000000000737475622e73746174652e7631",
            "0200000000000000",
            "0807060504030201",
        );
        assert_eq!(to_hex(&state.to_bytes()), expected);
    }

    #[test]
    fn state_round_trips() -> Result<()> {
        for (step, acc) in [(0u64, 0u64), (1, 42), (u64::MAX, u64::MAX)] {
            let state = StubState { step, acc };
            assert_eq!(StubState::from_bytes(&state.to_bytes())?, state);
        }
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = StubState { step: 2, acc: 3 }.to_bytes();
        for cut in 0..full.len() {
            assert!(
                matches!(StubState::from_bytes(&full[..cut]), Err(Error::Encoding(_))),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = StubState { step: 2, acc: 3 }.to_bytes();
        buf.push(0);
        assert!(matches!(
            StubState::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn from_bytes_rejects_a_wrong_tag() {
        let mut enc = Enc::new();
        enc.str("stub.state.v2").u64(2).u64(3);
        assert!(matches!(
            StubState::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }
}
