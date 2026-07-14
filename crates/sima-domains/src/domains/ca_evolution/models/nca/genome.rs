//! [`NcaGenome`]: the evolvable weight vector of the asynchronous Neural CA.

use sima_core::{Codec, Dec, Enc, Error, Result};

use super::{C_STATE, H, P};

/// Perception block: `P` depthwise filters, each 3×3.
const PERCEPTION: usize = P * 3 * 3;
/// First dense layer weights: `(C_state·P)` perception inputs by `H` hidden.
const W1: usize = (C_STATE * P) * H;
/// First dense layer bias: `H`.
const B1: usize = H;
/// Second dense layer weights: `H` hidden by `C_state` outputs.
const W2: usize = H * C_STATE;
/// Second dense layer bias: `C_state`.
const B2: usize = C_STATE;

/// The genome length in f32: the concatenated parameters of the perception
/// filters and the two-layer update network. `27 + 768 + 32 + 256 + 8 = 1091`.
pub(crate) const N: usize = PERCEPTION + W1 + B1 + W2 + B2;

/// The Neural CA genome: the flat weight vector of the perception filters and
/// the update network, `N` f32 in one frozen order,
///
/// ```text
/// offset  count  field                shape
/// ------  -----  -------------------  ---------------------------
/// 0       27     perception filters   P · 3 · 3       = 3·9   = 27
/// 27      768    W1 (input → hidden)  (C_state·P) · H = 24·32 = 768
/// 795     32     b1 (hidden bias)     H                       = 32
/// 827     256    W2 (hidden → output) H · C_state     = 32·8  = 256
/// 1083    8      b2 (output bias)     C_state                 = 8
/// ```
///
/// The canonical form is the `N` values as consecutive little-endian f32 with no
/// inner tag: the [`Spec`](sima_model::Spec) holding it frames the outer object,
/// exactly as for the Gray-Scott genome. The weights are `f32` — the width of
/// the grid state and the widest float WGSL offers — so CPU and GPU consume
/// bit-identical parameters.
///
/// Validation is exactly finiteness: every weight must be finite, and any finite
/// value (including `-0.0`) is a valid network parameter. The type derives no
/// identity from `PartialEq` — dedup and content addressing key off
/// [`to_bytes`](Codec::to_bytes) — so `-0.0` and `+0.0` are distinct byte images
/// that the format admits, and `PartialEq` serves only the round-trip test,
/// where both sides carry identical bytes.
///
/// The weights sit behind a `Box<[f32; N]>`: `N` f32 is 4 KB, too large to copy
/// implicitly, so the type is `Clone` and `PartialEq` but deliberately not
/// `Copy`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NcaGenome {
    weights: Box<[f32; N]>,
}

impl NcaGenome {
    /// Builds a genome, validating that every weight is finite (rejecting NaN
    /// and both infinities). A violation is [`Error::Validation`] naming the
    /// offending index. `-0.0` is admitted, since the rule is exactly finiteness.
    pub(crate) fn new(weights: Box<[f32; N]>) -> Result<NcaGenome> {
        for (index, &weight) in weights.iter().enumerate() {
            if !weight.is_finite() {
                return Err(Error::Validation(format!(
                    "nca genome weight {index} must be a finite value, got {weight}"
                )));
            }
        }
        Ok(NcaGenome { weights })
    }

    /// The weight vector, for packing into the kernel's uniform buffer.
    pub(crate) fn weights(&self) -> &[f32] {
        &self.weights[..]
    }
}

impl Codec for NcaGenome {
    /// Appends the `N` weights as a bare little-endian f32 sequence, its length
    /// fixed by `N`.
    fn encode(&self, enc: &mut Enc) {
        enc.f32_slice(&self.weights[..]);
    }

    /// Reads the `N` weights and funnels them through [`NcaGenome::new`] so
    /// decode and construction share one validation path.
    fn decode(dec: &mut Dec<'_>) -> Result<NcaGenome> {
        let values = dec.f32_vec(N)?;
        let weights: Box<[f32; N]> = values
            .into_boxed_slice()
            .try_into()
            .expect("f32_vec(N) returns exactly N elements");
        NcaGenome::new(weights)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::{Hash, hash_bytes};
    use sima_model::{FormatId, Spec, SpecId};

    use super::*;

    /// A genome whose weight `j` is `j` — a stated closed-form sequence, every
    /// value finite and exactly representable in f32 (`j < N < 2^24`).
    fn sample() -> NcaGenome {
        NcaGenome::new(Box::new(std::array::from_fn(|j| j as f32))).expect("valid sample genome")
    }

    /// The blake3 content id of [`sample`]'s `to_bytes()` — the 1091 weights
    /// `0.0, 1.0, …, 1090.0` packed as little-endian f32 — reproduced
    /// independently in Python:
    /// `blake3.blake3(b"".join(struct.pack('<f', float(j)) for j in range(1091))).hexdigest()`.
    /// Pinning the 32-byte content id keeps the constant legible, following the
    /// `Grid` `SAMPLE_CONTENT_ID_HEX` precedent.
    const SAMPLE_CONTENT_ID_HEX: &str =
        "5d6e3f0825d7325a2ee74197f608da6e52107422f3fe6dbafe2c9f84ef8922c9";

    /// blake3 of [`sample`]'s full spec bytes — str `"sima.spec.v1"`, str
    /// `"ca_evolution.nca.v1"`, then the length-prefixed payload, per the `Spec`
    /// layout — computed independently with the Python `blake3` package.
    const SAMPLE_SPEC_ID_HEX: &str =
        "0c44bfef7625c2672e7568c86efafcc005e065a455ebf5c168810a8b0f74e63e";

    #[test]
    fn n_is_the_documented_block_sum() {
        assert_eq!(N, 1091);
        assert_eq!((PERCEPTION, W1, B1, W2, B2), (27, 768, 32, 256, 8));
    }

    #[test]
    fn weights_exposes_all_values() {
        let genome = sample();
        assert_eq!(genome.weights().len(), N);
        assert_eq!(genome.weights()[0], 0.0);
        assert_eq!(genome.weights()[N - 1], (N - 1) as f32);
    }

    #[test]
    fn to_bytes_is_content_id_stable() -> Result<()> {
        let bytes = sample().to_bytes();
        assert_eq!(bytes.len(), 4 * N);
        assert_eq!(hash_bytes(&bytes), Hash::from_hex(SAMPLE_CONTENT_ID_HEX)?);
        Ok(())
    }

    #[test]
    fn round_trips_through_bytes() -> Result<()> {
        let genome = sample();
        assert_eq!(NcaGenome::from_bytes(&genome.to_bytes())?, genome);
        Ok(())
    }

    #[test]
    fn new_admits_negative_zero() -> Result<()> {
        // The rule is exactly finiteness, and -0.0 is finite: it is a valid
        // weight whose byte image differs from +0.0.
        let mut weights: [f32; N] = std::array::from_fn(|_| 0.0);
        weights[0] = -0.0;
        let genome = NcaGenome::new(Box::new(weights))?;
        assert_eq!(genome.weights()[0].to_bits(), (-0.0f32).to_bits());
        Ok(())
    }

    #[test]
    fn new_rejects_non_finite_naming_the_index() {
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let mut weights: [f32; N] = std::array::from_fn(|j| j as f32);
            weights[5] = bad;
            match NcaGenome::new(Box::new(weights)) {
                Err(Error::Validation(message)) => assert!(
                    message.contains('5'),
                    "message {message:?} must name the index 5"
                ),
                other => panic!("weight 5 = {bad} must be a validation error, got {other:?}"),
            }
        }
    }

    #[test]
    fn from_bytes_rejects_non_finite() {
        // Well-formed 4364 bytes whose last weight encodes a non-finite value:
        // the structure decodes, the value fails — decode funnels through `new`.
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let mut weights = [0.0f32; N];
            weights[N - 1] = bad;
            let mut enc = Enc::new();
            enc.f32_slice(&weights);
            assert!(matches!(
                NcaGenome::from_bytes(&enc.finish()),
                Err(Error::Validation(_))
            ));
        }
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = sample().to_bytes();
        for cut in [0, 1, 4, 4 * N - 4, 4 * N - 1] {
            assert!(
                matches!(NcaGenome::from_bytes(&full[..cut]), Err(Error::Encoding(_))),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = sample().to_bytes();
        buf.push(0);
        assert!(matches!(
            NcaGenome::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn spec_id_matches_independent_blake3() -> Result<()> {
        // Pins the candidate's actual store address end to end.
        let spec = Spec {
            format: FormatId::new("ca_evolution.nca.v1")?,
            bytes: sample().to_bytes(),
        };
        assert_eq!(spec.id(), SpecId::from_hex(SAMPLE_SPEC_ID_HEX)?);
        Ok(())
    }
}
