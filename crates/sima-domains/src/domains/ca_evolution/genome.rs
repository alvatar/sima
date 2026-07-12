//! [`CaEvolutionGenome`]: the evolvable parameters of the `ca_evolution` domain.

use sima_core::{Dec, Enc, Error, Result};

/// The Gray-Scott genome: the four evolvable scalars of the two-chemical
/// reaction-diffusion system
///
/// ```text
/// du/dt = Du * lap(u) - u*v^2 + f*(1 - u)
/// dv/dt = Dv * lap(v) + u*v^2 - (f + k)*v
/// ```
///
/// where `lap` is a discrete Laplacian, `f` (feed) replenishes `u` toward 1,
/// `k` (kill) drains `v` — `v` decays at total rate `f + k` — `u*v^2` is the
/// reaction converting `u` into `v`, and `Du`, `Dv` scale each channel's
/// diffusion.
///
/// The canonical form is the four values in the frozen order `feed`, `kill`,
/// `diffusion_u`, `diffusion_v`, each as its IEEE-754 bits in a little-endian
/// `u32`: exactly 16 bytes. All four are `f32` — the width of the grid state
/// and the widest float WGSL offers — so CPU and GPU consume bit-identical
/// parameters. The payload carries no inner tag: the
/// [`Spec`](sima_model::Spec) holding it frames the outer object with the
/// spec tag and the format id, both inside the hashed identity.
///
/// Validation is part of the format contract — decode funnels through it, so
/// changing it would retroactively change which stored specs decode — and
/// therefore rejects only what is meaningless under every possible engine.
/// Sampling bounds are generator configuration and integration stability is
/// engine policy; neither belongs in the permanent format.
// On constructed values the derived `PartialEq` is total and coincides with
// byte equality, because validation excludes NaN (which would make equality
// irreflexive) and -0.0 (the one value that equals another numerically while
// differing in bytes).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CaEvolutionGenome {
    /// Feed rate `f`: replenishes `u` toward 1.
    feed: f32,
    /// Kill rate `k`: `v` decays at total rate `f + k`.
    kill: f32,
    /// Diffusion coefficient of the `u` channel.
    diffusion_u: f32,
    /// Diffusion coefficient of the `v` channel.
    diffusion_v: f32,
}

/// Validates a rate parameter: finite with positive sign. Admits `+0.0` and
/// rejects NaN, both infinities, negatives, and `-0.0`. Rejecting `-0.0`
/// preserves one value, one byte image: `-0.0 == 0.0` numerically but their
/// bit patterns differ, so admitting both would give numerically identical
/// genomes distinct content ids.
fn finite_sign_positive(name: &str, value: f32) -> Result<f32> {
    if value.is_finite() && value.is_sign_positive() {
        Ok(value)
    } else {
        Err(Error::Validation(format!(
            "ca_evolution genome {name} must be a finite value with positive sign, got {value}"
        )))
    }
}

/// Validates a diffusion parameter: finite and greater than zero. Beyond the
/// sign rule this rejects `+0.0`, because zero diffusion removes the spatial
/// coupling that makes the model reaction-diffusion.
fn finite_positive(name: &str, value: f32) -> Result<f32> {
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(Error::Validation(format!(
            "ca_evolution genome {name} must be a finite value greater than zero, got {value}"
        )))
    }
}

impl CaEvolutionGenome {
    /// Builds a genome, validating each parameter: `feed` and `kill` must be
    /// finite with positive sign (`+0.0` is admitted); `diffusion_u` and
    /// `diffusion_v` must be finite and greater than zero. Any violation is
    /// [`Error::Validation`] naming the parameter.
    pub fn new(
        feed: f32,
        kill: f32,
        diffusion_u: f32,
        diffusion_v: f32,
    ) -> Result<CaEvolutionGenome> {
        Ok(CaEvolutionGenome {
            feed: finite_sign_positive("feed", feed)?,
            kill: finite_sign_positive("kill", kill)?,
            diffusion_u: finite_positive("diffusion_u", diffusion_u)?,
            diffusion_v: finite_positive("diffusion_v", diffusion_v)?,
        })
    }

    /// The feed rate `f`.
    pub fn feed(&self) -> f32 {
        self.feed
    }

    /// The kill rate `k`.
    pub fn kill(&self) -> f32 {
        self.kill
    }

    /// The diffusion coefficient of the `u` channel.
    pub fn diffusion_u(&self) -> f32 {
        self.diffusion_u
    }

    /// The diffusion coefficient of the `v` channel.
    pub fn diffusion_v(&self) -> f32 {
        self.diffusion_v
    }

    /// Appends the canonical form: the four parameters in the frozen field
    /// order, each via [`Enc::f32`].
    pub fn encode(&self, enc: &mut Enc) {
        enc.f32(self.feed)
            .f32(self.kill)
            .f32(self.diffusion_u)
            .f32(self.diffusion_v);
    }

    /// Reads a canonical form written by [`CaEvolutionGenome::encode`],
    /// funneling the values through [`CaEvolutionGenome::new`] so decode and
    /// construction share one validation path.
    pub fn decode(dec: &mut Dec<'_>) -> Result<CaEvolutionGenome> {
        let feed = dec.f32()?;
        let kill = dec.f32()?;
        let diffusion_u = dec.f32()?;
        let diffusion_v = dec.f32()?;
        CaEvolutionGenome::new(feed, kill, diffusion_u, diffusion_v)
    }

    /// The standalone canonical bytes — exactly the bytes a spec carries.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        self.encode(&mut enc);
        enc.finish()
    }

    /// Parses standalone canonical bytes, rejecting trailing input.
    pub fn from_bytes(bytes: &[u8]) -> Result<CaEvolutionGenome> {
        let mut dec = Dec::new(bytes);
        let genome = CaEvolutionGenome::decode(&mut dec)?;
        dec.finish()?;
        Ok(genome)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::to_hex;
    use sima_model::{FormatId, Spec, SpecId};

    use super::*;

    /// A pattern-forming point with the classical diffusion pair.
    fn sample() -> CaEvolutionGenome {
        CaEvolutionGenome::new(0.055, 0.062, 0.16, 0.08).expect("valid sample genome")
    }

    /// The sample's parameters with `value` substituted at `position`, in the
    /// frozen field order `feed`, `kill`, `diffusion_u`, `diffusion_v`.
    fn with_substituted(position: usize, value: f32) -> Result<CaEvolutionGenome> {
        let mut p = [0.055, 0.062, 0.16, 0.08];
        p[position] = value;
        CaEvolutionGenome::new(p[0], p[1], p[2], p[3])
    }

    /// The canonical bytes of [`sample`], derived by hand from the layout —
    /// each value's `to_bits()` as little-endian hex, concatenated in field
    /// order — and independently reproduced with Python `struct`:
    /// `''.join(struct.pack('<f', v).hex() for v in (0.055, 0.062, 0.16, 0.08))`.
    const SAMPLE_BYTES_HEX: &str = "ae47613db6f37d3d0ad7233e0ad7a33d";

    /// blake3 of the sample's full spec bytes — str `"sima.spec.v1"`, str
    /// `"ca_evolution.v1"`, then the length-prefixed payload, per the `Spec`
    /// layout — computed independently with the Python `blake3` package:
    /// `blake3.blake3(bytes.fromhex(spec_hex)).hexdigest()`.
    const SAMPLE_SPEC_ID_HEX: &str =
        "c4a3b78eeb056e08d796e72f95533601643dcd8d9513c337332399ffb7f1fb1f";

    #[test]
    fn new_accepts_the_classical_parameter_point() -> Result<()> {
        let genome = sample();
        assert_eq!(genome.feed().to_bits(), 0.055f32.to_bits());
        assert_eq!(genome.kill().to_bits(), 0.062f32.to_bits());
        assert_eq!(genome.diffusion_u().to_bits(), 0.16f32.to_bits());
        assert_eq!(genome.diffusion_v().to_bits(), 0.08f32.to_bits());
        // Zero feed and kill sit on the admitted boundary of the sign rule.
        with_substituted(0, 0.0)?;
        with_substituted(1, 0.0)?;
        Ok(())
    }

    #[test]
    fn new_rejects_non_finite_parameters() {
        let names = ["feed", "kill", "diffusion_u", "diffusion_v"];
        for (position, name) in names.iter().enumerate() {
            for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
                match with_substituted(position, bad) {
                    Err(Error::Validation(message)) => assert!(
                        message.contains(name),
                        "message {message:?} must name {name}"
                    ),
                    other => panic!("{name} = {bad} must be a validation error, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn new_rejects_sign_negative_parameters() {
        for position in 0..4 {
            for bad in [-0.055f32, -0.0] {
                assert!(
                    matches!(with_substituted(position, bad), Err(Error::Validation(_))),
                    "position {position} value {bad} must be rejected"
                );
            }
        }
    }

    #[test]
    fn new_rejects_zero_diffusion() -> Result<()> {
        assert!(matches!(
            with_substituted(2, 0.0),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            with_substituted(3, 0.0),
            Err(Error::Validation(_))
        ));
        // Pins the asymmetry between the two predicates: the zero the
        // diffusion rule rejects is admitted by the sign rule.
        CaEvolutionGenome::new(0.0, 0.0, 0.16, 0.08)?;
        Ok(())
    }

    #[test]
    fn to_bytes_is_byte_stable() {
        assert_eq!(to_hex(&sample().to_bytes()), SAMPLE_BYTES_HEX);
    }

    #[test]
    fn round_trips_through_bytes() -> Result<()> {
        let genome = sample();
        // Derived equality is exact here: validation excludes NaN and -0.0,
        // so `PartialEq` coincides with byte equality on constructed values.
        assert_eq!(CaEvolutionGenome::from_bytes(&genome.to_bytes())?, genome);
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = sample().to_bytes();
        for cut in 0..full.len() {
            assert!(
                matches!(
                    CaEvolutionGenome::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = sample().to_bytes();
        buf.push(0);
        assert!(matches!(
            CaEvolutionGenome::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn from_bytes_rejects_invalid_values() {
        // Sixteen well-formed bytes whose first four encode a NaN: the
        // structure decodes, the value fails — decode funnels through `new`.
        let mut enc = Enc::new();
        enc.f32(f32::NAN).f32(0.062).f32(0.16).f32(0.08);
        assert!(matches!(
            CaEvolutionGenome::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn spec_id_matches_independent_blake3() -> Result<()> {
        // Pins the candidate's actual store address end to end.
        let spec = Spec {
            format: FormatId::new("ca_evolution.v1")?,
            bytes: sample().to_bytes(),
        };
        assert_eq!(spec.id(), SpecId::from_hex(SAMPLE_SPEC_ID_HEX)?);
        Ok(())
    }

    #[test]
    fn adjacent_genomes_have_distinct_spec_ids() -> Result<()> {
        // Every representable parameter difference is a distinct candidate:
        // one bit up in any position changes the bytes and the spec id.
        let base = sample();
        let format = FormatId::new("ca_evolution.v1")?;
        let base_id = Spec {
            format: format.clone(),
            bytes: base.to_bytes(),
        }
        .id();
        let values = [
            base.feed(),
            base.kill(),
            base.diffusion_u(),
            base.diffusion_v(),
        ];
        for (position, value) in values.iter().enumerate() {
            let perturbed = with_substituted(position, f32::from_bits(value.to_bits() + 1))?;
            assert_ne!(perturbed.to_bytes(), base.to_bytes());
            let perturbed_id = Spec {
                format: format.clone(),
                bytes: perturbed.to_bytes(),
            }
            .id();
            assert_ne!(perturbed_id, base_id);
        }
        Ok(())
    }
}
