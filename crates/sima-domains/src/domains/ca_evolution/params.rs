//! [`CaEvolutionParams`]: the run parameters of the `ca_evolution` domain.

use sima_core::{Dec, Enc, Error, Result};

use super::CaEvolutionPatch;

/// The `ca_evolution` run parameters: the grid extents, the steps one task
/// advances, the integration step size, and the ignition patch
/// configuration.
///
/// The canonical form is `width`, `height`, `steps` as little-endian `u32`,
/// `dt` as its IEEE-754 bits in a little-endian `u32`, then the patch's
/// canonical form: exactly 44 bytes. The payload carries no inner tag: the
/// spec's format id frames the interpretation of the params blob, the same
/// rule the genome's spec payload follows.
///
/// `steps` counts one task's steps: with `segments = N` in the run config a
/// candidate runs as a chain of N tasks, so the full trajectory spans
/// `N * steps` steps.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CaEvolutionParams {
    /// Grid width in cells.
    width: u32,
    /// Grid height in cells.
    height: u32,
    /// Simulation steps one task advances.
    steps: u32,
    /// Integration step size.
    dt: f32,
    /// Ignition configuration of the initial grid.
    patch: CaEvolutionPatch,
}

/// Validates a count parameter: at least 1. Zero cells make no grid, and a
/// zero-step task would commit its input unchanged.
fn at_least_one(name: &str, value: u32) -> Result<u32> {
    if value >= 1 {
        Ok(value)
    } else {
        Err(Error::Validation(format!(
            "ca_evolution params {name} must be at least 1, got {value}"
        )))
    }
}

impl CaEvolutionParams {
    /// Builds run parameters, validating each field: `width`, `height`, and
    /// `steps` must be at least 1; `dt` must be finite and strictly greater
    /// than zero. The patch arrives already validated by
    /// [`CaEvolutionPatch::new`]. Any violation is [`Error::Validation`]
    /// naming the field.
    pub fn new(
        width: u32,
        height: u32,
        steps: u32,
        dt: f32,
        patch: CaEvolutionPatch,
    ) -> Result<CaEvolutionParams> {
        if !(dt.is_finite() && dt > 0.0) {
            return Err(Error::Validation(format!(
                "ca_evolution params dt must be a finite value greater than zero, got {dt}"
            )));
        }
        Ok(CaEvolutionParams {
            width: at_least_one("width", width)?,
            height: at_least_one("height", height)?,
            steps: at_least_one("steps", steps)?,
            dt,
            patch,
        })
    }

    /// The grid width in cells.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// The grid height in cells.
    pub fn height(&self) -> u32 {
        self.height
    }

    /// The simulation steps one task advances.
    pub fn steps(&self) -> u32 {
        self.steps
    }

    /// The integration step size.
    pub fn dt(&self) -> f32 {
        self.dt
    }

    /// The ignition configuration of the initial grid.
    pub fn patch(&self) -> CaEvolutionPatch {
        self.patch
    }

    /// Appends the canonical form: `width`, `height`, `steps` via
    /// [`Enc::u32`], `dt` via [`Enc::f32`], then the patch via
    /// [`CaEvolutionPatch::encode`], in the frozen field order.
    pub fn encode(&self, enc: &mut Enc) {
        enc.u32(self.width)
            .u32(self.height)
            .u32(self.steps)
            .f32(self.dt);
        self.patch.encode(enc);
    }

    /// Reads a canonical form written by [`CaEvolutionParams::encode`],
    /// funneling the values through [`CaEvolutionParams::new`] so decode and
    /// construction share one validation path.
    pub fn decode(dec: &mut Dec<'_>) -> Result<CaEvolutionParams> {
        let width = dec.u32()?;
        let height = dec.u32()?;
        let steps = dec.u32()?;
        let dt = dec.f32()?;
        let patch = CaEvolutionPatch::decode(dec)?;
        CaEvolutionParams::new(width, height, steps, dt, patch)
    }

    /// The standalone canonical bytes — exactly the bytes the run's params
    /// blob carries.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        self.encode(&mut enc);
        enc.finish()
    }

    /// Parses standalone canonical bytes, rejecting trailing input.
    pub fn from_bytes(bytes: &[u8]) -> Result<CaEvolutionParams> {
        let mut dec = Dec::new(bytes);
        let params = CaEvolutionParams::decode(&mut dec)?;
        dec.finish()?;
        Ok(params)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::to_hex;

    use super::*;

    /// Pearson's classical ignition configuration.
    fn pearson_patch() -> CaEvolutionPatch {
        CaEvolutionPatch::new(0.5, 0.25, 8, 0.02).expect("valid pearson patch")
    }

    fn sample() -> CaEvolutionParams {
        CaEvolutionParams::new(64, 48, 100, 1.0, pearson_patch()).expect("valid sample params")
    }

    /// The canonical bytes of [`sample`], derived by hand from the layout —
    /// width 64, height 48, steps 100 as little-endian `u32`, dt 1.0 as its
    /// f32 bits (`0x3F800000`) little-endian, then the patch's 28 canonical
    /// bytes — and independently reproduced with Python `struct`.
    const SAMPLE_BYTES_HEX: &str =
        "4000000030000000640000000000803f000000000000e03f000000000000d03f080000007b14ae47e17a943f";

    #[test]
    fn new_rejects_zero_counts() {
        let names = ["width", "height", "steps"];
        for (position, name) in names.iter().enumerate() {
            let mut p = [64u32, 48, 100];
            p[position] = 0;
            match CaEvolutionParams::new(p[0], p[1], p[2], 1.0, pearson_patch()) {
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
            match CaEvolutionParams::new(64, 48, 100, bad, pearson_patch()) {
                Err(Error::Validation(message)) => {
                    assert!(message.contains("dt"), "the error names dt: {message}")
                }
                other => panic!("dt = {bad} must be a validation error, got {other:?}"),
            }
        }
    }

    #[test]
    fn accessors_round_trip() -> Result<()> {
        let params = sample();
        assert_eq!(params.width(), 64);
        assert_eq!(params.height(), 48);
        assert_eq!(params.steps(), 100);
        assert_eq!(params.dt(), 1.0);
        assert_eq!(params.patch(), pearson_patch());
        Ok(())
    }

    #[test]
    fn round_trips_through_bytes() -> Result<()> {
        let params = sample();
        // Derived equality coincides with byte equality on constructed
        // values: validation excludes NaN and -0.0 everywhere.
        assert_eq!(CaEvolutionParams::from_bytes(&params.to_bytes())?, params);
        Ok(())
    }

    #[test]
    fn to_bytes_is_byte_stable() {
        assert_eq!(to_hex(&sample().to_bytes()), SAMPLE_BYTES_HEX);
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut buf = sample().to_bytes();
        buf.push(0);
        assert!(matches!(
            CaEvolutionParams::from_bytes(&buf),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = sample().to_bytes();
        for cut in 0..full.len() {
            assert!(
                matches!(
                    CaEvolutionParams::from_bytes(&full[..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_invalid_values() {
        // Well-formed 44 bytes encoding steps = 0: the structure decodes,
        // the value fails — decode funnels through `new`.
        let mut enc = Enc::new();
        enc.u32(64).u32(48).u32(0).f32(1.0);
        pearson_patch().encode(&mut enc);
        assert!(matches!(
            CaEvolutionParams::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }
}
