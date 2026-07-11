//! [`GrayScottPatch`]: the ignition configuration of the Gray-Scott domain's
//! initial grid.

use sima_core::{Error, Result};

/// The ignition configuration of the Gray-Scott domain's initial grid: the
/// base values dropped into the centered square patch, the divisor of the
/// shorter grid extent giving the patch side, and the full relative width of
/// the noise band around each base value.
///
/// Pearson's classical configuration is `(0.5, 0.25, 8, 0.02)`, spelled by
/// the caller: the values determine candidate identity, so there is no
/// `Default` — identity-determining values are always explicit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GrayScottPatch {
    /// Base value of chemical `u` inside the patch.
    base_u: f64,
    /// Base value of chemical `v` inside the patch.
    base_v: f64,
    /// Divisor of the shorter grid extent giving the patch side:
    /// `side = max(min(width, height) / side_divisor, 1)`.
    side_divisor: u32,
    /// Full relative width of the noise band around each base value:
    /// `(t - 0.5) * noise_width` spans ±`noise_width / 2` for `t ∈ [0, 1)`.
    noise_width: f64,
}

/// Validates a patch parameter: finite with positive sign. Admits `+0.0` —
/// zero noise width is the legitimate noiseless configuration — and rejects
/// NaN, both infinities, negatives, and `-0.0`, keeping one value, one byte
/// image once these values enter run params.
fn finite_sign_positive(name: &str, value: f64) -> Result<f64> {
    if value.is_finite() && value.is_sign_positive() {
        Ok(value)
    } else {
        Err(Error::Validation(format!(
            "gray-scott patch {name} must be a finite value with positive sign, got {value}"
        )))
    }
}

impl GrayScottPatch {
    /// Builds a patch configuration, validating each field: `base_u`,
    /// `base_v`, and `noise_width` must be finite with positive sign
    /// (`+0.0` admitted); `side_divisor` must be at least 1, since 0 would
    /// divide by zero in the side computation. Any violation is
    /// [`Error::Validation`] naming the field.
    pub fn new(
        base_u: f64,
        base_v: f64,
        side_divisor: u32,
        noise_width: f64,
    ) -> Result<GrayScottPatch> {
        if side_divisor == 0 {
            return Err(Error::Validation(
                "gray-scott patch side_divisor must be at least 1, got 0".to_string(),
            ));
        }
        Ok(GrayScottPatch {
            base_u: finite_sign_positive("base_u", base_u)?,
            base_v: finite_sign_positive("base_v", base_v)?,
            side_divisor,
            noise_width: finite_sign_positive("noise_width", noise_width)?,
        })
    }

    /// The base value of chemical `u` inside the patch.
    pub fn base_u(&self) -> f64 {
        self.base_u
    }

    /// The base value of chemical `v` inside the patch.
    pub fn base_v(&self) -> f64 {
        self.base_v
    }

    /// The divisor of the shorter grid extent giving the patch side.
    pub fn side_divisor(&self) -> u32 {
        self.side_divisor
    }

    /// The full relative width of the noise band around each base value.
    pub fn noise_width(&self) -> f64 {
        self.noise_width
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_rejects_invalid_values() -> Result<()> {
        // Each invalid value substituted into each f64 field; the error
        // names the field. -0.0 is rejected like the genome's rule: one
        // value, one byte image.
        let names = ["base_u", "base_v", "noise_width"];
        for (position, name) in names.iter().enumerate() {
            for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.5, -0.0] {
                let mut p = [0.5, 0.25, 0.02];
                p[position] = bad;
                match GrayScottPatch::new(p[0], p[1], 8, p[2]) {
                    Err(Error::Validation(message)) => assert!(
                        message.contains(name),
                        "message {message:?} must name {name}"
                    ),
                    other => panic!("{name} = {bad} must be a validation error, got {other:?}"),
                }
            }
        }
        // A zero divisor would divide by zero in the side computation.
        assert!(matches!(
            GrayScottPatch::new(0.5, 0.25, 0, 0.02),
            Err(Error::Validation(_))
        ));
        // A valid construction round-trips through the accessors.
        let patch = GrayScottPatch::new(0.5, 0.25, 8, 0.02)?;
        assert_eq!(patch.base_u(), 0.5);
        assert_eq!(patch.base_v(), 0.25);
        assert_eq!(patch.side_divisor(), 8);
        assert_eq!(patch.noise_width(), 0.02);
        Ok(())
    }
}
