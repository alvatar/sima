//! [`GrayScottPatch`]: the ignition configuration of the Gray-Scott domain's
//! initial grid.

use sima_core::{Dec, Enc, Error, Result, prng};

use crate::cellular::Grid;

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

    /// Appends the canonical form: `base_u` and `base_v` each via
    /// [`Enc::f64`], `side_divisor` via [`Enc::u32`], `noise_width` via
    /// [`Enc::f64`], in the frozen field order.
    pub fn encode(&self, enc: &mut Enc) {
        enc.f64(self.base_u)
            .f64(self.base_v)
            .u32(self.side_divisor)
            .f64(self.noise_width);
    }

    /// Reads a canonical form written by [`GrayScottPatch::encode`],
    /// funneling the values through [`GrayScottPatch::new`] so decode and
    /// construction share one validation path.
    pub fn decode(dec: &mut Dec<'_>) -> Result<GrayScottPatch> {
        let base_u = dec.f64()?;
        let base_v = dec.f64()?;
        let side_divisor = dec.u32()?;
        let noise_width = dec.f64()?;
        GrayScottPatch::new(base_u, base_v, side_divisor, noise_width)
    }

    /// The seeded initial grid every Gray-Scott evaluation ignites from:
    /// the exact fixed point `(u, v) = (1, 0)` everywhere except a centered
    /// square patch of this configuration's base values, each patch cell
    /// perturbed by seeded relative noise of this configuration's width.
    ///
    /// The uniform fixed point evolves into nothing, so the patch is what
    /// makes the trajectory exist, and the noise breaks the square's mirror
    /// symmetry — a deterministic symmetric rule maps a symmetric state to
    /// a symmetric state forever, so without it the evolution would stay
    /// mirror-symmetric and the task seed would be dead weight. The
    /// construction is identity-bearing: the patch values and the seed
    /// determine the trajectory, and the arithmetic below is frozen.
    ///
    /// A zero or overflowing extent is [`Error::Validation`] through
    /// [`Grid::new`].
    pub fn seeded_initial(&self, width: u32, height: u32, seed: u64) -> Result<Grid> {
        // The payload allocation needs the element count up front. Zero and
        // overflowing extents are Grid::new's rules: hand it an empty
        // payload so its own validation produces the error — its zero and
        // overflow checks precede the payload length check.
        let count = if width == 0 || height == 0 {
            None
        } else {
            (width as usize)
                .checked_mul(height as usize)
                .and_then(|cells| cells.checked_mul(2))
        };
        let Some(count) = count else {
            return Grid::new(width, height, 2, Vec::new());
        };
        // The background is the exact fixed point and takes no PRNG draws:
        // its bytes are exactly 1.0f32 / 0.0f32 and the PRNG cost is
        // proportional to the patch, not the grid.
        let mut data = Vec::with_capacity(count);
        for _ in 0..count / 2 {
            data.push(1.0f32);
            data.push(0.0f32);
        }
        // Patch geometry: a centered square spanning
        // [x0, x0 + side) x [y0, y0 + side), integer division throughout.
        let side = (width.min(height) / self.side_divisor()).max(1);
        let x0 = (width - side) / 2;
        let y0 = (height - side) / 2;
        for y in y0..y0 + side {
            for x in x0..x0 + side {
                // Cell index widened to u64 before multiplying, so the
                // substream tag is exact for every representable grid.
                let s = prng::derive(seed, y as u64 * width as u64 + x as u64);
                let t_u = prng::unit_f64(prng::next(s, 0));
                let t_v = prng::unit_f64(prng::next(s, 1));
                // Frozen identity-bearing draw: counter 0 perturbs u,
                // counter 1 perturbs v, each in f64 with one final cast.
                let u = (self.base_u() * (1.0 + (t_u - 0.5) * self.noise_width())) as f32;
                let v = (self.base_v() * (1.0 + (t_v - 0.5) * self.noise_width())) as f32;
                let idx = (y as usize * width as usize + x as usize) * 2;
                data[idx] = u;
                data[idx + 1] = v;
            }
        }
        Grid::new(width, height, 2, data)
    }
}

#[cfg(test)]
mod tests {
    use sima_core::{Dec, Enc, to_hex};

    use super::*;

    /// Pearson's classical ignition configuration.
    fn pearson_patch() -> GrayScottPatch {
        GrayScottPatch::new(0.5, 0.25, 8, 0.02).expect("valid pearson patch")
    }

    /// The canonical bytes of [`pearson_patch`], derived by hand from the
    /// layout — `base_u` and `base_v` as f64 bits little-endian,
    /// `side_divisor` as u32 little-endian, `noise_width` as f64 bits
    /// little-endian: `0.5 = 0x3FE0000000000000`, `0.25 =
    /// 0x3FD0000000000000`, `8`, `0.02 = 0x3F947AE147AE147B` — and
    /// independently reproduced with Python `struct`.
    const PEARSON_BYTES_HEX: &str = "000000000000e03f000000000000d03f080000007b14ae47e17a943f";

    #[test]
    fn codec_round_trips() -> Result<()> {
        // The noiseless patch sits on the admitted boundary of the sign
        // rule, so the round trip covers +0.0 explicitly.
        for patch in [pearson_patch(), GrayScottPatch::new(0.5, 0.25, 8, 0.0)?] {
            let mut enc = Enc::new();
            patch.encode(&mut enc);
            let buf = enc.finish();
            let mut dec = Dec::new(&buf);
            // Derived equality coincides with byte equality on constructed
            // values: validation excludes NaN and -0.0.
            assert_eq!(GrayScottPatch::decode(&mut dec)?, patch);
            dec.finish()?;
        }
        Ok(())
    }

    #[test]
    fn encoding_is_byte_stable() {
        let mut enc = Enc::new();
        pearson_patch().encode(&mut enc);
        assert_eq!(to_hex(&enc.finish()), PEARSON_BYTES_HEX);
    }

    #[test]
    fn decode_rejects_invalid_values() {
        // Well-formed 28 bytes whose base_u bits encode a NaN: the structure
        // decodes, the value fails — decode funnels through `new`.
        let mut enc = Enc::new();
        enc.f64(f64::NAN).f64(0.25).u32(8).f64(0.02);
        let buf = enc.finish();
        let mut dec = Dec::new(&buf);
        assert!(matches!(
            GrayScottPatch::decode(&mut dec),
            Err(Error::Validation(_))
        ));
    }

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

    #[test]
    fn the_background_is_the_exact_fixed_point() -> Result<()> {
        // 16x16: side = max(16 / 8, 1) = 2, x0 = y0 = (16 - 2) / 2 = 7, so
        // the patch spans [7, 9) x [7, 9); every other cell must carry the
        // fixed point's exact bit patterns.
        let grid = pearson_patch().seeded_initial(16, 16, 7)?;
        for y in 0..16usize {
            for x in 0..16usize {
                if (7..9).contains(&x) && (7..9).contains(&y) {
                    continue;
                }
                let idx = (y * 16 + x) * 2;
                let u = grid.data()[idx];
                let v = grid.data()[idx + 1];
                assert_eq!(u.to_bits(), 1.0f32.to_bits(), "u at ({x}, {y})");
                assert_eq!(v.to_bits(), 0.0f32.to_bits(), "v at ({x}, {y})");
            }
        }
        Ok(())
    }

    #[test]
    fn the_patch_is_centered_with_the_specified_side() -> Result<()> {
        // 64x64: side = 8, x0 = y0 = (64 - 8) / 2 = 28, so the patch spans
        // [28, 36) x [28, 36). The four cells just outside each boundary
        // are background; the two opposite patch corners are not.
        let grid = pearson_patch().seeded_initial(64, 64, 3)?;
        let is_background = |x: usize, y: usize| {
            let idx = (y * 64 + x) * 2;
            grid.data()[idx] == 1.0 && grid.data()[idx + 1] == 0.0
        };
        for (x, y) in [(27, 28), (36, 28), (28, 27), (28, 36)] {
            assert!(is_background(x, y), "({x}, {y}) must be background");
        }
        for (x, y) in [(28, 28), (35, 35)] {
            assert!(!is_background(x, y), "({x}, {y}) must be patch");
        }
        Ok(())
    }

    #[test]
    fn patch_values_stay_inside_the_noise_band() -> Result<()> {
        // ±1% around the Pearson base values 0.5 and 0.25. The draw maps
        // t ∈ [0, 1), so the true range is half-open; the closed bounds
        // here need not encode that.
        let grid = pearson_patch().seeded_initial(64, 64, 3)?;
        for y in 28..36usize {
            for x in 28..36usize {
                let idx = (y * 64 + x) * 2;
                let u = grid.data()[idx];
                let v = grid.data()[idx + 1];
                assert!((0.495..=0.505).contains(&u), "u at ({x}, {y}): {u}");
                assert!((0.2475..=0.2525).contains(&v), "v at ({x}, {y}): {v}");
            }
        }
        Ok(())
    }

    #[test]
    fn the_seed_selects_the_state() -> Result<()> {
        let one = pearson_patch().seeded_initial(16, 16, 1)?;
        let two = pearson_patch().seeded_initial(16, 16, 2)?;
        assert_ne!(one.to_bytes(), two.to_bytes());
        let again = pearson_patch().seeded_initial(16, 16, 1)?;
        assert_eq!(one.to_bytes(), again.to_bytes());
        Ok(())
    }

    #[test]
    fn a_tiny_grid_has_a_one_cell_patch() -> Result<()> {
        // side = max(1 / 8, 1) = 1: the single cell is patch, not
        // background.
        let grid = pearson_patch().seeded_initial(1, 1, 5)?;
        assert!((0.495..=0.505).contains(&grid.data()[0]));
        assert!((0.2475..=0.2525).contains(&grid.data()[1]));
        Ok(())
    }

    #[test]
    fn seeded_initial_rejects_a_zero_dimension() {
        for (width, height) in [(0, 16), (16, 0)] {
            assert!(matches!(
                pearson_patch().seeded_initial(width, height, 7),
                Err(Error::Validation(_))
            ));
        }
    }

    #[test]
    fn a_noiseless_patch_ignores_the_seed() -> Result<()> {
        // noise_width 0 is the legitimate noiseless configuration:
        // (t - 0.5) * 0.0 == 0.0 for every t the PRNG produces, so the draw
        // is exact and every patch cell carries the base values' own bit
        // patterns; the seed then selects nothing.
        let patch = GrayScottPatch::new(0.5, 0.25, 8, 0.0)?;
        let grid = patch.seeded_initial(16, 16, 1)?;
        let idx = (7 * 16 + 7) * 2;
        assert_eq!(grid.data()[idx].to_bits(), 0.5f32.to_bits());
        assert_eq!(grid.data()[idx + 1].to_bits(), 0.25f32.to_bits());
        let other = patch.seeded_initial(16, 16, 2)?;
        assert_eq!(grid.to_bytes(), other.to_bytes());
        Ok(())
    }
}
