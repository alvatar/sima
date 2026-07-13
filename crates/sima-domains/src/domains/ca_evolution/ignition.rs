//! [`seeded_patch`]: the general seeded-ignition primitive shared by CA models.

use sima_core::{Result, prng};

use crate::cellular::Grid;

/// Builds the seeded initial grid a CA model ignites from: the whole grid filled
/// with the per-channel `background` (the model's fixed point), then a centered
/// square of side `max(min(width, height) / side_divisor, 1)` whose every cell's
/// channel `c` is `(patch[c] * (1.0 + (t - 0.5) * noise)) as f32`, with
/// `t = unit_f64(next(derive(seed, y * width + x), c))`.
///
/// `background` and `patch` are length `channels`. The arithmetic is f64 and
/// identity-bearing: the patch values, `noise`, and the seed determine the
/// committed trajectory, and this arithmetic is frozen. `side_divisor` must be
/// at least 1 (the model's ignition config validates it).
///
/// A zero or overflowing extent is [`Error::Validation`](sima_core::Error)
/// through [`Grid::new`].
// The eight inputs are irreducibly separate: grid geometry, the per-channel
// background and patch bases, the patch side and noise, and the seed. This is
// the model-facing seam, so they stay loose rather than bundled into a type.
#[allow(clippy::too_many_arguments)]
pub(crate) fn seeded_patch(
    width: u32,
    height: u32,
    channels: u32,
    background: &[f64],
    patch: &[f64],
    side_divisor: u32,
    noise: f64,
    seed: u64,
) -> Result<Grid> {
    let channels = channels as usize;
    debug_assert_eq!(
        background.len(),
        channels,
        "one background value per channel"
    );
    debug_assert_eq!(patch.len(), channels, "one patch value per channel");
    // The payload allocation needs the element count up front. Zero and
    // overflowing extents are Grid::new's rules: hand it an empty payload so its
    // own validation produces the error — its zero and overflow checks precede
    // the payload length check.
    let count = if width == 0 || height == 0 {
        None
    } else {
        (width as usize)
            .checked_mul(height as usize)
            .and_then(|cells| cells.checked_mul(channels))
    };
    let Some(count) = count else {
        return Grid::new(width, height, channels as u32, Vec::new());
    };
    // The background takes no PRNG draws: its bytes are exactly each per-channel
    // base cast to f32, and the PRNG cost is proportional to the patch, not the
    // grid.
    let mut data = Vec::with_capacity(count);
    for _ in 0..count / channels {
        for &base in background {
            data.push(base as f32);
        }
    }
    // Patch geometry: a centered square spanning [x0, x0 + side) x [y0, y0 +
    // side), integer division throughout.
    let side = (width.min(height) / side_divisor).max(1);
    let x0 = (width - side) / 2;
    let y0 = (height - side) / 2;
    for y in y0..y0 + side {
        for x in x0..x0 + side {
            // Cell index widened to u64 before multiplying, so the substream tag
            // is exact for every representable grid.
            let s = prng::derive(seed, y as u64 * width as u64 + x as u64);
            let cell = (y as usize * width as usize + x as usize) * channels;
            for (c, &base) in patch.iter().enumerate() {
                // Frozen identity-bearing draw: counter c perturbs channel c, in
                // f64 with one final cast.
                let t = prng::unit_f64(prng::next(s, c as u64));
                data[cell + c] = (base * (1.0 + (t - 0.5) * noise)) as f32;
            }
        }
    }
    Grid::new(width, height, channels as u32, data)
}

#[cfg(test)]
mod tests {
    use sima_core::Error;

    use super::*;

    /// Gray-Scott's ignition through the primitive: the fixed point background
    /// `(1, 0)` with the Pearson patch base values and noise.
    fn pearson(width: u32, height: u32, seed: u64) -> Result<Grid> {
        seeded_patch(width, height, 2, &[1.0, 0.0], &[0.5, 0.25], 8, 0.02, seed)
    }

    #[test]
    fn the_background_is_the_exact_fixed_point() -> Result<()> {
        // 16x16: side = max(16 / 8, 1) = 2, x0 = y0 = (16 - 2) / 2 = 7, so the
        // patch spans [7, 9) x [7, 9); every other cell carries the background's
        // exact bit patterns.
        let grid = pearson(16, 16, 7)?;
        for y in 0..16usize {
            for x in 0..16usize {
                if (7..9).contains(&x) && (7..9).contains(&y) {
                    continue;
                }
                let idx = (y * 16 + x) * 2;
                assert_eq!(
                    grid.data()[idx].to_bits(),
                    1.0f32.to_bits(),
                    "u at ({x}, {y})"
                );
                assert_eq!(
                    grid.data()[idx + 1].to_bits(),
                    0.0f32.to_bits(),
                    "v at ({x}, {y})"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn the_patch_is_centered_with_the_specified_side() -> Result<()> {
        // 64x64: side = 8, x0 = y0 = (64 - 8) / 2 = 28, so the patch spans
        // [28, 36) x [28, 36). Cells just outside each boundary are background;
        // the two opposite patch corners are not.
        let grid = pearson(64, 64, 3)?;
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
        // ±1% around the Pearson base values 0.5 and 0.25.
        let grid = pearson(64, 64, 3)?;
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
        let one = pearson(16, 16, 1)?;
        let two = pearson(16, 16, 2)?;
        assert_ne!(one.to_bytes(), two.to_bytes());
        let again = pearson(16, 16, 1)?;
        assert_eq!(one.to_bytes(), again.to_bytes());
        Ok(())
    }

    #[test]
    fn a_tiny_grid_has_a_one_cell_patch() -> Result<()> {
        // side = max(1 / 8, 1) = 1: the single cell is patch, not background.
        let grid = pearson(1, 1, 5)?;
        assert!((0.495..=0.505).contains(&grid.data()[0]));
        assert!((0.2475..=0.2525).contains(&grid.data()[1]));
        Ok(())
    }

    #[test]
    fn a_zero_dimension_is_rejected() {
        for (width, height) in [(0, 16), (16, 0)] {
            assert!(matches!(
                pearson(width, height, 7),
                Err(Error::Validation(_))
            ));
        }
    }

    #[test]
    fn zero_noise_ignores_the_seed() -> Result<()> {
        // (t - 0.5) * 0.0 == 0.0 for every t, so each patch cell carries the base
        // values' own bit patterns and the seed selects nothing.
        let a = seeded_patch(16, 16, 2, &[1.0, 0.0], &[0.5, 0.25], 8, 0.0, 1)?;
        let idx = (7 * 16 + 7) * 2;
        assert_eq!(a.data()[idx].to_bits(), 0.5f32.to_bits());
        assert_eq!(a.data()[idx + 1].to_bits(), 0.25f32.to_bits());
        let b = seeded_patch(16, 16, 2, &[1.0, 0.0], &[0.5, 0.25], 8, 0.0, 2)?;
        assert_eq!(a.to_bytes(), b.to_bytes());
        Ok(())
    }

    #[test]
    fn a_single_channel_grid_ignites() -> Result<()> {
        // The primitive is channel-generic: one channel, background 0, patch 1.
        let grid = seeded_patch(8, 8, 1, &[0.0], &[1.0], 4, 0.0, 3)?;
        assert_eq!((grid.width(), grid.height(), grid.channels()), (8, 8, 1));
        // side = max(8 / 4, 1) = 2, centered at [3, 5); a corner cell is patch.
        assert_eq!(grid.data()[3 * 8 + 3].to_bits(), 1.0f32.to_bits());
        // A background cell is exactly 0.
        assert_eq!(grid.data()[0].to_bits(), 0.0f32.to_bits());
        Ok(())
    }
}
