//! Per-candidate stats: a per-channel summary of a CA candidate's final grid,
//! observational metadata carried on the executor's [`Stats`](sima_contracts::Stats)
//! channel.
//!
//! [`grid_stats`] summarizes the final grid into canonical bytes; [`describe_stats`]
//! reads them back into one human-readable line for `sima report`. Both halves
//! live here, so the encoding and its reader cannot drift. The bytes travel only
//! the observational path — executor `Stats` to the journal — and never enter a
//! record, a manifest, or any identity criterion; [`Enc`]/[`Dec`] is used because
//! the summary is structured and machine-decoded, and a byte pin keeps the format
//! stable for the renderer.

use std::fmt::Write;

use sima_core::{Dec, Enc, Error, Result};

use crate::cellular::Grid;

/// Summarizes `grid` into canonical bytes: a `u32` channel count, then per
/// channel — channel-major — the `f32` mean, population variance, min, and max.
///
/// Each channel is summarized in two passes over its cells in sequential order,
/// accumulating in `f64` and narrowing each result to `f32`: the first pass sums
/// for the mean and tracks the extremes, the second sums squared deviations from
/// that mean. The result is a pure function of the grid, so it is independent of
/// which attempt produced it. Variance separates a dead uniform grid (variance
/// near zero) from a patterned one; the channel means are the population-level
/// signal.
pub(crate) fn grid_stats(grid: &Grid) -> Vec<u8> {
    let channels = grid.channels() as usize;
    let data = grid.data();
    // Every grid dimension is at least 1, so the cell count is positive and the
    // mean and variance never divide by zero.
    let cell_count = data.len() / channels;

    let mut enc = Enc::new();
    enc.u32(grid.channels());
    for channel in 0..channels {
        let values = || data[channel..].iter().step_by(channels).copied();

        let mut sum = 0.0f64;
        let mut min = f32::INFINITY;
        let mut max = f32::NEG_INFINITY;
        for value in values() {
            sum += f64::from(value);
            min = min.min(value);
            max = max.max(value);
        }
        let mean = sum / cell_count as f64;

        let mut deviation_sq = 0.0f64;
        for value in values() {
            let delta = f64::from(value) - mean;
            deviation_sq += delta * delta;
        }
        let variance = deviation_sq / cell_count as f64;

        enc.f32(mean as f32).f32(variance as f32).f32(min).f32(max);
    }
    enc.finish()
}

/// Renders the bytes written by [`grid_stats`] into one line: each channel as
/// `c<index>: mean <m> var <v> min <lo> max <hi>`, channels joined by ` | `.
///
/// Malformed bytes — a wrong length, a truncated field, or trailing input — are
/// [`Error::Validation`]: `sima report` renders untrusted journal bytes, so a
/// summary the format does not recognize is a validation fault, not an internal
/// decode error.
pub(crate) fn describe_stats(bytes: &[u8]) -> Result<String> {
    render(bytes).map_err(|e| Error::Validation(format!("malformed ca_evolution stats: {e}")))
}

/// Decodes and formats the summary; decoder faults surface as
/// [`Error::Encoding`], which [`describe_stats`] restates as a validation fault.
fn render(bytes: &[u8]) -> Result<String> {
    let mut dec = Dec::new(bytes);
    let channels = dec.u32()?;
    let mut line = String::new();
    for channel in 0..channels {
        let mean = dec.f32()?;
        let variance = dec.f32()?;
        let min = dec.f32()?;
        let max = dec.f32()?;
        if channel > 0 {
            line.push_str(" | ");
        }
        // Writing to a String is infallible.
        let _ = write!(
            line,
            "c{channel}: mean {mean:.3} var {variance:.3} min {min:.3} max {max:.3}"
        );
    }
    dec.finish()?;
    Ok(line)
}

#[cfg(test)]
mod tests {
    use sima_core::{Error, to_hex};

    use super::*;

    /// A 2x1x2 grid with distinct per-channel values. Cell-major interleaved, so
    /// channel 0 is [1.0, 3.0] and channel 1 is [2.0, 5.0].
    fn sample_grid() -> Grid {
        Grid::new(2, 1, 2, vec![1.0, 2.0, 3.0, 5.0]).expect("valid sample grid")
    }

    /// The summary bytes of [`sample_grid`], derived independently:
    /// - channel count `2` (`02000000`),
    /// - channel 0: mean 2.0 (`00000040`), variance 1.0 (`0000803f`),
    ///   min 1.0 (`0000803f`), max 3.0 (`00004040`),
    /// - channel 1: mean 3.5 (`00006040`), variance 2.25 (`00001040`),
    ///   min 2.0 (`00000040`), max 5.0 (`0000a040`).
    ///
    /// Channel 0 variance: ((1-2)^2 + (3-2)^2)/2 = 1.0. Channel 1 variance:
    /// ((2-3.5)^2 + (5-3.5)^2)/2 = 2.25.
    const SAMPLE_STATS_HEX: &str = "020000000000004000\
00803f0000803f000040400000604000001040000000400000a040";

    #[test]
    fn grid_stats_is_byte_stable() {
        assert_eq!(to_hex(&grid_stats(&sample_grid())), SAMPLE_STATS_HEX);
    }

    #[test]
    fn a_uniform_channel_has_zero_variance() {
        // A dead grid: every cell equal, so the variance is exactly zero and the
        // mean, min, and max coincide. This is the signal the stats exist to show.
        // channel count 1 (`01000000`), then mean/var/min/max as
        // 0.25/0.0/0.25/0.25.
        let grid = Grid::new(4, 4, 1, vec![0.25; 16]).expect("uniform grid");
        assert_eq!(
            to_hex(&grid_stats(&grid)),
            "010000000000803e000000000000803e0000803e"
        );
    }

    #[test]
    fn describe_stats_renders_the_pinned_bytes() -> Result<()> {
        let line = describe_stats(&grid_stats(&sample_grid()))?;
        assert_eq!(
            line,
            "c0: mean 2.000 var 1.000 min 1.000 max 3.000 | \
c1: mean 3.500 var 2.250 min 2.000 max 5.000"
        );
        Ok(())
    }

    #[test]
    fn describe_stats_rejects_a_truncated_field() {
        let bytes = grid_stats(&sample_grid());
        // Every prefix that cuts a field mid-way is a validation fault.
        for cut in 0..bytes.len() {
            assert!(
                matches!(describe_stats(&bytes[..cut]), Err(Error::Validation(_))),
                "prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn describe_stats_rejects_trailing_bytes() {
        let mut bytes = grid_stats(&sample_grid());
        bytes.push(0);
        assert!(matches!(describe_stats(&bytes), Err(Error::Validation(_))));
    }
}
