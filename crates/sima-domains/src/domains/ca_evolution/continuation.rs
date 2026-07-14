//! Framed continuation state for a stepped CA model: the step index the
//! trajectory has reached, ahead of the grid's canonical bytes.
//!
//! A stepped model consumes the absolute step in its update, so the grid alone
//! is an incomplete continuation — the step must travel with it. This frame
//! carries a `u64` step and then the grid's own canonical bytes, both through
//! the identity-bearing [`Enc`]/[`Dec`] encoding: the committed `state`
//! artifact of a stepped model is exactly these bytes. A bare-grid model
//! (Gray-Scott) commits the grid alone and never uses this frame. An external
//! volume reader of a stepped model's state skips the 8-byte step header and
//! reads the grid that follows.

use sima_core::{Dec, Enc, Result};

use crate::cellular::Grid;

/// Encodes a stepped model's committed state: the `step` the trajectory has
/// reached as a little-endian `u64`, then the grid's canonical bytes.
pub fn encode_continuation(step: u64, grid: &Grid) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.u64(step);
    let mut bytes = enc.finish();
    bytes.extend_from_slice(&grid.to_bytes());
    bytes
}

/// Decodes the framed state written by [`encode_continuation`]: the `u64` step,
/// then the grid from the remaining bytes.
///
/// The step header is a fixed 8 bytes; the grid's canonical bytes are the whole
/// remainder, self-delimited by their own tag and length, so [`Grid::from_bytes`]
/// validates a truncated or trailing grid. A buffer shorter than the header is
/// [`sima_core::Error::Encoding`].
pub fn decode_continuation(bytes: &[u8]) -> Result<(u64, Grid)> {
    let mut dec = Dec::new(bytes);
    let step = dec.u64()?;
    // The u64 read above succeeds only with at least 8 bytes present, so the
    // header split is in range and the grid consumes the remainder.
    let grid = Grid::from_bytes(&bytes[8..])?;
    Ok((step, grid))
}

#[cfg(test)]
mod tests {
    use sima_core::{Error, to_hex};

    use super::*;

    /// A 2x1x2 sample grid, matching the grid codec's own byte pin.
    fn sample_grid() -> Grid {
        Grid::new(2, 1, 2, vec![1.5, -2.0, 0.25, 1024.0]).expect("valid sample grid")
    }

    /// The framed bytes for step 100 over [`sample_grid`]: step 100 as a
    /// little-endian `u64` (`6400000000000000`), then the grid's canonical
    /// bytes. Independently reproduced with Python `struct`.
    const SAMPLE_FRAME_HEX: &str = "6400000000000000150000000000000073696d612e63656c6c756c61722e6772\
69642e76310200000001000000020000000000c03f000000c00000803e00008044";

    #[test]
    fn encoding_is_byte_stable() {
        assert_eq!(
            to_hex(&encode_continuation(100, &sample_grid())),
            SAMPLE_FRAME_HEX
        );
    }

    #[test]
    fn round_trips_step_and_grid() -> Result<()> {
        // The step boundaries and the u64 extreme all round-trip; the grid comes
        // back byte-identical.
        for step in [0u64, 1, 50, 100, u64::MAX] {
            let frame = encode_continuation(step, &sample_grid());
            let (decoded_step, grid) = decode_continuation(&frame)?;
            assert_eq!(decoded_step, step);
            assert_eq!(grid.to_bytes(), sample_grid().to_bytes());
        }
        Ok(())
    }

    #[test]
    fn decode_rejects_a_truncated_header() {
        // Fewer than the eight header bytes: the u64 read errors before any grid.
        for cut in 0..8 {
            assert!(
                matches!(
                    decode_continuation(&[0u8; 8][..cut]),
                    Err(Error::Encoding(_))
                ),
                "prefix of {cut} header bytes must be rejected"
            );
        }
    }

    #[test]
    fn decode_rejects_a_truncated_grid() {
        let frame = encode_continuation(100, &sample_grid());
        // Every prefix that keeps the full header but cuts the grid is rejected.
        for cut in 8..frame.len() {
            assert!(
                matches!(decode_continuation(&frame[..cut]), Err(Error::Encoding(_))),
                "grid prefix of {cut} bytes must be rejected"
            );
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut frame = encode_continuation(100, &sample_grid());
        frame.push(0);
        // The extra byte extends the grid remainder, which Grid::from_bytes
        // rejects as trailing input.
        assert!(matches!(
            decode_continuation(&frame),
            Err(Error::Encoding(_))
        ));
    }
}
