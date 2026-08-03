//! [`Grid`]: the 2D multi-channel `f32` state of the cellular kind.

use sima_core::{Dec, Enc, Error, Hash, Result, hash_bytes};

/// Canonical format tag for a serialized [`Grid`], written first in its bytes
/// so a decoder rejects any other object outright.
const GRID_TAG: &str = "sima.cellular.grid.v1";

/// A 2D, multi-channel `f32` grid: the state a cellular update advances.
///
/// The payload is cell-major interleaved. For extent `(width, height)` and
/// `channels` channels, channel `c` of the cell at `(x, y)` sits at index
/// `((y * width) + x) * channels + c`. Every dimension is at least 1.
///
/// A grid is identity-bearing: [`Grid::to_bytes`] gives its canonical byte
/// form and [`Grid::content_id`] the blake3 address the store puts it under,
/// so it round-trips through content-addressed storage as an opaque snapshot.
#[derive(Debug, Clone)]
pub struct Grid {
    width: u32,
    height: u32,
    channels: u32,
    /// Cell-major interleaved payload, length `width * height * channels`.
    data: Vec<f32>,
}

/// The element count `width * height * channels`, or `None` on overflow. Shared
/// by construction and decode so both compute the count the same way.
fn element_count(width: u32, height: u32, channels: u32) -> Option<usize> {
    (width as usize)
        .checked_mul(height as usize)?
        .checked_mul(channels as usize)
}

impl Grid {
    /// Builds a grid from its dimensions and cell-major interleaved payload.
    ///
    /// Each dimension must be at least 1, `width * height` must fit a `u32`,
    /// and `data.len()` must equal `width * height * channels`; every violation
    /// is [`Error::Validation`], as is a dimension product that overflows a
    /// `usize`.
    pub fn new(width: u32, height: u32, channels: u32, data: Vec<f32>) -> Result<Grid> {
        if width == 0 || height == 0 || channels == 0 {
            return Err(Error::Validation(format!(
                "grid dimensions must each be at least 1, got {width}x{height}x{channels}"
            )));
        }
        // The cell count travels as a `u32` from here on — the dispatch harness
        // sizes its grid by it and every kernel reads it out of its dimensions
        // buffer — so an extent whose product does not fit one is refused here.
        // Unchecked, it would wrap: 65536x65536 becomes zero cells, dispatching
        // nothing and committing a grid of NaN scalars.
        let cells = width as u64 * height as u64;
        if cells > u64::from(u32::MAX) {
            return Err(Error::Validation(format!(
                "grid extent {width}x{height} is {cells} cells; a grid holds at most {}",
                u32::MAX
            )));
        }
        let count = element_count(width, height, channels).ok_or_else(|| {
            Error::Validation(format!(
                "grid dimensions {width}x{height}x{channels} overflow the element count"
            ))
        })?;
        if data.len() != count {
            return Err(Error::Validation(format!(
                "grid data length {} does not match dimensions {width}x{height}x{channels} ({count} elements)",
                data.len()
            )));
        }
        Ok(Grid {
            width,
            height,
            channels,
            data,
        })
    }

    /// The grid extent along x.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// The grid extent along y.
    pub fn height(&self) -> u32 {
        self.height
    }

    /// The number of channels per cell.
    pub fn channels(&self) -> u32 {
        self.channels
    }

    /// The cells in the grid, `width * height`.
    ///
    /// Infallible: [`Grid::new`] refuses an extent whose product does not fit a
    /// `u32`, which is the width the dispatch harness and every kernel's
    /// dimensions buffer carry it at.
    pub fn cell_count(&self) -> u32 {
        self.width * self.height
    }

    /// The cell-major interleaved payload.
    pub fn data(&self) -> &[f32] {
        &self.data
    }

    /// The cell-major interleaved payload, mutably. The slice's length is
    /// fixed, so a caller writes cells in place without disturbing the
    /// dimensions the payload length is tied to.
    pub fn data_mut(&mut self) -> &mut [f32] {
        &mut self.data
    }

    /// The canonical byte form: the format tag, the three dimensions as
    /// little-endian `u32`, then the payload as a bare little-endian `f32`
    /// sequence whose length the dimensions fix.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.str(GRID_TAG)
            .u32(self.width)
            .u32(self.height)
            .u32(self.channels)
            .f32_slice(&self.data);
        enc.finish()
    }

    /// Decodes the canonical byte form written by [`Grid::to_bytes`].
    ///
    /// Rejects a wrong tag, truncated input, and trailing bytes as
    /// [`Error::Encoding`]. The dimensions are untrusted input here, so a
    /// product that overflows the element count is [`Error::Encoding`]; a zero
    /// dimension or a payload of the wrong length is caught by [`Grid::new`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Grid> {
        let mut dec = Dec::new(bytes);
        let tag = dec.str()?;
        if tag != GRID_TAG {
            return Err(Error::Encoding(format!(
                "grid tag mismatch: expected {GRID_TAG:?}, found {tag:?}"
            )));
        }
        let width = dec.u32()?;
        let height = dec.u32()?;
        let channels = dec.u32()?;
        let count = element_count(width, height, channels).ok_or_else(|| {
            Error::Encoding(format!(
                "grid dimensions {width}x{height}x{channels} overflow the element count"
            ))
        })?;
        let data = dec.f32_vec(count)?;
        dec.finish()?;
        Grid::new(width, height, channels, data)
    }

    /// The blake3 address of the grid's canonical bytes — where the store puts
    /// it as a content-addressed object.
    pub fn content_id(&self) -> Hash {
        hash_bytes(&self.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::to_hex;

    /// A 2x1x2 grid whose payload, cell-major interleaved, is
    /// cell (0,0) = [1.5, -2.0], cell (1,0) = [0.25, 1024.0].
    fn sample() -> Grid {
        Grid::new(2, 1, 2, vec![1.5, -2.0, 0.25, 1024.0]).expect("valid sample grid")
    }

    /// The canonical bytes for [`sample`], derived by hand from the format:
    /// tag (u64 length prefix + UTF-8), three u32 dimensions, then four
    /// little-endian f32. Independently reproduced with Python `struct`.
    const SAMPLE_BYTES_HEX: &str = "150000000000000073696d612e63656c6c756c61722e677269642e7631020000\
0001000000020000000000c03f000000c00000803e00008044";

    /// blake3 of `SAMPLE_BYTES_HEX`, computed independently with the Python
    /// `blake3` package: `blake3.blake3(bytes.fromhex(...)).hexdigest()`.
    const SAMPLE_CONTENT_ID_HEX: &str =
        "4bcece3cf89b7c946acac80ea79b849f921ef4c94c8e12ee3054679788317a22";

    #[test]
    fn new_rejects_a_zero_dimension() {
        for dims in [(0, 1, 1), (1, 0, 1), (1, 1, 0)] {
            let (w, h, c) = dims;
            assert!(matches!(
                Grid::new(w, h, c, Vec::new()),
                Err(Error::Validation(_))
            ));
        }
    }

    #[test]
    fn new_rejects_a_data_length_mismatch() {
        // 2x1x2 needs four elements; three is a mismatch.
        assert!(matches!(
            Grid::new(2, 1, 2, vec![1.0, 2.0, 3.0]),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn new_rejects_overflowing_dimensions() {
        // width * height * channels overflows usize on any target.
        assert!(matches!(
            Grid::new(u32::MAX, u32::MAX, u32::MAX, Vec::new()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn new_rejects_a_cell_count_past_a_u32() {
        // The dispatch harness carries the cell count as a `u32` — it is what a
        // kernel reads out of its dimensions buffer — so an extent whose
        // product does not fit one is refused here rather than wrapping. A
        // 65536x65536 grid is exactly 2^32 cells, which would wrap to zero and
        // dispatch nothing while committing a grid of NaN scalars.
        let error = Grid::new(65536, 65536, 1, Vec::new()).expect_err("2^32 cells");
        let Error::Validation(message) = error else {
            panic!("expected a validation error");
        };
        assert!(
            message.contains("4294967296 cells"),
            "names the cell count: {message}"
        );

        // One row short of the bound is the largest extent the harness carries,
        // so what refuses the grid above is the product and not either extent.
        // With no payload it falls through to the length check instead.
        assert!(matches!(
            Grid::new(65536, 65535, 1, Vec::new()),
            Err(Error::Validation(message)) if message.contains("data length")
        ));
    }

    #[test]
    fn a_grids_cell_count_is_its_extent() {
        // Infallible by construction: `new` refused every extent whose product
        // does not fit, so no caller multiplies the two itself.
        assert_eq!(sample().cell_count(), 2);
    }

    #[test]
    fn to_bytes_is_byte_stable() {
        assert_eq!(to_hex(&sample().to_bytes()), SAMPLE_BYTES_HEX);
    }

    #[test]
    fn round_trips_through_bytes() -> Result<()> {
        let grid = sample();
        let decoded = Grid::from_bytes(&grid.to_bytes())?;
        assert_eq!(decoded.width(), 2);
        assert_eq!(decoded.height(), 1);
        assert_eq!(decoded.channels(), 2);
        // Byte identity is the property the store and cross-check guarantee.
        assert_eq!(decoded.to_bytes(), grid.to_bytes());
        Ok(())
    }

    #[test]
    fn content_id_matches_independent_blake3() -> Result<()> {
        assert_eq!(
            sample().content_id(),
            Hash::from_hex(SAMPLE_CONTENT_ID_HEX)?
        );
        Ok(())
    }

    #[test]
    fn from_bytes_rejects_a_wrong_tag() {
        let mut enc = Enc::new();
        enc.str("sima.cellular.grid.v2")
            .u32(1)
            .u32(1)
            .u32(1)
            .f32_slice(&[0.0]);
        assert!(matches!(
            Grid::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn from_bytes_rejects_every_truncation() {
        let full = sample().to_bytes();
        // Every proper prefix is truncated input and must be rejected.
        for len in 0..full.len() {
            assert!(
                matches!(Grid::from_bytes(&full[..len]), Err(Error::Encoding(_))),
                "prefix of length {len} was accepted"
            );
        }
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let mut bytes = sample().to_bytes();
        bytes.push(0);
        assert!(matches!(Grid::from_bytes(&bytes), Err(Error::Encoding(_))));
    }

    #[test]
    fn from_bytes_rejects_a_zero_dimension() {
        // Tag, then a zero width, then no payload: count is zero, so decode
        // reaches Grid::new, which rejects the zero dimension.
        let mut enc = Enc::new();
        enc.str(GRID_TAG).u32(0).u32(1).u32(1);
        assert!(matches!(
            Grid::from_bytes(&enc.finish()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn from_bytes_rejects_overflowing_dimensions() {
        // A dimension triple whose product overflows the element count is
        // rejected as malformed input before any payload is read.
        let mut enc = Enc::new();
        enc.str(GRID_TAG).u32(u32::MAX).u32(u32::MAX).u32(u32::MAX);
        assert!(matches!(
            Grid::from_bytes(&enc.finish()),
            Err(Error::Encoding(_))
        ));
    }
}
