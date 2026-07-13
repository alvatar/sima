//! Canonical byte encoding for identity-bearing data.
//!
//! Every structure that is ever hashed serializes through [`Enc`] and reads
//! back through [`Dec`]; this module exists so `to_le_bytes` never scatters
//! across the codebase. The format:
//!
//! - every integer little-endian at its natural width
//! - `i64` two's-complement little-endian
//! - bytes and str framed by a `u64` little-endian length prefix (str is its
//!   UTF-8 bytes)
//! - `Hash` as its 32 raw digest bytes
//! - `Option<Hash>` as a present-flag byte of value zero or one followed by the
//!   digest when present
//! - `Option<u64>` as the same present-flag byte followed by the little-endian
//!   value
//! - `f32` as its IEEE-754 bits in a little-endian `u32`
//! - an `f32` slice as those elements written back to back with no length prefix
//!   (the count is fixed by surrounding context)

use crate::error::{Error, Result};
use crate::hash::Hash;

/// Builder writing the canonical encoding into a `Vec<u8>`.
pub struct Enc {
    buf: Vec<u8>,
}

impl Enc {
    /// Starts an empty encoding.
    pub fn new() -> Self {
        Enc { buf: Vec::new() }
    }

    /// Consumes the builder, returning the encoded bytes.
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }

    /// Writes a `u8`.
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    /// Writes a `u16`, little-endian.
    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// Writes a `u32`, little-endian.
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// Writes a `u64`, little-endian.
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// Writes an `i64`, two's-complement little-endian.
    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// Writes three `u32`s in order, each little-endian.
    pub fn u32x3(&mut self, v: [u32; 3]) -> &mut Self {
        for part in v {
            self.u32(part);
        }
        self
    }

    /// Writes an `f32` as its IEEE-754 bits in a little-endian `u32`.
    pub fn f32(&mut self, v: f32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
        self
    }

    /// Writes each `f32` as its little-endian bits, contiguously, with no
    /// length prefix — the element count is fixed by surrounding context.
    pub fn f32_slice(&mut self, v: &[f32]) -> &mut Self {
        self.buf.reserve(v.len() * 4);
        for &value in v {
            self.buf.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        self
    }

    /// Writes a `u64` length prefix followed by the raw bytes.
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.u64(v.len() as u64);
        self.buf.extend_from_slice(v);
        self
    }

    /// Writes the string's UTF-8 bytes with the same framing as [`Enc::bytes`].
    pub fn str(&mut self, v: &str) -> &mut Self {
        self.bytes(v.as_bytes())
    }

    /// Writes the 32 digest bytes.
    pub fn hash(&mut self, v: &Hash) -> &mut Self {
        self.buf.extend_from_slice(v.as_bytes());
        self
    }

    /// Writes a present-flag byte, then the digest when present.
    pub fn opt_hash(&mut self, v: Option<&Hash>) -> &mut Self {
        match v {
            None => self.u8(0),
            Some(h) => self.u8(1).hash(h),
        }
    }

    /// Writes a present-flag byte, then the `u64` little-endian when present.
    pub fn opt_u64(&mut self, v: Option<u64>) -> &mut Self {
        match v {
            None => self.u8(0),
            Some(n) => self.u8(1).u64(n),
        }
    }
}

impl Default for Enc {
    fn default() -> Self {
        Enc::new()
    }
}

/// Cursor reading the canonical encoding with checked bounds; every reader
/// returns [`Error::Encoding`] on truncated or malformed input, never panics.
pub struct Dec<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Dec<'a> {
    /// Starts a cursor at the beginning of `input`.
    pub fn new(input: &'a [u8]) -> Self {
        Dec { input, pos: 0 }
    }

    /// Advances past `n` bytes, borrowing them; errors when fewer remain.
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let remaining = self.input.len() - self.pos;
        if n > remaining {
            return Err(Error::Encoding(format!(
                "truncated input: need {n} bytes at offset {}, {remaining} remaining",
                self.pos
            )));
        }
        let slice = &self.input[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    /// Reads exactly `N` bytes into an array.
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    /// Reads a `u8`.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    /// Reads a little-endian `u16`.
    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    /// Reads a little-endian `u32`.
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    /// Reads a little-endian `u64`.
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    /// Reads a two's-complement little-endian `i64`.
    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    /// Reads three little-endian `u32`s.
    pub fn u32x3(&mut self) -> Result<[u32; 3]> {
        Ok([self.u32()?, self.u32()?, self.u32()?])
    }

    /// Reads an `f32` from its little-endian IEEE-754 bits.
    pub fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_bits(u32::from_le_bytes(self.array()?)))
    }

    /// Reads `count` `f32` elements written back to back with no length
    /// prefix. The byte span is validated against the remaining input before
    /// any allocation, so an absurd `count` errors without allocating.
    pub fn f32_vec(&mut self, count: usize) -> Result<Vec<f32>> {
        let byte_len = count
            .checked_mul(4)
            .ok_or_else(|| Error::Encoding(format!("f32 count {count} overflows a byte length")))?;
        let raw = self.take(byte_len)?;
        let mut out = Vec::with_capacity(count);
        for chunk in raw.chunks_exact(4) {
            out.push(f32::from_bits(u32::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3],
            ])));
        }
        Ok(out)
    }

    /// Reads a `u64` length prefix, then borrows that many bytes. The length
    /// is validated against the remaining input before any use, so an absurd
    /// prefix errors without allocating.
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u64()?;
        let len = usize::try_from(len)
            .map_err(|_| Error::Encoding(format!("length prefix {len} exceeds address space")))?;
        self.take(len)
    }

    /// Reads [`Dec::bytes`] framing and validates the payload as UTF-8.
    pub fn str(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.bytes()?)
            .map_err(|e| Error::Encoding(format!("string payload is not UTF-8: {e}")))
    }

    /// Reads 32 digest bytes.
    pub fn hash(&mut self) -> Result<Hash> {
        Ok(Hash::from_bytes(self.array()?))
    }

    /// Reads a present-flag byte (0 or 1), then the digest when present.
    pub fn opt_hash(&mut self) -> Result<Option<Hash>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.hash()?)),
            flag => Err(Error::Encoding(format!(
                "invalid Option<Hash> flag byte {flag}, expected 0 or 1"
            ))),
        }
    }

    /// Reads a present-flag byte (0 or 1), then the little-endian `u64` when
    /// present.
    pub fn opt_u64(&mut self) -> Result<Option<u64>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            flag => Err(Error::Encoding(format!(
                "invalid Option<u64> flag byte {flag}, expected 0 or 1"
            ))),
        }
    }

    /// Ends decoding, rejecting trailing bytes.
    pub fn finish(self) -> Result<()> {
        let trailing = self.input.len() - self.pos;
        if trailing != 0 {
            return Err(Error::Encoding(format!(
                "{trailing} trailing bytes after decode at offset {}",
                self.pos
            )));
        }
        Ok(())
    }
}

/// A type with a canonical byte encoding: its fields written through [`Enc`] in
/// declaration order and read back through [`Dec`]. Implemented by hand per type,
/// one [`encode`](Codec::encode)/[`decode`](Codec::decode) pair reproducing the
/// type's frozen layout; [`to_bytes`](Codec::to_bytes) and
/// [`from_bytes`](Codec::from_bytes) are the standalone forms, provided here in
/// terms of that pair.
pub trait Codec: Sized {
    /// Appends the canonical form to `enc`.
    fn encode(&self, enc: &mut Enc);

    /// Reads a canonical form written by [`encode`](Codec::encode). A type that
    /// routes decode through a validating constructor surfaces an invalid value
    /// as [`Error::Validation`]; a truncated buffer is [`Error::Encoding`].
    fn decode(dec: &mut Dec<'_>) -> Result<Self>;

    /// The standalone canonical bytes.
    fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        self.encode(&mut enc);
        enc.finish()
    }

    /// Parses standalone canonical bytes, rejecting trailing input.
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut dec = Dec::new(bytes);
        let value = Self::decode(&mut dec)?;
        dec.finish()?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::to_hex;

    fn sample_hash(fill: &str) -> Result<Hash> {
        Hash::from_hex(&fill.repeat(Hash::LEN))
    }

    /// Composite covering every helper, reused by the round-trip and
    /// byte-stability tests.
    fn composite() -> Result<Vec<u8>> {
        let h1 = sample_hash("11")?;
        let h2 = sample_hash("22")?;
        let mut enc = Enc::new();
        enc.u8(0x01)
            .u16(0x0203)
            .u32(0x0405_0607)
            .u64(0x0809_0A0B_0C0D_0E0F)
            .i64(-2)
            .u32x3([1, 2, 3])
            .bytes(&[0xAA, 0xBB])
            .str("hi")
            .hash(&h1)
            .opt_hash(None)
            .opt_hash(Some(&h2))
            .opt_u64(None)
            .opt_u64(Some(0x1011_1213_1415_1617));
        Ok(enc.finish())
    }

    #[test]
    fn round_trip_every_helper() -> Result<()> {
        let buf = composite()?;
        let mut dec = Dec::new(&buf);
        assert_eq!(dec.u8()?, 0x01);
        assert_eq!(dec.u16()?, 0x0203);
        assert_eq!(dec.u32()?, 0x0405_0607);
        assert_eq!(dec.u64()?, 0x0809_0A0B_0C0D_0E0F);
        assert_eq!(dec.i64()?, -2);
        assert_eq!(dec.u32x3()?, [1, 2, 3]);
        assert_eq!(dec.bytes()?, &[0xAA, 0xBB]);
        assert_eq!(dec.str()?, "hi");
        assert_eq!(dec.hash()?, sample_hash("11")?);
        assert_eq!(dec.opt_hash()?, None);
        assert_eq!(dec.opt_hash()?, Some(sample_hash("22")?));
        assert_eq!(dec.opt_u64()?, None);
        assert_eq!(dec.opt_u64()?, Some(0x1011_1213_1415_1617));
        dec.finish()
    }

    /// The encoding is the identity-bearing byte layout; this pins it.
    /// Expected hex is derived by hand from the format documented in the
    /// module docs, field by field, in encoding order.
    #[test]
    fn composite_encoding_is_byte_stable() -> Result<()> {
        let expected = [
            "01",                                     // u8
            "0302",                                   // u16 LE
            "07060504",                               // u32 LE
            "0f0e0d0c0b0a0908",                       // u64 LE
            "feffffffffffffff",                       // i64 -2, two's complement LE
            "010000000200000003000000",               // [u32; 3] LE each
            "0200000000000000aabb",                   // bytes: u64 len + payload
            "02000000000000006869",                   // str: u64 len + UTF-8 "hi"
            &"11".repeat(Hash::LEN),                  // hash digest
            "00",                                     // Option<Hash> absent
            &format!("01{}", "22".repeat(Hash::LEN)), // Option<Hash> present
            "00",                                     // Option<u64> absent
            "011716151413121110",                     // Option<u64> present: flag + u64 LE
        ]
        .concat();
        assert_eq!(to_hex(&composite()?), expected);
        Ok(())
    }

    #[test]
    fn f32_round_trips_representative_values() -> Result<()> {
        let values = [
            1.5f32,
            -0.0,
            0.0,
            f32::MIN,
            f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ];
        for v in values {
            let mut enc = Enc::new();
            enc.f32(v);
            let buf = enc.finish();
            let mut dec = Dec::new(&buf);
            // Compare by bits so -0.0 and the infinities are distinguished.
            assert_eq!(dec.f32()?.to_bits(), v.to_bits());
            dec.finish()?;
        }
        Ok(())
    }

    /// `f32` is written as its IEEE-754 bits in a little-endian `u32`. `1.5f32`
    /// has bit pattern `0x3FC00000`, little-endian bytes `00 00 c0 3f`.
    #[test]
    fn f32_encoding_is_byte_stable() {
        let mut enc = Enc::new();
        enc.f32(1.5);
        assert_eq!(to_hex(&enc.finish()), "0000c03f");
    }

    #[test]
    fn f32_slice_round_trips_without_a_length_prefix() -> Result<()> {
        let values = [1.5f32, -2.0, 0.25, 1024.0];
        let mut enc = Enc::new();
        enc.f32_slice(&values);
        let buf = enc.finish();
        // No length prefix: four contiguous 4-byte elements, 16 bytes.
        assert_eq!(buf.len(), values.len() * 4);
        let mut dec = Dec::new(&buf);
        let read = dec.f32_vec(values.len())?;
        dec.finish()?;
        let read_bits: Vec<u32> = read.iter().map(|v| v.to_bits()).collect();
        let want_bits: Vec<u32> = values.iter().map(|v| v.to_bits()).collect();
        assert_eq!(read_bits, want_bits);
        Ok(())
    }

    #[test]
    fn f32_truncation_errors_never_panics() {
        assert!(matches!(Dec::new(&[0u8; 3]).f32(), Err(Error::Encoding(_))));
        // A vector read past the remaining input errors before allocating.
        assert!(matches!(
            Dec::new(&[0u8; 7]).f32_vec(2),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn truncated_input_errors_never_panics() {
        // Every fixed-width reader against input one byte short.
        assert!(matches!(Dec::new(&[]).u8(), Err(Error::Encoding(_))));
        assert!(matches!(Dec::new(&[0]).u16(), Err(Error::Encoding(_))));
        assert!(matches!(Dec::new(&[0; 3]).u32(), Err(Error::Encoding(_))));
        assert!(matches!(Dec::new(&[0; 7]).u64(), Err(Error::Encoding(_))));
        assert!(matches!(Dec::new(&[0; 7]).i64(), Err(Error::Encoding(_))));
        assert!(matches!(
            Dec::new(&[0; 11]).u32x3(),
            Err(Error::Encoding(_))
        ));
        assert!(matches!(Dec::new(&[0; 31]).hash(), Err(Error::Encoding(_))));
        // Present-flag says a digest follows, but input ends.
        assert!(matches!(Dec::new(&[1]).opt_hash(), Err(Error::Encoding(_))));
        // Present-flag says a u64 follows, but input ends.
        assert!(matches!(
            Dec::new(&[1; 8]).opt_u64(),
            Err(Error::Encoding(_))
        ));
        // Length prefix itself truncated.
        assert!(matches!(
            Dec::new(&[2, 0, 0]).bytes(),
            Err(Error::Encoding(_))
        ));
    }

    #[test]
    fn length_prefix_beyond_remaining_is_rejected() {
        // Claims 5 payload bytes, provides 2.
        let mut buf = 5u64.to_le_bytes().to_vec();
        buf.extend_from_slice(&[0xAA, 0xBB]);
        assert!(matches!(Dec::new(&buf).bytes(), Err(Error::Encoding(_))));
        // Absurd length must error cleanly, not attempt allocation.
        let huge = u64::MAX.to_le_bytes();
        assert!(matches!(Dec::new(&huge).bytes(), Err(Error::Encoding(_))));
    }

    #[test]
    fn str_rejects_invalid_utf8() {
        let mut enc = Enc::new();
        enc.bytes(&[0xFF, 0xFE]);
        let buf = enc.finish();
        assert!(matches!(Dec::new(&buf).str(), Err(Error::Encoding(_))));
    }

    #[test]
    fn opt_hash_rejects_invalid_flag() {
        // Flag byte 2 followed by a full digest's worth of bytes.
        let buf = [2u8; 1 + Hash::LEN];
        assert!(matches!(Dec::new(&buf).opt_hash(), Err(Error::Encoding(_))));
    }

    #[test]
    fn opt_u64_rejects_invalid_flag() {
        // Flag byte 2 followed by a full u64's worth of bytes.
        let buf = [2u8; 9];
        assert!(matches!(Dec::new(&buf).opt_u64(), Err(Error::Encoding(_))));
    }

    #[test]
    fn finish_rejects_trailing_bytes() -> Result<()> {
        let mut enc = Enc::new();
        enc.u8(7);
        let mut buf = enc.finish();
        buf.push(0);
        let mut dec = Dec::new(&buf);
        assert_eq!(dec.u8()?, 7);
        assert!(matches!(dec.finish(), Err(Error::Encoding(_))));
        Ok(())
    }
}
