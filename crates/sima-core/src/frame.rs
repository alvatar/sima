//! Length-prefixed framing over a byte stream: the transport discipline for
//! the canonical codec.
//!
//! A frame is a `u32` little-endian payload length followed by the payload.
//! Frames are transport encoding, never identity-bearing — no frame is ever
//! hashed — so the length prefix is plain little-endian rather than a canonical
//! `Enc` integer. The payload is opaque here; a protocol built on top fills it
//! with `Enc`/`Dec` message bytes.
//!
//! Two consumers share this framing: the worker transport (`sima-transport`)
//! and the store-to-store sync protocol (`sima-store`). Both sit above
//! `sima-core`, so the framing lives here where neither depends on the other.

use std::io::{Read, Write};

use crate::{Error, Result};

/// Upper bound on a frame payload. A length above it is a transport error —
/// the guard against a corrupt length prefix allocating unboundedly.
pub const MAX_PAYLOAD: u32 = 256 * 1024 * 1024;

/// Writes one frame: the payload's `u32` little-endian length, the payload,
/// and a flush, so the frame reaches the peer immediately. A payload above
/// [`MAX_PAYLOAD`] is refused before anything is written — the encoder honors
/// the same cap the decoder enforces.
pub fn write_frame(writer: &mut dyn Write, payload: &[u8]) -> Result<()> {
    let len = u32::try_from(payload.len())
        .ok()
        .filter(|len| *len <= MAX_PAYLOAD)
        .ok_or_else(|| {
            Error::Transport(format!(
                "frame payload of {} bytes exceeds the {MAX_PAYLOAD} byte cap",
                payload.len()
            ))
        })?;
    let write = |result: std::io::Result<()>| {
        result.map_err(|e| Error::Transport(format!("frame write failed: {e}")))
    };
    write(writer.write_all(&len.to_le_bytes()))?;
    write(writer.write_all(payload))?;
    write(writer.flush())
}

/// Reads one frame's payload. `Ok(None)` is end-of-stream at a frame
/// boundary — the peer closed the pipe cleanly; a stream ending inside a
/// frame, a length above [`MAX_PAYLOAD`], and any read failure are
/// [`Error::Transport`].
pub fn read_frame(reader: &mut dyn Read) -> Result<Option<Vec<u8>>> {
    // The length prefix is read byte-wise so end-of-stream before the first
    // byte — the clean shutdown — is distinguishable from a torn prefix.
    let mut prefix = [0u8; 4];
    let mut filled = 0;
    while filled < prefix.len() {
        match reader.read(&mut prefix[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => {
                return Err(Error::Transport(format!(
                    "frame length truncated after {filled} bytes"
                )));
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => {
                return Err(Error::Transport(format!("frame read failed: {e}")));
            }
        }
    }
    let len = u32::from_le_bytes(prefix);
    if len > MAX_PAYLOAD {
        return Err(Error::Transport(format!(
            "frame length {len} exceeds the {MAX_PAYLOAD} byte cap"
        )));
    }
    let mut payload = vec![0u8; len as usize];
    reader
        .read_exact(&mut payload)
        .map_err(|e| Error::Transport(format!("frame payload read failed: {e}")))?;
    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_payload_round_trips_through_a_frame() -> Result<()> {
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &[1, 2, 3, 4])?;
        let mut reader = pipe.as_slice();
        assert_eq!(read_frame(&mut reader)?.expect("a frame"), vec![1, 2, 3, 4]);
        assert_eq!(read_frame(&mut reader)?, None, "the stream ends cleanly");
        Ok(())
    }

    #[test]
    fn eof_at_a_frame_boundary_is_a_clean_end() -> Result<()> {
        assert_eq!(read_frame(&mut [].as_slice())?, None);
        Ok(())
    }

    #[test]
    fn a_truncated_length_prefix_is_a_transport_error() {
        // Two of the four length bytes: the stream died inside a frame.
        let mut reader = [0x10u8, 0x00].as_slice();
        assert!(matches!(read_frame(&mut reader), Err(Error::Transport(_))));
    }

    #[test]
    fn a_truncated_payload_is_a_transport_error() -> Result<()> {
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &[1, 2, 3, 4])?;
        pipe.truncate(pipe.len() - 1);
        let mut reader = pipe.as_slice();
        assert!(matches!(read_frame(&mut reader), Err(Error::Transport(_))));
        Ok(())
    }

    #[test]
    fn an_oversize_length_prefix_is_rejected_before_allocating() {
        // A corrupt prefix claiming just past the cap: the reader must refuse
        // it from the four length bytes alone.
        let over = (MAX_PAYLOAD + 1).to_le_bytes();
        let mut reader = over.as_slice();
        assert!(matches!(read_frame(&mut reader), Err(Error::Transport(_))));
        // An absurd prefix likewise.
        let absurd = u32::MAX.to_le_bytes();
        let mut reader = absurd.as_slice();
        assert!(matches!(read_frame(&mut reader), Err(Error::Transport(_))));
    }

    #[test]
    fn a_payload_at_the_cap_boundary_frames_and_reads_back() -> Result<()> {
        // The cap is inclusive: a payload of exactly MAX_PAYLOAD bytes passes
        // both endpoints; one byte more is refused by the writer.
        let payload = vec![0u8; MAX_PAYLOAD as usize];
        let mut pipe = Vec::new();
        write_frame(&mut pipe, &payload)?;
        let mut reader = pipe.as_slice();
        assert_eq!(
            read_frame(&mut reader)?.expect("a frame").len(),
            payload.len()
        );
        let oversize = vec![0u8; MAX_PAYLOAD as usize + 1];
        let mut sink = Vec::new();
        assert!(matches!(
            write_frame(&mut sink, &oversize),
            Err(Error::Transport(_))
        ));
        assert!(sink.is_empty(), "a refused frame writes nothing");
        Ok(())
    }
}
