"""Length-prefixed framing over a byte stream.

A frame is a ``u32`` little-endian payload length followed by the payload.
Frames are transport encoding, never identity-bearing, so the length prefix is
plain little-endian rather than a canonical integer, and no frame is ever
hashed.
"""

from __future__ import annotations

import struct
from typing import BinaryIO

__all__ = ["MAX_PAYLOAD", "TransportError", "read_frame", "write_frame"]

#: Upper bound on a frame payload. A longer length is a transport error on both
#: sides — the guard against a corrupt prefix allocating unboundedly.
MAX_PAYLOAD = 256 * 1024 * 1024

_PREFIX = struct.Struct("<I")


class TransportError(OSError):
    """A framing violation: a torn frame, an oversize length, or a dead pipe."""


def write_frame(stream: BinaryIO, payload: bytes) -> None:
    """Writes one frame — the payload's ``u32`` little-endian length, then the
    payload — and flushes it, so the frame reaches the peer immediately.

    A payload above :data:`MAX_PAYLOAD` is refused before anything is written:
    the encoder honors the cap the decoder enforces.
    """
    if len(payload) > MAX_PAYLOAD:
        raise TransportError(
            f"frame payload of {len(payload)} bytes exceeds the {MAX_PAYLOAD} byte cap"
        )
    stream.write(_PREFIX.pack(len(payload)))
    stream.write(payload)
    stream.flush()


def read_frame(stream: BinaryIO) -> bytes | None:
    """Reads one frame's payload, or ``None`` at a clean end of stream.

    ``None`` is end of stream at a frame boundary — the peer closed the pipe,
    which is the protocol's shutdown signal. A stream ending inside a frame, a
    length above :data:`MAX_PAYLOAD`, and any read failure raise
    :class:`TransportError`.
    """
    prefix = _read_exactly(stream, _PREFIX.size)
    if prefix is None:
        return None
    if len(prefix) < _PREFIX.size:
        raise TransportError(f"frame length truncated after {len(prefix)} bytes")
    (length,) = _PREFIX.unpack(prefix)
    if length > MAX_PAYLOAD:
        raise TransportError(f"frame length {length} exceeds the {MAX_PAYLOAD} byte cap")
    payload = _read_exactly(stream, length) or b""
    if len(payload) < length:
        raise TransportError(
            f"frame payload truncated: {len(payload)} of {length} bytes arrived"
        )
    return payload


def _read_exactly(stream: BinaryIO, count: int) -> bytes | None:
    """Reads exactly ``count`` bytes, returning what arrived.

    ``None`` says nothing arrived at all — the clean end of stream, which the
    caller distinguishes from a torn read. A short read is returned as it is;
    the caller names what it was reading.
    """
    chunks = bytearray()
    while len(chunks) < count:
        chunk = stream.read(count - len(chunks))
        if not chunk:
            break
        chunks.extend(chunk)
    if not chunks:
        return None
    return bytes(chunks)
