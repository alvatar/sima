"""The canonical byte encoding: :class:`Enc` writes it, :class:`Dec` reads it.

The same encoding carries identity-bearing values inside a frame and inside a
stored object, so a value encoded once means one thing everywhere:

- every integer little-endian at its natural width, ``i64`` two's-complement
- ``f32`` and ``f64`` as their IEEE-754 bits in a little-endian ``u32``/``u64``
- ``bytes`` and ``str`` framed by a ``u64`` little-endian length prefix, a
  string over its UTF-8 bytes
- a digest as its 32 raw bytes
- an optional value as a present-flag byte of 0 or 1, then the value when
  present
"""

from __future__ import annotations

import struct

__all__ = ["Dec", "Enc", "EncodingError", "HASH_LEN"]

#: Length of a digest on the wire.
HASH_LEN = 32

_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")
_I64 = struct.Struct("<q")
_F32 = struct.Struct("<f")
_F64 = struct.Struct("<d")


class EncodingError(ValueError):
    """A payload that does not decode: truncated, malformed, or overlong."""


class Enc:
    """Builder appending the canonical encoding into a byte buffer.

    Every writer returns the builder, so a message is one chained expression;
    :meth:`finish` (or ``bytes(enc)``) takes the bytes out.
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def finish(self) -> bytes:
        """The encoded bytes."""
        return bytes(self._buf)

    def __bytes__(self) -> bytes:
        return self.finish()

    def u8(self, value: int) -> Enc:
        """Writes a ``u8``."""
        self._buf.append(_checked(value, 0, 0xFF, "u8"))
        return self

    def u16(self, value: int) -> Enc:
        """Writes a ``u16``, little-endian."""
        self._buf += _U16.pack(_checked(value, 0, 0xFFFF, "u16"))
        return self

    def u32(self, value: int) -> Enc:
        """Writes a ``u32``, little-endian."""
        self._buf += _U32.pack(_checked(value, 0, 0xFFFF_FFFF, "u32"))
        return self

    def u64(self, value: int) -> Enc:
        """Writes a ``u64``, little-endian."""
        self._buf += _U64.pack(_checked(value, 0, 0xFFFF_FFFF_FFFF_FFFF, "u64"))
        return self

    def i64(self, value: int) -> Enc:
        """Writes an ``i64``, two's-complement little-endian."""
        self._buf += _I64.pack(_checked(value, -(2**63), 2**63 - 1, "i64"))
        return self

    def f32(self, value: float) -> Enc:
        """Writes an ``f32`` as its IEEE-754 bits in a little-endian ``u32``."""
        self._buf += _F32.pack(value)
        return self

    def f64(self, value: float) -> Enc:
        """Writes an ``f64`` as its IEEE-754 bits in a little-endian ``u64``."""
        self._buf += _F64.pack(value)
        return self

    def bytes(self, value: bytes) -> Enc:
        """Writes a ``u64`` length prefix followed by the raw bytes."""
        self.u64(len(value))
        self._buf += value
        return self

    def str(self, value: str) -> Enc:
        """Writes the string's UTF-8 bytes with the framing of :meth:`bytes`."""
        return self.bytes(value.encode("utf-8"))

    def hash(self, value: bytes) -> Enc:
        """Writes the 32 digest bytes."""
        if len(value) != HASH_LEN:
            raise EncodingError(f"a digest is {HASH_LEN} bytes, got {len(value)}")
        self._buf += value
        return self

    def opt_bytes(self, value: bytes | None) -> Enc:
        """Writes a present-flag byte, then the framed bytes when present."""
        if value is None:
            return self.u8(0)
        return self.u8(1).bytes(value)

    def opt_hash(self, value: bytes | None) -> Enc:
        """Writes a present-flag byte, then the digest when present."""
        if value is None:
            return self.u8(0)
        return self.u8(1).hash(value)

    def opt_u64(self, value: int | None) -> Enc:
        """Writes a present-flag byte, then the ``u64`` when present."""
        if value is None:
            return self.u8(0)
        return self.u8(1).u64(value)


class Dec:
    """Cursor reading the canonical encoding with checked bounds.

    Every reader raises :class:`EncodingError` on truncated or malformed input;
    :meth:`finish` rejects trailing bytes, so a payload must be consumed
    exactly.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    def _take(self, count: int) -> bytes:
        """Advances past ``count`` bytes, returning them."""
        remaining = len(self._data) - self._pos
        if count > remaining:
            raise EncodingError(
                f"truncated input: need {count} bytes at offset {self._pos}, "
                f"{remaining} remaining"
            )
        taken = self._data[self._pos : self._pos + count]
        self._pos += count
        return taken

    def u8(self) -> int:
        """Reads a ``u8``."""
        return self._take(1)[0]

    def u16(self) -> int:
        """Reads a little-endian ``u16``."""
        return _U16.unpack(self._take(_U16.size))[0]

    def u32(self) -> int:
        """Reads a little-endian ``u32``."""
        return _U32.unpack(self._take(_U32.size))[0]

    def u64(self) -> int:
        """Reads a little-endian ``u64``."""
        return _U64.unpack(self._take(_U64.size))[0]

    def i64(self) -> int:
        """Reads a two's-complement little-endian ``i64``."""
        return _I64.unpack(self._take(_I64.size))[0]

    def f32(self) -> float:
        """Reads an ``f32`` from its little-endian IEEE-754 bits."""
        return _F32.unpack(self._take(_F32.size))[0]

    def f64(self) -> float:
        """Reads an ``f64`` from its little-endian IEEE-754 bits."""
        return _F64.unpack(self._take(_F64.size))[0]

    def bytes(self) -> bytes:
        """Reads a ``u64`` length prefix, then that many bytes."""
        return self._take(self.u64())

    def str(self) -> str:
        """Reads :meth:`bytes` framing and decodes it as UTF-8."""
        raw = self.bytes()
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise EncodingError(f"string payload is not UTF-8: {e}") from e

    def hash(self) -> bytes:
        """Reads 32 digest bytes."""
        return self._take(HASH_LEN)

    def opt_bytes(self) -> bytes | None:
        """Reads a present-flag byte, then the framed bytes when present."""
        return self.bytes() if self._flag("optional-bytes") else None

    def opt_hash(self) -> bytes | None:
        """Reads a present-flag byte, then the digest when present."""
        return self.hash() if self._flag("optional-hash") else None

    def opt_u64(self) -> int | None:
        """Reads a present-flag byte, then the ``u64`` when present."""
        return self.u64() if self._flag("optional-u64") else None

    def flag(self) -> bool:
        """Reads a boolean flag byte, rejecting values other than 0 and 1."""
        return self._flag("flag")

    def finish(self) -> None:
        """Ends decoding, rejecting trailing bytes."""
        trailing = len(self._data) - self._pos
        if trailing:
            raise EncodingError(f"{trailing} trailing bytes after decode at offset {self._pos}")

    def _flag(self, what: str) -> bool:
        """Reads a present or boolean flag byte, which is 0 or 1 and nothing else."""
        byte = self.u8()
        if byte > 1:
            raise EncodingError(f"invalid {what} flag byte {byte}, expected 0 or 1")
        return byte == 1


def _checked(value: int, low: int, high: int, width: str) -> int:
    """The integer, refused when it does not fit the width it is written at."""
    if not low <= value <= high:
        raise EncodingError(f"{value} does not fit a {width}")
    return value
