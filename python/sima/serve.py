"""The two conversations a program answers, and :func:`serve`, which runs them.

One process answers both roles a run needs, and the argument vector says which:
bare, it is a **worker** executing tasks; under ``--serve-domain <format>``, it
is the **domain service** answering what that format binds. A whole program is
its :class:`~sima.model.Domain`, its :class:`~sima.model.Generator` list, and
this call.

Both roles open with a handshake carrying :data:`PROTOCOL_VERSION` and end when
the parent closes the pipe.
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from typing import BinaryIO, Sequence

from .encode import Dec, Enc
from .frame import TransportError, read_frame, write_frame
from .model import (
    Checkpoint,
    Completed,
    DeviceBinding,
    DeviceInfo,
    DeviceType,
    Domain,
    ExecutionContext,
    Executor,
    Failed,
    Generator,
    NoCheckpoint,
    Outcome,
    Rejected,
    Spec,
    TaskInput,
)

__all__ = ["PROTOCOL_VERSION", "SERVE_DOMAIN", "serve"]

#: The wire protocol version. The handshake refuses a mismatch on both sides.
PROTOCOL_VERSION = 5

#: The flag that asks a program for the domain-service role, followed by the
#: format id it is asked about.
SERVE_DOMAIN = "--serve-domain"

# Parent to domain-service message tags.
_ASK_HELLO = 0
_ASK_DESCRIBE = 1
_ASK_ENUMERATE_DEVICES = 2
_ASK_TRANSLATE_CONFIG = 3
_ASK_TRANSLATE_GENERATOR_CONFIG = 4
_ASK_GENERATE = 5
_ASK_GOODBYE = 6

# Domain-service to parent message tags.
_ANSWER_READY = 0
_ANSWER_DESCRIBED = 1
_ANSWER_ENUMERATED_DEVICES = 2
_ANSWER_TRANSLATED_CONFIG = 3
_ANSWER_GENERATED = 4
_ANSWER_FAILED = 5

# Parent to worker message tags.
_TO_HELLO = 0
_TO_ASSIGN = 1

# Worker to parent message tags.
_FROM_READY = 0
_FROM_SAVE = 1
_FROM_DONE = 2
_FROM_PANICKED = 3
_FROM_FAULT = 4
_FROM_EVENT = 5

# Outcome tags inside a `Done` payload.
_OUTCOME_COMPLETED = 0
_OUTCOME_FAILED = 1
_OUTCOME_REJECTED = 2

#: Wall-clock cadence value that disables the axis.
_CADENCE_DISABLED_MS = 2**64 - 1


def serve(
    domain: Domain,
    generators: Sequence[Generator] = (),
    argv: Sequence[str] | None = None,
    reader: BinaryIO | None = None,
    writer: BinaryIO | None = None,
) -> None:
    """Hosts ``domain`` over whichever role the arguments ask for.

    Returns when the parent says goodbye or closes the pipe. Raises on a
    handshake refusal, a frame violation, or a broken pipe; a program maps that
    to a nonzero exit.

    The streams and the argument vector default to this process's, and are
    parameters so a caller can drive the loops over any pipe.
    """
    arguments = list(sys.argv if argv is None else argv)
    stream_in = reader if reader is not None else sys.stdin.buffer
    stream_out = writer if writer is not None else sys.stdout.buffer
    served_format = _role(arguments)
    if served_format is None:
        _serve_worker(domain, stream_in, stream_out)
    else:
        _served(domain, served_format)
        _serve_domain_service(domain, list(generators), stream_in, stream_out)


def _role(argv: Sequence[str]) -> str | None:
    """The format the domain-service role was asked for, or ``None`` for the
    worker role.

    The vector is scanned for the flag, so arguments the role vocabulary does
    not name belong to whoever wrapped the program.
    """
    for position, argument in enumerate(argv):
        if argument != SERVE_DOMAIN:
            continue
        if position + 1 >= len(argv):
            raise ValueError(f"{SERVE_DOMAIN} takes the format id the domain service is asked about")
        return argv[position + 1]
    return None


def _served(domain: Domain, format: str) -> None:
    """Confirms ``format`` is the one this program serves. One binary serves one
    format, so a question about another is refused rather than answered for the
    one it does serve."""
    if domain.format() != format:
        raise ValueError(f"unknown format id {format!r}; this program serves {domain.format()!r}")


# The domain service.


def _serve_domain_service(
    domain: Domain, generators: list[Generator], reader: BinaryIO, writer: BinaryIO
) -> None:
    """Answers questions about the format the domain binds, for the life of the
    session: handshake, then one answer per question until the farewell or the
    end of the pipe."""
    payload = read_frame(reader)
    if payload is None:
        raise TransportError("the pipe closed before the handshake")
    dec = Dec(payload)
    if dec.u8() != _ASK_HELLO:
        raise TransportError("expected the Hello handshake as the first frame")
    _check_version(dec.u32(), "domain service")
    write_frame(writer, Enc().u8(_ANSWER_READY).u32(PROTOCOL_VERSION).finish())

    while True:
        payload = read_frame(reader)
        if payload is None:
            return
        dec = Dec(payload)
        tag = dec.u8()
        if tag == _ASK_GOODBYE:
            return
        if tag == _ASK_HELLO:
            raise TransportError("unexpected second Hello after the handshake")
        # What the program cannot answer crosses as its own rendering, and the
        # session survives it: the next question is still answered.
        try:
            answer = _answer(domain, generators, tag, dec)
        except Exception as e:
            answer = Enc().u8(_ANSWER_FAILED).str(str(e)).finish()
        write_frame(writer, answer)


def _answer(domain: Domain, generators: list[Generator], tag: int, dec: Dec) -> bytes:
    """The answer frame for one question, decoded from the rest of its payload."""
    enc = Enc()
    if tag == _ASK_DESCRIBE:
        _served(domain, dec.str())
        dec.finish()
        enc.u8(_ANSWER_DESCRIBED)
        domain.environment().encode(enc)
    elif tag == _ASK_ENUMERATE_DEVICES:
        _served(domain, dec.str())
        dec.finish()
        devices = domain.enumerate_devices()
        enc.u8(_ANSWER_ENUMERATED_DEVICES).u64(len(devices))
        for device in devices:
            _encode_device(enc, device)
    elif tag == _ASK_TRANSLATE_CONFIG:
        _served(domain, dec.str())
        toml, segmented = dec.str(), dec.flag()
        dec.finish()
        enc.u8(_ANSWER_TRANSLATED_CONFIG).bytes(domain.translate_config(toml, segmented))
    elif tag == _ASK_TRANSLATE_GENERATOR_CONFIG:
        generator = _generator(generators, dec.str())
        toml = dec.str()
        dec.finish()
        enc.u8(_ANSWER_TRANSLATED_CONFIG).bytes(generator.translate_config(toml))
    elif tag == _ASK_GENERATE:
        generator = _generator(generators, dec.str())
        _served(domain, dec.str())
        root_seed, params = dec.u64(), dec.bytes()
        dec.finish()
        specs = generator.generate(root_seed, params)
        enc.u8(_ANSWER_GENERATED).u64(len(specs))
        for spec in specs:
            _encode_spec(enc, spec)
    else:
        raise TransportError(f"unknown parent-to-domain message tag {tag}")
    return enc.finish()


def _encode_device(enc: Enc, device: DeviceInfo) -> None:
    """Appends one enumerated device: class, name, type tag, member."""
    enc.str(device.clazz).str(device.name).u8(DeviceType(device.device_type)).u32(device.member)


def _encode_spec(enc: Enc, spec: Spec) -> None:
    """Appends one produced spec in its canonical form."""
    if not isinstance(spec, Spec):
        raise TypeError(f"a generator produces Spec values, got {type(spec).__name__}")
    spec.encode(enc)


def _generator(generators: list[Generator], id: str) -> Generator:
    """The generator registered under ``id``."""
    for generator in generators:
        if generator.id() == id:
            return generator
    raise ValueError(f"unknown generator id {id!r}")


# The worker.


def _serve_worker(domain: Domain, reader: BinaryIO, writer: BinaryIO) -> None:
    """Executes tasks for the life of the run: handshake, then one assignment
    after another until the parent closes the pipe."""
    payload = read_frame(reader)
    if payload is None:
        raise TransportError("the pipe closed before the handshake")
    dec = Dec(payload)
    if dec.u8() != _TO_HELLO:
        raise TransportError("expected the Hello handshake as the first frame")
    _check_version(dec.u32(), "worker")
    # The worker slot's id: sima attributes this process's events to it itself,
    # and each assignment carries the id of the slot running it.
    dec.u64()
    format = dec.str()
    interval_ms, interval_steps = dec.u64(), dec.u64()
    device = DeviceBinding(dec.str(), dec.u32()) if dec.flag() else None
    dec.finish()

    # Resolving here, before Ready, is what makes a device the program cannot
    # open fail the handshake rather than the first task.
    _served(domain, format)
    executor = domain.executor(device)
    name, driver = domain.device_desc(device)
    write_frame(writer, Enc().u8(_FROM_READY).u32(PROTOCOL_VERSION).str(name).str(driver).finish())

    while True:
        payload = read_frame(reader)
        if payload is None:
            return
        dec = Dec(payload)
        if dec.u8() != _TO_ASSIGN:
            raise TransportError("unexpected second Hello after the handshake")
        _execute(executor, dec, format, interval_ms, interval_steps, writer)


def _execute(
    executor: Executor,
    dec: Dec,
    format: str,
    interval_ms: int,
    interval_steps: int,
    writer: BinaryIO,
) -> None:
    """Executes one assignment and writes its terminal frame: ``Done`` for an
    outcome, ``Panicked`` for a raised exception, ``Fault`` for an outcome that
    cannot be put on the wire."""
    candidate, params = dec.bytes(), dec.bytes()
    seed, environment = dec.u64(), dec.hash()
    input_state, resume = dec.opt_bytes(), dec.opt_bytes()
    attempt, assigned_worker = dec.u32(), dec.u64()
    checkpointing = dec.flag()
    dec.finish()

    task = TaskInput(
        spec=Spec(format=format, candidate=candidate),
        params=params,
        seed=seed,
        environment=environment,
        input_state=input_state,
    )
    context = ExecutionContext(attempt=attempt, worker=assigned_worker)
    channel: Checkpoint = (
        _SaveChannel(interval_ms, interval_steps, resume, writer)
        if checkpointing
        else NoCheckpoint()
    )

    outcome: Outcome | None = None
    panic: tuple[str, str] | None = None
    try:
        outcome = executor.execute(task, context, channel)
    except Exception as e:
        panic = (f"panic: {type(e).__name__}: {e}", traceback.format_exc())

    # A save that tore the stream poisons every later frame, so it surfaces
    # instead of a terminal frame written onto a broken pipe.
    if isinstance(channel, _SaveChannel) and channel.failure is not None:
        raise channel.failure

    if panic is not None:
        # The rendered traceback crosses first as a structured diagnostic, then
        # the Panicked frame settles the attempt. sima attributes the
        # diagnostic to this worker slot itself.
        reason, rendered = panic
        _write_event(writer, _panic_diagnostic(rendered))
        write_frame(writer, Enc().u8(_FROM_PANICKED).str(reason).finish())
        return

    try:
        frame = _done(outcome)
    except Exception as e:
        write_frame(writer, Enc().u8(_FROM_FAULT).str(str(e)).finish())
        return
    write_frame(writer, frame)


def _done(outcome: Outcome) -> bytes:
    """The ``Done`` frame of an outcome: one flat layout across the three arms,
    with the fields an arm does not carry written empty."""
    if isinstance(outcome, Completed):
        tag, artifacts, reason = _OUTCOME_COMPLETED, outcome.artifacts, ""
    elif isinstance(outcome, Failed):
        tag, artifacts, reason = _OUTCOME_FAILED, (), outcome.reason
    elif isinstance(outcome, Rejected):
        tag, artifacts, reason = _OUTCOME_REJECTED, (), outcome.reason
    else:
        raise TypeError(f"an executor returns Completed, Failed, or Rejected, got {outcome!r}")
    enc = Enc().u8(_FROM_DONE).u8(tag).u64(len(artifacts))
    for artifact in artifacts:
        enc.str(artifact.name).bytes(artifact.bytes)
    stats = outcome.stats
    enc.u64(len(stats.scalars))
    for name, value in stats.scalars:
        enc.str(name).f64(value)
    return enc.bytes(stats.blob).str(reason).finish()


def _panic_diagnostic(rendered: str) -> dict:
    """The structured diagnostic that precedes a ``Panicked`` frame. The worker
    slot and the host are left unset: sima fills the attribution it knows."""
    return {
        "event": "diagnostic",
        "level": "error",
        "source": "panic",
        "message": rendered,
    }


def _write_event(writer: BinaryIO, event: dict) -> None:
    """Frames one structured event. Observational data never decides the
    conversation's fate, so an event that will not serialize is dropped; a
    broken pipe still surfaces."""
    try:
        payload = json.dumps(event).encode("utf-8")
    except (TypeError, ValueError):
        return
    write_frame(writer, Enc().u8(_FROM_EVENT).bytes(payload).finish())


class _SaveChannel(Checkpoint):
    """The program side of the checkpoint contract: the cadence decides whether
    an offer is written, and a due offer crosses as a one-way ``Save`` frame.

    The two cadence axes are unioned — a save is due when either fires — and
    both reset on save.
    """

    def __init__(
        self, interval_ms: int, interval_steps: int, resume: bytes | None, writer: BinaryIO
    ) -> None:
        self._interval = None if interval_ms == _CADENCE_DISABLED_MS else interval_ms / 1000.0
        self._step_interval = interval_steps or None
        self._resume = resume
        self._writer = writer
        self._last_saved = time.monotonic()
        self._offers_since_save = 0
        #: The first save-write failure, latched: an offer cannot fail, so the
        #: host surfaces it after execute returns instead of losing it.
        self.failure: BaseException | None = None

    def resume(self) -> bytes | None:
        return self._resume

    def offer(self, produce) -> None:
        if self.failure is not None or not self._save_due():
            return
        # The cadence resets before the write is attempted, so a persistently
        # failing pipe degrades once per cadence period instead of once per
        # offer.
        self._last_saved = time.monotonic()
        self._offers_since_save = 0
        try:
            write_frame(self._writer, Enc().u8(_FROM_SAVE).bytes(produce()).finish())
        except Exception as e:
            self.failure = e

    def _save_due(self) -> bool:
        """Whether this offer triggers a save, under either axis. The step axis
        counts every offer exactly once; the clock axis reads the time since the
        last save."""
        step_due = False
        if self._step_interval is not None:
            self._offers_since_save += 1
            step_due = self._offers_since_save >= self._step_interval
        clock_due = (
            self._interval is not None
            and time.monotonic() - self._last_saved >= self._interval
        )
        return step_due or clock_due


def _check_version(protocol: int, role: str) -> None:
    """Refuses a parent speaking another version, naming both numbers. There is
    no negotiation: the refusal precedes ``Ready``, so a missing ``Ready`` is
    the parent's spawn-failure signal."""
    if protocol != PROTOCOL_VERSION:
        raise TransportError(
            f"protocol version mismatch: the parent speaks {protocol}, "
            f"this {role} speaks {PROTOCOL_VERSION}"
        )
