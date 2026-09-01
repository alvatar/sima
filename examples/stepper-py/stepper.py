#!/usr/bin/env python3
"""A sima program in Python: a stepped accumulator.

The whole contract fits in this file. A candidate is one byte, the increment.
A task adds that increment to an accumulator for a fixed number of steps, keyed
by the absolute step index, so the trajectory is the same wherever a
segmentation cuts it. The state a segment commits is what the next segment
continues from, and the checkpoint the program offers at every step boundary is
what a restarted attempt resumes from.

Run it under a configuration that routes the format to this file and declares
``sdk = "python"``, which is what puts the ``sima`` package on the interpreter's
path. ``search.toml`` beside it is a runnable one.

Three environment variables arm failure paths, so the infrastructure's recovery
can be exercised without a second program. Each is inert when unset:

- ``STEPPER_EXIT_AT_STEP=N`` — die without a frame right after the checkpoint
  offer at absolute step ``N``. The restarted attempt resumes past ``N``, so the
  condition never fires twice and the search converges.
- ``STEPPER_FAIL_ONCE=1`` — the first attempt of every task returns a transient
  failure, which sima retries.
- ``STEPPER_RAISE_ONCE=1`` — every task raises, once: the exception crosses as
  a diagnostic and a panic, sima rejects the task definitively, and the search
  ends failed.
"""

from __future__ import annotations

import os
import sys
import tomllib

import sima

#: The format this program serves.
FORMAT = "example.stepper.v1"
#: The generator that draws candidates for it.
GENERATOR = "example.stepper.candidates"
#: The device class the work runs on: this interpreter's processor.
DEVICE_CLASS = "example:cpu"
DEVICE_NAME = "python host processor"
DRIVER = "example.stepper v1"

#: The accumulator is a `u64`, so every addition wraps at 64 bits.
WRAP = 2**64
#: A state is a `u64` step and a `u64` accumulator, little-endian.
STATE_LEN = 16
#: Widest candidate count a search may ask for.
MAX_COUNT = 32


def encode_state(step: int, acc: int) -> bytes:
    """The 16 state bytes: the absolute step reached, then the accumulator."""
    return sima.Enc().u64(step).u64(acc).finish()


def decode_state(raw: bytes) -> tuple[int, int]:
    """The step and accumulator a state carries. Raises when it is not a state."""
    if len(raw) != STATE_LEN:
        raise ValueError(f"a stepper state is {STATE_LEN} bytes, got {len(raw)}")
    dec = sima.Dec(raw)
    return dec.u64(), dec.u64()


class StepperExecutor(sima.Executor):
    """Adds the candidate's increment to the accumulator, one step at a time."""

    def execute(self, task, context, checkpoint):
        increment = task.spec.candidate[0] if task.spec.candidate else 0
        # A candidate that adds nothing can never produce a result, so it is
        # rejected rather than failed: sima never retries it.
        if increment == 0:
            return sima.Rejected(reason="zero increment")
        if os.environ.get("STEPPER_FAIL_ONCE") and context.attempt == 0:
            return sima.Failed(reason="armed transient failure")
        # No attempt guard, unlike the transient arm above: a panic rejects the
        # task definitively, so this attempt is the only one there will be.
        if os.environ.get("STEPPER_RAISE_ONCE"):
            raise RuntimeError("armed panic")

        params = sima.Dec(task.params)
        steps = params.u64()
        params.finish()
        # A segment continues the state its predecessor committed; segment 0 and
        # a stateless task start from the task's seed.
        if task.input_state is None:
            step, acc = 0, task.seed
        else:
            step, acc = decode_state(task.input_state)
        start, end = step, step + steps

        # A saved checkpoint is adopted only when it decodes and its step lies
        # inside this task's range. Anything else is stale: resuming shortens
        # re-execution and never changes the committed bytes.
        saved = checkpoint.resume()
        if saved is not None and len(saved) == STATE_LEN:
            saved_step, saved_acc = decode_state(saved)
            if start <= saved_step < end:
                step, acc = saved_step, saved_acc

        armed_exit = os.environ.get("STEPPER_EXIT_AT_STEP")
        exit_at = int(armed_exit) if armed_exit else None
        executed = 0
        while step < end:
            acc = (acc + increment) % WRAP
            step += 1
            executed += 1
            checkpoint.offer(lambda: encode_state(step, acc))
            if step == exit_at:
                # Dying without a terminal frame: the parent meets a broken
                # pipe, fails the attempt transiently, and respawns.
                os._exit(1)

        return sima.Completed(
            artifacts=(sima.Artifact(name=sima.STATE_ARTIFACT, bytes=encode_state(step, acc)),),
            # The steps this attempt actually executed, so a resume that
            # shortened re-execution is visible in the journal.
            stats=sima.Stats(scalars=(("steps", float(executed)), ("acc", float(acc)))),
        )


class StepperDomain(sima.Domain):
    """What ``example.stepper.v1`` binds."""

    def format(self):
        return FORMAT

    def environment(self):
        # What results depend on: this executor's arithmetic, versioned. Change
        # the arithmetic and bump the version, and every result the old one
        # stored keeps its own address.
        return sima.Environment((sima.EnvironmentComponent("example.stepper.executor", version="v1"),))

    def enumerate_devices(self):
        return [
            sima.DeviceInfo(
                clazz=DEVICE_CLASS,
                name=DEVICE_NAME,
                device_type=sima.DeviceType.CPU,
                member=0,
            )
        ]

    def translate_config(self, toml, segmented):
        """``[search.params]`` in, canonical params bytes out.

        An unread key is refused: a silently ignored setting would give the search
        an identity promising something the executor never applied.
        """
        table = parse_section(toml, "[search.params]")
        for key in table:
            if key != "steps":
                raise ValueError(f"[search.params] carries {key!r}; {FORMAT} takes steps alone")
        steps = table.get("steps")
        if not isinstance(steps, int) or isinstance(steps, bool) or steps < 1:
            raise ValueError(f"steps is an integer of at least 1, got {steps!r}")
        return sima.Enc().u64(steps).finish()

    def executor(self, device):
        return StepperExecutor()

    def device_desc(self, device):
        return (DEVICE_NAME, DRIVER)


class StepperCandidates(sima.Generator):
    """Draws one-byte candidates from the search's root seed."""

    def id(self):
        return GENERATOR

    def format(self):
        return FORMAT

    def translate_config(self, toml):
        """``[search.generator]`` minus its ``id`` in, the generator's blob out."""
        table = parse_section(toml, "[search.generator]")
        for key in table:
            if key not in ("count", "value"):
                raise ValueError(
                    f"[search.generator] carries {key!r}; {GENERATOR} takes count and value"
                )
        count = table.get("count", 1)
        if not isinstance(count, int) or isinstance(count, bool) or not 1 <= count <= MAX_COUNT:
            raise ValueError(f"count is an integer of 1 to {MAX_COUNT}, got {count!r}")
        value = table.get("value")
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 255
        ):
            raise ValueError(f"value is an integer of 0 to 255, got {value!r}")
        return sima.Enc().u64(count).opt_u64(value).finish()

    def generate(self, root_seed, params):
        """The search's candidates: one byte each, drawn from the root seed unless
        ``value`` fixes them. One root seed always yields the same specs."""
        dec = sima.Dec(params)
        count, value = dec.u64(), dec.opt_u64()
        dec.finish()
        return [
            sima.Spec(
                format=FORMAT,
                candidate=bytes([value if value is not None else (root_seed + i) % 256]),
            )
            for i in range(count)
        ]


def parse_section(toml: str, section: str) -> dict:
    """The keys a configuration section declared. Empty text is no keys."""
    try:
        return tomllib.loads(toml)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"{section} is not valid TOML: {e}") from e


def main() -> int:
    """Answers whichever conversation this process was spawned for."""
    try:
        sima.serve(StepperDomain(), [StepperCandidates()])
    except Exception as e:
        print(f"stepper: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
