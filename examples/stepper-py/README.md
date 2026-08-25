# stepper-py

A sima program written in Python: a stepped accumulator over one-byte
candidates. It is the smallest program that exercises the whole boundary —
both roles of the protocol, segmented state chaining, checkpoint save and
resume, all three outcomes, structured events, and the panic path — so it
doubles as the worked example of `docs/protocol.md` and as what
`crates/sima-integration/tests/python_program.rs` drives through the full
spine.

## What it computes

A candidate is one byte, the increment. A task adds that increment to a
`u64` accumulator once per step, for the number of steps `[run.params]`
declares, and commits the reached state — the absolute step and the
accumulator, 16 little-endian bytes — as the artifact named `state`. Under
`[run] segments = N` the next segment continues from exactly those bytes, so
the trajectory does not depend on where the cuts fall. A candidate byte of `0`
adds nothing and is rejected.

## Where it runs

`sima run search.toml` drives it here. `sima migrate search.toml` moves the
whole run onto another machine: the entry's `payload` key states what travels,
the destination installs it at load, and the results come home to this store.
The program travels as the payload and the SDK travels inside the `sima`
binary, so the destination needs nothing installed beyond sima itself.

Turning that on takes two declarations, both commented out in `search.toml`: an
`[orchestrator] migrate` key naming the destination, and the `[host.<name>]`
entry with that machine's connection details.

Spreading one run's tasks across several machines — `sima run --fleet` — is not
available to a registered format: a worker elsewhere would need this program
installed there, and nothing installs it. That workflow is available to the
formats sima carries in process.

## Running it

From this directory:

```
sima run search.toml
```

`import sima` resolves from the SDK the binary vends: the entry declares
`sdk = "python"`, and sima puts the package it carries on the interpreter's
path. `search.toml` routes `example.stepper.v1` to `./stepper.py`, so the file
must stay executable.

## Arming a failure

Three environment variables arm a failure path, each inert when unset. The
integration tests set them from a wrapper script, which is also how a run
arms one by hand: the configuration carries no arming state, so an armed and
an unarmed run of one configuration share an identity.

- `STEPPER_EXIT_AT_STEP=N` — die without a frame right after the checkpoint
  offer at absolute step `N`. sima meets a broken pipe, respawns the worker,
  and the restarted attempt resumes from the checkpoint past `N`, so the run
  converges.
- `STEPPER_FAIL_ONCE=1` — the first attempt of every task returns a transient
  failure, which sima retries.
- `STEPPER_RAISE_ONCE=1` — every task raises, once. The traceback crosses as a
  diagnostic and the attempt reports a panic, which sima treats as a definitive
  rejection: the task is never retried and the run ends failed.
