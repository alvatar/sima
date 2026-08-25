# The program protocol

sima runs a program as its own process and talks to it over that process's
stdin and stdout. This document is the contract: framing, canonical encoding,
the two message sets, and the obligations a program takes on. **Speaking it is
the whole requirement for building for sima.** A program in any language that
frames these bytes plugs into a run; an SDK is a convenience over the same
bytes.

Two implementations of the program side live in this repository, and the Rust
one is normative: where this document and its behavior disagree, the Rust
implementation decides and the document is corrected. `sima-api` is the Rust
SDK over it — a program implements two traits and calls `sima_api::serve`. The
Python implementation is the `sima` package under `python/`, a full client of
this document, and `examples/stepper-py/` is a program written against it that
exercises every message below.

The protocol version is 1.

## The two roles

One binary answers both conversations a run needs of it, and the argument
vector says which:

- **worker** — the process was spawned with no `--serve-domain` flag. It
  executes tasks over the worker protocol. The orchestrator spawns one process
  per worker slot, for the life of a run.
- **domain service** — the argument vector carries `--serve-domain <format>`.
  The process answers questions about that one format, over the domain-service
  protocol. The orchestrator opens one session per configured format when a
  configuration loads.

The vector is scanned for the flag; the argument after it is the format id.
Anything else in the vector belongs to whoever wrapped the program, so a vector
without the flag is the worker role. `--serve-domain` with nothing after it is
an error the program reports before any frame.

A domain service serves exactly one format. A question naming another format is
answered `Failed`, not answered for the format the program does serve.

## Session lifetimes

Both roles open with a handshake and end when the parent closes the program's
stdin. End of stream at a frame boundary is a clean shutdown; the program exits
zero.

- A **domain-service session** stays open for the whole configuration, so a
  program that loads assets or opens a device pays that cost once. `Goodbye`
  ends it, and the program exits without answering.
- A **worker session** stays open for the run. After the handshake the program
  executes one assignment after another until stdin closes.

Frames travel on stdin (parent to program) and stdout (program to parent) only.
stderr carries no frames: the parent reads it line by line and journals each
line as a diagnostic, so a program is free to write anything there.

## Framing

- A frame is a `u32` little-endian payload length, then exactly that many bytes
  of payload.
- The length prefix is plain little-endian rather than a canonical integer,
  because frames are transport encoding: **no frame is ever hashed.**
- `MAX_PAYLOAD` is $256 \times 1024 \times 1024$ bytes. A length above it is a
  transport error on both sides, refused from the four prefix bytes alone,
  before any allocation.
- Every frame is flushed on write, so a frame reaches the peer as soon as it is
  written.
- End of stream at a frame boundary is a clean shutdown. End of stream inside a
  frame — a torn length prefix or a short payload — is a transport error.

Each payload starts with a `u8` message tag; the fields after it are written
with the canonical primitives below, in the order the tables state.

## Canonical encoding

The same encoding carries identity-bearing values inside a frame and inside a
stored object, so a program encodes once and the bytes mean one thing
everywhere.

| Primitive | Bytes |
|---|---|
| `u8`, `u16`, `u32`, `u64` | little-endian at natural width |
| `i64` | two's-complement little-endian |
| `f32` | IEEE-754 bits in a little-endian `u32` |
| `f64` | IEEE-754 bits in a little-endian `u64` |
| `bytes` | `u64` little-endian length, then the raw bytes |
| `str` | the same framing over the string's UTF-8 bytes |
| `hash` | 32 raw digest bytes |
| optional value | a present flag byte of `0` or `1`, then the value when present |

A flag byte other than `0` or `1` is an encoding error. A decoder rejects
trailing bytes: a payload must be consumed exactly.

### Names

- A **format id** and a **generator id** are 1 to 64 bytes of `[a-z0-9._-]`.
- An **environment component name** and an **artifact name** follow the same
  rule.
- A **device class** is 1 to 64 bytes of `[a-z0-9._:-]` — the colon is the
  difference, so a class minted from configuration-space identifiers
  (`8086:7d51`) is spelled as the backend mints it.

Names are lowercase-only, which keeps one spelling per identity. Both endpoints
validate them, so a name outside the rule fails at the frame rather than inside
a dispatch.

## The domain-service protocol

Each question is answered by exactly one frame: the answer it names, or
`Failed`. A failed question does not end the session — the next question is
still answered.

| Tag | Parent to program | Payload after the tag |
|---|---|---|
| 0 | `Hello` | `u32` protocol version |
| 1 | `Describe` | `str` format |
| 2 | `EnumerateDevices` | `str` format |
| 3 | `TranslateConfig` | `str` format, `str` TOML text, `u8` segmented flag |
| 4 | `TranslateGeneratorConfig` | `str` generator, `str` TOML text |
| 5 | `Generate` | `str` generator, `str` format, `u64` root seed, `bytes` generator params |
| 6 | `Goodbye` | nothing; the program exits |

| Tag | Program to parent | Payload after the tag |
|---|---|---|
| 0 | `Ready` | `u32` protocol version |
| 1 | `Described` | an `Environment` |
| 2 | `EnumeratedDevices` | `u64` count, then per device: `str` class, `str` name, `u8` type, `u32` member |
| 3 | `TranslatedConfig` | `bytes`, the answer to either translation |
| 4 | `Generated` | `u64` count, then that many `Spec` |
| 5 | `Failed` | `str` message, which the parent surfaces verbatim |

What each question asks for:

- `Describe` — the environment the format's results depend on. It enters every
  task's identity.
- `EnumerateDevices` — the devices the format's work can run on, as the
  program's execution backend enumerates them. A format that opens no device
  answers an empty list.
- `TranslateConfig` — the run's `[run.params]` section, to be translated into
  the format's canonical params bytes. The segmented flag says whether the run
  divides each candidate's evaluation into a chain of segments, so a program
  can refuse a combination it does not support.
- `TranslateGeneratorConfig` — the `[run.generator]` section minus its `id`
  key, translated into the generator's opaque params blob.

- `Generate` — the run's candidate specs, from the named generator, under the
  run's root seed and the blob the previous question produced.

Both translations receive the section's **body**, re-serialized as TOML: the
keys and their values, without the `[run.params]` or `[run.generator]` header
line. A run that states no such section sends an empty string. The bytes a
translation answers are opaque to sima — it stores them, hashes them into the
run's identity, and hands the params bytes back verbatim in every `Assign`.

Device type tags: `0` discrete, `1` integrated, `2` virtual, `3` cpu, `4`
other.

## The worker protocol

| Tag | Parent to program | Payload after the tag |
|---|---|---|
| 0 | `Hello` | `u32` protocol version, `u64` worker id, `str` format, `u64` checkpoint interval ms, `u64` checkpoint interval steps, optional device: the flag byte, then `str` class and `u32` member |
| 1 | `Assign` | `bytes` spec, `bytes` params, `u64` seed, 32-byte environment id, optional `bytes` input state, optional `bytes` resume, `u32` attempt, `u64` worker id, `u8` checkpointing flag |

| Tag | Program to parent | Payload after the tag |
|---|---|---|
| 0 | `Ready` | `u32` protocol version, `str` device name, `str` driver version, `str` program digest |
| 1 | `Save` | `bytes` continuation state; one-way, the parent persists it |
| 2 | `Done` | the outcome, laid out below |
| 3 | `Panicked` | `str` rendered panic |
| 4 | `Fault` | `str` infrastructure failure |
| 5 | `Event` | `bytes`, one structured event as JSON, carried opaquely |

The conversation: `Hello` in, `Ready` out, then per assignment zero or more
`Save` and `Event` frames and exactly one of `Done`, `Panicked`, or `Fault`.
`Event` frames may also cross between assignments, any time after `Ready`.

The `Hello` fields:

- The **format id** travels once, here. Every assignment's spec bytes are of
  that format, so an assignment does not repeat it.
- The **worker id** labels this process's slot. It is observational: a program
  may log it, and must never let it reach a committed artifact.
- The **checkpoint cadence** has two axes, unioned. `u64::MAX` milliseconds
  disables the wall-clock axis and `0` steps disables the step axis; a cadence
  of `0` milliseconds makes a save due at every offer.
- The **device** is the one this worker's executor is to be built for. Absent,
  the program picks by its own default selection.

The `Ready` device name and driver version are the program's own account of
where it computes, journaled verbatim by the parent. A program that opens no
device answers both empty.

The `Ready` **program digest** is the value of the environment variable
`SIMA_PROGRAM_DIGEST`, answered verbatim and empty when the variable is unset.
The direction is one way: sima sets the variable at spawn to state which
program it sent, the program echoes it back, and sima compares the answer
against what it sent. A program computes nothing here and reads nothing into
the value — a script's executable is its interpreter, and a built entry point
is not the payload that travelled, so the digest of the sources is knowable
only to the side that shipped them.

The `Assign` fields split in two. The spec, params, seed, environment id, and
input state are **identity-bearing**: they determine the task key and every
committed artifact. The attempt number and worker id are the **per-attempt
facts**: visible, and forbidden from influencing any committed artifact. The
resume bytes and the checkpointing flag belong to the checkpoint contract and
enter no identity either.

### The `Done` payload

One flat layout across the three outcomes, with the fields an arm does not
carry written empty:

- `u8` outcome tag: `0` completed, `1` failed, `2` rejected.
- `u64` artifact count, then per artifact `str` name and `bytes` content. A
  failed or rejected outcome writes a count of zero.
- `u64` scalar count, then per scalar `str` name and `f64` value. These are the
  observational stats; a non-finite value is allowed and says the candidate
  diverged.
- `bytes` stats blob, for anything richer than a scalar. Empty when the scalars
  carry everything.
- `str` reason. A completed outcome writes an empty string.

The three outcomes differ in what the parent does with them:

- **completed** — the artifacts are committed under the task's key.
- **failed** — a transient failure. The parent may retry the task, within the
  same session.
- **rejected** — a definitive failure. The candidate cannot produce a result,
  so the task is never retried.

Reasons are observational: journal and reporting material, never
identity-bearing.

## Nested forms

**`Spec`** — a candidate, tagged and self-describing:

- `str` `"sima.spec.v1"`
- `str` format id
- `bytes` candidate

**`Environment`** — what a format's results depend on:

- `str` `"sima.environment.v1"`
- `u64` component count, at least one
- then per component: `str` name, `u8` arm, and either `str` version (arm `0`)
  or a 32-byte digest (arm `1`)

Components arrive sorted by name with no duplicates, so equal environments have
equal bytes regardless of the order a program built them in. A version string
is non-empty. Components are content-derived only — an executor version
constant, the digest of a compiled shader. Anything machine-derived (hostname,
device, driver, path, time) is journal metadata and never a component, because
two machines with equal environments must produce equal results.

## Structured events

An `Event` frame carries one JSON object: an `event` key naming the kind in
snake_case, and that kind's fields beside it. The vocabulary is sima's — the
journal's own event kinds — and a program is expected to emit one of them:

```json
{"event": "diagnostic", "level": "error", "source": "panic", "message": "..."}
```

- `level` is one of `info`, `warn`, `error`.
- `source` names the component the line came from.
- `message` is the text.
- `worker`, `host`, and `task` are optional attribution keys. A program leaves
  all three unset: sima fills the worker slot and the host, which it knows, and
  a task key is an id a program never computes.

Events are observational and one-way. A frame that does not parse is journaled
as a warning and dropped; it never decides the conversation's fate.

## The handshake and the version rule

Each side states its version in the opening frame, and the program answers
`Ready` with its own. A mismatch is refused immediately on whichever side sees
it first, naming both numbers, and no work follows. There is no negotiation:
a parent and a program that disagree do not run together.

The program refuses before writing `Ready`, so a missing `Ready` is the
parent's spawn-failure signal. The parent refuses on reading a `Ready` that
carries another number.

Both roles carry one version, because one binary answers both.

## Obligations

Beyond the message layouts, a program takes on the following. They are what
makes a run reproducible; a program that breaks one produces results sima
cannot stand behind.

### Determinism

- One root seed always yields the same specs from `Generate`, in the same
  order.
- A task's committed artifacts are a pure function of its identity-bearing
  inputs: the spec, the params, the seed, the environment, and the input state.
- The attempt number and the worker id may reach stats and logs. They may not
  reach an artifact.
- Using or ignoring the resume bytes yields byte-identical committed artifacts.
  A checkpoint changes recovery time and nothing else.

### Segments and the state artifact

A run may divide each candidate's evaluation into a chain of $N$ tasks. Every
segment of a chain carries the same seed, and each hop works as follows:

- Segment 0 receives no input state.
- A segment commits its continuation state as an artifact named `state`.
- The parent addresses that artifact and hands its bytes to the next segment as
  the `Assign` input state.

A segmented run over a program that never commits `state` fails on the parent
as a validation error naming the artifact. An unsegmented run's tasks are
stateless: they receive no input state and need commit no `state` artifact.

### A program computes no ids

The 32-byte environment id in `Assign` is carried and compared, never
recomputed. A program hashes nothing: sima addresses what a program returns.

### Timeouts

The parent bounds every domain-service answer by its configured
`answer_timeout_ms`, including the handshake. An expiry kills the program — it
owes an answer it will not give.

`Generate` is the one question with no bound: generation is computation
proportional to the batch, so a deadline sized for answers would kill a
legitimate large batch. A runaway generator is interrupted the way any run is.

The worker handshake is bounded the same way. Execution of an assignment is
not: a task runs as long as it runs.

### The checkpoint contract

A checkpoint is the disposable crash-resume mechanism, and the two sides split
it:

- The **program** decides what bytes capture its continuation state and when
  offering them is safe — a step boundary from which resuming reproduces the
  same result.
- The **parent** decides whether an offer is written and performs all storage.
  A program never touches a store.

The cadence arrives in `Hello`; the `Assign` checkpointing flag says whether
this task checkpoints at all. Only chain tasks do — a stateless task never
checkpoints, and its flag is clear. A due offer crosses as a one-way `Save`
frame, which the parent persists into the chain's slot; the cadence resets
before the write, so a persistently failing pipe degrades once per cadence
period rather than once per offer.

The `Assign` resume bytes are what a previous attempt of this same task saved.
The program validates them itself and starts fresh when they do not apply. A
persist failure on the parent degrades the checkpoint and execution continues;
a load failure on resume degrades to a fresh start.

### What the parent does with each frame

Domain service:

- `Failed` — surfaced verbatim as a configuration-load error. The program's
  words reach the user, not the parent's guess at them.
- an answer of the wrong shape — a protocol violation naming what was expected.

Worker:

- `Done` completed — the artifacts are committed under the task's key.
- `Done` failed — the attempt failed transiently; the parent retries at its own
  discretion.
- `Done` rejected — definitive; the task is never retried.
- `Panicked` — definitive; the task is never retried, exactly as `Done`
  rejected. A program that panics reports it as a `Panicked` frame rather than
  dying, and may precede it with an `Event` carrying the diagnostic.
- `Fault` — an infrastructure failure, which fails the run.
- `Save` — persisted into the task's checkpoint slot, or dropped when the task
  selected none.
- `Event` — parsed and forwarded to the run's collector, which journals it. It
  never decides the conversation's fate: an event that fails to parse is
  dropped.
- a broken pipe or a torn frame — the attempt fails transiently, and the parent
  respawns the worker and retries within the same session.
