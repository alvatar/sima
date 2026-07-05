# Architecture

sima is a substrate for deterministic, reproducible execution of GPU
programs and search over them. A program is opaque compute: the substrate
runs it, records the result under a content address, and never interprets
what it computes. The current specialization is cellular-automata-like
programs, and the same model extends to neural networks. `README.md` records
the motivation and design; `TODO.md` the roadmap; `AGENTS.md` the project
rules and settled invariants. This document describes the implemented
system.

sima runs in three execution modes:

- **local** — one machine runs the search end to end.
- **slingshot** — a local process dispatches execution to a more powerful
  remote machine and collects the results.
- **distributed** — many machines run many experiments together.

Local is implemented first. The store and identity model are designed so
slingshot and distributed add transport over the same objects, not new
semantics: a result computed on any machine carries the same content address
and commits to the same catalog.

## Layering

Strictly downward dependencies, enforced by workspace crate edges.

| Layer | Crate            | Responsibility                                                        |
|-------|------------------|-----------------------------------------------------------------------|
| L0    | `sima-core`      | error type, canonical encoding, content hash, PRNG                    |
| L1    | `sima-model`     | identity vocabulary: spec, params, environment, task key, record, run config |
| L2    | `sima-store`     | durable state: CAS, task index, run manifests, journals               |
| L3    | `sima-contracts` | generator/executor traits (arrives M1.4)                              |
| L4    | `sima-scheduler` | task sources, leases, lifecycle state machine (arrives M1.5)          |
| L5    | `sima-pipeline`  | orchestration, resume, re-evaluation (arrives M1.6)                   |
| L6    | `sima`           | CLI binary                                                            |

The store is the only durable state. Queues, schedulers, and orchestrators
are ephemeral: the runnable frontier is derived from (config, store state),
so resume, crash-recovery, and re-run are one code path.

## Two serialization worlds

- **Identity-bearing bytes** — anything hashed — use the canonical binary
  `Enc`/`Dec` encoding exclusively. The encoding is deterministic and
  self-delimiting: a value has exactly one byte image, and that image parses
  without external length hints. A value's id is the blake3 hash of its
  standalone canonical bytes, so the id doubles as its address in the CAS.
- **Human-readable data** uses serde: JSON for the run manifest, plain-text
  lines for the journal. It carries observational or index data and is never
  identity-bearing.

## `sima-core` (L0)

The foundation every crate shares:

- A single error type — one closed enum spanning encoding, validation, I/O,
  corruption, and missing-object failures — so `Result` means the same thing
  in every crate.
- A canonical binary encoding: deterministic and self-delimiting, the sole
  route for identity-bearing bytes.
- Content addressing over blake3: a value's id is the hash of its canonical
  bytes, and that hash is its address in the store.
- A CPU/GPU-stable PRNG: counter-based, specified for identical results on
  CPU and GPU, pinned by known-answer tests. The `rand` crate is barred from
  result-affecting paths.

## `sima-model` (L1)

Pure data, no I/O; depends on `sima-core` only. Every encoding opens with a
length-prefixed string domain tag, fixed forever (a layout change mints a
`.v2`):

| Tag                   | Type           | Id            |
|-----------------------|----------------|---------------|
| `sima.spec.v1`        | `Spec`         | `SpecId`      |
| `sima.params.v1`      | `Params`       | `ParamsId`    |
| `sima.environment.v1` | `Environment`  | `EnvironmentId` |
| `sima.task.v1`        | `TaskIdentity` | `TaskKey`     |
| `sima.task-record.v1` | `TaskRecord`   | —             |
| `sima.run-config.v1`  | `RunConfig`    | `RunId`       |

- A spec is (format id, opaque bytes): the candidate. Params is a second
  opaque blob: the evaluation settings. The spec's format id governs the
  interpretation of both; generators produce specs, config produces params.
- The task key preimage is spec ‖ params ‖ seed ‖ environment ‖
  input-state-ref (`None` for stateless tasks; segments differing only in
  input state get distinct keys).
- `Environment` components are content-derived only (versioned constants,
  content digests). Machine-derived facts (hostname, device, time) are
  journal material, or run portability fails by construction.
- Identity and execution split: `RunConfig` holds the identity-bearing
  config only — `RunId = blake3(config bytes)` survives changes in worker
  count, store path, or hardware. `TaskRecord` holds identity plus artifact
  references only — attempts, timings, and workers live in the journal.
- Names (format ids, generator ids, component and artifact names): 1..=64
  bytes of `[a-z0-9._-]`.

## `sima-store` (L2)

One `Store` type over a root directory:

```
<root>/objects/<aa>/<hash>       CAS object bytes; aa = first two hex chars
<root>/tmp/<pid>-<seq>           in-flight writes
<root>/tasks/<task-key>          index entry: record-hash hex + newline
<root>/runs/<run-id>/manifest.json
<root>/runs/<run-id>/journal
```

### Components

- **object** — a content-addressed blob: bytes stored under the blake3 hash
  of those bytes. Every model id is the address of its object.
- **task index** — the `tasks/` tree recording, for each answered task, the
  result that answers it. A task source reads it to derive what remains
  runnable.
- **index entry** — the file at `tasks/<task-key>` holding the hex address
  of the record object answering that task, newline-terminated: the task
  index's per-task cell.
- **record** — a `TaskRecord` object: the identity of one evaluation plus
  the addresses of its artifacts. One record answers one task.
- **run** — one search invocation, identified by `RunId`, the hash of its
  config's canonical bytes, which is also the address of the config object.
- **manifest** — a finalized run's `(task, record)` list sorted by task key:
  the fixed set of tasks the run comprises.
- **journal** — a run's append-only observational history, one event per
  line.
- **closure** — the deduplicated, sorted set of objects a finalized run
  depends on: config, records, and every object those records reference. The
  unit of run portability and store sync.

### Content addressing

Storing an object is idempotent — same bytes, same hash, same path. Reading
one re-hashes what it read and returns `Corruption` on mismatch, so every
read is verified. Every model id is an address in this store, so specs,
params, environments, records, configs, state snapshots, and artifacts all
land here as objects.

### Atomic durability

Every durable file is written to `tmp/<pid>-<seq>`, fsynced, then placed into
its destination, after which the parent directory is fsynced. Placement uses
one of two modes, chosen by purpose:

- **rename**, which replaces, for content-addressed objects: one path means
  one content, so a racing writer carries identical bytes and replacement is
  harmless.
- **a hard link that fails when the destination already exists**, for index
  entries and manifests: two different results must never silently overwrite
  each other, so a collision compares bytes — equal is idempotent, different
  is `Corruption`.

Directories are created with their parent fsynced, so a new directory entry
survives a crash together with the files inside it. POSIX rename and link
atomicity means a reader — including a process resuming after SIGKILL —
observes a complete file or none. Directory fsync is specific to unix, so
the crate builds on unix targets only; the build is refused elsewhere rather
than silently dropping durability across a crash. Leftover `tmp/` files after
a crash are inert; sweeping them is retention work (P6).

### Write ordering

Committing a task result verifies every object the record references
(artifacts and identity components) is already durable, then writes the
record object, then the index entry. An index entry therefore proves
everything beneath it exists. Recommitting an equal record is idempotent; a
conflicting record for the same key is `Corruption` — one result per task
key, ever.

### Manifest and finalization

Finalizing a run writes its manifest once, atomically: the `(task, record)`
list in serde JSON with entries sorted by task key, so the bytes are
independent of worker completion order. It is the object every
equality-based acceptance criterion compares. The `run` field is verified
against the directory name on read, and — because `RunId` is the hash of the
config's canonical bytes — it is simultaneously the address of the run's
config object, stored when the run is registered. A store therefore contains
the definition of every run it holds.

### Journal

A run's observational history: append-only, one event per line, its meaning
owned by the layers that emit events (scheduler and above). A payload is one
nonempty line free of embedded line breaks; appends are single-write,
newline-terminated, fsynced. On read, a torn final line (bytes past the last
newline) is ignored; invalid UTF-8 inside the intact region is corruption.
Journals legitimately differ between identical runs and are excluded from
every equality criterion.

### Concurrency

Store methods take `&self` and are safe under concurrent writers: identical
content converges through rename and link atomicity, and a conflicting racer
on an index entry or manifest fails loudly instead of overwriting. A run's
journal has a single writer, the orchestrator; single-writer-per-run is
enforced by the orchestrator lease file (arrives M1.6).

## Determinism proof obligations

Anything claimed deterministic is proven by test: same config in two fresh
stores → byte-identical manifests; a run killed at any crashpoint and
resumed → manifest identical to an uninterrupted run (crash-injection
harness, M1.6); re-evaluation touches no executor; a copied store resumes
with an identical manifest.
