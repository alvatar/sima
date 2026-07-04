# Architecture

sima is a local-first substrate for deterministic evolutionary search over
cellular-automata-like computation. `README.md` records the motivation and
design; `TODO.md` the roadmap; `AGENTS.md` the project rules and settled
invariants. This document describes the implemented system.

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

- **Identity-bearing bytes** — anything hashed — go through the canonical
  `Enc`/`Dec` encoding exclusively. A value's id is the blake3 hash of its
  standalone canonical bytes, so the id doubles as its address in the CAS.
- **Human-readable artifacts** — manifest JSON, journals — are serde, carry
  observational or index data, and are never identity-bearing.

## `sima-core` (L0)

- `Error`/`Result`: one closed enum for all crates. Failure classes:
  `Encoding` (bad framing/truncation), `Validation` (a value or call
  violates an invariant), `Io` (filesystem failure, carries path and
  source), `Corruption` (store content contradicts a store invariant),
  `MissingObject` (a referenced or requested object is absent — the class
  distribution sync later negotiates on).
- `Enc`/`Dec`: little-endian integers at natural width, `u64`
  length-framed `bytes`/`str`, 32-byte `hash`, flag-byte `opt_hash`;
  `Dec::finish` rejects trailing bytes.
- `Hash` / `hash_bytes`: 32-byte blake3 digest, lowercase-hex `Display` and
  `from_hex`.
- PRNG: counter-based SplitMix64 — `next(seed, counter)`, `derive(seed,
  tag)`, `unit_f64`, and a sequential `Stream`. Pinned known-answer tests
  keep it byte-stable; it is specified for identical implementation on CPU
  and GPU, and the `rand` crate is banned from result-affecting paths.

## `sima-model` (L1)

Pure data, no I/O; depends on `sima-core` only. Every encoding opens with a
str-framed domain tag, fixed forever (a layout change mints a `.v2`):

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
- Identity/execution split: `RunConfig` holds the identity-bearing config
  only — `RunId = blake3(config bytes)` survives changes in worker count,
  store path, or hardware. `TaskRecord` holds identity + artifact refs
  only — attempts, timings, and workers live in the journal.
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

- **CAS**: `put` is idempotent — same bytes, same hash, same path; `get`
  re-hashes what it read and returns `Corruption` on mismatch, so every
  read is verified. Model ids are CAS addresses: specs, params,
  environments, records, configs, state snapshots, and artifacts all land
  here as objects.
- **Atomic writes**: every durable file is written to `tmp/<pid>-<seq>`,
  fsynced, renamed into place, and the parent directory fsynced (the
  directory fsync is unix-only; the project targets linux). POSIX rename
  atomicity means a reader — including a process resuming after SIGKILL —
  observes a complete file or none. Leftover `tmp/` files after a crash
  are inert; sweeping them is retention work (P6).
- **Write ordering**: committing a task result verifies every object the
  record references (artifacts and identity components) is already durable,
  then writes the record object, then the index entry. Recommitting an
  equal record is a no-op; a conflicting record for the same key is
  `Corruption` — one result per task key, ever.
- **Manifest**: written once, atomically, at run finalization; the object
  every equality-based acceptance criterion compares. Serde JSON:
  `{ "run": <hex>, "entries": [{ "task": <hex>, "record": <hex> }] }`,
  entries sorted by task key so bytes are independent of worker completion
  order. The `run` field is verified against the directory name on read,
  and — because `RunId` is the hash of the config's canonical bytes — it is
  simultaneously the CAS address of the run's config, which `create_run`
  stores. A store therefore contains the definition of every run it holds.
- **Journal**: the observational history of a run — append-only,
  line-framed, schema owned by the layers that emit events (scheduler and
  above). A payload is one nonempty line free of embedded line breaks;
  appends are single-write, newline-terminated, fsynced. On read, a torn
  final line (bytes past the last newline) is ignored; invalid UTF-8
  inside the intact region is corruption. Journals legitimately differ
  between identical runs and are excluded from every equality criterion.
- **Closure**: manifest → config object + records → specs, params,
  environments, input states, artifacts; sorted and deduplicated. The unit
  of run portability and of store sync.
- **Concurrency**: store methods take `&self` and are safe under
  concurrent writers through rename atomicity and idempotence. A run's
  journal has a single writer (the orchestrator). Single-writer-per-run is
  enforced by the orchestrator lease file (arrives M1.6).

## Determinism proof obligations

Anything claimed deterministic is proven by test: same config in two fresh
stores → byte-identical manifests; a run killed at any crashpoint and
resumed → manifest identical to an uninterrupted run (crash-injection
harness, M1.6); re-evaluation touches no executor; a copied store resumes
with an identical manifest.
