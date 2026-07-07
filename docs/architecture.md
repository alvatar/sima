# Architecture

sima is a substrate for deterministic, reproducible execution of GPU
programs and search over them. A program is opaque compute: the substrate
runs it, records the result under a content address, and never interprets
what it computes. The current specialization is neural networks and
cellular-automata-like programs. `README.md` records
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
| L3    | `sima-contracts` | generator/executor contracts over opaque specs and params; stub generator and executor |
| L4    | `sima-scheduler` | task sources, worker pool, leases, retry, run driver                 |
| L5    | `sima-pipeline`  | orchestration, resume, re-evaluation (planned)                       |
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

The two modes differ only in how they treat a destination that already
holds content:

| Mode | duplicate, identical bytes | duplicate, different bytes |
|------|----------------------------|----------------------------|
| rename — objects | overwrites and succeeds (which copy lands is unobservable) | overwrites and succeeds (cannot arise for objects) |
| exclusive link — index entries, manifests | succeeds, an idempotent re-write | `Corruption` |

The split follows from what can conflict. A content-addressed object cannot:
its path is the hash of its bytes, so racing writers carry identical content
by construction, and an unconditional rename is both correct and cheap —
which writer's copy lands is unobservable, and different bytes never reach
that path. An index entry or a manifest can be handed conflicting content —
two different results claiming one task key, two manifests for one run — so
there the write compares against what already exists: an identical re-write
is idempotent (the path taken on retry and resume), and a differing one is
reported as `Corruption` instead of silently overwriting.

Directories are created with their parent fsynced, so a new directory entry
survives a crash together with the files inside it. POSIX rename and link
atomicity means a reader — including a process resuming after SIGKILL —
observes a complete file or none. Directory fsync is specific to unix, so
the crate builds on unix targets only; the build is refused elsewhere rather
than silently dropping durability across a crash. Leftover `tmp/` files after
a crash are inert; sweeping them is retention work.

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
journal has a single writer, the orchestrator; single-writer-per-run will be
enforced by the orchestrator lease file.

## `sima-contracts` (L3)

The two seams the search substrate runs candidates through, plus deterministic
stub implementations of both. A `Generator` produces a run's candidate specs
from `(root_seed, params, format)`, deterministically. An `Executor`
interprets one format: it receives one candidate and returns what that
evaluation produced. Both are pure compute over `sima-model` values; the crate
depends on `sima-model` and `sima-core` only and never touches the store, so
the trust boundary — executors never reach durable state — is visible in the
crate graph. The worker is what carries executor output into the store.

The distinction the contract encodes in the type system is the split between
identity inputs and execution context. A `TaskInput` carries the identity
inputs — spec, params, seed, environment, and the loaded input-state bytes —
which determine the task key and the committed artifacts. An
`ExecutionContext` carries the attempt number and worker id, which the executor
may read but which never influence a committed artifact. The input-state slot
mirrors the key's `input_state`: the key holds the state object's digest, the
executor receives the bytes; it enables segmented execution, present in the
identity surface here and unused by the stub except as identity.

An execution yields an `Outcome` with three arms: `Completed { artifacts,
stats }` commits; `Failed { reason, stats }` is a transient failure the
scheduler may retry; `Rejected { reason, stats }` is a definitive failure the
scheduler never retries, for a candidate the executor cleanly judges unable to
produce a result. All three are domain outcomes the family owns, so they are
ordinary `Ok` values, distinct from `Err`, which is reserved for an
infrastructure fault such as a spec whose bytes are not a valid program. This
keeps candidate failure out of the shared `sima-core::Error` enum. An
`Artifact` is produced bytes — a name
and a blob the worker stores in the CAS and references from the `TaskRecord`
through a model `ArtifactRef` — and must be a pure function of the identity
inputs. `Stats` is opaque observational bytes destined for the journal; it may
reflect the execution context and never enters a record.

The stub generator and stub executor supply this contract without a GPU or a
store: a spec carries a stub program selecting one behavior — succeed, flaky
(fail a bounded number of attempts, then succeed), reject definitively, panic,
or sleep — so the scheduler has a deterministic, programmable substrate for its
failure matrix. The
stub's committed artifact is the digest of the identity inputs alone, so it
reproduces across attempts and workers; the attempt number folds only into the
stats and into the gate that decides whether the behavior fails this attempt or
completes. That gate is the one sanctioned read of the attempt number, and the
artifact the behavior eventually commits does not depend on which attempt
reached it.

## `sima-scheduler` (L4)

Runs a search from `(RunConfig, store state)`. It is the layer that bridges
pure executor output into durable store state, so the executor trust boundary
lives on its worker seam: the executor returns values, and only the worker
writes to the store. It depends on both `sima-contracts` (to run generators and
executors) and `sima-store` (to commit results); `sima-contracts` itself stays
free of the store, so the boundary holds in the crate graph.

### Task source

A task source derives the currently-runnable frontier from `(config, store
state)`. The static-batch source runs a resolved generator once, stores each
spec object, builds each task identity — spec, params, the per-task seed
`derive(root_seed, i)`, environment, no input state — and separates the keys
the store already answers from those still to run, so a resume runs only the
unfinished work. The frontier is a pure function of `(config, environment)`.
One interface covers this and, later, a segment chain that derives successors
as predecessors commit; the driver polls it in a loop, which is the seam a
dynamic source reuses.

### Worker pool and the outcome classifier

A fixed pool of worker threads, created once inside a scope so they borrow the
store and executor without `Arc`, pulls tasks from a shared FIFO queue. A
worker leases a task, builds its `TaskInput` and `ExecutionContext`, and runs
the executor inside a panic handler wrapping only that call. It then classifies
the outcome, the one place an outcome is turned into an action:

- `Completed` → commit through the store's single commit path (store each
  artifact, then the record), and emit `Committed`.
- `Failed` → retry: re-enqueue at the next attempt until the attempt cap, after
  which the transient failure becomes definitive.
- `Rejected`, or a `Failed` whose retries are exhausted, or a panic escaping
  `execute` → a definitive failure that terminates the run. A panic is caught
  and classified as a rejection with the payload as its reason; a panic
  anywhere else is a scheduler bug and propagates. `Err` from the executor is
  an infrastructure fault and fails the run with an error.

Nothing from the execution context reaches a committed record: the worker
carries only identity into the `TaskRecord`, and the attempt and worker travel
solely to the journal.

### Definitive failure, clean and resumable

The store models success only — a record records artifacts. A definitive
failure is recorded in the journal and terminates the run: no manifest is
written, every committed success remains, and nothing is left that blocks a
re-run. Fixing the cause and re-running the same config re-executes only the
unfinished work. If the fix changes an identity input, the task keys change and
the re-run is a fresh recompute — correct by the determinism model. The run
returns `Finalized` once every task committed and the manifest is written, or
`Failed { task, reason }` on a definitive failure; the two mirror the
executor's own `Ok(Outcome)`/`Err` split one level up.

An infrastructure fault — an executor error, a commit failure, a state-load
failure — outranks a definitive candidate failure: the result path itself
broke, so the run surfaces the error and emits a `Faulted` event for the task
rather than reporting a clean `Failed`. A journal fault, by contrast, yields to
a domain `Failed` outcome: the journal is observational, so a definitive
candidate failure is returned intact even when the journal degraded, and the
journal fault resurfaces on the next run that finalizes over the same store.

### Leases and the watchdog

Leases live in memory — `task → (worker, attempt, leased_at)` — since durable
progress is the committed records; a process death drops all leases and resume
re-derives the frontier. The timeout is a soft target: a watchdog thread scans
the lease table and emits one `TaskOverran` event per lease whose age exceeds
`attempt_timeout`, reporting only. Comparing the lease's age against the timeout
is a duration comparison that cannot overflow, so a timeout larger than any
attempt (for example `Duration::MAX`) simply disables overrun reporting. A
memory-safe runtime has no safe forced thread termination, so forced preemption
requires process isolation and is not yet built; the in-process worker delivers
overrun detection, not termination.

### Journal events

The scheduler owns the journal's meaning. A typed `LifecycleEvent` serializes
to one JSON line, with ids and stats rendered as hex. The vocabulary:

- **run started** — the run began, over every task key of the run, those
  already committed and those still to run.
- **queued** — a task entered the ready queue.
- **leased** — a worker leased a task for one attempt.
- **committed** — a task's result was committed, referencing its record.
- **failed** — an attempt failed transiently and may be retried.
- **retried** — a failed task was re-enqueued for another attempt.
- **rejected** — a task failed definitively and will not be retried.
- **faulted** — an infrastructure fault (an executor error, a commit failure,
  or an input-state load failure) hit a task's attempt; the run terminates
  with an error.
- **task overran** — a lease's age ran past the attempt timeout; detection
  only, no preemption.
- **run finalized** — every task committed and the manifest was written.
- **run failed** — a definitive candidate failure terminated the run; no
  manifest was written.

A single journal-writer thread owns the `JournalWriter` and drains an `mpsc`
channel the workers, watchdog, and driver send to, which is the single-writer
seam the append contract requires. Event arrival order across threads varies
between runs; the journal is observational and excluded from every equality
criterion, so the manifest — sorted by task key at finalize — is byte-identical
across runs regardless.

## Determinism proof obligations

Anything claimed deterministic is proven by test: same config in two fresh
stores → byte-identical manifests; a run killed at any crashpoint and
resumed → manifest identical to an uninterrupted run (crash-injection
harness); re-evaluation touches no executor; a copied store resumes
with an identical manifest.
