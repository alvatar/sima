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
| L3    | `sima-contracts` | generator/executor contracts over opaque specs and params            |
| L4    | `sima-domains`   | per-format executors, generators, codecs, environments, id dispatch, and config translation; the reference stub domain |
| L5    | `sima-scheduler` | task sources, worker pool, leases, retry, run driver                 |
| L6    | `sima-pipeline`  | config loading, orchestration, run status                            |
| L7    | `sima`           | CLI: run, status, tui                                                 |

The store is the only durable state. Queues, schedulers, and orchestrators
are ephemeral: the runnable frontier is derived from (config, store state),
so resume, crash-recovery, and re-run are one code path.

Execution backends sit off the spine as a sibling group under
`crates/toolkits/` (`sima-toolkit-*`). A toolkit is a compute library a domain
computes on, below `sima-domains` and beside `sima-contracts`: it depends on
`sima-core` (and `sima-contracts` when it needs the contract types) and each
isolates its own dependency set. `sima-domains` depends on the toolkits its
executors use. See [Execution toolkits](#execution-toolkits).

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
<root>/runs/<run-id>/orchestrator.lock
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
journal has a single writer, the orchestrator; one orchestrator per run is
enforced by `Store::acquire_run_lock`, the OS file lock on the run's
`orchestrator.lock` file. The kernel releases that lock the instant its
holder exits, however it exits, so no staleness protocol exists. The file's
content (pid, hostname) is diagnostic only — it names the holder in the
validation error a second acquirer receives — and is never consulted for
liveness.

## `sima-contracts` (L3)

The two seams the search substrate runs candidates through. A `Generator`
produces a run's candidate specs from `(root_seed, params, format)`,
deterministically. An `Executor`
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
identity surface here and, for an executor that does not segment, carried
only as identity.

An execution yields an `Outcome` with three arms: `Completed { artifacts,
stats }` commits; `Failed { reason, stats }` is a transient failure the
scheduler may retry; `Rejected { reason, stats }` is a definitive failure the
scheduler never retries, for a candidate the executor cleanly judges unable to
produce a result. All three are candidate outcomes the domain owns, so they
are ordinary `Ok` values, distinct from `Err`, which is reserved for an
infrastructure fault such as a spec whose bytes are not a valid program. This
keeps candidate failure out of the shared `sima-core::Error` enum. An
`Artifact` is produced bytes — a name
and a blob the worker stores in the CAS and references from the `TaskRecord`
through a model `ArtifactRef` — and must be a pure function of the identity
inputs. `Stats` is opaque observational bytes destined for the journal; it may
reflect the execution context and never enters a record.

## `sima-domains` (L4)

The executable substance behind each format id. A `Domain` groups what a
format id binds: the executor that evaluates the format's specs, the
environment that enters task identity, and the translation of the
domain-owned `[run.params]` section into the opaque canonical params bytes.
Generators dispatch separately — one format has one executor but many
generators — and each generator owns the translation of its own
`[run.generator]` keys. Both dispatches are static matches keyed on the id; an
unknown id is a validation error. Each domain's pieces — executor, generator,
codecs, environment, and translation — live in its own module under
`domains/`. The crate depends on `sima-contracts` for the traits and on `toml`
for the translation, and owns the canonical codecs its specs and params hash
through.

### The translation seam

Human-facing TOML becomes the model's opaque canonical bytes only here,
through each domain's own codecs — an identity-bearing encoding is never
hand-rolled at the config layer. The pipeline parses the file structurally and
hands each opaque section (`[run.params]`, the `[run.generator]` keys) to the
domain and generator the ids name; the domain returns the canonical bytes.
`toml` is a dependency of both layers: the pipeline reads the file, a domain
reads its own sections.

### The stub domain

The stub domain supplies the contracts without a GPU or a store, so the
scheduler has a deterministic, programmable substrate for its failure matrix:
a spec carries a stub program selecting one behavior — succeed, flaky (fail a
bounded number of attempts, then succeed), reject definitively, panic, or
sleep. The stub's committed artifact is the digest of the identity inputs
alone, so it reproduces across attempts and workers; the attempt number folds
only into the stats and into the gate that decides whether the behavior fails
this attempt or completes. That gate is the one sanctioned read of the attempt
number, and the artifact the behavior eventually commits does not depend on
which attempt reached it. The pipeline reaches this domain through the id
dispatch and the scheduler tests through a dev-dependency; the shipped
scheduler library never depends on `sima-domains`.

### The stencil kind

The float families — reaction-diffusion, Neural CA, Lenia — share one executor
kind, the **stencil/convolution kind**: a multi-channel float grid advanced by
a WGSL update kernel dispatched over it, each output cell a function of a
neighborhood of the input. Its state, dispatch harness, and cross-check
scaffold live once in the `stencil` module; a family supplies the update
kernel, the genome, and the CPU reference, differing in those and the channel
count, not in the state shape or the harness.

**Grid state.** `Grid` is a 2D, multi-channel `f32` field: extent
$(width, height)$, a channel count, and a cell-major interleaved payload where
channel $c$ of the cell at $(x, y)$ is at index
$((y \cdot width) + x) \cdot channels + c$. It serializes to identity-bearing
bytes through the canonical encoding — a domain tag, the three dimensions as
`u32`, then the payload as a bare `f32` sequence whose length the dimensions
fix — and addresses itself by the blake3 of those bytes. So a grid round-trips
through the content-addressed store as an opaque snapshot object, the address
the store returns equal to the grid's content id. The cells are `f32`; the kind
carries no dtype tag.

**Dispatch harness.** `run` advances a grid by a step count of double-buffered
dispatches. It allocates two device buffers plus a fixed dimensions buffer,
uploads the payload, and for each step dispatches the kernel over the whole
grid — one invocation per cell, $\lceil width \cdot height / 64 \rceil$ groups
along x — then swaps the input and output buffers. The swap after each dispatch
leaves the result on the most recently written buffer for both even and odd
step counts. Each step is one fence-waited submission, reusing the toolkit's
per-op synchronization. The harness is neighborhood-agnostic: a small stencil
and a large-radius convolution are both just the kernel argument.

**Binding and dispatch convention.** The kind's kernels bind group-0 storage
buffers in a fixed order:

- binding 0 the input grid,
- binding 1 the output grid,
- binding 2 the dimensions `[width, height, channels]`,
- bindings 3+ the family parameters.

A kernel runs one invocation per cell under `@workgroup_size(64)`, guards the
cell index against the cell count, and loops its channels internally. The
harness ping-pongs bindings 0 and 1 each step and holds the dimensions and
parameters fixed.

**CPU reference and cross-check.** `StencilRule` is the CPU reference a family's
kernel is checked against: one step maps a whole input grid to a whole output
grid. A family confirms its kernel by advancing the same initial grid through
the reference and through the harness for equal step counts and comparing the
resulting grids. Where a kernel uses only exact operations — a neighborhood max
— the two agree byte for byte with no tolerance; agreement across distinct GPU
backend classes is a separate tolerance policy.

## Execution toolkits

Execution backends live under `crates/toolkits/` as `sima-toolkit-*` crates. A
toolkit is a compute library, not an `Executor`: it holds no store handle and
builds no run identity, so it sits below `sima-domains` and beside
`sima-contracts` in the crate graph. A domain depends on the toolkits its
executors compute on, and each toolkit isolates its own dependency set behind a
surface stated in the developer-facing contract — the authoring language, not
the runtime that powers it.

### `sima-toolkit-wgsl`

Runs WGSL compute kernels on Vulkan without a domain author writing raw Vulkan
or seeing an `ash` or `vk` type. Kernels are authored in **WGSL** and compiled
to **SPIR-V** in process with `naga`; `ash` drives Vulkan 1.3, and the system
Vulkan loader is opened at runtime, so the crate builds with no native
toolchain. The surface is three types:

- **`Context`** — owns the Vulkan instance, logical device, compute queue, and
  command pool for one headless compute session, and is the single owner of the
  true Vulkan lifetime. It allocates buffers, compiles kernels, uploads and
  downloads bytes, and dispatches.
- **`Buffer`** — a device-local storage buffer. Host transfers go through a
  per-transfer host-visible staging buffer; there is no pooled allocator.
- **`Kernel`** — a compute pipeline compiled from WGSL for one entry point,
  plus the identity inputs it surfaces.

**Device selection** keeps devices exposing a compute queue family and picks
deterministically by type — discrete, then integrated, then virtual, then CPU,
then other — with the lowest enumeration index breaking ties;
`SIMA_GPU_DEVICE` overrides the pick by index. Validation is opt-in under
`SIMA_VULKAN_VALIDATION` and off at zero cost otherwise.

**Ownership** follows a wait-idle-before-drop contract. `Buffer` and `Kernel`
each hold a cloned `ash::Device` and free their own Vulkan objects on drop; the
`Context` outlives them and drains the device before its own teardown, so no
per-object drop synchronizes. Construction rolls back through per-object guards,
so a mid-build failure orphans nothing.

**Synchronization** favors obvious correctness: each upload, dispatch, and
download is a separate one-time command buffer, submitted to the one compute
queue and fence-waited before the next, with a leading buffer memory barrier
carrying the dependency between stages.

**Binding model.** The group-0 storage buffers a kernel declares are reflected
from the parsed `naga` module and become one descriptor set of
`STORAGE_BUFFER` bindings; a dispatch binds one buffer per binding in ascending
order. Kernels take no push constants or uniforms — a domain that needs
parameters passes them as an additional storage buffer, and in-shader bounds
come from `arrayLength`.

**Identity surface.** The toolkit does not build an `Environment`; it surfaces
the two inputs a domain records for a kernel: the **source digest** (blake3 of
the WGSL bytes) and the **compiler id**, a canonical string naming the compiler
and the output-affecting options. This places a WGSL-on-Vulkan domain at the
content-reproducible tier (README, Determinism): its identity is the shader
source plus the compiler that produced it. A known-answer test pins the emitted
SPIR-V so a `naga` change that shifts output fails the build and forces a
deliberate compiler-id update.

## `sima-scheduler` (L5)

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

### Run control: observer and interrupt

The driver takes a `RunControl` — the caller's handles into a running
search:

- **observer** — invoked with each typed event on the journal-sink thread,
  immediately after the event's line is appended: typed events, journal
  order, one calling thread. Progress rendering consumes this seam.
- **interrupt** — a level-triggered flag the driver polls within a bounded
  wait. Once set, the run winds down gracefully: no more tasks are handed
  out, in-flight attempts finish and commit, queued tasks are abandoned,
  and the run returns `Interrupted` with no manifest written — the store
  stays resumable and the next orchestration continues the abandoned work.

The wind-down states form a precedence order — running < interrupted <
failed < fault — and each setter only upgrades: a definitive failure or an
infrastructure fault landing during an interrupt wind-down still decides
the run, and among faults the first wins.

### Leases and the watchdog

Leases live in memory — `task → (worker, attempt, leased_at)` — since durable
progress is the committed records; a process death drops all leases and resume
re-derives the frontier. The timeout is a soft target: a watchdog thread scans
the lease table and emits one `LeaseExpired` event per lease whose age exceeds
`attempt_timeout`, reporting only. Comparing the lease's age against the timeout
is a duration comparison that cannot overflow, so a timeout larger than any
attempt (for example `Duration::MAX`) simply disables expiry reporting. A
memory-safe runtime has no safe forced thread termination, so forced preemption
requires process isolation and is not yet built; the in-process worker delivers
lease-expiry detection, not termination.

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
- **lease expired** — a lease's age ran past the attempt timeout; detection
  only, no preemption.
- **run finalized** — every task committed and the manifest was written.
- **run failed** — a definitive candidate failure terminated the run; no
  manifest was written.
- **run interrupted** — the caller interrupted the run: in-flight attempts
  drained and committed, no manifest was written, and the store is
  resumable.

A single journal-writer thread owns the `JournalWriter` and drains an `mpsc`
channel the workers, watchdog, and driver send to, which is the single-writer
seam the append contract requires. Event arrival order across threads varies
between runs; the journal is observational and excluded from every equality
criterion, so the manifest — sorted by task key at finalize — is byte-identical
across runs regardless.

## `sima-pipeline` (L6)

The layer a person's configuration enters: it loads `sima.toml`, translates
it through the domain and generator the config names, and drives the
scheduler over the configured store.

### Identity and execution in the file

The config file carries the same split the model enforces:

- **`[run]`** — the identity section, canonicalized into `RunConfig`, so
  its fields define the `RunId`: the root seed, the format, the generator
  (its id plus the generator-owned keys), and the domain-owned run params.
- **`[execution]`** — the operational section: the store path (resolved
  relative to the config file's directory), worker count, attempt cap, and
  an optional attempt timeout whose absence disables lease-expiry
  reporting. Never hashed — a run resumed with different execution
  settings keeps its id.

The structural keys are strict: an unknown key at any level is a validation
error naming it.

### Config routing

The pipeline parses the file's structure and routes each config section to
the code that owns it, never interpreting the content itself: the format and
generator ids dispatch through `sima-domains` (see L4), and the opaque
`[run.params]` and `[run.generator]` tables pass to the domain and generator
translations that turn them into canonical bytes. Identity-bearing bytes are
produced only by those codecs, never hand-rolled here.

### Orchestration

`orchestrate` opens the store (creating it where missing), takes the run's
orchestrator lock, dispatches the domain and the generator, and calls the
scheduler; the lock is held for the whole call and releases on return.
Resume and re-evaluation are this same call — the frontier re-derives from
store state, so an interrupted or failed run continues where it stopped,
and a finalized one re-finalizes idempotently without touching an executor.

### Run status

`status` computes a run's observable state from its journal alone. The
counters — tasks, committed, retried, rejected, faulted, lease expiries —
sum across every resume segment, and the last run-level event decides the
state. A journal ending mid-run reads as in progress: a dead orchestrator
is indistinguishable from a live one by the journal alone.

## `sima` (L7)

The CLI holds no orchestration logic — parsing, rendering, signal
registration, exit codes, and, for `tui`, an interactive terminal
frontend over the observer seam:

- **`sima run <config.toml>`** — drives the configured run, printing one
  plain line per meaningful event from the observer seam. SIGINT sets the
  interrupt flag for a graceful wind-down; a second SIGINT falls through
  to default death, which is exactly the crash the recovery guarantees
  cover.
- **`sima status <config.toml>`** — prints the status block. The config
  file is the one argument: its execution section names the store and its
  identity section derives the run id.
- **`sima tui <config.toml>`** — drives the same run inside a full-screen
  terminal UI: an idle screen lists the configured workers, a keypress
  starts the run, and the tui applies each observer event as it arrives, so
  the worker rows and counters update live, with keys to wind the run down
  gracefully or leave and a `?` overlay listing every binding.
  It requires a terminal; with stdout not a TTY it exits 1. `ratatui`
  and its `crossterm` backend are the terminal-UI dependencies, and they
  enter the workspace at this layer alone.

Exit codes (shared across `run` and `tui`):

- **0** — the run finalized (or `status` answered);
- **2** — a definitive candidate failure;
- **130** — interrupted, store resumable;
- **1** — everything else: infrastructure fault, config error, usage error.

## Determinism proof obligations

Anything claimed deterministic is proven by test: same config in two fresh
stores → byte-identical manifests; a run killed at any crashpoint and
resumed → manifest identical to an uninterrupted run (crash-injection
harness); re-evaluation touches no executor; a copied store resumes
with an identical manifest.

The crash harness rides the crashpoint facility: with the `crash-injection`
cargo feature compiled in, the `SIMA_CRASHPOINT` environment variable arms
one named point (`name`, or `name:k` for the k-th hit) and the process
SIGKILLs itself on reaching it; without the feature the planted calls
compile to nothing.
