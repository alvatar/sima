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
| L0.5  | `sima-trace`     | structured events: the typed vocabulary, journal records, emitters, the collector |
| L1    | `sima-model`     | identity vocabulary: spec, params, environment, task key, record, run config |
| L2    | `sima-store`     | durable state: CAS, task index, run manifests, journals               |
| L3    | `sima-contracts` | generator/executor contracts over opaque specs and params            |
| L4    | `sima-transport` | worker transport: wire protocol, executor host loop, subprocess and loopback links |
| L5    | `sima-domains`   | per-format executors, generators, codecs, environments, id dispatch, and config translation; the reference stub domain |
| L6    | `sima-scheduler` | task sources, worker pool, leases, retry, device placement, run driver |
| L7    | `sima-pipeline`  | config loading, orchestration, run and per-task queries               |
| L8    | `sima`           | CLI: run, status, report, timeline, rm, tui                           |

Beside the spine, `sima-worker` is the worker binary: it depends on
`sima-transport` (the executor host loop) and `sima-domains` (the executor
its format id resolves to), and nothing depends on it. The orchestrator
spawns one `sima-worker` process per worker slot.

The store is the only durable state. Queues, schedulers, and orchestrators
are ephemeral: the runnable frontier is derived from (config, store state),
so resume, crash-recovery, and re-run are one code path.

`sima-provider` also sits beside the spine, above the store: it is the
rented-hardware control plane, depending on `sima-core`, `sima-model`, and
`sima-store`, and it carries the contract, the offer model, and the
in-memory stub. Backends speaking to a real service are a sibling group
under `crates/providers/`, named `sima-provider-<name>` after the service
each one speaks to, so an HTTP client enters exactly one crate. See
[`sima-provider`](#sima-provider).

Execution backends sit off the spine as a sibling group under
`crates/toolkits/` (`sima-toolkit-*`). A toolkit is a compute library a domain
computes on, below `sima-domains` and beside `sima-contracts`: it depends on
`sima-core` (and `sima-contracts` when it needs the contract types) and each
isolates its own dependency set. `sima-domains` depends on the toolkits its
executors use. See [Execution toolkits](#execution-toolkits).

## Anatomy of a run

The component hierarchy of one running `sima run` process, with the store it
drives. Later sections describe each component in detail.

```
sima run  (one orchestrator process per run; OS file lock on the store)
│
├─ Store ················ the only durable state
│    objects/                 content-addressed bytes (specs, params,
│                             states, artifacts)
│    tasks/<key>              committed task records
│    runs/<run-id>/
│      manifest.json          written once, at finalize
│      journal                append-only lifecycle events
│      checkpoint/<chain>     mutable resume scratch, one slot per chain
│      placement/<chain>      the device class each chain's work runs on
│
└─ Driver ··············· polls the source on queue drain, feeds the
   │                      queue, finalizes on empty poll + idle pool
   │
   ├─ TaskSource ········ derives the runnable frontier from (config, store)
   │   ├─ StaticBatch        segments absent: one stateless task per candidate
   │   └─ SegmentChain       segments = N: one chain per candidate
   │       └─ Chain ····· the source's in-memory cursor over one
   │           │          candidate's chain — no thread, no lease, no
   │           │          durable form of its own:
   │           │            spec, spec_id   the candidate under evaluation
   │           │            seed            constant across the chain
   │           │            frontier        next uncommitted segment's
   │           │                            identity; None when walked out
   │           │            committed       segments walked past so far
   │           │            handed_out      frontier is out with a worker
   │           │
   │           └─ Segment (yielded one at a time, as RunnableTask)
   │                key = hash(spec, params, seed, env, input_state)
   │                segment 0:   input_state = None
   │                segment j+1: input_state = hash of segment j's
   │                             committed "state" artifact
   │
   ├─ Coordinator ······· in-memory queue + leases + release counter
   ├─ Collector ········· one thread: events → stamp → journal → observer
   │
   └─ Worker pool (N threads, stateless leaseholders)
       └─ Worker ········ leases one task = drives one attempt on its child
           │
           ├─ WorkerLink · the conversation with one sima-worker process:
           │               assign over its stdin, events off its stdout,
           │               the attempt deadline as the wait bound, SIGKILL
           │               as preemption
           │
           │      sima-worker (child process, pure compute)
           │        └─ Executor ·· execute(input, ctx, checkpoint); offers
           │                       continuation bytes at its own cadence,
           │                       never touches the store — it has no path
           │
           ├─ checkpoint slot ··· parent-side: loads resume bytes before
           │                      assignment, persists each received save
           │                      to checkpoint/<chain>
           │
           └─ commit ···· artifacts → objects, then the task record
```

A chain exists durably only as the trail of committed records linked by
`input_state` hashes in the store; `Chain` is the cursor `SegmentChain`
rebuilds from that trail at construction, which is why crash recovery,
interrupt resume, and re-run are one code path.

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

## `sima-trace` (L0.5)

The structured-event facade: one typed vocabulary any layer can emit,
funneled through one collector into the run's journal and out to the
run's live observer. The crate sits directly above `sima-core`, so scheduler,
transport, and worker host all emit without an upward edge. Events are
observational — they record what happened, never run identity — so their
serialization world is serde, and the stream is excluded from every
equality criterion.

- **`Event`** — the vocabulary: the run-lifecycle variants (see
  [Journal events](#journal-events)) plus `Diagnostic`, a correlated line
  of observational text: a level (`info`/`warn`/`error`), a source (for
  example `worker stderr`, `panic`, `transport`), the message, and optional
  `worker`/`host`/`task` context keys — optional because a diagnostic may
  precede any lease or follow the run's end.
- **`Record`** — the journal line type: a `ts_ms` wall-clock stamp plus
  the event, flattened, so the line is flat — the event's own keys sit
  beside `ts_ms` at the top level. The stamp is required: a line lacking
  it is a parse error.
- **`Emitter`** — a cloneable channel handle; `emit` is fire-and-forget,
  and a closed channel drops the event silently. Components that emit
  receive an emitter explicitly at construction — there is no process
  global and no thread-local propagation. Causality context is the natural
  keys: events carry `run`, `task`, `attempt`, `worker`, and `host`
  directly as fields.
- **`Collector`** — one scoped thread drains the channel; for each event
  it stamps `ts_ms` (a single clock, read at append time — remote events
  are stamped on arrival), appends the record's line through a
  **`DurableSink`**, then hands the record to the run's observer. The
  ordering guarantee: the journal write for an event happens before the
  observer sees it, and the observer sees records in journal order, from
  one calling thread. The first append or encoding
  failure stops the collector and surfaces when it is joined.

`DurableSink` is the seam that keeps the crate below the store:
`sima-store` implements it for its journal writer, so the collector appends
through the same crash-safe path as any other journal line.

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
  count, store path, or hardware. The optional segment count is part of it:
  work division is structural, so runs of different segmentation are
  different runs. `TaskRecord` holds identity plus artifact
  references only — attempts, timings, and workers live in the journal.
- Names (format ids, generator ids, component and artifact names): 1..=64
  bytes of `[a-z0-9._-]`.

## `sima-store` (L2)

One `Store` type over a root directory:

```
<root>/objects/<aa>/<hash>       CAS object bytes; aa = first two hex chars
<root>/tmp/<pid>-<seq>           in-flight writes
<root>/tasks/<task-key>          index entry: record-hash hex + newline
<root>/instances/<tag>           one rented instance's ledger record
<root>/spend/<owner>/<tag>-<started-ms>  one closed rental's cost
<root>/runs/<run-id>/manifest.json
<root>/runs/<run-id>/journal
<root>/runs/<run-id>/orchestrator.lock
<root>/runs/<run-id>/checkpoint/<slot>   mutable resume scratch
<root>/runs/<run-id>/placement/<chain>  the chain's device class
<root>/runs/<run-id>/remove-intent      a removal's resumable plan
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
- **checkpoint slot** — a run's mutable per-chain scratch file holding the
  latest continuation state a running segment offered. Written atomically,
  latest-only, never hashed and never part of a manifest or closure; a slot
  that is missing, torn, or keyed to another task loads as nothing.
- **closure** — the deduplicated, sorted set of objects a finalized run
  depends on: config, records, and every object those records reference. The
  unit of run portability and store sync.
- **spend entry** — what one rental cost, from its ledger record's stamp to
  its confirmed destruction. It is written when the rental is closed out and
  outlives the record, so a run's total spend survives every machine it
  rented.
- **removal** — deleting a run and every object no surviving run's closure
  references, guarded so an object a live run needs is never removed. The plan
  is recorded in the run's `remove-intent` slot before any deletion, so a
  removal interrupted by a crash resumes to the same end state.

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
owned by the emitting layers above the trace facade. The store owns the
framing only: a payload is one nonempty line free of embedded line breaks;
appends are single-write, newline-terminated, fsynced. On read, a torn
final line (bytes past the last newline) is ignored; invalid UTF-8 inside
the intact region is corruption. Journals legitimately differ between
identical runs and are excluded from every equality criterion.

Each line is a `sima-trace` `Record`: the collector's `ts_ms` wall-clock
stamp plus one event — a lifecycle event or a `diagnostic` line. The
collector stamps every line it writes, so a line lacking `ts_ms` is
corruption. The store implements the collector's `DurableSink` seam on its
journal writer, which is how records reach this framing.

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

## `sima-provider`

The seam between a run and the machines it rents. A provider lists a
marketplace, rents one machine, reports its state, and destroys it; the
crate turns that into an owned instance with guaranteed teardown.

The contract targets any service meeting the obligations the `Provider` trait
module documents, whether it is a peer-to-peer marketplace or a first-party
cloud renting from a fixed catalog. Two normalizations carry that breadth: a
backend billing in another currency converts to micro-USD as part of its own
configuration, and a first-party backend, whose hosts are its own datacenter
machines, states full reliability and verified status where a marketplace
reports host trust.

### The offer model

`offers` returns concrete offers, never instance types: **(GPU model, VRAM,
GPU count, $/hr, host reliability, verified status, disk, bandwidth,
location, offer id)**, normalized across providers. A marketplace is the
general case — a fixed type catalog degenerates into one offer per type at
the type's list price — so the model that carries a marketplace carries
both.

Prices are integer micro-USD per hour (`Price`), giving ranking a total
order without float comparison, at a granularity that covers sub-cent
marketplace rates. Reliability stays `f64` in $[0, 1]$ as providers report
it, threshold-compared and never ordered.

### Selection

Selection is two separate steps, and keeping them separate is the design:

- **Hard constraints disqualify.** Minimum VRAM, GPU count, disk and
  bandwidth; a maximum price; a reliability floor; verified hosts only; and
  an any-of list of acceptable GPU models, matched case-insensitively by
  substring — the rule `[[execution.device]]` selectors already use for
  hardware names. Each optional constraint judges only when set.
- **One scalar objective ranks.** `Objective::CheapestPerHour` sorts
  ascending by price, ties broken by offer id, so the order is
  deterministic. There is no weighted multi-criteria score: the reason one
  offer outranks another is always a single comparison.

### The acquisition loop

`acquire` takes the acquiring run's orchestrator lock by reference, and the
ledger record's owner is stamped from it. The lock is the capability: a
reference to it proves the run holds it, which is what reconciliation reads
as the owner still running, here and for every other live run.

It then reconciles, lists, ranks, and walks the ranked offers:

```
reconcile ── destroy what an earlier crash left running
offers  ──── the live marketplace, normalized
select  ──── constraints disqualify, the objective ranks
for each ranked offer:
    write the intent record         (ledger, state: intent)
    provision(offer, tag)
        OfferGone ───────────────── clear the record, next offer
        error ───────────────────── abort, leaving the intent record
        Provisioned(instance) ───── upgrade the record (state: live)
            poll until ready
                Ready ───────────── return the guard
                gone or timed out ─ destroy, clear the record, next offer
list exhausted ──────────────────── Error::Provider
```

A lost offer is an outcome, not a failure: on a marketplace another renter
taking a machine first is normal operation, and the next-ranked offer is the
answer. An API error is different — it would repeat against every remaining
offer — so it aborts the loop and propagates, and the attempt's intent
record stays behind: an error answer does not say whether the request
landed, so a machine that may carry the tag must remain discoverable, and
reconciliation resolves it. A machine that never reports
itself ready is a bad offer: it is destroyed and the walk continues.
Readiness is the provider's own answer, carrying the SSH endpoint; whether
sshd is listening is the bootstrap layer's question.

### The teardown guarantee

A rented machine costs money for as long as it runs, so it is torn down on
success, on failure, and on interrupt. Three tiers cover the exits:

- **In process.** `InstanceGuard` owns the instance. `release` is the
  deliberate path and reports what failed; drop is the backstop that covers
  an error returning through `?` and a panic unwinding, discarding the
  outcome because a destructor has nowhere to report it. Teardown also
  closes the rental's spend entry, so what the machine cost outlives it.
- **Interrupt.** The first SIGINT is a graceful wind-down, so it unwinds
  through the guard like any other exit.
- **Crash.** SIGKILL or power loss runs no code at all. What covers it is
  durable state: the ledger, plus reconciliation.

### The ledger and its write ordering

One record per acquisition attempt lives in the store at
`instances/<tag>`, placed with the store's atomic write. The tag —
`sima-<owner16>-<pid>-<rand8hex>-<seq>` — is both the ledger key and the
label the provider attaches to the machine, so record and machine carry one
name. It is an operational identifier and enters no hash. The random
component is drawn once per process from OS entropy, which is what keeps two
processes from producing one tag: a pid the operating system recycles, with
a per-process attempt counter that starts at zero, would otherwise reproduce
the tags of a process that died.

**The intent record is durable before the provider is called.** That
ordering is the whole crash argument: at every point where a process can
die, the orphan is discoverable.

| Crash point            | What is left                            | What reconciliation does                        |
|------------------------|-----------------------------------------|-------------------------------------------------|
| before the intent write| nothing                                 | nothing to do                                   |
| after the intent write | an intent record, perhaps a machine carrying the tag | scan for the tag, destroy what it finds, close the rental out |
| after the live write   | a record naming the instance            | destroy the instance, close the rental out      |
| after the destroy      | a record whose machine is gone          | close the rental out                            |

The record's `created_ms` is the accounting anchor: it stamps where the
rental's charged window opens, and the window closes when the rental's
spend entry is written at `spend/<owner>/<tag>-<started-ms>`. Closing a
rental out always precedes clearing its record, so a record is removed only
once what it cost is durable.

### Reconciliation

`reconcile` runs at the start of every acquisition, so orphans stop costing
money before a new machine is paid for, and is public so a command can
invoke it on its own. It considers only records naming the given provider.
Owner liveness is the run's orchestrator lock: the kernel releases it the
moment its holder exits, so a free lock means the owning process is gone.

| Record state | Provider says           | Owner run lock | Action                               |
|--------------|-------------------------|----------------|--------------------------------------|
| live         | instance exists         | held           | keep: a running orchestrator owns it  |
| live         | instance exists         | free           | destroy instance, close the rental out |
| live         | instance gone           | free           | close the rental out                  |
| intent       | —                       | held           | keep: an acquisition is in flight     |
| intent       | tag scan finds instance | free           | destroy it, then close the rental out |
| intent       | tag scan finds nothing  | free           | close the rental out                  |

Every orphan this pass reaps leaves its spend entry behind. The last row
charges a machine the scan never found, because a close-out lost to a crash
and a provision that never landed leave the same state, and overcounting a
phantom attempt is the safe direction to be wrong in.

The scan carries each instance's rate, and a rental closed out from a
machine it found is charged that rate: it is what the marketplace bills, so
the entry follows the bill in both directions. The record's own rate stands
where the scan found no machine, and where the listing states no rate.

A record is judged by its owner's lock alone, so a run holding its lock
keeps every record naming it — including one an earlier process of that same
run left behind. Such a leftover is indistinguishable from a machine the
live process is using, and it is reaped like any orphan once the run's lock
is free.

One pass is one ledger scan plus one provider instance listing. A ledger
holding no record for the provider reaches no provider API at all, so a store
holding no rentals never depends on a provider being reachable.

### The budget guard

A run's rentals are bounded by a `Budget`: a ceiling on total spend, and a
ceiling on the wall-clock the rental phase may span. Both are optional, and
an absent one is unlimited. The spend cap is cumulative over the whole run,
across every rental and every provider — one pool of money — which is a
different question from the per-offer `$/hr` cap among the selection
constraints. The wall-clock ceiling is anchored at the run's first rental,
derived from durable state, so it survives a crash and a resume.

**The charged window** of one rental opens at its ledger record's stamp,
written before the provider is called, and closes when the rental is closed
out — at teardown, or at the reconciliation pass that cleans up after a
crash. Spend is our own clock times the rate the entry books — the rate the
reconciliation scan's listing states for the machine, and the rate the record
carries wherever there is no such listing; the provider's billing API is
never read. Every systematic path counts at least what the provider bills:

- **Rounding is up.** `Cost` is micro-USD, the unit `Price` states rates in,
  and every started fraction of an hour-rate charge counts in full.
- **A record's window opens before its machine exists.** The interval
  between the record write and the provider's answer is charged, though no
  machine was running for it.
- **An intent record carries the offer's rate**, since its writer died
  before the provider named the machine's own. That guess is what the entry
  books only when the machine is absent from the reconciliation scan or its
  listing states no rate; otherwise the listed rate replaces it.
- **A rental whose close-out was lost is charged again**, from the same
  window, with a later end.

**Close-out** writes the entry and then clears the record. Every path that
clears a record writes an entry first, except an offer another renter took:
there the provider itself answered that no machine exists. The entry is
keyed by the rental's tag and start stamp, both read from the record, so
closing one rental twice reproduces one key and overwrites. Keying by tag
alone would let a later rental's close replace an earlier one's entry, which
is the one error direction this design forbids. Two rentals can only share a
key by sharing a tag, and the tag's per-process random component makes that
impossible across processes: within one process the attempt counter
separates them.

Two crash windows follow from that ordering, and neither loses a charge:

- **Between the entry write and the record clear**, both exist; the next
  reconciliation pass finds the record with its machine gone and rewrites
  the entry under the same key, with a later end.
- **Between the destroy and the entry write**, the record remains and its
  machine is gone; the next pass writes the entry then.

**Accrual** is one fold over durable state: the sum of closed entries' costs,
plus every record of the run charged from its stamp to now. A record whose
tag and stamp already have an entry is that entry's own, pending removal,
and is left out — which is what keeps the two windows above from
double-counting. `spend_report` is that fold, and `assess` reads its verdict
off it: `Within`, carrying the accrued total and the deadline when one
exists, or `Exhausted`, naming the spend cap or the deadline that was
reached. A limit is reached at equality, so a budget exactly consumed admits
nothing further; when both are, the spend is reported.

`acquire` takes the budget and refuses an exhausted one with
`Error::Provider` naming the limit and the numbers — once before the
marketplace is listed, and again before each attempt of the walk, so a
machine that consumed the budget during a failed readiness wait is not
followed by another rental. Reconciliation still runs first: destroying
orphans stops spending, which matters most precisely when the budget is
gone.

Admission compares what stands now and projects nothing: how long the
rental being admitted will run is unknowable at that point. **The guard
supplies the verdict; the enforcement cadence belongs to its caller** — the
orchestrator that polls `assess` while a fleet runs and tears the fleet down
when it reports exhaustion.

### The stub provider

`StubProvider` is an in-memory marketplace with scriptable behavior — a lost
offer, a machine that stays provisioning, a failing API — and is public so
that the layers above can test their acquisition paths against it.

### Image delivery

A rented machine boots the worker image, and the host pulls it from a
registry before the container exists: the image is published to ghcr.io
under the repository owner's account, public, so a create request carries no
pull credentials and no registry credential ever reaches a provider.
Shipping the image to each instance after boot is rejected because the pull
happens before there is anything to ship it to, and because a
multi-gigabyte transfer per instance is bounded by the uplink it would leave
from.

### `sima-provider-vast`

The Vast.ai backend, under the `crates/providers/` namespace that mirrors
`crates/toolkits/`: it holds the workspace's HTTP and JSON dependencies and
nothing else depends on it. It implements the contract above and adds
nothing to it — five methods over five endpoints:

| Contract method | Endpoint                          |
|-----------------|-----------------------------------|
| `offers`        | `POST /api/v0/bundles/`           |
| `provision`     | `PUT /api/v0/asks/{offer}/`       |
| `instance`      | `GET /api/v0/instances/{id}/`     |
| `instances`     | `GET /api/v1/instances/`          |
| `destroy`       | `DELETE /api/v0/instances/{id}/`  |

Three decisions shape it:

- **The query narrows only to the backend's scope.** The search asks for
  rentable machines rented on demand, and everything it answers is
  normalized and handed up. Constraints and ranking stay in
  [Selection](#selection), so what disqualifies an offer is one code path
  across every backend.
- **The API key is read from the environment.** Run configuration is
  content-addressed and identity-bearing, so a key placed there would enter
  run hashes and the store. The backend reads `VAST_API_KEY` at
  construction and sends it as a bearer token.
- **An instance carrying no label is omitted from the listing.** A ledger
  record exists only for a tag this backend wrote, so an unlabeled instance
  corresponds to no record, and the tag is the only key reconciliation has.
  For the same reason only the status a missing instance answers with reads
  as `Gone`: any other failing answer is an error, since reading it as
  absence would report a machine still running and billed as destroyed.

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

The third `execute` parameter is the attempt's `Checkpoint` handle — the
crash-resume channel under the same discipline. The executor decides what
bytes capture its continuation state and when a step boundary makes them safe
to offer; the handle decides whether an offer is written and performs all
I/O, so executors still never touch durable state. `resume` serves bytes a
previous attempt saved, and using or ignoring them must yield byte-identical
committed artifacts. Stateless call sites pass the inert `NoCheckpoint`. The
crate also fixes the state-artifact convention: a segmented executor commits
its continuation state under the artifact name `STATE_ARTIFACT` (`"state"`),
the name the chain source walks.

## `sima-domains` (L5)

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
bounded number of attempts, then succeed), reject definitively, panic, sleep,
or accumulate, the stateful behavior segmented runs chain through: k steps of
`acc = derive(acc, step)` keyed by the absolute step index, committing the
resulting `(step, acc)` state under the `state` artifact and offering it as a
checkpoint at every step boundary. The absolute-step keying makes the
trajectory invariant under where the segmentation cuts fall, which is what
the segmented-equals-unsegmented acceptance proof leans on. The stub's committed artifact is the digest of the identity inputs
alone, so it reproduces across attempts and workers; the attempt number folds
only into the stats and into the gate that decides whether the behavior fails
this attempt or completes. That gate is the one sanctioned read of the attempt
number, and the artifact the behavior eventually commits does not depend on
which attempt reached it. The pipeline reaches this domain through the id
dispatch and the scheduler tests through a dev-dependency; the shipped
scheduler library never depends on `sima-domains`.

### The cellular kind

The float families — reaction-diffusion, Neural CA, Lenia — share one executor
kind, the **cellular kind**: a multi-channel float grid advanced by
a WGSL update kernel dispatched over it, each output cell a function of a
neighborhood of the input. Its state, dispatch harness, and cross-check
scaffold live once in the `cellular` module; a family supplies the update
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

**CPU reference and cross-check.** `CellularRule` is the CPU reference a family's
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

## `sima-scheduler` (L6)

Runs a search from `(RunConfig, store state)`. It is the layer that bridges
pure executor output into durable store state, so the executor trust boundary
lives on its worker seam: the executor returns values, and only the worker
writes to the store. It depends on `sima-contracts` (to run generators and
executors), `sima-store` (to commit results), and `sima-transport` (the worker
seam it drives); `sima-contracts` itself stays free of the store, so the
boundary holds in the crate graph.

### Task source

A task source derives the currently-runnable frontier from `(config, store
state)`. One interface covers both sources, selected by the run config's
segment count; a source returns each runnable task exactly once across the
run, and its key set is complete once a poll has returned empty at an idle
pool — the finalize point.

The static-batch source runs a resolved generator once, stores each spec
object, builds each task identity — spec, params, the per-task seed
`derive(root_seed, i)`, environment, no input state — and separates the keys
the store already answers from those still to run, so a resume runs only the
unfinished work. The frontier is a pure function of `(config, environment)`.

The driver polls the source whenever the ready queue is empty and a lease
release has happened since the last poll — a release is the only point in a
run where the store state a source derives new tasks from can have changed —
so a chain's successor is handed out the moment its predecessor commits,
leases outstanding or not, and the bounded-wait wakeups carry no polling
storm. An empty poll finalizes only at an idle pool; with leases outstanding
it waits, and the releases of those leases trigger the polls that follow.

### Segmented execution

Segmentation is the committed work-division mechanism: a long simulation runs
as a chain of tasks, each advancing state $S_n$ by its segment's steps and
committing $S_{n+1}$ as a store object. The segment-chain source materializes
one chain per generated spec:

- **Chain identity** — chain $i$'s seed is `derive(root_seed, i)`, the same
  substream a static batch would give candidate $i$, constant across the
  chain's segments; segments are distinguished by `input_state` alone.
  Segment 0 carries no input state (the executor initializes from spec,
  params, and seed); segment $j{+}1$'s input state is the object hash of
  segment $j$'s committed `state` artifact. A chain has exactly the config's
  segment count of tasks.
- **The state-artifact convention** — a segmented executor commits its
  continuation state under the artifact name `state`. A committed segment
  record without it is a validation fault naming the artifact and the task
  key: the misconfiguration signal for a segmented run over a stateless
  domain.
- **Resume is construction** — the source fast-forwards each chain against
  the store at construction, walking past every already-committed segment,
  and re-runs that walk whenever a handed-out segment may have committed.
  The frontier is derived from `(config, store state)`, so crash recovery,
  interrupt resume, and re-run are the one code path.
- **Convergence** — identities are content-addressed, so a state fixed point
  (or cycle) makes later segments reuse earlier keys: the walk stays bounded
  by the segment count, already-committed successors advance instantly, and
  the key set deduplicates, satisfying finalize's duplicate rejection.
  Cross-run reuse of chain prefixes in a shared store is the same mechanism:
  a longer run over a store holding a shorter run's chain re-executes only
  the segments past the shared prefix.

### Resume checkpoints

Checkpoints are the disposable crash-resume mechanism, orthogonal to
segmentation: during a segment's execution the running task periodically
writes its full continuation state to the run's per-chain slot, overwriting
the previous write, and a restarted attempt continues from the saved bytes
instead of the segment's start.

- **The slot** — `runs/<run-id>/checkpoint/<chain>`, owned by the store and
  written through its one atomic-write path. Latest-only: the next save, and
  the next segment's saves, overwrite it; nothing is ever deleted.
- **Staleness** — the slot frame carries the owning task's key. A slot that
  is missing, malformed, or keyed to another task loads as nothing, never an
  error — the previous segment's leftover state cannot leak into the next.
- **The contract split** — the executor offers continuation bytes at step
  boundaries and may adopt served resume bytes; the cadence (the first save
  becomes due one full interval after the attempt starts) is evaluated in
  the worker process, so offers stay in-process and only due saves cross
  the pipe; the parent loads the slot before assignment and performs every
  write. A save or load failure emits a `checkpoint degraded` journal event
  and execution continues.
- **Disposability** — checkpoint bytes enter no hash, no record, and no
  manifest; the committed result is byte-identical with checkpointing on,
  off, or resumed. Checkpoints change recovery time only, so losing one
  costs a partial re-execution and nothing else.

### Worker transport

Execution happens in worker processes: the parent spawns one `sima-worker`
child per worker slot, alive until the run ends and replaced only when it
dies. The child hosts the domain executor and nothing else — it is never
given the store path, so the pure-compute executor invariant is OS-enforced.
The seam is two traits the scheduler is written against: `WorkerTransport`
spawns workers, `WorkerLink` converses with one; the production transport
spawns subprocesses, and a loopback test transport runs the same host loop
and wire protocol over in-memory pipes. The traits, the wire protocol, the
executor host loop, and both transports live in `sima-transport` (L4),
beside the contract whose vocabulary the protocol carries across the process
boundary; the scheduler consumes the traits, and `sima-worker` the host
loop.

The wire protocol frames messages on the child's stdin and stdout: a `u32`
little-endian payload length, then a payload built with the canonical
`Enc`/`Dec` primitives — used for their checked framing; frames are
transport encoding, never identity-bearing. The handshake carries the
protocol version, the slot's worker id (so the child can attribute events
without a side channel), the run's format id (the child resolves its
executor from it, once), the checkpoint cadence, and the device the child
computes on; each task is one `Assign` frame answered by zero or more
`Save` frames and one terminal frame, and `Event` frames may interleave
anywhere after `Ready`. There is no shutdown message: the parent closing
the child's stdin is the shutdown signal. A per-child reader thread decodes
stdout frames into a channel, so the parent's deadline wait is a plain
timed receive and a kill never races a blocking read.

An `Event` frame carries the serde_json bytes of a `sima-trace` event,
opaquely inside the canonical frame — observational serde-world data
traveling the same way spec and params bytes travel opaquely elsewhere. The
reader thread parses each one and emits it to the run's collector, filling
a diagnostic's worker and host attribution where the child left them unset;
the frames never reach the lease loop, so diagnostics cannot perturb
outcome classification. Malformed event bytes never kill the worker: they
degrade to a warning diagnostic naming the decode failure. The first
producer is the executor panic path: the worker's panic hook latches the
message and backtrace, and the host emits an error diagnostic — source
`panic`, the worker id, and the task key rebuilt from the assignment's
identity inputs — before the `Panicked` frame settles the attempt.

The child's stderr is captured: a second per-child thread consumes it line
by line and emits each line as an info diagnostic
attributed to the worker and the pool's host label, capped at 4096 bytes
with a trailing truncation marker. The cap bounds the assembly buffer
itself — bytes past it are discarded as they arrive — so a child that
streams without newlines (a progress bar repainting with carriage returns)
costs a constant buffer for the life of the run. Everything a child prints
— toolkit
validation messages, pre-handshake errors, the default panic hook's output
— lands in the journal correlated to the worker and host that produced it.
The thread exits when the child's death closes the pipe. The remote
transport inherits the capture through the shared spawn machinery: ssh
carries the container's stderr to the local client's stderr pipe.

The device travels as a `DeviceBinding` — a class and the member within it —
and is absent for a run that leaves the choice to the backend. The child
answers `Ready` with the name of the device it opened, empty for a domain
that uses no device; the parent journals that answer as `WorkerBound`, so the
record of where work ran is the child's own account rather than the parent's
assumption. The child resolves its executor and its device before answering
`Ready`, so a binding naming a device the machine does not have fails the
handshake rather than the first task.

Orphan protection is layered: the child sets `PR_SET_PDEATHSIG(SIGKILL)`
first, so a parent death — even by SIGKILL — kills it; the stdin
end-of-stream exit covers the graceful paths.

### Worker pool and the outcome classifier

A fixed pool of worker threads, created once inside a scope so they borrow
the store and transport without `Arc`, pulls tasks from a shared FIFO queue.
A worker leases a task, loads its input-state and resume bytes, and hands
everything to its child as values; the child builds the `TaskInput` and
`ExecutionContext` and runs the executor inside a panic handler wrapping
only that call. The parent classifies what comes back, the one place an
outcome is turned into an action:

- `Done(Completed)` → commit through the store's single commit path (store
  each artifact, then the record), and emit `Committed`.
- `Done(Failed)` → retry: re-enqueue at the next attempt until the attempt
  cap, after which the transient failure becomes definitive. The child
  stays alive and may execute the retry.
- `Done(Rejected)`, or a `Failed` whose retries are exhausted, or a
  `Panicked` frame → a definitive failure that terminates the run. The
  child catches the executor's panic and reports it with the payload as its
  reason; classification authority stays with the parent. A panic anywhere
  in the parent's own path is a scheduler bug and propagates.
- A `Fault` frame — the executor returned `Err` — is an infrastructure
  fault and fails the run with an error.
- A child death without an outcome — crash, OOM kill, externally killed —
  is a transient failure, retried up to the attempt cap, and the worker
  spawns a replacement child before pulling the next task. A frame that
  violates the protocol classifies the same way, after the untrusted child
  is killed.

Nothing from the execution context reaches a committed record: the parent
carries only identity into the `TaskRecord`, and the attempt and worker
travel solely to the journal.

### Remote workers

A run's worker pool extends to manually provisioned remote machines over SSH.
The wire protocol already ships every task input and output inline — the store
never leaves the orchestrator — so a remote worker is the subprocess transport
with a longer command line. The subprocess transport takes a command vector, a
program and its arguments: a local worker runs the bare `sima-worker`, and a
remote worker runs

```
ssh -o BatchMode=yes <host> -- <runtime> run --rm -i --name <container>
  <run_args> <image> sima-worker
```

The same framed stdio protocol flows through `ssh → sshd → the container
runtime → the worker`, unchanged. `BatchMode=yes` turns an unauthenticated
host into a clean spawn error rather than a hang; the system `ssh` binary
carries authentication, so there is no ssh library dependency. A
`[[execution.remote]]` entry with no `host` runs its container on this machine:
the transport omits the ssh prefix and runs the runtime directly, the same
mechanism minus the ssh hop.

- **Worker pools.** A run drives a slice of pools: the local pool first, then
  one pool per remote in config order. Each pool pairs a transport with the
  host its workers run on and the device slots to spawn against it; worker ids
  stay global and sequential across pools. Placement is untouched — a device
  class is `(vendor_id, device_id)` regardless of machine, so a chain bound to
  a class runs on whichever pool holds it, exactly as within one pool.

- **Preemption is two-stage.** Closing the pipe alone would let a mid-compute
  container run until its next write, so `kill` first fires
  `<runtime> kill <container>` (ssh-wrapped when remote, best-effort and never
  awaited so a dead connection cannot block the scheduler), then kills the
  local client. The container's `--rm` reaps it, the fallback even if the
  second-channel kill never lands.

- **Cross-machine provenance.** The one variable an environment hash cannot see
  across machines of one class is the driver version. The `Ready` answer
  reports it, and `WorkerBound` records the host and driver alongside the
  device, so a cross-machine divergence within a class is diagnosable from the
  journal alone. `sima status` composes the device line by `(device, host)`,
  rendering `device @ host` for a remote pool. Driver parity within a class is
  the operator's responsibility, as it is across a driver upgrade on one
  machine.

- **Remote device selection.** `[[execution.remote]]` entries carry the same
  device tables as a local pool. Resolving them needs the remote's device
  list, so `sima-worker` doubles as the probe: `sima-worker --enumerate` prints
  the enumerated devices as JSON and exits. At run start the orchestrator
  verifies each remote's image is present, then runs the probe through the
  remote's container and reuses the local selector resolution unchanged.

- **The image.** A multi-stage `Containerfile` builds `sima-worker` in a
  `rust:<pinned>-bookworm` stage whose glibc matches the `debian:bookworm-slim`
  runtime stage — the development host's glibc is newer than any stable base,
  so the binary is built inside. The runtime stage bakes the Vulkan loader and
  the Mesa ICDs; NVIDIA user-space libraries are not baked, since they must
  match the host kernel driver, and the host's nvidia-container-toolkit injects
  them at container start through CDI. Delivery to a manual remote is
  `podman save | ssh <host> docker load`.

**Store synchronization** is a separate, standalone piece built here for
`sima migrate` to compose: a have/want protocol over any byte pipe, living in
`sima-store` over `sima-core`'s framing. Each side advertises what it holds,
computes `want = theirs − mine`, and streams the difference; received objects
are re-hashed against their advertised digest (content addressing is the
integrity check), and a record held on both sides under one key must be
byte-identical or the sync fails naming the key. Its scope is deliberately
narrow:

| Data              | Synced? | Why                                              |
|-------------------|---------|--------------------------------------------------|
| task records      | yes     | the run's durable results and closure            |
| CAS objects       | yes     | the artifact bytes records reference             |
| checkpoint slots  | no      | mid-segment scratch; segments are the resume point |
| placement slots   | no      | advisory; re-binds greedily on the other side    |
| journal           | no      | observational; stays with its orchestrator       |
| manifest          | no      | finalize re-derives it from records              |

### Device placement

A machine's GPUs are rarely equal, and a run spreads its pool across them.
The `[[execution.device]]` config entries name the devices and how many
workers each carries; the pool is their sum, and one slot per (entry, worker)
round-robins over the class's cards. A **device class** is the `(vendor id,
device id)` pair the backend reports — two identical cards are one class with
two members, interchangeable by declaration, which is why a class carries no
member and work bound to one may run on either card. A selector names a
device by a case-insensitive substring of its name or by its exact
`vendor:device` hex pair, and resolves against the machine's hardware when a
run starts, never when a config is read: `sima status` and `sima report` work
where no device exists.

**What a class identity is.** The `(vendor id, device id)` pair is PCI
vocabulary, and the shape's reach is exactly the reach of PCI:

- **Scope.** The pair is what PCI-enumerating GPU APIs report, so it is
  neutral across them: two backends looking at the same physical card mint the
  same class. The identity belongs to the hardware, not to the API that found
  it.
- **Limit.** The shape assumes PCI-identified hardware. A backend whose
  devices carry no PCI ids — integrated Apple devices, virtual or remote
  device abstractions — falls outside it.
- **Why holding it is safe.** The two integers are interpreted only at the
  execution-backend seam. Above it, the scheduler compares and hashes classes,
  the pipeline matches the rendered `vendor:device` string, the store holds
  opaque bytes, and the transport encodes the fields.

A backend without PCI ids turns class identity into an opaque token the
backend mints, with today's classes remaining valid tokens in their rendered
hex form. That costs a protocol version bump (both binaries ship together, so
a bump is free), invalidation of the advisory per-run placement slots (an
unbound chain binds again), and the config selector's exact-id form matching
tokens. Nothing identity-bearing carries the shape, so the change migrates no
durable state.

**The principle: device binding is derived operational state, never
identity.** A run id never encodes devices; the store records what actually
happened; hardware changes never strand a run.

Placement is **greedy** and **sticky**:

- **Greedy** — an unbound chain goes to whichever class pulls it first, so a
  card several times faster naturally takes several times the chains. There
  are no shares to tune, no idle tail from a mis-tuned ratio, and a card that
  thermally throttles simply pulls less.
- **Sticky** — once bound, every segment, retry, and resumed attempt of that
  chain runs on the same class. One candidate is one device class, so each
  candidate's trajectory is internally coherent and a retried attempt
  reproduces what the failed attempt would have committed.
- **Rebind is loud, and only on necessity** — a binding moves when its class
  is absent from the run's devices, which means the hardware changed between
  sessions. Run continuity outranks placement, so the work moves rather than
  stranding, and the journal records the move as `ChainRebound`.

The binding lives in a per-run operational slot beside the resume
checkpoints — `runs/<run-id>/placement/<chain>`, keyed by the same chain id —
holding the class as serde JSON: the human-readable operational world the
manifest and journal live in, never the canonical identity encoding. It is
written when a chain first binds and overwritten only by a rebind. A crash
between binding in memory and persisting the slot simply re-binds greedily on
resume; the binding is advisory coherence state, not correctness state, which
is why it needs no write-ordering discipline. A chain-less task is a chain of
length one: its retry stickiness is coordinator memory, and after it commits
there is nothing left to place, so it gets no slot.

**Starvation is bounded, and the price of coherence.** With stickiness, a
class's workers can idle while another class finishes chains bound to it. The
bound is (chains in flight on that class × their remaining segments); an
unbound chain is available to every class. A run with one implicit class reads
no placement state at all: its workers take the head of the queue, exactly as
a run with no placement state does.

**What this deliberately does not give:** a fresh re-run of a multi-class
config is not bit-reproducible for float domains, because which class first
pulls a given chain depends on scheduling timing. Single-class runs keep full
per-backend determinism, and exact reproduction of a specific candidate means
running it on a single-device config. This is the two-tier determinism
philosophy applied to placement.

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

- **observer** — the collector's record consumer: invoked with each typed
  record on the collector thread, immediately after its line is appended —
  typed records, journal order, one calling thread. Progress rendering
  consumes this seam.
- **interrupt** — a level-triggered flag the driver polls within a bounded
  wait. Once set, the run winds down gracefully: no more tasks are handed
  out, in-flight attempts finish and commit, queued tasks are abandoned,
  and the run returns `Interrupted` with no manifest written — the store
  stays resumable and the next orchestration continues the abandoned work.

The wind-down states form a precedence order — running < interrupted <
failed < fault — and each setter only upgrades: a definitive failure or an
infrastructure fault landing during an interrupt wind-down still decides
the run, and among faults the first wins.

### Leases and preemption

Leases live in memory — the set of tasks in flight — since durable progress
is the committed records; a process death drops all leases and resume
re-derives the frontier. `attempt_timeout` is enforced per worker: the
parent thread that owns the child uses it as the deadline of the wait it
already performs on the child's messages. On expiry it journals
`LeaseExpired`, SIGKILLs the child, reaps it, classifies the attempt as a
transient failure — retried up to the attempt cap — and spawns a
replacement before pulling the next task. Process isolation is what makes
the kill safe: a memory-safe runtime has no safe forced thread termination.
A timeout larger than any attempt (for example `Duration::MAX`) never lands
on the clock and so disables enforcement.

### Journal events

The scheduler is the journal's principal emitter. A typed `sima-trace`
`Event` serializes to one JSON line, with ids and stats rendered as hex.
The vocabulary:

- **run started** — the run began; carries the planned task total, those
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
- **lease expired** — an attempt ran past the attempt timeout: its worker
  process is killed and the attempt fails transiently.
- **checkpoint degraded** — a checkpoint save or load failed; execution
  continues and the attempt's result is unaffected, so this event is the
  only trace.
- **worker bound** — a worker's child reported the device it computes on, at
  every spawn and respawn.
- **chain rebound** — a chain's device class was absent from the run's
  devices, so its work moved to a class that is present.
- **run finalized** — every task committed and the manifest was written.
- **run failed** — a definitive candidate failure terminated the run; no
  manifest was written.
- **run interrupted** — the caller interrupted the run: in-flight attempts
  drained and committed, no manifest was written, and the store is
  resumable.
- **diagnostic** — a correlated line of observational text: captured worker
  stderr (info), a transport degradation (warn), or an executor panic's
  backtrace (error), attributed to its source, worker, host, and task where
  known. Status ignores it entirely; the CLI renders warn and error lines
  and keeps info journaled, never echoed.

The driver spawns the trace collector over the run's journal writer, with
the caller's observer as its record consumer; the driver, the workers, and
the transports' reader threads emit through cloned emitters, and the one
collector thread is the single-writer seam the append contract requires.
The journal write for an event happens before the observer sees it, and the
observer sees records in journal order. Event arrival order across threads
varies between runs; the journal is observational and excluded from every
equality criterion, so the manifest — sorted by task key at finalize — is
byte-identical across runs regardless.

## `sima-pipeline` (L7)

The layer a person's configuration enters: it loads `sima.toml`, translates
it through the domain and generator the config names, and drives the
scheduler over the configured store.

### Identity and execution in the file

The config file carries the same split the model enforces:

- **`[run]`** — the identity section, canonicalized into `RunConfig`, so
  its fields define the `RunId`: the root seed, the format, the optional
  segment count (at least 1; absent means a static batch), the generator
  (its id plus the generator-owned keys), and the domain-owned run params.
- **`[execution]`** — the operational section: the store path (resolved
  relative to the config file's directory), worker count, attempt cap, an
  optional attempt timeout whose absence disables the enforced attempt
  deadline, and an optional checkpoint interval whose absence disables
  checkpointing.
  Never hashed — a run resumed with different execution settings keeps its
  id.
- **`[[execution.device]]`** — one entry per device the run spreads over:
  `select` names it (a case-insensitive substring of its name, or its exact
  `vendor:device` hex pair) and `workers` says how many workers it carries.
  With entries present the pool is their sum, so the top-level `workers` key
  must be absent; without them it is required. Absent entries leave the
  device to the backend's own selection.

The structural keys are strict: an unknown key at any level is a validation
error naming it.

### Config routing

The pipeline parses the file's structure and routes each config section to
the code that owns it, never interpreting the content itself: the format and
generator ids dispatch through `sima-domains` (see L5), and the opaque
`[run.params]` and `[run.generator]` tables pass to the domain and generator
translations that turn them into canonical bytes. Identity-bearing bytes are
produced only by those codecs, never hand-rolled here.

### Orchestration

`orchestrate` opens the store (creating it where missing), takes the run's
orchestrator lock, dispatches the domain and the generator, locates the
worker binary (the `SIMA_WORKER` environment variable, then `sima-worker`
beside the current executable, then in its directory's parent), and calls
the scheduler over the subprocess transport; the lock is held for the whole
call and releases on return.
Resume and re-evaluation are this same call — the frontier re-derives from
store state, so an interrupted or failed run continues where it stopped,
and a finalized one re-finalizes idempotently without touching an executor.

### Run status

`status` computes a run's observable state from its journal alone. The
counters — tasks, committed, retried, rejected, faulted, lease expiries,
checkpoint degradations —
sum across every resume segment, and the last run-level event decides the
state. A journal ending mid-run reads as in progress: a dead orchestrator
is indistinguishable from a live one by the journal alone.

Every read-only query is a fold over records: one half reads a run's journal,
the other projects the view. The two are separate functions, so the same fold
serves records read from a local store and records streamed from the host that
drives the run. The folds that render stats take the run's format id, which is
all they need a config for.

## `sima` (L8)

The CLI holds no orchestration logic — parsing, rendering, signal
registration, exit codes, and, for `tui`, an interactive terminal
frontend over the observer seam. The read-only commands additionally take
`--on <ssh-dest>`, which addresses a run on the host its orchestrator runs on;
the flag is split out of the arguments before the command match, so every
command form keeps its shape whether or not a host is named:

- **`sima run <config.toml>`** — drives the configured run, printing one
  plain line per meaningful event from the observer seam. SIGINT sets the
  interrupt flag for a graceful wind-down; a second SIGINT falls through
  to default death, which is exactly the crash the recovery guarantees
  cover.
- **`sima status <config.toml>`** — reports execution. The config file is
  the one argument: its execution section names the store and its identity
  section derives the run id. `--task <key>` prints one task's attempt
  timeline instead — every lease it took, the worker, device and host that
  ran it, how each attempt ended, and the span the collector observed over
  it. `--failed` digests the tasks that did not commit, naming the terminal
  outcome and reason of each.
- **`sima report <config.toml>`** — reports results: the stats each
  committed task's domain renders, grouped by distinct value with a count.
  `--all` prints one line per committed task instead, and `--task <key>`
  one task's line. A task that never committed has no report; its
  execution history is what `status --task` answers.
- **`sima timeline <config.toml>`** — reports how efficiently the run
  executed. A summary block carries the run's wall-clock, commit count, and
  throughput; three retry ratios state retry volume, prevalence, and the share
  of attempts that were wasted, each printed with the counts it is taken over;
  and a per-worker table names each worker's spawn latency, respawns,
  utilization, commits, and attempts. Beneath them a fixed-width chart draws
  commits over time and one occupancy bar per worker on a single axis, so a
  worker provisioned late shows its spawn gap as leading blanks drawn to
  scale.

  Rates and per-worker figures cover the **latest run session** — a resumed
  run's journal spans downtime that would collapse them — while the commit
  count is run-wide. Utilization is over each worker's **own lifespan**, from
  its first binding to the session's end, so the cost of provisioning a remote
  pool reads as spawn latency rather than as idleness. Every duration is
  elapsed wall-clock as the journal stamped it.

`status`, `report`, and `timeline` split along what they report — how the run
is running, what it produced, and how efficiently it got there. `status` and
`report` each cover the whole run by default or one task under `--task <key>`;
`timeline` is run-scoped, since efficiency is a property of the pool rather
than of a task. A `<key>` is any prefix of a task
key that names exactly one of the tasks the journal carries a lifecycle
event for; a prefix matching none, or more than one, is an error. Every
query reads the journal alone and never perturbs a run, so it answers over
a live run, a finished one, and a failed one alike, and a query exits 0
whenever it answered — independently of the run's own outcome.
- **`sima follow <config.toml>`** — streams the run's events to stdout, one
  line per event through the same renderer the tui's log uses, and exits when
  the run reaches a terminal state, carrying that outcome's exit code. It is
  the pipeable counterpart of `tui`: no terminal, no raw mode, no keys. A
  finished run prints what it recorded and leaves; a run nobody drives prints
  its history and leaves successfully, since such a run is resumable and
  `status` is where that state is read.
- **`sima tui <config.toml>`** — drives the same run inside a full-screen
  terminal UI: an idle screen lists the configured workers, a keypress
  starts the run, and the tui applies each observer event as it arrives, so
  the worker rows and counters update live, with keys to wind the run down
  gracefully or leave and a `?` overlay listing every binding.
  It requires a terminal; with stdout not a TTY it exits 1. `ratatui`
  and its `crossterm` backend are the terminal-UI dependencies, and they
  enter the workspace at this layer alone.

### Following a run over SSH

`status`, `report`, `timeline`, `tui`, and `follow` each accept
`--on <ssh-dest>`, which names the host the run's orchestrator runs on.
Three properties of the system decide the shape, and none of them is a
choice:

- **A run's identity is the hash of its config**, and its store path resolves
  relative to the config file's directory. A local copy of the config would
  resolve the store to a path that does not exist here.
- **A journal lives with its orchestrator.** It is observational and never
  travels in a store sync, so following a run means reading that file, on that
  machine.
- **Liveness is an advisory lock**, meaningful only to the kernel holding it.
  Probed across a network filesystem it does not reflect the real holder.

So the config path travels unresolved and the far side interprets it. `sima
follow-serve <config> [--once]`, spawned there over
`ssh -o BatchMode=yes`, computes the identical run id, resolves its own store,
tails its own journal, and probes its own kernel lock, writing frames to
stdout; this side folds and renders them. `follow-serve` is the far half of a
transport rather than a verb a user invokes, so it stays out of the usage
text, and its stdout carries frames alone — every diagnostic goes to stderr,
which ssh keeps on its own channel.

**One seam, two implementations.** Every live view consumes a `RunFeed`: the
records a run gains, its lock holder, and the `FeedInfo` a renderer needs but
cannot derive from records — the run id, the format whose domain renders
stats, and the worker count. `LocalFeed` pairs a `RunObserver` with the
metadata its loaded config carries; `RemoteFeed` reads them off the stream.
The one-shot views take the same records in one call and fold them the same
way, so a remote view renders byte for byte what the local view renders.

**The wire.** Framing is `sima-core`'s length-prefixed carrier, payloads are
canonical `Enc`/`Dec` with a leading tag, mirroring the worker protocol. The
opening frame is a version-carrying handshake; a mismatch is refused by name
rather than decoded, so two builds that disagree never interpret each other's
bytes. Records travel as raw journal lines, so one parser and one torn-write
rule serve both ends. A failure on the far side crosses as a fault frame
carrying the text that machine rendered, and surfaces here unchanged: the
machine that failed owns the classification.

**Observation is read-only, and remote observation is observe-only.** The far
side takes no lock and writes nothing to the store, so a followed run reaches
a manifest byte-identical to an unobserved one. A run is driven where its
hardware is, so `tui --on <host>` never offers the take-over affordance, and
`run` and `rm` — which drive and mutate — take no `--on` at all.

Authentication is the user's SSH configuration. Keys, agents, `~/.ssh/config`
aliases, and jump hosts are configured exactly as for remote worker pools;
`BatchMode=yes` scopes interactive authentication out: a host that is
unreachable, or that would ask for a password, fails promptly with a named
cause. A host that authenticates and then stalls before its first frame is a
live connection, and the near side waits on it.

Exit codes (shared across `run`, `tui`, and `follow`):

- **0** — the run finalized, `status` answered, or `follow` reached the end of
  a run nobody is driving, which is resumable rather than failed;
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
