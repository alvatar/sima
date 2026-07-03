# TODO

Roadmap. Read `AGENTS.md` (project rules, settled invariants) and `README.md`
(design document) before this file. Phases are large project stages;
milestones are PR-sized units of work, each with a fully elaborated
`work/TODO-<topic>.md` while in flight. `work/` is gitignored and
machine-local: elaborations are written fresh from this document's decisions,
which must therefore carry everything durable. This document is living —
structure and content evolve through discussion.

Settled context: Rust, local-first. Workspace under `crates/`, one crate per
layer (below). GPU execution via Vulkan compute (`ash`), shaders compiled to
SPIR-V at build time (shaderc). Content addressing with blake3. Concurrency
via std threads and channels — no async runtime (revisit only if P3
transports force it). All randomness in result-affecting paths comes from the
project's counter-based SplitMix64 PRNG (`sima-core`), implemented identically
on CPU and GPU; the `rand` crate is banned from result paths. Candidates are
specs — opaque bytes plus a format id; families interpret them (CA families
call theirs genomes). CA is the substrate; the research object is
learned/evolved computation on it. Primary workload shape: huge grids, 3D
included — a single simulation can saturate a GPU; small grids are supported
via within-launch batching (P7), never the design driver. Grid state model:
extent × channels × dtype, double-buffered (totalistic: 1 channel u8; NCA: N
channels f16/f32). Visualization is out of scope: snapshots in the store are
consumed by external tools (the `../luz` renderer reads them as volumes).
Evaluation research and model-family research are standing tracks,
deliberately out of the phase ladder.

Layering (strictly downward dependencies, enforced by workspace crate edges;
layer numbers follow the dependency order):
`sima-core` (L0: error, encode, prng, hash) → `sima-model` (L1: spec, task
key, provenance, run config) → `sima-store` (L2: cas, catalog, journal
modules) → `sima-contracts` (L3: generator/executor traits + stubs) →
`sima-scheduler` (L4: task sources, leases, lifecycle state machine) →
`sima-pipeline` (L5: orchestration, resume, re-evaluation) → `sima` (L6: CLI
binary). Implementation crates at L3: `sima-gpu` (ash wrapper, depends on
core) and `sima-families` (rule families: CPU references + GPU kernels;
depends on contracts + gpu), arriving in P2.

Running model: one orchestrator per run — the `sima run` process itself; no
daemon. Single-writer per run enforced by a stale-detectable lease file.
Workers are stateless leaseholders (threads in P1, processes and remote
workers later). Executors are pure compute; workers commit results through
the catalog. The store is the only durable state; the orchestrator process is
disposable at any instant.

## P1 — Infrastructure spine

The system with no CA in it: content-addressed store, provenance, generator
and executor contracts, scheduler, pipeline, CLI. Stub implementations of both
contracts prove determinism, resumability, crash-retry, and
re-evaluation-from-store end-to-end.

Phase-level decisions:
- The store is the only durable state. The queue is ephemeral and derived: a
  task source yields the currently-runnable tasks given (config, store state).
  A static batch (config-enumerated tasks minus completed) is the degenerate
  case; segmented chains (P2), whose successor keys depend on produced state
  hashes, are the general one — the scheduler is built against the general
  interface from the start. Resume, crash-recovery, and re-run are the same
  code path: re-derive the frontier, continue.
- Interrupt robustness is a first-class property, not an edge case: death at
  any point, graceful or violent, converges on resume. This is what makes
  preemptible (cheapest) hardware safe to use, and it is proven by
  crash-injection tests, not asserted.
- Candidates are opaque at the infrastructure layer: a spec is (format id,
  opaque bytes), content-addressed. "Genome" is family-level vocabulary;
  contracts and store speak in specs.
- Two serialization worlds: identity-bearing bytes (anything hashed) go
  through canonical encoding exclusively; human-readable artifacts (manifest
  JSON) are serde and never identity-bearing.
- Manifests are canonicalized (entries sorted by task key) on finalization, so
  run output hashes are independent of worker completion order. Journals
  (attempt histories, timings) are observational, legitimately differ between
  runs, and are excluded from every equality-based acceptance criterion —
  equality always compares manifests.
- Acceptance for the phase: (a) the same config run in two fresh, separate
  stores produces identical manifests hash-for-hash — execution determinism,
  not merely storage stability; (b) a run killed at any crashpoint and
  resumed equals a run never interrupted; (c) a re-evaluation pass over a
  recorded run touches no executor; (d) copying the store to another location
  and resuming the config there yields a manifest identical to never having
  moved — run portability with zero migration code.

- [ ] M1.1 Crate skeleton (`sima-core`): `Error`/`Result`, canonical encoding
      (`Enc`/`Dec`), counter-based PRNG with pinned known-answer tests;
      workspace scaffolding
- [ ] M1.2 Model (`sima-model`): spec, task key (spec ‖ seed ‖ env ‖
      input-state-ref, empty ref for stateless tasks — segments differing
      only in input state must have distinct keys), environment-hash
      mechanism, provenance records, run identity = hash of canonicalized
      config; pure types + canonical encodings, no I/O
- [ ] M1.3 Store (`sima-store`): disk layout `objects/<aa>/<hash>` (CAS,
      two-char fan-out) + `tasks/<task-key>` (index) + `runs/<run-id>/`
      (manifest, journal); cas module — blake3 objects, atomic writes
      (temp + fsync + rename), idempotent puts; catalog module — task index
      with write-ordering discipline (result objects durable before the index
      entry referencing them), run manifest with canonical ordering,
      run-closure enumeration (all objects a run references); journal module
      — append-only per-run journal beside the manifest, line-framed
      crash-safe appends (a torn final line is detected and ignored on read),
      non-identity-bearing
- [ ] M1.4 Contracts + stubs (`sima-contracts`): executor and generator
      contracts over opaque specs; task definition carries an optional
      input-state object reference (segmented-execution enabler, unused by
      the stub, part of the task key); the contract distinguishes identity
      inputs (spec, seed, env, input-state — determine the key and the
      committed artifacts) from execution context (attempt number, worker id
      — visible to the executor, forbidden from influencing committed
      artifacts); seeded stub generator; spec-programmed stub executor (spec
      bytes select behavior: succeed / fail N times then succeed / panic /
      sleep — the fail-N behavior reads the attempt number and is the
      sanctioned exception, its eventual artifact still attempt-independent)
      so scheduler failure tests are deterministic; run-twice →
      identical-hashes tests
- [ ] M1.5 Scheduler (`sima-scheduler`): task-source interface (yields
      currently-runnable tasks from config + store state; static batch as the
      P1 implementation, chain-frontier arrives with P2 segmentation), leases,
      heartbeat/timeout, retries with idempotent commit, backpressure; task
      lifecycle state machine (defined → queued → leased → executing →
      committed | failed → retried) with transitions written through the
      store's journal module; thread-worker transport; failure matrix driven
      by the programmable stub
- [ ] M1.6 Config + pipeline + CLI (`sima-pipeline`, `sima`): TOML schema +
      canonicalization; pipeline orchestration with static format-id →
      implementation match; orchestrator lease file; typed progress events
      rendered by the CLI; basic `sima status <run>` from the journal;
      graceful interrupt, resume, re-evaluation pass; crash-injection
      harness (subprocess SIGKILL at controlled crashpoints —
      mid-object-write, between object and index, mid-lease, during
      finalization — resume, assert manifest identical to uninterrupted
      reference); end-to-end tests for the four phase acceptance criteria

## P2 — GPU executor + totalistic family

First real executor. Outer-totalistic 3D CA as the simplest family: exercises
the GPU path and the family boundary while staying trivial to verify.

- [ ] M2.1 ash compute bringup (`sima-gpu`): device selection, buffers,
      dispatch, build-time shaderc, SPIR-V hashes folded into the environment
      hash; device and driver recorded as provenance metadata but kept out of
      the env hash for integer families (results are device-independent —
      README, Determinism; float families revisit this in P7)
- [ ] M2.2 Totalistic family (`sima-families`): outer-totalistic 3D rule —
      Moore-26 neighborhood, u8 state (0 = dead, 1..states-1 = decay levels),
      genome = birth mask ‖ survive mask (bitmasks over live-neighbor counts)
      ‖ state count, toroidal wrap as the only boundary condition; genome
      encode/validate, seeded generator, mutation + CPU reference engine with
      known-answer tests (mutation is dormant until the first evolutionary
      loop in P7; built here for family completeness)
- [ ] M2.3 GPU totalistic kernel + CPU/GPU bit-equality cross-check matrix
      (extents, genomes, step counts)
- [ ] M2.4 First real search: ≥1000 genomes on the local GPU through the full
      spine; per-candidate result stats recorded as metadata (final population
      count from the result snapshot — inspection aid, not a funnel);
      throughput numbers recorded here. Result snapshots are stored in full
      (re-evaluation and portability require them), so extent × batch is
      chosen to a stated disk budget; retention policy is deliberately
      deferred to P6
- [ ] M2.5 Segmented execution: a long simulation runs as a chain of tasks
      (state Sₙ + k steps → state Sₙ₊₁), checkpoint states as store objects,
      segment length from config; chain-frontier task source (successor keys
      derived from produced state hashes) plugging into M1.5's interface;
      determinism test: N steps + resume N ≡ 2N steps, bit-exact. This is
      what makes pausing and migrating a specific in-progress simulation
      possible, not just a whole job

## P3 — Distribution

The distributed-systems heavy lifting, done early against the real P2
workload. The scheduler contract from M1.5 gains transports. Content-addressed
idempotent tasks make at-least-once delivery, retry, dedup, and spot-check
verification safe by construction.

- [ ] M3.1 Multi-process worker transport (same scheduler contract)
- [ ] M3.2 Multi-GPU on one host
- [ ] M3.3 Remote worker over SSH: container image with Vulkan runtime, worker
      bootstrap, bidirectional store sync (have/want negotiation — results
      home, closures out; the same protocol M5.8's migrate later composes)
      against a manually provisioned machine

Expected to be re-split when reached; remote transport hides surprises.

## P4 — Run control & observability

The view layer over the lifecycle journal, positioned before slingshot: paid
remote hardware is not operated blind. The journal and state machine already
exist (P1); this phase builds the surfaces that read them.

- [ ] M4.1 `sima status` / `sima inspect <task>`: run and task state, attempt
      history, durations, failure summaries — local and remote runs alike
- [ ] M4.2 Live follow: workers emit events over their transport, the
      orchestrator journals them; follow tails the journal into one
      aggregated view (works from another terminal against a running
      orchestrator, local or SSH)
- [ ] M4.3 Run timeline and summary report: throughput, retry rates, worker
      utilization per run

## P5 — Slingshot

One command sends an experiment to rented hardware and brings results home:
provision, bootstrap, run, sync, tear down. Teardown must be guaranteed —
leaked instances are leaked money.

- [ ] M5.1 Provider abstraction: provision / destroy / list / price query;
      instance lifecycle owned by the run, teardown on success, failure, and
      interrupt
- [ ] M5.2 Vast.ai backend
- [ ] M5.3 Hetzner backend
- [ ] M5.4 AWS backend
- [ ] M5.5 On-worker stats reduction: kernel-side population/activity counts
      so remote runs return stats always, snapshots only on a cheap predicate
      (bandwidth guard until the evaluation funnel exists)
- [ ] M5.6 Budget guard: max price, max wall-clock, spend accounting per run
- [ ] M5.7 Trust-tiered scheduling: redundant execution, spot-check
      verification across trust classes
- [ ] M5.8 End-to-end slingshot consolidation (phase acceptance): start a
      search locally; interrupt it mid-simulation (inside a segment chain);
      `sima migrate` to a freshly provisioned instance — sync closure, resume
      remotely, follow events live; sync results home; teardown verified.
      Assert the final manifest and segment states are identical to an
      uninterrupted local reference run

## P6 — Evaluation funnel v1

Deliberately simple. The funnel machinery, with the cheapest deterministic
metrics only; metric research lives in its own track.

- [ ] M6.1 Periodic snapshot/stats recording: segment boundaries (M2.5) are
      the natural sampling points; this milestone adds the recording policy,
      not a new mechanism
- [ ] M6.2 Verdict classification: dead / frozen / exploding / cyclic,
      thresholds from config
- [ ] M6.3 Staged cheapest-first funnel + re-evaluation from recorded runs
      without re-execution

## P7 — Neural CA

The primary model family. Genomes are parameter vectors of arbitrary update
functions: perception kernels and update weights both evolvable.

- [ ] M7.1 Float/multi-channel grid state, strict-IEEE shader path, tolerance
      policy for cross-substrate checks
- [ ] M7.2 NCA family: genome (perception + update parameters), CPU reference
- [ ] M7.3 GPU NCA kernel + cross-substrate tolerance tests
- [ ] M7.4 ES-based search loop over NCA genomes
- [ ] M7.5 Within-launch population batching for small grids

## Research tracks (standing)

Parallel to the phase ladder, each eventually feeding it:

- **Model families beyond NCA** — Lenia-family continuous CAs, graph CAs,
  attention-based update rules; each lands as a rule family on unchanged infra
- **Evaluation / interestingness** — novelty, diversity, complexity metrics;
  the funnel machinery (P6) is the harness, the metrics are open research
- **Gradient-based training** — backprop through CA steps changes the executor
  contract from "run" to "run + accumulate gradients"; NCA literature
  precedent exists
- **IR / DSL** — composition-as-data over data-parallel primitives
  (map/stencil/reduce, later matmul), one definition compiling to both GPU
  kernel and CPU reference; surface syntax last

## Done

## Dropped
