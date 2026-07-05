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
call theirs genomes). Run parameters (extent, steps, budgets) are a separate
opaque params blob: generators produce specs, config produces params, and the
spec's format id governs the interpretation of both. CA is the substrate; the research object is
learned/evolved computation on it. Primary workload shape: huge grids, 3D
included — a single simulation can saturate a GPU; small grids are supported
via within-launch batching (P7), never the design driver. Families divide by
executor kind — the compute shape their engine has:
- Stencil/convolution kind: double-buffered grid state (extent × channels ×
  dtype); each output cell is a function of a neighborhood of the input grid.
  Covers totalistic (integer, 1×u8 — P2) and reaction-diffusion,
  Lenia/Flow-Lenia, Neural CA (float, N channels — P7).
- Agent-field kind: state is an agent population (position, heading) plus a
  field grid; agents sense the field, move, deposit onto it, and the field
  diffuses and decays. Covers Physarum (P8).
At the infra layer both are opaque content-addressed state; the family owns
serialization and the compute shape. Required families across the ladder:
totalistic, reaction-diffusion, Lenia/Flow-Lenia, Neural CA, Physarum.
Visualization is out of scope: snapshots in the store are
consumed by external tools (the `../luz` renderer reads them as volumes).
CI is in place (`.github/workflows/ci.yml`: fmt + clippy + workspace tests on
every push and PR); GPU-gated tests are skipped in hosted CI and run on the
dev machine — self-hosted runner revisited in P3. Evaluation research and
model-family research are standing tracks, deliberately out of the phase
ladder.

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
  contracts and store speak in specs. Run parameters travel as a second
  opaque content-addressed blob (params), produced by config; a task
  evaluates the pair (spec, params).
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

- [x] M1.1 Crate skeleton (`sima-core`): `Error`/`Result`, canonical encoding
      (`Enc`/`Dec`), counter-based PRNG with pinned known-answer tests;
      workspace scaffolding
- [x] M1.2 Model (`sima-model`): spec, params, task key (spec ‖ params ‖
      seed ‖ env ‖ input-state-ref, empty ref for stateless tasks — segments
      differing only in input state must have distinct keys), environment-hash
      mechanism — content-derived components only (versioned constants for
      engine/executor identity; compiled-shader hashes join in P2); anything
      machine-derived is provenance metadata, never key material, or
      acceptance (d) fails by construction. P1 stub env hash = stub version
      constant. Provenance records, run identity = hash of canonicalized
      config; pure types + canonical encodings, no I/O
- [x] M1.3 Store (`sima-store`): disk layout `objects/<aa>/<hash>` (CAS,
      two-char fan-out) + `tasks/<task-key>` (index) + `runs/<run-id>/`
      (manifest, journal); cas module — blake3 objects, atomic writes
      (temp + fsync + rename), idempotent puts; catalog module — task index
      with write-ordering discipline (result objects durable before the index
      entry referencing them), run manifest with canonical ordering,
      run-closure enumeration (all objects a run references); journal module
      — append-only per-run journal beside the manifest, one event per line,
      newline-terminated crash-safe appends (a torn final line is detected
      and ignored on read), non-identity-bearing
- [ ] M1.4 Contracts + stubs (`sima-contracts`): executor and generator
      contracts over opaque specs and params; task definition carries an
      optional input-state object reference (segmented-execution enabler,
      unused by the stub, part of the task key); the contract distinguishes
      identity inputs (spec, params, seed, env, input-state — determine the
      key and the
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
the GPU path and the family boundary while staying trivial to verify. It
establishes the stencil/convolution executor kind in its integer, bit-exact-
everywhere form; the float variant of this kind arrives in P7 and the second
kind (agent-field) in P8.

Phase acceptance: GPU results bit-identical to the CPU reference across the
full M2.3 matrix; a ≥1000-genome search completes through the spine within
its stated disk budget; a segment chain interrupted and resumed is
bit-identical to an unsegmented run of equal length.

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
      deferred to P6. Revisit CAS cost here, where disk volume and write
      throughput first bite: measure the cost of re-hashing every object on
      read and decide whether large artifacts get a bulk read that skips
      verification while identity objects stay verified; measure the
      per-object fsync write cost and weigh batching many objects behind one
      group-commit fsync
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

Phase acceptance: the same config run (i) locally single-worker and (ii)
spread across processes, multiple GPUs, and one SSH remote yields identical
manifests — determinism is transport-invariant; killing a remote worker
mid-lease converges through retry with no manifest difference.

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

Phase acceptance: a live multi-worker run is observable end-to-end (status,
inspect, follow, timeline) from another terminal; observation is read-only
over journal and store and never perturbs the run — proven by manifest
equality between an observed and an unobserved run.

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
      (placed here as the bandwidth guard; P6's funnel metrics consume the
      same reduction — the mechanism is shared)
- [ ] M5.6 Budget guard: max price, max wall-clock, spend accounting per run
- [ ] M5.7 Trust-tiered scheduling: redundant execution, quorum validation,
      spot-check sampling, host reputation — the BOINC playbook; the largest
      mechanism in this phase, expected to split into several PRs at
      elaboration
- [ ] M5.8 End-to-end slingshot consolidation (phase acceptance): start a
      search locally; interrupt it mid-simulation (inside a segment chain);
      `sima migrate` to a freshly provisioned instance — sync closure, resume
      remotely, follow events live; sync results home; teardown verified.
      Assert the final manifest and segment states are identical to an
      uninterrupted local reference run

Expected to be re-split when reached; provider APIs and trust mechanisms hide
surprises.

## P6 — Evaluation funnel v1

Deliberately simple. The funnel machinery, with the cheapest deterministic
metrics only; metric research lives in its own track.

Phase acceptance: verdicts are pure functions of recorded data — re-running
the funnel over a recorded run reproduces identical verdicts, and changing
thresholds re-classifies without any re-execution.

- [ ] M6.1 Periodic snapshot/stats recording: segment boundaries (M2.5) are
      the natural sampling points; this milestone adds the recording policy,
      not a new mechanism. Absorbs the snapshot retention policy deferred
      from M2.4: what is kept, for how long, and what re-evaluation minimally
      requires
- [ ] M6.2 Verdict classification: dead / frozen / exploding / cyclic,
      thresholds from config
- [ ] M6.3 Staged cheapest-first funnel + re-evaluation from recorded runs
      without re-execution
- [ ] M6.4 Object packing for scale, beside retention (M6.1) as the other
      store-scaling lever: millions of small objects press on inode and
      directory limits; a pack format — many objects in one file with an
      index — is the answer

## P7 — Continuous CA families

Float, multi-channel grid families on the stencil/convolution executor kind.
First the float-determinism foundation, then families in ascending complexity:
reaction-diffusion, Lenia/Flow-Lenia, Neural CA. Neural CA is one family here,
not the phase's headline — the trainable, self-repairing family, distinct from
the emergent-dynamics families beside it.

Phase acceptance: on one pinned backend class, every family here is
bit-identical run-to-run; across two distinct backend classes, results agree
within the recorded tolerance policy; a seeded search run reproduces its
fitness trajectory exactly. The tolerance policy (M7.1) is a written
deliverable — comparison metric, bound, and its provenance record format —
not an aspiration; it is the hardest determinism question in the project and
"done" is defined by tests against it.

- [ ] M7.1 Float/multi-channel grid state, strict-IEEE shader path, tolerance
      policy for cross-substrate checks (the policy document + tests are the
      deliverable — see phase acceptance). Shared foundation for every family
      below
- [ ] M7.2 Reaction-diffusion (Gray-Scott): 2-channel float, small-stencil
      Laplacian + local reaction — the simplest float family and the first
      consumer of the tolerance policy, proving the float path before the
      harder families. Genome = feed / kill / diffusion rates; seeded
      generator, mutation, CPU reference + GPU kernel + cross-substrate
      tolerance tests
- [ ] M7.3 Lenia / Flow-Lenia: large-radius convolution kernel + growth
      function; Flow-Lenia adds mass-conserving advection (semi-Lagrangian
      transport — the mass-redistribution scatter needs an order-independent
      scheme or it is nondeterministic even on one device) and spatially
      localized parameters (genome becomes a per-region field, not one global
      vector). CPU reference + GPU kernel + tolerance tests
- [ ] M7.4 Neural CA: genome = perception + update parameters, both evolvable;
      the trainable, self-repairing family (regrows when disturbed, two
      trained textures graftable). CPU reference + GPU kernel + tolerance
      tests
- [ ] M7.5 Search loop over continuous genomes (ES; gradient-based training is
      a standing research track — it changes the executor contract from "run"
      to "run + accumulate gradients")
- [ ] M7.6 Within-launch population batching for small grids

## P8 — Physarum (agent-field family)

The second executor kind: a stigmergic multi-agent model (Jones's slime-mould
transport networks). State is an agent population plus a trail field; the step
is sense → move → deposit → diffuse/decay, not a cell-local grid update.
Everything below this phase on the ladder — store, scheduler, distribution,
slingshot, funnel — is unchanged; the phase adds an executor kind in the
families layer and nothing beneath it. It is the proof that the infra is
family-agnostic.

Determinism approach (integer tier, bit-exact everywhere like totalistic — to
confirm at M8.2): fixed-point agent state (position, heading) and nearest-cell
sensing keep motion exact on any hardware; deposits accumulate as fixed-point
integers via order-independent atomic add (integer addition is associative and
exact, so scatter ordering cannot change the sum); the field diffuse/decay is
an integer stencil. This keeps Physarum out of the P7 float-tolerance
machinery. The alternative — float agent state — moves it into that tier; the
tradeoff (dynamic range and motion smoothness vs bit-exactness) is the open
decision resolved in M8.2.

Phase acceptance: CPU/GPU bit-equality across an agent-count × field-extent ×
step-count matrix; a segmented agent-field run (compound state checkpoint)
resumed equals an unsegmented run of equal length, bit-exact.

- [ ] M8.1 Agent-field executor kind (`sima-families`): compound state (agent
      buffer ‖ field grid) serialized as one opaque snapshot object;
      segmentation composes — the checkpoint is the compound state, the
      task-key input-state-ref mechanism is unchanged
- [ ] M8.2 Physarum family: fixed-point agent state, nearest-cell sensing,
      order-independent integer deposit, field diffuse/decay stencil; genome =
      sensor geometry (angles, distance), turn rate, deposit amount,
      decay/diffusion rates; seeded generator, mutation, CPU reference with
      known-answer tests; the fixed-point-vs-float determinism decision is
      resolved here
- [ ] M8.3 GPU kernels (agent update + field update) + CPU/GPU bit-equality
      matrix
- [ ] M8.4 First Physarum search through the full spine (funnel, slingshot,
      distribution unchanged); network-structure interestingness metrics feed
      the P6 funnel via the standing evaluation track

## Research tracks (standing)

Parallel to the phase ladder, each eventually feeding it:

- **Further model families** — graph CAs, attention-based update rules,
  program-shaped candidates; each lands as a rule family on unchanged infra.
  The ladder's own family phases (reaction-diffusion, Lenia/Flow-Lenia, Neural
  CA in P7; Physarum in P8) are the first proof of that promise
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
