# TODO

Roadmap. Read `AGENTS.md` (project rules, settled invariants) and `README.md`
(design document) before this file. Phases are large project stages;
milestones are PR-sized units of work, each with a fully elaborated
`work/TODO-<topic>.md` while in flight. `work/` is gitignored and
machine-local: elaborations are written fresh from this document's decisions,
which must therefore carry everything durable. This document is living —
structure and content evolve through discussion.

Settled context: Rust; local execution first, distributed by design. Workspace under `crates/`, one crate per
layer (below). GPU execution via Vulkan compute (`ash`); kernels are authored
in WGSL and compiled to SPIR-V by `naga` (pure Rust, no build-time C
toolchain). Content addressing with blake3. Concurrency via std threads and
channels — no async runtime (revisit only if P4 transports force it). All
randomness in result-affecting paths comes from the project's counter-based
SplitMix64 PRNG (`sima-core`), implemented identically on CPU and GPU; the
`rand` crate is banned from result paths. Candidates are specs — opaque bytes
plus a format id; domains interpret them (CA families call theirs genomes).
Run parameters (extent, steps, budgets) are a separate opaque params blob:
generators produce specs, config produces params, and the spec's format id
governs the interpretation of both. The research object is learned/evolved
computation on data-parallel substrates; cellular automata are the first
family, neural cellular automata the near-term target (Lenia in P8). Primary
workload shape: huge grids, 3D included — a single simulation can saturate a
GPU; small grids are supported via within-launch batching (P8), never the
design driver. Families divide by executor kind — the compute shape their
engine has:
- Cellular kind: double-buffered grid state (extent × channels ×
  dtype); each output cell is a function of a neighborhood of the input grid.
  Covers reaction-diffusion, Lenia, and Neural CA (float, N channels — P3),
  with Flow-Lenia and cross-substrate rigor in P8.
- Agent-field kind: state is an agent population (position, heading) plus a
  field grid; agents sense the field, move, deposit onto it, and the field
  diffuses and decays. Covers Physarum (P9).
At the infra layer both are opaque content-addressed state; the domain owns
serialization and the compute shape. Required families across the ladder:
reaction-diffusion, Lenia/Flow-Lenia, Neural CA, Physarum.
Visualization is out of scope: snapshots in the store are
consumed by external tools (the `../luz` renderer reads them as volumes).
CI is in place (`.github/workflows/ci.yml`: fmt + clippy + workspace tests on
every push and PR); GPU-gated tests are skipped in hosted CI and run on the
dev machine — self-hosted runner revisited in P4. Reproducibility is declared
per domain across two tiers (README, Determinism), a property rather than the
objective. Evaluation research and model-family research are standing tracks,
deliberately out of the phase ladder.

Layering (strictly downward dependencies, enforced by workspace crate edges;
layer numbers follow the dependency order):
`sima-core` (L0: error, encode, prng, hash) → `sima-model` (L1: spec, task
key, provenance, run config) → `sima-store` (L2: cas, catalog, journal
modules) → `sima-contracts` (L3: generator/executor traits + stubs) →
`sima-scheduler` (L4: task sources, leases, lifecycle state machine) →
`sima-pipeline` (L5: orchestration, resume, re-evaluation) → `sima` (L6: CLI
binary). Implementation crates at L3: execution toolkits under
`crates/toolkits/` (`sima-toolkit-wgsl`: WGSL → naga → ash compute, depends on
core), arriving in P2; `sima-domains` (rule families: CPU references + WGSL
kernels; depends on contracts + the toolkits it uses), with its first real
domain in P3.

Running model: one orchestrator per run — the `sima run` process itself; no
daemon. Single-writer per run enforced by a stale-detectable lease file.
Workers are stateless leaseholders (threads in P1, processes and remote
workers later). Executors are pure compute; workers commit results through the
catalog. The store is the only durable state; the orchestrator process is
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
  opaque bytes), content-addressed. "Genome" is domain-level vocabulary;
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
- [x] M1.4 Contracts + stubs (`sima-contracts`): executor and generator
      contracts over opaque specs and params; the executor receives the full
      identity, including the input-state reference already carried by the
      task key (segmented-execution enabler, unused by the stub); the
      contract distinguishes identity inputs (spec, params, seed, env,
      input-state — determine the key and the committed artifacts) from
      execution context (attempt number, worker id
      — visible to the executor, forbidden from influencing committed
      artifacts); seeded stub generator; spec-programmed stub executor (spec
      bytes select behavior: succeed / fail N times then succeed / panic /
      sleep — the fail-N behavior reads the attempt number and is the
      sanctioned exception, its eventual artifact still attempt-independent)
      so scheduler failure tests are deterministic; run-twice →
      identical-hashes tests
- [x] M1.5 Scheduler (`sima-scheduler`): task-source interface (yields
      currently-runnable tasks from config + store state; the flat batch is
      the P1 implementation, the ordered chain of tasks arrives with P2
      segmentation), leases, heartbeat/timeout, retries with idempotent
      commit, backpressure; task lifecycle state machine (defined → queued →
      leased → executing → committed | failed → retried) emitted as typed
      lifecycle events — structured data, not formatted strings — with the
      store's journal as the one sink they serialize into now (the same events
      feed the distributed trace facade when it arrives, M4.4); thread-worker
      transport (interim: replaced outright by the M4.1 subprocess worker; the
      two worker models never coexist — see M4.1); a watchdog that stamps each
      lease with a deadline and journals a `TaskOverran` event when an attempt
      runs past it (in-process execution cannot be preempted, so an overrun is
      detected and reported, never killed — real preemption is M4.1); failure
      matrix driven by the programmable stub; an `ExecutionConfig` injected as
      a struct (worker count, timeouts, retry cap — operational,
      never hashed), whose file form is the execution section of M1.6's
      `sima.toml`.
      Failure model (resolved at elaboration): an execution yields one of
      three outcomes — `Completed { artifacts, stats }`, `Failed { reason,
      stats }` (transient, retried up to the cap), or `Rejected { reason,
      stats }` (definitive: the candidate cannot produce a result, never
      retried). A panic escaping the wrapped `execute()` call was raised inside
      the program execution → `Rejected`; a panic anywhere else in the
      scheduler is a SIMA fault → `Err`. The store models only success (a
      `TaskRecord` records artifacts), so a definitive failure is journal-only:
      it terminates the run, writes no manifest, and leaves the store clean and
      resumable (committed successes remain, no failure marker), so fixing the
      cause and re-running the same config re-executes only the unfinished
      work. This is the final design, not a placeholder — correct code never
      fails definitively. (The `Failed`-carries-stats half was the
      M1.4-deferred decision; `Stats` stays opaque and empty-costs-nothing.)
- [x] M1.6 Config + pipeline + CLI (`sima-pipeline`, `sima`): one `sima.toml`
      with two sections — an identity section (root_seed, format, generator,
      params) canonicalized into the `RunConfig` bytes and thus `RunId`, and an
      execution section (worker count, timeouts, retry cap — the file form of
      M1.5's `ExecutionConfig`, operational and never hashed); pipeline
      orchestration with static format-id →
      implementation match; orchestrator lease file; typed progress events
      rendered by the CLI; basic `sima status <run>` from the journal;
      graceful interrupt, resume, re-evaluation pass; crash-injection
      harness (subprocess SIGKILL at controlled crashpoints —
      mid-object-write, between object and index, mid-lease, during
      finalization — resume, assert manifest identical to uninterrupted
      reference); end-to-end tests for the four phase acceptance criteria.
      Resolved while building the dispatch: a `Domain` groups what a format
      id binds — the executor, the environment entering task identity, and
      the translation of the domain-owned config sections into canonical
      bytes; generators stay a separate plug with their own dispatch and
      their own config translation, so the bundle never pairs executor and
      generator 1:1

## P2 — GPU compute toolkit + float foundation

First real GPU compute. The execution toolkit that lets a domain run WGSL
kernels on the GPU, plus the float grid state the near-term families need. No
domain and no family land here — this phase builds what the P3 families run
on. Reproducibility is a per-domain property (README), not this phase's
organizing concern.

Phase acceptance: the toolkit runs a WGSL compute kernel end to end on the
local GPU (compile, allocate, transfer, dispatch, read back); float
multi-channel grid state round-trips through the store as an opaque snapshot;
a run split into a chain of segments and resumed equals an unsegmented run of
equal length.

- [x] M2.1 `sima-toolkit-wgsl` (`crates/toolkits/`): WGSL → naga → ash compute
      toolkit. `Context` (instance, device selection, compute queue, command
      pool), `Buffer` (device-local storage with staging up/download),
      `Kernel` (WGSL compiled to SPIR-V, compute pipeline; exposes source
      digest + compiler id for a domain to record), `Dispatch`. The runtime is
      hidden behind a WGSL-only API; ash is never exposed to domains. One
      throwaway WGSL smoke kernel proves the path; no family, no domain.
      Add an `Error::Gpu` variant to `sima-core`. GPU tests are `#[ignore]`
      (dev-machine only); the crate builds with no native toolchain. Fully
      elaborated in `work/TODO-2.1.md`
- [x] M2.2 Float grid state foundation: multi-channel float grid state as an
      opaque content-addressed snapshot object; the WGSL compute path for
      float stencils and convolutions; the CPU-reference pattern the families
      cross-check against. Minimal — enough to run the float families; the
      cross-substrate tolerance policy is P8
- [x] M2.3 Segmented execution and resume checkpoints: two distinct
      mechanisms, kept separate because one names committed work and the other
      is disposable resume state.
      (a) Segmentation — the committed work-division mechanism. A long
      simulation runs as a chain of tasks (state Sₙ + k steps → state Sₙ₊₁);
      each segment's output state is committed as a store object; a task
      source yields the next uncommitted task in each chain (successor keys
      derived from produced state hashes), plugging into M1.5's interface.
      Segment length k comes from config and must be deterministic, because a
      segment boundary is a committed task whose key enters the run manifest,
      and the same config must produce the same manifest (P1 acceptance a).
      Segments are the portable resume point that `sima migrate` (P4) ships in
      a run closure, and the leasable unit for distribution. General over
      families. Determinism check: N steps + resume ≡ 2N steps on a pinned
      backend.
      (b) Resume checkpoints — the disposable crash-resume mechanism. During
      execution, every X steps or every T wall-clock seconds (from config, and
      free to choose because a checkpoint enters no hash and no manifest), the
      running task writes its full continuation state to a mutable per-run
      scratch slot (`runs/<run-id>/checkpoint/<chain>`), overwriting the prior
      write — latest-only, so it costs one state per chain and needs no
      deletion. The write holds everything required to continue identically to
      an uninterrupted run: grid state, step index, the counter-based PRNG's
      counter (one integer), and domain aux. On start a task resumes from its
      scratch checkpoint if present, else from its segment input; the committed
      result is identical either way, so this is a local capability inside
      execution that touches no key, manifest, or work decomposition. Only the
      final result is held deterministic; checkpoint timing is not.
      Result/run reclamation: committed segment states and result snapshots do
      accumulate. A reference-guarded deletion primitive — remove an object
      only when no live manifest references it; remove a run's exclusive
      closure — lands when result objects first fill disk (M3.4 at the latest,
      earlier if M2.2/M2.3 test runs pile objects up), not before. It is the
      same retention lever P7 (M7.1) formalizes; it arrives early only because
      disk pressure does.

## P3 — First model families

The near-term research targets, running end to end and explorable: reaction-
diffusion and Neural CA (Lenia is descoped to P8, M8.2). Each lands as a
domain (CPU reference + WGSL kernel + seeded generator) on the P2 toolkit and
float foundation. Determinism
is at the pragmatic per-backend level — deterministic run to run on one
machine, exact where it is cheap — without the cross-substrate tolerance
apparatus (P8). The point is having the families in hand and iterating.

Phase acceptance: NCA runs through the full spine (generate → execute
→ commit → inspect) from a `sima.toml`; a local search over a float family
completes and records per-candidate stats; a recorded run re-evaluates from
the store without re-execution; results reproduce run to run on the dev
machine.

- [x] M3.1 Reaction-diffusion (Gray-Scott): the simplest float family — 2
      channels, small-stencil Laplacian + local reaction. First float family
      through the toolkit, proving the float path before the harder two.
      Split into four sub-milestones, one PR each, each reviewed on its own:
- [x] M3.1a Genome: genome = feed / kill / diffusion rates as the spec's
      untagged payload (the spec frames it with the format id, per the stub
      program precedent); canonical encode/decode plus validation, with
      pinned byte-stability and independently computed spec-id tests
- [x] M3.1b Seeded generator: a deterministic genome population from
      (config, seed) through the generator contract — the first generator
      producing real specs
- [x] M3.1c CPU reference engine: the Gray-Scott rule as a `CellularRule`
      with known-answer tests — pinned states for fixed genome/seed/steps,
      plus qualitative checks (a known pattern-forming (f, k) point forms
      structure; a known dead point decays)
- [x] M3.1d WGSL kernel and full spine: the rule as a compute kernel through
      `sima-toolkit-wgsl`, cross-checked against the CPU reference within a
      stated per-backend tolerance; the domain wired end to end (generate →
      execute → commit → inspect) from a `sima.toml`
- [x] M3.2 Neural CA (asynchronous), the second `ca_evolution` model beside
      Gray-Scott: genome = perception filters + update-network weights,
      seed-sampled from the generator; asynchronous stochastic update at fire
      rate ½ keyed on the per-step index the harness supplies, with committed
      state framed as that step ahead of the grid, so segments continue
      byte-identically; WGSL kernel over an in-shader SplitMix64 PRNG.
      Training and mutation deferred to P8/M8.3, fitness scoring to P7; CPU
      reference descoped as in M3.1d
- M3.3 Lenia: descoped from P3 to P8, folded into M8.2 beside Flow-Lenia, so
      the whole Lenia line (plain and flow variants) lands in one place under
      the cross-substrate rigor apparatus
- [x] M3.4 First real search: a family search of ≥1000 candidates on the local
      GPU through the full spine; per-candidate result stats recorded as
      metadata (population/activity from the result snapshot — inspection aid,
      not a funnel); throughput numbers recorded here. Result snapshots are
      stored in full (re-evaluation and portability require them), so extent ×
      batch is chosen to a stated disk budget. The retention policy — what is
      kept and for how long — is deferred to P7, but the reference-guarded
      deletion primitive it will drive lands here (see M2.3), because this is
      where object volume first fills disk. Revisit CAS cost here, where disk
      volume and write throughput first
      bite: measure the cost of re-hashing every object on read and decide
      whether large artifacts get a bulk read that skips verification while
      identity objects stay verified; measure the per-object fsync write cost
      and weigh batching many objects behind one group-commit fsync.
      Step-count checkpoint cadence lands here too: `[execution]` gains
      optional `checkpoint_interval_steps = N` — the worker's checkpoint
      handle saves on every Nth offer since the last save (an offer is a
      domain step boundary, so with a domain offering per step this is
      "every N steps"), unioned with the wall-clock interval (either due →
      save; either knob present enables checkpointing). M2.3(b) specified
      both cadence axes; M2.3 landed the wall-clock axis only, and this is
      where cadence tuning first meets measured GPU throughput and real
      state sizes.

      Run: the Gray-Scott family (`examples/gray-scott-search.toml`), 1000
      candidates on a 128×128×2 grid over 2000 steps, two workers. Recorded on
      the dev GPU:
      - wall-clock ≈ 619 s (≈ 10.3 min), ≈ 1.6 committed tasks/s;
      - store 143 MB, 3003 objects (1000 states + 1000 records + 1000 specs +
        the shared config, params, and environment);
      - re-evaluation of the finalized run re-finalizes in ≈ 36 ms, committing
        nothing and touching no executor;
      - reference-guarded removal of the run empties the store to its skeleton
        (3003 objects, 1000 index entries).

      CAS cost decision: keep verified reads and per-object fsync unchanged.
      The measured write path (≈ 0.1 s over the run's 1000 snapshots) is under
      0.05 % of the search wall-clock, and verified-read throughput
      (1.7–1.9 GB/s) is far above the 500 MB/s floor below which hashing, not
      I/O, would bound a full-closure read. Neither the bulk unverified read
      nor group-commit fsync is warranted at this workload.
- [x] M3.5 TUI observer mode: `sima tui` on a run another orchestrator holds
      observes it live — journal tail seeded and followed through one offset,
      lock probe for holder and liveness, take-over through the normal resume
      path once the lock frees.

## P4 — Distribution

The distributed-systems heavy lifting, done early against the real family
workload. The scheduler contract from M1.5 gains transports. Content-addressed
idempotent tasks make at-least-once delivery, retry, dedup, and spot-check
verification safe by construction.

Phase acceptance: the same config run (i) locally single-worker and (ii)
spread across processes, multiple GPUs, and one SSH remote yields identical
manifests whenever every task lands on the same device class — determinism
is transport-invariant. Heterogeneous device sets are first-class and
throughput-first: assignment is greedy (any free worker pulls the next
unbound chain), a chain binds to the device class that first picks it up
and stays there across segments, retries, and resumes, and a resume whose
recorded class is absent from the current device set rebinds the chain
loudly (journaled) and converges — a run always continues across hardware
changes. Device binding is derived operational state, never identity: the
run id is device-independent, and mixed-set float manifests are
schedule-dependent by design (single-class runs remain the per-backend
determinism mode; bit-agreement across device classes is P8, M8.1).
Killing a remote worker mid-lease converges through retry with no manifest
difference.

- [x] M4.1 Multi-process worker transport (same scheduler contract): replaces
      the M1.5 in-process thread worker outright — the two worker models never
      coexist, since mixing them would mean inconsistent execution guarantees —
      and brings real timeout preemption (kill the subprocess) that the
      in-process watchdog can only detect, not enforce. Landed: the
      `sima-worker` executor-host binary over a framed stdin/stdout protocol,
      enforced `attempt_timeout`, worker-death retry with checkpoint resume,
      and PDEATHSIG orphan protection; the watchdog thread is gone.
- [x] M4.2 Multi-GPU on one host. Settled at elaboration: config gains
      `[[execution.device]]` tables (a `select` matching a device by
      case-insensitive name substring or exact vendor:device hex pair, plus
      per-device `workers`; no tables = today's single-device selection);
      placement is greedy with durable chain stickiness — a chain binds to
      the class that first pulls it, recorded in a per-run operational slot
      beside the checkpoint slots, honored across resume, rebound with a
      journaled event when the class is absent; the binding crosses to the
      child in `Hello` (protocol v2) and `Ready` reports the bound device
      name back for the journal; the toolkit gains device enumeration and
      selection by (vendor id, device id, member); `sima status` shows the
      run's device composition. Device identity never enters task keys or
      the environment hash (that is M8.1).
- [x] M4.3 Remote worker over SSH, against a manually provisioned machine.
      Settled at elaboration; split into three sequential PRs:
      (a) the two pre-existing test flakes (orchestrator-lock race in the
      segmented suite; resume-progress undercount in the crash suite —
      `RunStarted` gains a store-derived committed count so the display
      never depends on journal-flush timing);
      (b) the have/want store sync, standalone in `sima-store` over frame
      I/O lifted into `sima-core`: records and referenced CAS objects only
      (checkpoints are mid-segment scratch, placement re-binds, journals
      stay home), caller supplies the task-key set, content addressing is
      the transfer integrity check; first production consumer is M6.7's
      migrate;
      (c) the remote worker: the existing framed stdio protocol runs
      unchanged through `ssh <host> docker run -i` (the store never leaves
      the orchestrator — task inputs and results already cross inline);
      a multi-stage container image bakes the worker binary, Vulkan
      loader, and Mesa ICDs, with NVIDIA user-space libraries injected at
      start by the host's container toolkit; preemption kills the remote
      container through a second ssh, then the local ssh; device classes
      stay global across machines and protocol v3 makes `Ready` report
      the driver version, journaled with the host in `WorkerBound`;
      `[[execution.remote]]` entries carry per-remote device tables,
      resolved at run start through a `sima-worker --enumerate` probe;
      `sima status` shows per-host composition; acceptance runs over
      `ssh localhost`. The ssh acceptance suite is written and gated on a
      `SIMA_TEST_REMOTE` destination, pending the first SSH-reachable host;
      the container image build was not exercised in the build environment
      (rootless podman is unavailable there — nosuid mount, read-only
      cgroups), so the image and its `--enumerate` device checks await a
      host with a working container runtime.
- [x] M4.4 Distributed trace facade: a low-level structured-event interface
      usable from every crate at every layer, placed at or near `sima-core`
      so the strict downward layering holds and any layer can emit without an
      upward edge. It carries the typed lifecycle events M1.5 defines plus
      cross-process span and causality context. The durable per-run journal
      stays one sink; a live-aggregation collector is the second. This is the
      deferred half of the M1.5 logging split: the concept is separated at
      M1.5 (typed events vs. journal sink), and the cross-crate facade lands
      here, where distribution is the real second consumer that justifies it.
      Landed: the `sima-trace` crate at L0.5 (directly above `sima-core`) — the
      typed `Event` vocabulary (the lifecycle variants plus a `Diagnostic`),
      a `Record` carrying a required `ts_ms` stamp, cloneable `Emitter`s, and
      one `Collector` thread that stamps each event, appends the journal line
      through the `DurableSink` the store implements, then hands the record to
      the run's single observer. Causality rides the events as natural keys
      (run, task, attempt, worker, host), not synthetic spans. Protocol v4
      adds a `ToParent::Event` frame carrying a child-produced event; the first
      producers are executor-panic diagnostics and captured worker stderr, each
      correlated to its task. Diagnostics are excluded from run identity — a
      run with diagnostics commits a manifest byte-identical to a silent one.

Expected to be re-split when reached; remote transport hides surprises.

## P5 — Run control & observability

The view layer over the lifecycle journal, positioned before slingshot: paid
remote hardware is not operated blind. The journal and state machine already
exist (P1); this phase builds the surfaces that read them.

Phase acceptance: a live multi-worker run is observable end-to-end (status,
inspect, follow, timeline) from another terminal; observation is read-only
over journal and store and never perturbs the run — proven by manifest
equality between an observed and an unobserved run.

- [x] M5.1 Per-task inspection folded into `sima status` along a content/scope
      axis: `status --task <key>` (attempt timeline), `status --failed`
      (failure digest), `report --task <key>`, `report --all` — run and task
      state, attempt history, durations, failure summaries over the journal.
      Worker host attribution is journaled; reading a remote run's journal
      lands in M5.2
- [ ] M5.2 Live follow: workers emit events over their transport, the
      orchestrator journals them; follow tails the journal into one
      aggregated view (works from another terminal against a running
      orchestrator, local or SSH)
- [x] M5.3 Run timeline and summary report: throughput, retry rates, worker
      utilization per run

## P6 — Slingshot

One command sends an experiment to rented hardware and brings results home:
provision, bootstrap, run, sync, tear down. Teardown must be guaranteed —
leaked instances are leaked money.

- [x] M6.1 Provider abstraction: provision / destroy / list / price query;
      instance lifecycle owned by the run, teardown on success, failure, and
      interrupt. `list` returns concrete, normalized offers, not instance
      types — (GPU model, VRAM, GPU count, $/hr, host reliability, verified
      status, disk, bandwidth, location, offer id); a marketplace query is
      the general case and a fixed type catalog degenerates into it.
      Selection is provider-agnostic and splits into two parts kept
      deliberately separate: hard constraints in config (min VRAM,
      acceptable GPU models, verified hosts only, min reliability, max
      $/hr) that disqualify, and a single scalar ranking objective over
      qualifying offers (default: cheapest $/hr; no weighted multi-criteria
      scoring). Provision treats "offer no longer available" as a normal
      outcome and falls through to the next-ranked offer
- [x] M6.2 Vast.ai backend — the single provider for now; further providers
      (Hetzner, AWS, ...) are added on demand as separate milestones when
      needed. Translates the normalized filter into Vast.ai's marketplace
      query and maps its offer fields (reliability score, verified status,
      rental mode) into the normalized offer form; on-demand rentals only —
      interruptible bidding waits for the trust and budget machinery (M6.4,
      M6.6). Settle image delivery here: a manual remote takes
      `podman save | ssh docker load`, but Vast.ai pulls from a registry,
      so bootstrap implies publishing the image or loading it on-instance
      after boot
- [ ] M6.3 On-worker stats reduction: kernel-side population/activity counts
      so remote runs return stats always, snapshots only on a cheap predicate
      (placed here as the bandwidth guard; P7's funnel metrics consume the
      same reduction — the mechanism is shared). "Stats always" covers the
      failed-evaluation case: M1.5 gave `Outcome::Failed` stats symmetric with
      `Completed`, so the reduction covers failures too and a failed evaluation
      returns its cheap counts over the wire like a success. This is also the first
      real producer of stats, so it forces the `Stats` type decision M1.4
      deferred: M1.4 ships `Stats` as opaque bytes, and here it should likely
      become structured named scalars (population, activity, ...) the P7 funnel
      can threshold family-agnostically, plus an optional opaque family blob
      for anything richer — decide the shape here, consumed at M7.2
- [x] M6.4 Budget guard: total spend cap and rental-phase wall-clock limit
      per run, durable spend accounting
- [ ] M6.5 Distributed run: one local orchestrator drives a provisioned
      fleet beside the local devices. Run config declares (provider,
      constraints, objective, machine count); at run start the pipeline
      acquires through M6.1's loop against the M6.2 backend and registers
      each Ready instance as an SSH worker — the M4.3 framed stdio
      protocol through `ssh <host> podman run -i` on the published image —
      in the same pool as local thread, process, and GPU workers; the
      scheduler contract, leases, and frontier re-derivation are unchanged,
      so a slow host simply takes fewer leases. The store never leaves the
      orchestrator (task inputs and results cross inline, the M4.3
      settlement), and instances are not durable state: on crash or
      resume, reconcile destroys strays and acquisition re-derives the
      fleet from config — the same store-only recovery path as tasks. A
      worker failure is a lease expiry; an instance failure is `Gone` at
      the next poll; replacement is a fresh acquire against the same
      config, bounded by the M6.4 budget guard: the orchestrator polls the
      budget verdict on its heartbeat and tears the fleet down when it
      reports exhaustion; journal events and any CLI spend surface for
      rentals are settled here too. Teardown runs on every exit path
      (success, failure, interrupt) through guards plus reconcile.
      Acceptance: a real family search over the local machine
      plus ≥2 rented instances produces a manifest identical to the
      local-only reference run, and the provider account holds zero
      instances afterwards. This is the fleet M6.6's trust tiers assume
      and delivers the README's elastic scale-out principle
- [ ] M6.6 Trust-tiered scheduling: redundant execution, quorum validation,
      spot-check sampling, host reputation — the BOINC playbook; the largest
      mechanism in this phase, expected to split into several PRs at
      elaboration
- [ ] M6.7 End-to-end slingshot consolidation (phase acceptance): start a
      search locally; interrupt it mid-simulation (inside a segment chain);
      `sima migrate` to a freshly provisioned instance — sync closure, resume
      remotely, follow events live; sync results home; teardown verified. The
      have/want store sync `sima migrate` composes already exists in
      `sima-store` (built and tested standalone in M4.3); this milestone wires
      it into the migrate command.
      Assert the final manifest and segment states are identical to an
      uninterrupted local reference run

Expected to be re-split when reached; provider APIs and trust mechanisms hide
surprises.

## P7 — Evaluation funnel v1

Deliberately simple. The funnel machinery, with the cheapest deterministic
metrics only; metric research lives in its own track.

Phase acceptance: verdicts are pure functions of recorded data — re-running
the funnel over a recorded run reproduces identical verdicts, and changing
thresholds re-classifies without any re-execution.

- [ ] M7.1 Periodic snapshot/stats recording: segment boundaries (M2.3) are
      the natural sampling points; this milestone adds the recording policy,
      not a new mechanism. Absorbs the snapshot retention policy deferred
      from M3.4: what is kept, for how long, and what re-evaluation minimally
      requires
- [ ] M7.2 Verdict classification: dead / frozen / exploding / cyclic,
      thresholds from config. Classification reads named numeric metrics
      generically, so it requires the structured `Stats` decided at M6.3 rather
      than the opaque bytes M1.4 shipped — opaque stats would force a per-family
      decoder here and defeat the funnel's family-agnostic design
- [ ] M7.3 Staged cheapest-first funnel + re-evaluation from recorded runs
      without re-execution
- [ ] M7.4 Object packing for scale, beside retention (M7.1) as the other
      store-scaling lever: millions of small objects press on inode and
      directory limits; a pack format — many objects in one file with an
      index — is the answer

## P8 — Continuous-family rigor

Final research on the float families: the hard determinism, the harder
variants, and scale. The families already run and are explorable (P3); this
phase makes them rigorous and complete.

Phase acceptance: on one pinned backend class, every float family is
bit-identical run-to-run; across two distinct backend classes, results agree
within the recorded tolerance policy; a seeded search run reproduces its
fitness trajectory exactly. The tolerance policy (M8.1) is a written
deliverable — comparison metric, bound, and its provenance record format —
not an aspiration; it is the hardest determinism question in the project and
"done" is defined by tests against it.

- [ ] M8.1 Cross-substrate float tolerance policy: strict-IEEE shader path,
      fixed reduction order, and the tolerance policy document + tests (the
      comparison metric, bound, and provenance record format — the
      deliverable, per phase acceptance). Folds the compiled kernel and driver
      into the environment hash for the float families. Shared foundation for
      cross-substrate agreement across every family in P3
- [ ] M8.2 Lenia and Flow-Lenia (Lenia descoped here from P3). Lenia:
      large-radius convolution kernel + growth function; genome = kernel /
      growth parameters; seeded generator, CPU reference + WGSL kernel.
      Flow-Lenia: mass-conserving advection (semi-Lagrangian transport —
      the mass-redistribution scatter needs an order-independent scheme or it
      is nondeterministic even on one device) and spatially localized
      parameters (genome becomes a per-region field, not one global vector).
      CPU reference + WGSL kernel + cross-substrate tolerance tests
- [ ] M8.3 Search loop over continuous genomes (ES; gradient-based training is
      a standing research track — it changes the executor contract from "run"
      to "run + accumulate gradients")
- [ ] M8.4 Within-launch population batching for small grids

## P9 — Physarum (agent-field family)

The second executor kind: a stigmergic multi-agent model (Jones's slime-mould
transport networks). State is an agent population plus a trail field; the step
is sense → move → deposit → diffuse/decay, not a cell-local grid update.
Everything below this phase on the ladder — store, scheduler, distribution,
slingshot, funnel — is unchanged; the phase adds an executor kind in the
families layer and nothing beneath it. It is the proof that the infra is
family-agnostic.

Determinism approach (integer tier, bit-exact everywhere — to confirm at
M9.2): fixed-point agent state (position, heading) and nearest-cell sensing
keep motion exact on any hardware; deposits accumulate as fixed-point integers
via order-independent atomic add (integer addition is associative and exact,
so scatter ordering cannot change the sum); the field diffuse/decay is an
integer stencil. This keeps Physarum out of the P8 float-tolerance
machinery. The alternative — float agent state — moves it into that tier; the
tradeoff (dynamic range and motion smoothness vs bit-exactness) is the open
decision resolved in M9.2.

Phase acceptance: CPU/GPU bit-equality across an agent-count × field-extent ×
step-count matrix; a segmented agent-field run (compound segment state)
resumed equals an unsegmented run of equal length, bit-exact.

- [ ] M9.1 Agent-field executor kind (`sima-domains`): compound state (agent
      buffer ‖ field grid) serialized as one opaque snapshot object;
      segmentation composes — the segment boundary state is the compound
      state, the task-key input-state-ref mechanism is unchanged
- [ ] M9.2 Physarum family: fixed-point agent state, nearest-cell sensing,
      order-independent integer deposit, field diffuse/decay stencil; genome =
      sensor geometry (angles, distance), turn rate, deposit amount,
      decay/diffusion rates; seeded generator, mutation, CPU reference with
      known-answer tests; the fixed-point-vs-float determinism decision is
      resolved here
- [ ] M9.3 GPU kernels (agent update + field update) + CPU/GPU bit-equality
      matrix
- [ ] M9.4 First Physarum search through the full spine (funnel, slingshot,
      distribution unchanged); network-structure interestingness metrics feed
      the P7 funnel via the standing evaluation track

## P10 — Out-of-tree executors (extensibility without forking)

Through P9 every executor and generator is an in-tree trait implementation
selected by a compile-time format-id match (M1.6). This phase opens the
contract as a public extension surface: a custom executor — and generator, by
the same mechanism — is added against a stable API and registered at runtime,
with no sima source edit and no fork. The pure-compute trust boundary
(executors never touch the store; workers commit through the catalog) is what
makes loading foreign code safe; here it is enforced by isolation, not only by
convention. The contract must be proven by several real in-tree families first,
which is why the phase sits after the family phases rather than beside M1.4.

Mechanism is deliberately open, chosen at elaboration and informed by the P3 /
P8 / P9 families built against the same contract: dynamic library loading, a
subprocess/IPC protocol, WASM, or a manifest plus external binary each trade
isolation, performance, and packaging differently.

Phase acceptance: a custom executor built entirely against the published API,
in a separate repository with no sima source edit, runs through the full spine
(search, commit, re-evaluation, distribution) with results byte-reproducible
and portable exactly as an in-tree family's; its identity folds into the
environment hash, so two machines that load the same custom executor agree and
one that loads a different build is distinguished — determinism and store
portability (P1 acceptance (d)) hold across the boundary.

- [ ] P10.1 Stable contract API: freeze the executor/generator traits and their
      wire types (spec, params, artifact, stats, task input, execution context)
      as a versioned public surface decoupled from internal crate churn;
      document the compatibility guarantee.
- [ ] P10.2 Runtime registration: an out-of-tree executor announces its format
      id and is selected without editing sima's dispatch — the static
      format-id match (M1.6) becomes a registry. Registration and loading
      mechanism decided here. The registration unit follows the `Family`-bundle
      decision from M1.6: a third party registers the format-bound bundle
      (codec + executor + reference + kernel) as one object, with generators a
      separate plug targeting the format — do not fuse executor and generator.
- [ ] P10.3 Isolation and trust: run out-of-tree executors process-isolated so
      the pure-compute boundary is OS-enforced (foreign code cannot reach the
      store); their results feed the trust-tiered validation (P6.7).
- [ ] P10.4 Identity and packaging: fold a custom executor's identity (version,
      build/content hash) into the environment hash so runs stay reproducible
      and portable; define how a custom family is packaged, versioned, and
      pinned.
- [ ] P10.5 Reference out-of-tree executor: a worked example family in a
      separate repository, built only against the published API and exercised
      through the full spine — the phase's proof that no fork is required.

Expected to be re-split when reached; the registration and isolation mechanism
hides surprises.

## Research tracks (standing)

Parallel to the phase ladder, each eventually feeding it:

- **Further model families** — graph CAs, attention-based update rules,
  program-shaped candidates; each lands as a rule family on unchanged infra.
  The ladder's own family phases (reaction-diffusion, Lenia/Flow-Lenia, Neural
  CA in P3; Physarum in P9) are the first proof of that promise
- **Evaluation / interestingness** — novelty, diversity, complexity metrics;
  the funnel machinery (P7) is the harness, the metrics are open research
- **Gradient-based training** — backprop through CA steps changes the executor
  contract from "run" to "run + accumulate gradients"; NCA literature
  precedent exists
- **IR / DSL** — composition-as-data over data-parallel primitives
  (map/stencil/reduce, later matmul), one definition compiling to both GPU
  kernel and CPU reference; surface syntax last

## Done

## Dropped

- Totalistic family (outer-totalistic 3D CA): dropped as a domain. It was
  scaffolding to exercise the GPU path; the M2.1 toolkit's own smoke kernel
  serves that role, and the float families (P3) are the real first workload.
