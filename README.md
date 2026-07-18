<div align="center">

# SIMA

**Distributed GPU Program Search and Execution**

<sub><em>Search In the Manifold of Automata</em></sub>

[![ci](https://github.com/alvatar/sima/actions/workflows/ci.yml/badge.svg)](https://github.com/alvatar/sima/actions/workflows/ci.yml)

</div>

SIMA generates candidate programs in volume, executes them deterministically on
GPUs, and evaluates them through a staged, cost-aware funnel — recording every
result with complete provenance. A candidate is data, not code: a spec (a
network's weights, a cellular-automaton genome) interpreted by a fixed engine,
so there is nothing to sandbox and execution cost is bounded by construction.

Systems in this space (FunSearch, AlphaEvolve) assume a homogeneous, trusted,
effectively unbounded cluster. SIMA assumes a laptop, and treats everything
beyond it as an elastic, heterogeneous extension.

## What you can do

- **Search a family of automata.** Declare a run in one `sima.toml`: a
  generator seeds a population of genomes, the scheduler fans candidates out
  across your GPUs, and every result lands in a content-addressed store.
  Gray-Scott reaction-diffusion and asynchronous neural cellular automata are
  in-tree; domains are pluggable.
- **Reproduce any result.** A task is identified by content — spec, run
  parameters, seed, environment, input state — so a recorded result can be
  regenerated from its identity alone, and any backend that returns a result
  for a given key is interchangeable with any other. This is the model of
  Bazel's Remote Execution API, applied to program search.
- **Stop and continue.** The store is the only durable state; resume, crash
  recovery, and re-run are one code path that re-derives the runnable frontier
  from the store. Kill `sima run` at any point and run it again.
- **Scale from one GPU to many machines.** Multi-GPU on one host through
  device classes; remote workers run in containers over ssh from a
  `[[execution.remote]]` entry, speaking the same wire protocol — task inputs
  and results cross inline, and the store never leaves the orchestrator.
  Worker faults converge through idempotent retry.
- **Slingshot a run.** Start a search on the laptop, then carry it to rented
  GPU machines: a have/want store-sync protocol transfers exactly the missing
  records and objects over any byte pipe. The sync engine is built; `sima
  migrate`, wiring it over ssh, is on the roadmap.
- **Watch it run.** `sima tui` drives a run in a full-screen live view;
  `sima status` and `sima report` print run state and per-candidate stats.

The roadmap (`TODO.md`) continues with provisioned backends (Vast.ai, Hetzner,
AWS), budget guards, trust-tiered scheduling — redundant execution and quorum
validation for cheap untrusted spot hardware — and a staged evaluation funnel
with verdict classification.

## Quick start

Requires a Vulkan-capable GPU.

```sh
cargo build --release
target/release/sima run examples/gray-scott-search    # drive the run
target/release/sima tui examples/gray-scott-search    # or watch it live
target/release/sima report examples/gray-scott-search # per-candidate stats
```

## How it works

A batch pipeline of four stages, independent and communicating through the
store:

```
generate → execute → evaluate → record
```

- **Generation** produces candidate specs in discrete batches. Generators are
  pluggable — procedural, evolutionary, or model-based; model-based generation
  follows the AlphaEvolve findings (edits against a high-scoring parent,
  prompts assembled from evaluation feedback, a cheap/strong model ensemble).
- **Execution** runs candidates on fixed engines: GPU via Vulkan compute, with
  CPU reference implementations for verification. A single orchestrator drives
  stateless workers under process isolation; the trust boundary sits on the
  worker seam, and executors never touch the store.
- **Evaluation** reduces a batch to the few candidates worth attention,
  cheapest stage first, so expensive model-based scoring runs on a small
  fraction. The funnel doubles as a trust funnel: untrusted backends are
  confined to stages whose results are cheap to verify, and survivors are
  re-verified on a trusted tier.
- **Provenance** links specs, seeds, environments, outputs, and verdicts in a
  content-addressed store, so any result can be regenerated and traced.

## Determinism

SIMA reproduces a result from its recorded identity, across two tiers a domain
declares through its environment components:

- **Tier 1 — reproducible by content.** The engine's arithmetic is controlled
  end to end; its identity is the engine source plus the compiler that
  produced it. Integer engines are bit-exact on every device. Float engines
  hold within a backend class once compiler and reduction order are pinned,
  with a recorded tolerance across classes. *Example:* a Vulkan kernel
  compiled from WGSL to SPIR-V by naga.
- **Tier 2 — reproducible by declaration.** The engine calls into an external
  library or model (PyTorch, an LLM) whose internal determinism SIMA does not
  control. SIMA records the declared identity of everything in the path,
  treats the library as a determinism boundary, and compares by tolerance or
  rubric score instead of hash equality.

Device binding is derived operational state, never identity: placement is
greedy — a faster card naturally takes more work — and sticky, so a
candidate's trajectory stays on one device class and moves only when its
device is gone, with the journal saying so. All randomness in a
result-affecting path derives from a counter-based PRNG implemented
identically on every substrate.

Each execution toolkit pins the canonical version id of the engine or compiler
it runs kernels through; the id is hashed into the task key and guarded by a
known-answer test, so a dependency bump that changes the emitted program
forces a deliberate update in the same change:

| Toolkit | Canonical id | Tier | Guard |
|---|---|---|---|
| `sima-toolkit-wgsl` | `naga 30.0.0; spirv=1.5; opt=none` | 1 | SPIR-V known-answer test (`compile.rs`) |

## Design principles

- **Reproducibility.** All randomness is seeded and captured; a recorded
  specification reproduces its output exactly.
- **Candidates as data.** Specs are interpreted by fixed engines; there is no
  untrusted code path, and execution cost is a deterministic function of the
  task itself (for a cellular automaton, cells × steps).
- **Elastic scale-out.** A run is fully functional on one machine; remote
  backends extend capacity without changing semantics.
- **Backend-agnostic determinism.** A task's result is a function of its
  content-addressed inputs, not of where it ran.
- **Cost- and trust-aware evaluation.** Expensive scoring runs only on
  candidates that survive cheap deterministic filtering; untrusted backends
  are confined to stages whose results are cheap to verify.
- **Pluggable generation and evaluation.** Generators and evaluators are
  decoupled from the execution and provenance layers.

## References

- **AlphaEvolve** — Novikov et al., 2025.
  [arXiv:2506.13131](https://arxiv.org/abs/2506.13131). State of the art in
  LLM-driven program search; SIMA adopts its algorithm-layer ideas as
  pluggable components and replaces its trusted-cluster substrate.
- **FunSearch** — Romera-Paredes et al., Nature, 2024.
  [nature.com/articles/s41586-023-06924-6](https://www.nature.com/articles/s41586-023-06924-6).
  Proof of concept for the loop SIMA industrializes; showed the economics of
  the loop, not just the model, determine what is reachable.
- **MAP-Elites** — Mouret & Clune, 2015.
  [arXiv:1504.04909](https://arxiv.org/abs/1504.04909). Diversity-preserving
  search; the basis of SIMA's novelty and diversity scoring stage.
- **OpenEvolve** —
  [github.com/codelion/openevolve](https://github.com/codelion/openevolve).
  Open-source AlphaEvolve reimplementation on a single machine; illustrates
  the gap SIMA targets — the search loop is replicable, the substrate is not.
- **Bazel Remote Execution API** —
  [github.com/bazelbuild/remote-apis](https://github.com/bazelbuild/remote-apis).
  The direct model for SIMA's task key: content-addressed actions, results
  cached by digest, any conforming executor interchangeable.
- **Nix** — Dolstra, PhD thesis, 2006.
  [edolstra.github.io/pubs/phd-thesis.pdf](https://edolstra.github.io/pubs/phd-thesis.pdf).
  Environments identified by the hash of everything that went into them; the
  model for the task key's environment component.
- **WebAssembly / WASI** — Haas et al., PLDI 2017.
  [wasi.dev](https://wasi.dev). Reference substrate for a possible future in
  which candidates are arbitrary evolved programs; the current
  candidates-as-data design gets determinism, cost bounds, and safety from
  its fixed engines instead.
- **BOINC** — Anderson, 2019.
  [arXiv:1903.01699](https://arxiv.org/abs/1903.01699). Two decades of
  computing on heterogeneous, unreliable, untrusted hardware; its redundancy,
  quorum, and spot-checking mechanisms are the playbook for trust-tiered
  scheduling.
- **Ray** — Moritz et al., OSDI 2018.
  [arXiv:1712.05889](https://arxiv.org/abs/1712.05889). The ergonomics
  benchmark for annexing machines without restructuring the program; Ray
  tasks are not content-addressed or deterministic by construction, which is
  what SIMA's provenance layer requires.
