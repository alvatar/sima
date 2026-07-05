# SIMA

## *Search In the Manifold of Automata*

Distributed infrastructure for generating candidate programs, executing them
deterministically at scale, and evaluating them through a staged, cost-aware
pipeline. A candidate is a GPU program treated as opaque compute — currently
neural networks and cellular-automata-like models (including neural cellular
automata), with the infrastructure agnostic to what a candidate computes. SIMA
targets workloads where candidates are produced in large volume — including by
models — and must be run and assessed reliably, reproducibly, and at low cost.

A SIMA run starts on a single machine and scales out, when
needed, to pluggable remote execution backends — cheap spot marketplaces,
reliable cloud instances, or anything in between — without changing the
workload or losing determinism. Systems in this space (FunSearch, AlphaEvolve)
assume a homogeneous, trusted, effectively unbounded cluster; SIMA assumes a
laptop, and treats everything beyond it as an elastic, heterogeneous extension.

## Overview

SIMA proposes candidates as specs — opaque parameter data interpreted by fixed
engines (a network's weights, a cellular-automaton genome) — runs them on GPU or
CPU, scores them through a
staged evaluation pipeline, and records each with complete provenance. The space
of candidates is large and mostly low-value; the system exists to traverse it
efficiently and surface the results worth attention while keeping the cost of
generation and evaluation under control.

Generation and evaluation are model-aware. Candidates may be produced
procedurally, by evolutionary search, or by a model; evaluation may combine cheap
deterministic checks with model-based scoring. Expensive model calls are batched
and confined to the late stages of the pipeline so that most candidates are
resolved without them.

## Architecture

SIMA is a batch pipeline with four stages.

**Generation.** Produces candidate specs — for CA families, genomes: parameter
vectors of the update rule. Generators are pluggable — procedural,
evolutionary, or model-based — and run in discrete batches rather than as a
persistent service. Model-based generators follow the AlphaEvolve findings:
edits against a high-scoring parent rather than regeneration from scratch,
prompts assembled from prior candidates and their evaluation feedback, and an
ensemble mixing a cheap model for breadth with an expensive one for occasional
high-quality jumps.

**Execution.** Runs candidates on fixed engines — GPU via Vulkan compute, with
CPU reference implementations for verification. Candidates are data, not code:
there is nothing to sandbox, and execution cost is bounded by construction (for
a cellular automaton, cells × steps). Work is scheduled across available devices — and
across configured remote backends — with backpressure when generation outpaces
execution; worker failures are contained by process isolation and converge
through idempotent retry. Outputs are captured deterministically.

**Evaluation.** Reduces a batch to the few candidates worth attention through a
staged funnel, cheapest stage first: validity and liveness filters, deterministic
metrics, novelty and diversity scoring, and optional model- or human-in-the-loop
review of survivors. Later stages run only on candidates that pass earlier ones,
so expensive scoring is applied to a small fraction of each batch.

**Provenance.** Records every candidate so that any result can be regenerated and
traced. Specifications, seeds, environment, outputs, and verdicts are stored under
a content-addressed scheme and linked in a lineage graph.

## Pipeline

```
generate → execute → evaluate → record
```

Stages are independent and communicate through the store, so a batch can be
re-run, resumed, or re-evaluated without regeneration.

## Scaling model

A task is identified by content: `(spec hash, params hash, seed, environment
hash, optional input-state hash) → result`. The spec is the candidate; params
are the run parameters it is evaluated under (extent, steps, budgets), kept
separate so the same candidate is addressable across evaluation stages.
Because tasks are pure functions of
their inputs, any backend that
returns a result for a given key is interchangeable with any other, and results
are cacheable, retryable, and independently verifiable. This is the same model
as Bazel's Remote Execution API, applied to program search instead of builds.
The remote layer is not bolted onto the pipeline; it falls out of the
provenance scheme.

**Backends are pluggable and heterogeneous.** The default backend is the local
machine. Additional backends — a Vast.ai spot instance, a fleet of AWS
machines, another box on the LAN — are declared in the run configuration and
differ in cost, reliability, and trust. The scheduler places work according to
those parameters rather than treating the pool as uniform.

**Determinism survives heterogeneity.** Candidates are specs — data interpreted
by fixed engines — so determinism is a property of the engines, established once,
per family (see Determinism below). Execution cost is a deterministic function of
the task itself (for a cellular automaton, cells × steps), so
fitness and cost remain comparable between a laptop and a rented GPU box
without metering instrumentation.

**Trust is a scheduling dimension.** Cheap marketplace hardware is untrusted: a
result may be wrong or fabricated. The evaluation funnel therefore doubles as a
trust funnel — early, cheap stages may run on untrusted backends, and
survivors are re-verified on a trusted tier (local or reserved cloud) before
they reach expensive model review. Content-addressed tasks make redundant
execution and spot-checking trivial, in the tradition of BOINC.

## Determinism

Determinism is decided per family by the arithmetic of its engine, and it is a
tested property, never an assumption: the same task run twice — or on two
substrates — must produce identical content hashes, or agree within a recorded
tolerance, and CI enforces it.

**Integer families are bit-exact everywhere.** A synchronous, double-buffered
stencil over integer state makes each output cell a pure integer function of
the previous grid. Scheduling order, workgroup shape, GPU model, vendor, and
driver are all irrelevant to the result: the same genome and seed produce the
identical grid on a CPU reference implementation, the local GPU, and any
rented backend. Cross-substrate verification compares hashes for equality, so
spot-checking an untrusted worker is exact.

**Float families — continuous automata and neural networks — are reproducible
per backend class.** The variables are the compiler and the kernel: FMA
contraction, reassociation, fast-math relaxations, and the order of any
reduction differ across drivers, vendors, and library versions. SIMA compiles
with strict IEEE settings, holds reductions to a fixed order, and folds the
compiled kernel and driver into the environment hash, so a task key pins the
exact arithmetic: the same GPU model and driver class reproduce results
bit-for-bit, and comparisons across classes use a recorded tolerance policy.
Where an engine can remove a hazard structurally it does — a double-buffered
stencil has no cross-cell reduction to order — but a neural engine's matmuls
and convolutions do, so there determinism is deliberate: deterministic kernels
and a fixed reduction order rather than a gift of the substrate. If
unconditional bit-exactness is ever required, fixed-point integer arithmetic
restores it at some cost in dynamic range.

**Nondeterminism is controlled at the engine, not assumed away.** The sources
that plague general GPU compute — atomics-ordered accumulation, multi-kernel
async races, library autotuning, unordered reductions — are designed out of the
stencil engines and pinned down in the neural ones. A candidate is data for a
fixed engine, never arbitrary code, so the only surface where nondeterminism
can enter is the engine itself — established and tested once per family, not per
candidate.

All randomness is derived from a counter-based PRNG implemented identically on
every substrate; no result-affecting path uses a platform RNG.

## Design principles

- **Reproducibility.** All randomness is seeded and captured; a recorded
  specification reproduces its output exactly.
- **Candidates as data.** Specs are interpreted by fixed engines; there is no
  untrusted code path, and execution cost is a deterministic function of the
  task itself.
- **Elastic scale-out.** A run is fully functional on one machine;
  remote backends extend capacity without changing semantics.
- **Backend-agnostic determinism.** A task's result is a function of its
  content-addressed inputs, not of where it ran.
- **Cost- and trust-aware evaluation.** Expensive and model-based scoring runs
  only on candidates that survive cheap deterministic filtering; untrusted
  backends are confined to stages whose results are cheap to verify.
- **Pluggable generation and evaluation.** Generators and evaluators are decoupled
  from the execution and provenance layers.

## References

Systems and papers SIMA draws on, with notes on what each contributes.

### Program search

- **AlphaEvolve** — Novikov et al., *AlphaEvolve: A coding agent for scientific
  and algorithmic discovery*, 2025. [arXiv:2506.13131](https://arxiv.org/abs/2506.13131)
  ([DeepMind blog](https://deepmind.google/blog/alphaevolve-a-gemini-powered-coding-agent-for-designing-advanced-algorithms/)).
  The state of the art in LLM-driven program search. An asynchronous pipeline of
  a controller, LLM samplers, and evaluator nodes iterates over an evolutionary
  database of programs: parents and high-scoring "inspirations" are assembled
  into prompts, a model ensemble (fast model for breadth, strong model for
  quality) proposes diffs against marked code blocks, and candidates pass
  through evaluation cascades of increasing cost. Found a 48-multiplication
  algorithm for 4×4 complex matrix multiplication (first improvement over
  Strassen's construction in that setting in 56 years) and optimized production
  systems at Google. SIMA adopts its algorithm-layer ideas as pluggable
  components; its substrate — a homogeneous trusted cluster — is what SIMA
  replaces.

- **FunSearch** — Romera-Paredes et al., *Mathematical discoveries from program
  search with large language models*, Nature, 2024.
  [nature.com/articles/s41586-023-06924-6](https://www.nature.com/articles/s41586-023-06924-6).
  AlphaEvolve's predecessor and the proof of concept for the loop SIMA
  industrializes: an LLM proposes program variants, a cheap deterministic
  evaluator scores them, and an island-based evolutionary database selects what
  to evolve next. Used millions of samples from a weak model where AlphaEvolve
  uses thousands from strong ones — a useful reminder that the economics of the
  loop, not just the model, determine what is reachable.

- **MAP-Elites** — Mouret & Clune, *Illuminating search spaces by mapping
  elites*, 2015. [arXiv:1504.04909](https://arxiv.org/abs/1504.04909).
  Diversity-preserving search: instead of keeping the N best candidates, keep
  the best candidate in each cell of a grid of behavioral features. The
  standard defense against mode collapse in evolutionary search, and the basis
  of AlphaEvolve's program database. Relevant to SIMA's novelty and diversity
  scoring stage.

- **OpenEvolve** — open-source AlphaEvolve reimplementation.
  [github.com/codelion/openevolve](https://github.com/codelion/openevolve).
  Reproduces the algorithm layer on a single machine. Illustrates the gap SIMA
  targets: the search loop is replicable, the execution substrate is not.

### Execution substrate

- **Bazel Remote Execution API (REv2)** — protocol specification.
  [github.com/bazelbuild/remote-apis](https://github.com/bazelbuild/remote-apis).
  The direct model for SIMA's scaling layer. Defines remote execution as
  content-addressed actions: inputs live in a content-addressable store (CAS),
  an action is a hash of its inputs and command, results are cached by action
  digest, and any conforming executor is interchangeable. Battle-tested by
  Bazel, Buck2, and BuildStream against exactly SIMA's problem shape — large
  volumes of small, hermetic, cacheable tasks fanned out to heterogeneous
  workers.

- **Nix** — Dolstra, *The Purely Functional Software Deployment Model*, PhD
  thesis, 2006. [edolstra.github.io/pubs/phd-thesis.pdf](https://edolstra.github.io/pubs/phd-thesis.pdf).
  Hermetic environments identified by the hash of everything that went into
  them. The model for the `environment hash` component of SIMA's task key: two
  backends agree on what "the environment" is because it is a content address,
  not a description.

- **WebAssembly / WASI** — Haas et al., *Bringing the Web up to Speed with
  WebAssembly*, PLDI 2017.
  [dl.acm.org/doi/10.1145/3062341.3062363](https://dl.acm.org/doi/10.1145/3062341.3062363);
  [wasi.dev](https://wasi.dev). A portable, sandboxed, deterministic execution
  format with instruction-metered runtimes ("fuel"). Retained as the reference
  substrate for a possible future in which candidates are arbitrary evolved
  programs rather than parameter data; the current genomes-as-data design gets
  determinism, cost bounds, and safety from its fixed engines instead, and
  needs no sandbox.

### Distributed execution

- **BOINC** — Anderson, *BOINC: A Platform for Volunteer Computing*, 2019.
  [arXiv:1903.01699](https://arxiv.org/abs/1903.01699). Two decades of
  high-throughput computing on hardware that is heterogeneous, unreliable, and
  untrusted — the adversarial version of a spot marketplace. Its mechanisms
  (redundant execution, quorum validation, spot-checking, adaptive replication
  based on host reputation) are the playbook for SIMA's trust-tiered
  scheduling.

- **Ray** — Moritz et al., *Ray: A Distributed Framework for Emerging AI
  Applications*, OSDI 2018. [arXiv:1712.05889](https://arxiv.org/abs/1712.05889).
  Elastic task scheduling with a distributed object store, and the ergonomics
  benchmark for "annex more machines without restructuring the program." A UX
  reference rather than a foundation: Ray tasks are not content-addressed or
  deterministic by construction, which is precisely what SIMA's provenance
  layer requires.

## License

See `LICENSE`.
