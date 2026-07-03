# SIMA

**Search In the Manifold of Automata**

Distributed infrastructure for generating candidate programs, executing them
safely at scale, and evaluating them through a staged, cost-aware pipeline. SIMA
targets workloads where programs are produced in large volume — including by
models — and must be run and assessed reliably, reproducibly, and at low cost.

SIMA is local-first. A run starts on a single machine and scales out, when
needed, to pluggable remote execution backends — cheap spot marketplaces,
reliable cloud instances, or anything in between — without changing the
workload or losing determinism. Systems in this space (FunSearch, AlphaEvolve)
assume a homogeneous, trusted, effectively unbounded cluster; SIMA assumes a
laptop, and treats everything beyond it as an elastic, heterogeneous extension.

## Overview

SIMA proposes candidate programs, runs them under isolation, scores them through a
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

**Generation.** Produces candidate programs as executable specifications.
Generators are pluggable — procedural, evolutionary, or model-based — and run in
discrete batches rather than as a persistent service. Model-based generators
follow the AlphaEvolve findings: diff-based edits against a parent rather than
whole-program regeneration, prompts assembled from high-scoring prior candidates
and their evaluation feedback, and an ensemble mixing a cheap model for breadth
with an expensive one for occasional high-quality jumps.

**Execution.** Runs untrusted, generated programs in isolated workers with
enforced CPU, memory, and wall-clock limits. Work is scheduled across available
cores — and across configured remote backends — with backpressure when
generation outpaces execution; non-terminating or runaway programs are detected
and terminated. Outputs are captured deterministically.

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

A task is identified by content: `(program hash, seed, environment hash) →
result`. Because tasks are pure functions of their inputs, any backend that
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

**Determinism survives heterogeneity.** Portable workloads run on a
deterministic sandboxed substrate (WebAssembly/WASI) so that the same task key
yields bit-identical results on any backend. Execution cost is metered in
instructions ("fuel") rather than wall-clock time, so fitness scores remain
comparable between a laptop and a rented GPU box. Workloads that require native
or accelerator execution relax this to statistical reproducibility on a pinned
backend class, and the relaxation is recorded in provenance.

**Trust is a scheduling dimension.** Cheap marketplace hardware is untrusted: a
result may be wrong or fabricated. The evaluation funnel therefore doubles as a
trust funnel — early, cheap stages may run on untrusted backends, and
survivors are re-verified on a trusted tier (local or reserved cloud) before
they reach expensive model review. Content-addressed tasks make redundant
execution and spot-checking trivial, in the tradition of BOINC.

## Design principles

- **Reproducibility.** All randomness is seeded and captured; a recorded
  specification reproduces its output exactly.
- **Isolation.** Untrusted, generated programs run under strict resource and time
  limits in isolated workers.
- **Local-first, elastic scale-out.** A run is fully functional on one machine;
  remote backends extend capacity without changing semantics.
- **Backend-agnostic determinism.** A task's result is a function of its
  content-addressed inputs, not of where it ran.
- **Cost- and trust-aware evaluation.** Expensive and model-based scoring runs
  only on candidates that survive cheap deterministic filtering; untrusted
  backends are confined to stages whose results are cheap to verify.
- **Pluggable generation and evaluation.** Generators and evaluators are decoupled
  from the execution and provenance layers.

## Usage

A run is defined by a configuration specifying the generator, execution limits,
backends, and evaluation stages:

```
sima run config.yaml
```

Example configuration:

```yaml
generator:
  type: model          # procedural | evolutionary | model
  batch_size: 1024

execution:
  workers: 8
  fuel_limit: 10G      # instruction budget; wall-clock is a backstop
  time_limit: 5s
  memory_limit: 512MB
  isolation: strict
  backends:
    - type: local      # default; also the trusted verification tier
    - type: vast
      max_price: 0.20/h
      trust: untrusted # early stages only; survivors re-verified
    - type: aws
      instance: c7g.xlarge
      trust: trusted

evaluation:
  - filter: validity
  - filter: liveness
  - metric: novelty
  - review: model      # runs only on survivors

provenance:
  store: ./runs
```

Surviving candidates and their lineage are written to the provenance store for
inspection and re-evaluation.

## Project layout

```
sima/
  generate/     candidate generators
  execute/      sandboxed execution and scheduling
  backends/     pluggable local and remote execution backends
  evaluate/     staged evaluation pipeline
  provenance/   content-addressed store and lineage graph
  cli/          command-line interface
```

## Status

Early development. Interfaces are subject to change.

## License

See `LICENSE`.
