# SIMA

**Search In the Manifold of Automata**

Distributed infrastructure for generating candidate programs, executing them
safely at scale, and evaluating them through a staged, cost-aware pipeline. SIMA
targets workloads where programs are produced in large volume — including by
models — and must be run and assessed reliably, reproducibly, and at low cost.

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
discrete batches rather than as a persistent service.

**Execution.** Runs untrusted, generated programs in isolated workers with
enforced CPU, memory, and wall-clock limits. Work is scheduled across available
cores with backpressure when generation outpaces execution; non-terminating or
runaway programs are detected and terminated. Outputs are captured
deterministically.

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

## Design principles

- **Reproducibility.** All randomness is seeded and captured; a recorded
  specification reproduces its output exactly.
- **Isolation.** Untrusted, generated programs run under strict resource and time
  limits in isolated workers.
- **Cost-aware evaluation.** Expensive and model-based scoring runs only on
  candidates that survive cheap deterministic filtering.
- **Pluggable generation and evaluation.** Generators and evaluators are decoupled
  from the execution and provenance layers.

## Usage

A run is defined by a configuration specifying the generator, execution limits,
and evaluation stages:

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
  time_limit: 5s
  memory_limit: 512MB
  isolation: strict

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
  evaluate/     staged evaluation pipeline
  provenance/   content-addressed store and lineage graph
  cli/          command-line interface
```

## Status

Early development. Interfaces are subject to change.

## License

See `LICENSE`.
