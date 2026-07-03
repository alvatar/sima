# TODO

Roadmap. Phases are large project stages; milestones are PR-sized units of
work, each with a fully elaborated `work/TODO-<topic>.md` while in flight.
This document is living — structure and content evolve through discussion.

Settled context: Rust, local-first. GPU execution via Vulkan compute (`ash`),
shaders compiled to SPIR-V at build time. Content addressing with blake3.
Candidates are genomes — data interpreted by fixed engines, pluggable through
a rule-family boundary. CA is the substrate; the research object is
learned/evolved computation on it. Evaluation research and model-family
research are standing tracks, deliberately out of the phase ladder.

## P1 — Infrastructure spine

The system with no CA in it: content-addressed store, provenance, scheduler,
executor contract, pipeline, CLI. Executor #1 is a deterministic CPU stub that
exists to prove determinism, resumability, crash-retry, and
re-evaluation-from-store end-to-end.

- [ ] M1.1 Crate skeleton: `Error`/`Result`, canonical encoding, counter-based
      PRNG with pinned known-answer tests
- [ ] M1.2 Content-addressed store: blake3 CAS, atomic writes, object
      round-trips
- [ ] M1.3 Task keys, provenance records, run manifests
- [ ] M1.4 Executor contract + deterministic CPU stub executor + determinism
      tests (run twice → identical hashes)
- [ ] M1.5 Scheduler: queue, leases, heartbeat/timeout, retries, backpressure;
      thread-worker transport
- [ ] M1.6 Pipeline orchestration + `sima run` CLI + end-to-end determinism,
      resume, and re-evaluation-from-store tests

## P2 — GPU executor + totalistic family

First real executor. Outer-totalistic 3D CA as the simplest family: exercises
the GPU path and the family boundary while staying trivial to verify.

- [ ] M2.1 ash compute bringup: device selection, buffers, dispatch,
      build-time shaderc, SPIR-V hashes folded into the environment hash
- [ ] M2.2 Totalistic family: genome, generator, mutation + CPU reference
      engine with known-answer tests
- [ ] M2.3 GPU totalistic kernel + CPU/GPU bit-equality cross-check matrix
      (extents, genomes, step counts)
- [ ] M2.4 First real search: ≥1000 genomes on the local GPU through the full
      spine; throughput and survivor numbers recorded here

## P3 — Evaluation funnel v1

Deliberately simple. The funnel machinery, with the cheapest deterministic
metrics only; metric research lives in its own track.

- [ ] M3.1 Periodic snapshot/stats recording through the executor contract
- [ ] M3.2 Verdict classification: dead / frozen / exploding / cyclic,
      thresholds from config
- [ ] M3.3 Staged cheapest-first funnel + re-evaluation from recorded runs
      without re-execution

## P4 — Neural CA

The primary model family. Genomes are parameter vectors of arbitrary update
functions: perception kernels and update weights both evolvable.

- [ ] M4.1 Float/multi-channel grid state, strict-IEEE shader path, tolerance
      policy for cross-substrate checks
- [ ] M4.2 NCA family: genome (perception + update parameters), CPU reference
- [ ] M4.3 GPU NCA kernel + cross-substrate tolerance tests
- [ ] M4.4 ES-based search loop over NCA genomes
- [ ] M4.5 Within-launch population batching for small grids

## P5 — Distribution

The scheduler contract from M1.5 gains transports. Content-addressed
idempotent tasks make at-least-once delivery, retry, dedup, and spot-check
verification safe by construction.

- [ ] M5.1 Multi-process worker transport (same scheduler contract)
- [ ] M5.2 Multi-GPU on one host
- [ ] M5.3 First remote backend: container over SSH, Vast.ai manually
      provisioned
- [ ] M5.4 Trust-tiered scheduling: redundant execution, spot-check
      verification

Expected to be re-split when reached; remote transport hides surprises.

## Research tracks (standing)

Parallel to the phase ladder, each eventually feeding it:

- **Model families beyond NCA** — Lenia-family continuous CAs, graph CAs,
  attention-based update rules; each lands as a rule family on unchanged infra
- **Evaluation / interestingness** — novelty, diversity, complexity metrics;
  the funnel machinery (P3) is the harness, the metrics are open research
- **Gradient-based training** — backprop through CA steps changes the executor
  contract from "run" to "run + accumulate gradients"; NCA literature
  precedent exists
- **IR / DSL** — composition-as-data over data-parallel primitives
  (map/stencil/reduce, later matmul), one definition compiling to both GPU
  kernel and CPU reference; surface syntax last

## Done

## Dropped
