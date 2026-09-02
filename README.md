<div align="center">

# SIMA<br/><sub><sup><sub><sup><sub><em>Search In the Manifold of Automata</em></sub></sup></sub></sup></sub>

**Distributed Program Search on Heterogeneous GPUs**

[![ci](https://github.com/alvatar/sima/actions/workflows/ci.yml/badge.svg)](https://github.com/alvatar/sima/actions/workflows/ci.yml)

</div>

*Code in this project is AI-generated under rigorous human review and
engineering discipline.*

SIMA generates candidate programs in volume, executes them deterministically on
GPUs, and evaluates them through a staged, cost-aware funnel, recording every
result with complete provenance. A candidate is data: a spec interpreted by a
fixed engine, so there is nothing to sandbox and execution cost is bounded by
construction. Where FunSearch and AlphaEvolve assume a trusted cluster, SIMA
assumes a laptop and treats everything beyond it as an elastic extension.

## What you can do

- **Search a space of programs.** One `sima.toml` declares a generator
  (procedural, evolutionary, or LLM-driven), and the scheduler fans candidates
  out across your GPUs. Every result lands in a content-addressed store.
- **Scale to many machines.** `[host.<name>]` declares a machine, `[fleet]`
  lists the members a search may use, and `sima search --fleet` uses them.
  Workers run in a container over ssh; the store stays on the orchestrator.
- **Migrate a search.** `sima migrate` moves the store and the orchestrator to
  another machine, resumes there, and streams events back. The far search is
  detached: a dropped connection or Ctrl-C leaves it computing. `sima recall`
  brings the results home.
- **Run one command on rented hardware.** An `[exec]` job names a shell
  command, a payload, output globs, and a rented host. `sima exec` delivers,
  streams the log, and fetches the outputs. `--attach`, `--end`, and
  `--one-shot` control the machine's lifetime.
- **Watch from anywhere.** `sima tui`, `sima follow`, `sima status`, and
  `sima report` observe a search, locally or with `--on <ssh-host>`. Observation
  takes no lock and writes nothing.
- **Bring your own program.** Register a binary under `[domain."<format>"]`. It
  speaks two small stdin/stdout protocols (`docs/protocol.md`); `sima-api` is
  the Rust SDK and the `sima` Python package the other. `examples/stepper-py/`
  is a complete program. A `payload` beside the binary is what delivers it to
  other machines.
- **Reproduce any result.** A task is identified by content, so a result is
  regenerated from its key alone and any conforming backend is interchangeable.
- **Stop and continue.** The store is the only durable state. Resume, crash
  recovery, and running again are one code path.

## Requirements

- **Rust**, edition 2024.
- **A Vulkan GPU**: the default backend (WGSL lowered to SPIR-V). Needs the
  loader and a device ICD.
- **NVIDIA driver** *(optional)*: the CUDA backend. The workspace builds
  without it. Kernel compilation is pinned to NVRTC 12.0.x, vendored beside the
  binaries at build time; `SIMA_NVRTC_DIR` supplies a local copy for offline
  builds.
- **ssh and a container runtime** *(optional)*: fleet, migrate, and exec.

## Quick start

```sh
cargo build --release
target/release/sima search examples/gray-scott-search         # drive the search
target/release/sima tui examples/gray-scott-search            # watch it live
target/release/sima report examples/gray-scott-search         # per-candidate stats
target/release/sima status examples/gray-scott-search --on gpubox
target/release/sima report examples/gray-scott-search --spend # rental spend
```

`TUTORIAL.md` walks a program through a complete search.
`containers/sima/README.md` covers the image rented machines run.

## How it works

Four independent stages communicate through the store:

```
generate → execute → evaluate → record
```

- **Generation** produces candidate specs in batches. Model-based generation
  follows AlphaEvolve: edits against a high-scoring parent, prompts from
  evaluation feedback, a cheap/strong model ensemble.
- **Execution** runs candidates on fixed engines, GPU or CPU reference. One
  orchestrator drives stateless workers; executors never touch the store.
- **Evaluation** filters cheapest stage first, so expensive scoring runs on a
  small fraction. Untrusted backends are confined to cheaply verified stages.
- **Provenance** links specs, seeds, environments, outputs, and verdicts in the
  store.

`docs/architecture.md` is the full design.

## Determinism

Two tiers, declared by each domain:

- **Tier 1, reproducible by content.** The engine's arithmetic is controlled
  end to end. Integer engines are bit-exact everywhere; float engines hold
  within a backend class once compiler and reduction order are pinned.
- **Tier 2, reproducible by declaration.** The engine calls an external library
  or model. SIMA records its declared identity and compares by tolerance or
  rubric instead of hash equality.

Device binding is operational state, never identity. All result-affecting
randomness derives from a counter-based PRNG identical on every substrate.

Each toolkit pins the canonical id of its compiler; the id is hashed into the
task key and guarded by a known-answer test, so a dependency bump that changes
the emitted program forces a deliberate update:

| Toolkit | Canonical id | Guard |
|---|---|---|
| `sima-toolkit-wgsl` | `naga 30.0.0; spirv=1.5; opt=none` | SPIR-V known-answer test |
| `sima-toolkit-cuda` | `ptx; arch=compute_75` | PTX regeneration test per kernel |

## Known caveat

A hard crash (`SIGKILL`, power cut) leaves a rented machine running and
billing. Any acquisition against the same store reconciles first, and
`sima reconcile <config>` runs that pass alone. Until one runs, the provider's
console is the way to end it.

## References

- **AlphaEvolve**, Novikov et al., 2025. [arXiv:2506.13131](https://arxiv.org/abs/2506.13131).
- **FunSearch**, Romera-Paredes et al., Nature, 2024. [doi:10.1038/s41586-023-06924-6](https://www.nature.com/articles/s41586-023-06924-6).
- **MAP-Elites**, Mouret & Clune, 2015. [arXiv:1504.04909](https://arxiv.org/abs/1504.04909).
- **OpenEvolve**. [github.com/codelion/openevolve](https://github.com/codelion/openevolve).
- **Bazel Remote Execution API**. [github.com/bazelbuild/remote-apis](https://github.com/bazelbuild/remote-apis). Model for the task key.
- **Nix**, Dolstra, 2006. [thesis](https://edolstra.github.io/pubs/phd-thesis.pdf). Model for the environment component.
- **BOINC**, Anderson, 2019. [arXiv:1903.01699](https://arxiv.org/abs/1903.01699). Playbook for trust-tiered scheduling.
- **Ray**, Moritz et al., OSDI 2018. [arXiv:1712.05889](https://arxiv.org/abs/1712.05889).

## License

Apache-2.0 ([LICENSE-APACHE](LICENSE-APACHE)) or MIT ([LICENSE-MIT](LICENSE-MIT)),
at your option. Contributions are dual licensed as above unless stated otherwise.
