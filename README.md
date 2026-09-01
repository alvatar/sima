<div align="center">

# SIMA<br/><sub><sup><sub><sup><sub><em>Search In the Manifold of Automata</em></sub></sup></sub></sup></sub>

**Distributed Program Search on Heterogeneous GPUs**

[![ci](https://github.com/alvatar/sima/actions/workflows/ci.yml/badge.svg)](https://github.com/alvatar/sima/actions/workflows/ci.yml)

</div>

*Code in this project is AI-generated under rigorous human review and
engineering discipline.*

SIMA generates candidate programs in volume — procedurally, evolutionarily, or
proposed by LLMs — executes them deterministically on GPUs, and evaluates them
through a staged, cost-aware funnel — recording every result with complete
provenance. It targets workloads where models produce and judge candidates at
scale: neural networks, LLM-driven autoresearch loops, and evolved program
families. A candidate is data, not code: a spec (a network's weights, a
genome) interpreted by a fixed engine, so there is nothing to sandbox and
execution cost is bounded by construction.

Systems in this space (FunSearch, AlphaEvolve) assume a homogeneous, trusted,
effectively unbounded cluster. SIMA assumes a laptop, and treats everything
beyond it as an elastic, heterogeneous extension.

## What you can do

- **Search a space of programs.** Declare a search in one `sima.toml`: a
  generator produces candidate specs — procedural, evolutionary, or an LLM
  proposing edits against high-scoring parents — the scheduler fans them out
  across your GPUs, and every result lands in a content-addressed store. The
  primary workloads are neural networks and LLM-driven autoresearch loops; a
  domain binds a spec format to its executor and generator, and
  cellular-automata evolution (Gray-Scott, asynchronous neural CA) is the
  first domain in-tree.
- **Scale from one GPU to many machines.** A search declares the machines it can
  use by naming them once: `[host.<name>]` is one machine, `[host_class.<name>]`
  several identical ones scaled by a count, `[fleet]` lists the members a search
  may draw on, and `[orchestrator]` is the machine you typed the command on.
  Multi-GPU on one host through device classes; a declared host runs its workers
  in a container over ssh, speaking the same wire protocol — task inputs and
  results cross inline, and the store never leaves the orchestrator. `sima search`
  uses the orchestrator alone and `sima search --fleet` adds every member, so
  declaring a machine says a search *may* use it and the invocation says it does.
  Worker faults converge through idempotent retry.
- **Migrate a search.** Start a search on the laptop, interrupt it, and
  `sima migrate` moves the whole search — its store and its orchestrator — onto the
  machine `[orchestrator].migrate` names, resumes it there, streams its events
  back, and brings the results home. A have/want store sync transfers exactly
  the missing records and objects, so what crosses is the difference and nothing
  else. The far search is detached, so nothing on your machine ends it — a dropped
  connection, a closed terminal, and a Ctrl-C all leave the destination
  computing, and re-running attaches to it again. `sima recall` is what winds it
  down and brings the results home; the manifest a migrated search writes is
  byte-identical to one that never left.
- **Run one command on rented hardware.** An `[exec]` job names one opaque shell
  command, a payload, output globs, and one rented `[host.*]`. `sima exec`
  delivers the payload, streams the command's log, and fetches the declared
  files plus the log on every exit. The machine stays available by default:
  another invocation adopts it, `--attach` follows a detached command, and
  `--end` fetches and releases it. `--one-shot` releases it after one command.
  The store is limited to rental accounting and payload objects for this
  contract; search state remains exclusive to a search.
- **Watch it run, from anywhere.** `sima tui` drives a search in a full-screen
  live view and `sima follow` streams its events to a pipe; `sima status` and
  `sima report` print search state and per-candidate stats. Every one of them
  takes `--on <ssh-host>` to observe a search driven on another machine — the
  config is interpreted there, where the store and the orchestrator are, and
  the view renders here. Observation takes no lock and writes nothing, so
  watching a search cannot perturb it.
- **Bring your own program.** A compute program outside this workspace — a
  renderer, a simulator, anything with its own GPU context and its own
  dependency tree — is registered by naming its binary:
  `[domain."acme.thing.v1"] binary = "/opt/acme/worker"`. What it must do is
  speak two small protocols over its own stdin and stdout, written down in
  `docs/protocol.md` — any language that can frame bytes qualifies. `sima-api`
  is the Rust SDK over that contract and the `sima` Python package the other,
  vended by the binary itself, so a program declaring `sdk = "python"` imports
  it here and on every machine the search reaches;
  `examples/stepper-py/` is a whole program written against the latter. sima
  spawns the binary, asks it what its format binds, and runs the search through
  it. It runs as its own process, so it loads its assets once and then streams
  tasks, and the store stays on sima's side of the boundary. Naming a `payload`
  beside the binary is what sends it elsewhere: `sima migrate` moves the search onto
  a machine that installs it, and `sima search --fleet` delivers it to every machine
  the fleet draws on, each of which then answers the digest of what it installed.
- **Reproduce any result.** A task is identified by content — spec, search
  parameters, seed, environment, input state — so a recorded result can be
  regenerated from its identity alone, and any backend that returns a result
  for a given key is interchangeable with any other.
- **Stop and continue.** The store is the only durable state; resume, crash
  recovery, and running again are one code path that re-derives the runnable frontier
  from the store. Kill `sima search` at any point and run it again.

## Requirements

- **Rust**, edition 2024 — `cargo build --release`.
- **A Vulkan GPU** — the default execution backend (WGSL lowered to SPIR-V).
  Needs the Vulkan loader and a device ICD.
- **NVIDIA driver** *(optional)* — the CUDA backend. Needs `libcuda`, opened at
  run time. The workspace builds without it; it is required only to run the CUDA
  backend and its tests.

  Kernel compilation is pinned to NVRTC 12.0.x, which emits the PTX ISA version
  that keeps the committed artifacts loadable on r525 and newer drivers. The
  build vendors that release beside its binaries and puts it on their `RUNPATH`,
  so it is the one that compiles whatever CUDA toolkit the machine has installed
  elsewhere. A build with no network, or one supplying its own copy, sets
  `SIMA_NVRTC_DIR` to a directory holding `libnvrtc.so`.
- **ssh and a container runtime** *(optional)* — remote and fleet execution
  (`sima search --fleet`, `sima migrate`) run workers on other machines.

## Quick start

```sh
cargo build --release
target/release/sima search examples/gray-scott-search    # drive the search
target/release/sima tui examples/gray-scott-search    # or watch it live
target/release/sima report examples/gray-scott-search # per-candidate stats
target/release/sima status examples/gray-scott-search --on gpubox # or a search elsewhere
target/release/sima report examples/gray-scott-search --spend # rented-instance spend
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
  worker protocol, and executors never touch the store.
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
| `sima-toolkit-cuda` | `ptx; arch=compute_75` | 1 | PTX regeneration test per kernel |

The two toolkits reach the same tier by opposite routes. WGSL is lowered during
the search, so a domain records the shader source and names the compiler that
lowers it. CUDA kernels are compiled ahead of time and their PTX is committed,
so a domain records the digest of that artifact and the canonical id states only
what it targets. Committed PTX is regenerated with NVRTC 12.0.x, which fixes the
PTX ISA version at 8.0 and so keeps the artifacts loadable on r525 and newer
drivers; the architecture, `compute_75`, is the separate axis the id names.

## Design principles

- **Reproducibility.** All randomness is seeded and captured; a recorded
  specification reproduces its output exactly.
- **Candidates as data.** Specs are interpreted by fixed engines; there is no
  untrusted code path, and execution cost is a deterministic function of the
  task itself (for a cellular automaton, cells × steps).
- **Elastic scale-out.** A search is fully functional on one machine; remote
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

## Known caveats

One accepted limitation of the rental layer, operational rather than
architectural.

### A hard crash leaves a machine running until something reconciles

While a search is alive, the guard destroys the machines it rented on every
exit path it can observe, including interrupts and panics. A hard crash —
`SIGKILL`, a power cut — runs no code at all. The machine stays up and keeps
billing.

Cleanup is tied to an invocation: the store is the only durable state and
there is no daemon, so nothing watches in between. Two invocations reconcile.
Any acquisition against the same store runs the pass before it rents, and
`sima reconcile <config>` is that pass on its own — from a shell after a
crash, or from whatever schedules periodic maintenance. Either one finds the
record the crashed process left behind and destroys the machine.

Until one of them runs, the machine bills. If nothing is ever invoked against
that store again, the provider's own console remains the way to end it.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
