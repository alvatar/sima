# sima-domains

The executable substance behind each format id. A **domain** supplies
everything the infrastructure needs to run one format's candidates: the
executor that evaluates a spec, the generator that produces a search's specs, the
codecs that give specs and params their canonical bytes, the environment its
results depend on, and the translation of the human-facing TOML config
sections into those bytes. The `Domain` type and the id dispatch are the
crate's surface; each domain's pieces live in its own module under `domains/`.

The infrastructure below this crate stays agnostic: a spec is `(format id,
opaque bytes)` and params are a second opaque blob, both content-addressed.
Only the domain interprets them. A domain is deterministic in `(spec, params,
seed, environment)` — the same inputs yield the same committed artifact — and
its reproducibility hooks are the seed (which pins sampling) and the
environment (which pins the versions a result depends on: engine, model,
toolchain, hardware).

Today the **stub** and **`ca_evolution`** domains carry code. The stub is a
deterministic, programmable substrate the infrastructure layers test against;
`ca_evolution` runs a GPU reaction-diffusion executor. The remaining sections
below are the planned roster; each lands as its own module under `domains/`
when it gets real work.

## `llm_autoresearch`

LLM-driven autonomous research loops.

- **Searches:** the loop program that drives a model through an autonomous
  research cycle — hypothesize, run an experiment, read the result, revise.
- **Spec:** the encoded loop program — its prompts, tool grants, control flow,
  and stopping rules.
- **Generation:** sample and mutate loop programs from the search seed.
- **Evaluation:** run the loop against a suite of research tasks and score the
  outcome by a rubric over result quality, step count, and cost. The committed
  artifact is the transcript digest and the score.
- **Fit:** the environment pins the model id and version; the seed pins
  sampling, so a fixed program reproduces its transcript.

## `gpu_kernels`

CUDA and Triton kernels evolved against correctness and runtime.

- **Searches:** kernel implementations of a fixed operation — a matmul, an
  attention variant, a reduction.
- **Spec:** the kernel source, or a parameterized schedule over tiling,
  unrolling, and memory layout.
- **Generation:** sample and mutate schedules or source from the search seed.
- **Evaluation:** compile the kernel, check correctness against a reference,
  and measure runtime. A non-compiling or incorrect kernel fails; a correct
  one is scored by latency under a fixed workload.
- **Fit:** the environment pins the GPU model, driver, and toolchain; the
  committed artifact is the kernel and its measured statistics.

## `nca`

Neural cellular automata.

- **Searches:** the update-rule network that grows and maintains a target
  pattern from a seed state.
- **Spec:** the rule parameters — the update network's weights, or a genome
  that decodes to them.
- **Generation:** seed-derived initialization and mutation of the rule.
- **Evaluation:** run the automaton for a fixed step count from a seeded
  initial state and score by fidelity to the target pattern and by stability
  under continued iteration.
- **Fit:** integer or pinned-float arithmetic reproduces bit-exactly; the
  environment pins the engine version.

## `ca_evolution`

Evolution of cellular automata.

- **Searches:** cellular-automaton rules that exhibit a target dynamical
  behavior.
- **Spec:** the rule encoding — the transition table and neighborhood.
- **Generation:** seed-derived sampling and mutation of rules.
- **Evaluation:** simulate the rule on seeded initial conditions and score by a
  behavioral metric such as dynamical class, complexity, or reachability.
- **Fit:** integer arithmetic is bit-exact and deterministic, and evaluation is
  cheap, so the substrate can run a wide rule space per unit compute.

## `agent_evolution`

Agent scaffolds and prompt programs.

- **Searches:** the scaffold wrapping one or more models — its control flow,
  tool set, and prompt program.
- **Spec:** the scaffold definition — the graph of steps, the prompts, and the
  tools each step may call.
- **Generation:** sample, mutate, and recombine scaffolds from the search seed.
- **Evaluation:** run the agent over a task suite and score by success rate
  against cost. The committed artifact is the transcript digest and the score.
- **Fit:** the environment pins the model and tool versions; the seed pins
  sampling, so a fixed scaffold reproduces its result.

## `neural_architecture_search`

Architecture variants evaluated by proxy training evaluations.

- **Searches:** neural network architectures for a fixed learning task.
- **Spec:** the architecture description — the layers, their connectivity, and
  the hyperparameters that define the graph.
- **Generation:** seed-derived sampling and mutation of architectures.
- **Evaluation:** a proxy training evaluation — a short schedule or a learned
  predictor — scores each architecture by a validation metric under a fixed
  compute budget.
- **Fit:** the environment pins the dataset, framework, and hardware; the seed
  pins initialization and data order; the committed artifact is the trained
  weights digest and the metric.
