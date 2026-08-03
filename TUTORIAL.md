# Tutorial: a full run with an external program

This walk-through takes sima from a fresh clone to a finished, inspected, reproduced run, using a program that lives outside the workspace: `examples/stepper-py`, a Python executor. Every command is run from the repository root unless stated otherwise.

## What sima does

sima drives **searches**: a generator proposes candidates, executors evaluate them, and every result lands in a content-addressed store under a **task key** derived from exactly the inputs that determine the result. The same config on the same store resumes instead of recomputing, and two machines that ran the same work can prove it byte for byte.

The pieces you will touch:

- **Run** — one search, declared by a `sima.toml` config. Its identity is the hash of the config's identity-bearing fields.
- **Format** — an id naming what a task's spec means and who can execute it, e.g. `example.stepper.v1`.
- **Domain** — the program-side object bound to a format: its devices, its environment, its params translation, and the executor it builds.
- **Generator** — one way of proposing candidates for a format. A format has one executor and may have many generators.
- **Task key** — the hash of (spec, params, environment, chain position). It is the address of the result; anything that would change the result changes the key.
- **Store** — a content-addressed object store plus a per-run journal of events. The store holds results; the journal narrates how they happened.

A format can be answered in-process (the built-in domains) or by an **external program**: any binary that speaks the domain-service and worker protocols over pipes. The protocol is published in `docs/protocol.md`; `sima-api` is the Rust SDK over it and `python/` the Python one. Speaking the protocol is the whole requirement, so a program in any language qualifies.

## Prerequisites

- A Rust toolchain (build the CLI once): `cargo build --release -p sima`. The binary lands at `target/release/sima`; put it on your `PATH` or call it by path.
- Python 3 for the example program.
- The Python SDK importable. Either `pip install ./python`, or for one shell: `export PYTHONPATH=$(pwd)/python`.

No GPU is needed: the stepper program declares a CPU device.

## The external program

`examples/stepper-py/stepper.py` is one file and both halves of the contract. What it computes: a candidate is one byte, the increment; a task adds it to a `u64` accumulator once per step and commits the reached `(step, accumulator)` state as an artifact named `state`.

The file has three parts:

- **`StepperExecutor(sima.Executor)`** — `execute(task, context, checkpoint)` decodes the spec and params, does the steps, offers a checkpoint at every step boundary, and returns `sima.Completed` with artifacts and stats — or `sima.Rejected` / `sima.Failed`.
- **`StepperDomain(sima.Domain)`** — binds the format id: `environment()` (the components a result's identity depends on), `enumerate_devices()` (one CPU device), `translate_config(toml, segmented)` (turns the `[run.params]` table into canonical bytes), and `executor(device)`.
- **`StepperCandidates(sima.Generator)`** — its own id, its `[run.generator]` translation, and `generate(root_seed, params)` producing the candidate specs.

The entry point is one call, `sima.serve(...)`: it reads the role from the process arguments, so the same binary answers the run driver's questions (`--serve-domain`) and executes tasks inside workers. sima spawns it; you never run it by hand.

Two properties matter more than the plumbing:

- **Everything identity-bearing goes through the canonical encoding** (`sima.Enc`/`sima.Dec`), so the bytes — and therefore the task keys — are the same from any language.
- **The executor never touches the store.** It returns values over a pipe; only sima writes. Process isolation enforces the boundary.

## The config

`examples/stepper-py/search.toml`:

```toml
[run]
root_seed = 7
format = "example.stepper.v1"
segments = 3

[run.generator]
id = "example.stepper.candidates"
count = 2

[run.params]
steps = 5

[config]
store = "./store"
checkpoint_interval_ms = 0

[[orchestrator.device]]
select = "example:cpu"
workers = 2

[domain."example.stepper.v1"]
binary = "./stepper.py"
```

Reading it top to bottom:

- `[run]` is the identity: seed, format, and `segments = 3` cuts each candidate's trajectory into a chain of 3 tasks, each continuing from the previous segment's committed state.
- `[run.generator]` names the generator and carries its config — here, 2 candidates. The keys after `id` belong to the program; sima passes the table through as TOML text.
- `[run.params]` belongs to the program too: `steps = 5` per segment.
- `[config]` and `[[orchestrator.device]]` are operational — store location, checkpoint cadence, which device class runs the work and with how many workers. Changing them changes no task key.
- `[domain."example.stepper.v1"]` is the registration: this format is answered by `./stepper.py` (relative to the config file). Without this section the format would have to be one the build carries in-process.

## Run it

```
cd examples/stepper-py
sima run search.toml
```

sima loads the config, spawns `stepper.py --serve-domain example.stepper.v1` to read its environment, devices, and translations, derives the task keys, spawns worker processes (each hosting the program in its executor role), streams tasks to them, and journals every event into `./store`. With 2 candidates × 3 segments the run commits 6 tasks and finalizes. Rerunning the same command is a no-op ending in the same state: every key is already answered.

## Inspect it

- `sima status search.toml` — the run's state: committed, in flight, failed.
- `sima status search.toml --task <key>` — one task's attempt timeline; `<key>` is any unambiguous prefix.
- `sima report search.toml` — committed tasks counted per distinct stats value.
- `sima report search.toml --all` — every committed task's stats (the stepper reports `steps` and `acc`).
- `sima report search.toml --timeline` — the run's metrics over time.
- `sima follow search.toml` — stream the run's events live; useful in a second terminal during longer runs.
- `sima tui search.toml` — the same, as a full-screen terminal UI driving the run.

## Prove the determinism

The claims above are checkable from the shell:

- Run `sima report search.toml --all` and note a task key and its `acc`. Delete the run — `sima rm search.toml` — and run it again: the same keys carry the same stats. The seed, the specs, the params, and the program's environment fully determine them.
- Change `root_seed` and run again: every task key changes, and both runs coexist in the store, addressed by their own keys.
- Change `workers = 2` to `1`: nothing changes. Operational settings are outside the identity.
- Change `steps` in `[run.params]`: every key changes, because params are inside it.

Segmentation is part of the proof: with `segments = 3`, segment *k*'s state is the input of segment *k+1*, keyed by absolute step — so the trajectory is invariant under where the cuts fall.

## Watch it fail

The stepper arms failure paths through environment variables, each inert when unset, so an armed and an unarmed run share an identity:

- `STEPPER_EXIT_AT_STEP=N sima run search.toml` — the program dies right after the checkpoint offer at absolute step `N`. sima meets the broken pipe, respawns the worker, and the retried attempt resumes from the checkpoint rather than step zero.
- `STEPPER_FAIL_ONCE=1` — every task's first attempt returns a transient failure; sima retries and the run converges.
- `STEPPER_RAISE_ONCE=1` — every task raises once. The traceback crosses as a diagnostic, the attempt reports a panic, and sima treats it as definitive: the run ends failed, and `sima status search.toml --failed` digests why.

## Where to go from here

- `docs/protocol.md` — the wire contract your own program implements: framing, the canonical encoding, both message sets, and the obligations.
- `docs/architecture.md` — the full design: the store, the scheduler, journals, sync, migration, fleets.
- `examples/gray-scott-search.toml` — a built-in GPU domain (reaction-diffusion on WGSL/Vulkan), the same commands end to end.
- `sima migrate`, `--fleet`, `sima reconcile` — moving a run onto rented machines and cleaning up after them, once one machine is not enough.
