# Tutorial: running your own program under sima

This tutorial takes a program that sima knows nothing about and drives it through a complete run: install, execute, inspect, change the code, and understand what that change does to the results already in the store.

The worked example is `examples/stepper-py`, a Python program. It stands in for yours. Every command runs from the repository root unless a section says otherwise.

**Scope.** Everything here runs on one machine until the last section, where a whole run moves onto another one. Spreading a single run's tasks across several machines at once is a workflow for the formats sima carries in process — see [Running on other machines](#running-on-other-machines).

## What sima is

sima drives **searches**. A generator proposes candidates, executors evaluate them, and every result lands in a content-addressed store under a **task key** derived from exactly the inputs that determined it. Rerunning a config resumes instead of recomputing, and two machines that ran the same work can prove they agree byte for byte.

The vocabulary:

- **Run** — one search, declared by a `sima.toml`. Its id is the hash of the config's identity-bearing fields.
- **Format** — an id naming what a candidate means and who can evaluate it, such as `example.stepper.v1`.
- **Domain** — the program-side object bound to a format: its devices, its environment, its config translation, and the executor it builds.
- **Generator** — one way of proposing candidates for a format. A format has one executor and may have many generators.
- **Task key** — the hash of the spec, the params, the environment, and the position in a chain. It is the address of the result.
- **Store** — content-addressed objects plus a per-run journal. The store holds results; the journal narrates how they were produced.

A format is answered either **in-process** by a domain this build carries, or by an **external program**: a binary speaking the domain-service and worker protocols over pipes. `docs/protocol.md` publishes that contract; `sima-api` is the Rust SDK over it and the `sima` Python package, which the binary vends, the Python one. Speaking the protocol is the only requirement, so the program can be written in any language.

## Install

Build the CLI:

```
cargo build --release -p sima
export PATH="$PWD/target/release:$PATH"
sima
```

`sima` prints its command list.

The SDK needs no install of its own. **sima never links your code**: it spawns your program and talks over pipes, and the SDK is the library that speaks that wire protocol, which is what keeps your program an ordinary executable. The binary carries the package, so a config entry declaring `sdk = "python"` is what puts it on the interpreter's path — here and on any machine the run moves to.

To read it, or to develop against it in an editor, write it out:

```
sima sdk python --out ./vendor
```

## What your program supplies

Open `examples/stepper-py/stepper.py`. It is one file holding three objects and one call.

**`StepperExecutor(sima.Executor)`** — `execute(task, context, checkpoint)` receives a candidate, does the work, and returns an outcome. It never writes to the store; it returns values over a pipe, and sima writes.

**`StepperDomain(sima.Domain)`** — binds one format id:

- `format()` — the id this domain answers for.
- `environment()` — the components every result's identity depends on.
- `enumerate_devices()` — the devices the work can run on.
- `translate_config(toml, segmented)` — turns the `[run.params]` table into canonical bytes.
- `executor(device)` — builds the executor for a chosen device.

**`StepperCandidates(sima.Generator)`** — `id()`, `format()`, its own `translate_config`, and `generate(root_seed, params)`.

**`sima.serve(domain, [generators])`** — the entry point. It reads its role from the process arguments, so one file is both what sima interrogates about the format and what runs inside workers.

### Why the executor is separate from the domain

The two are read in different processes, and often on different machines.

- The **domain** is interrogated where the run is driven: what devices exist, what environment identifies results, how to translate the config. This must be cheap — no device, no loaded assets.
- The **executor** exists only inside a worker, on a chosen device. The orchestrator cannot build one, because the device is not known until a worker is placed.

So `Domain.executor(device)` is a **constructor**. The domain answers questions anywhere; the executor does work on a device. Task keys include the environment, which is why key derivation must not require a device: a laptop with no GPU can still derive every key in a run.

### How ids bind

An id is a string your program returns from a method. Matching is by string, checked before any work starts.

```python
FORMAT    = "example.stepper.v1"
GENERATOR = "example.stepper.candidates"

class StepperDomain(sima.Domain):
    def format(self):  return FORMAT

class StepperCandidates(sima.Generator):
    def id(self):      return GENERATOR
    def format(self):  return FORMAT
```

The config reaches them by name:

- `[domain."example.stepper.v1"]` names the binary answering for that format. The spawn carries the id as an argument, and a program whose `format()` disagrees is refused.
- `[run.generator] id = "..."` is matched against each served generator's `id()`. No match is a config error naming the id.
- The matched generator's `format()` must equal the run's format, or it is refused naming both ids.

Passing several generators to `serve` is how one format gets several search strategies — a sweep, a random sample, a mutation of a previous run's winners. The generator id and its params are identity-bearing, so two strategies keep separate task keys.

### The outcomes an executor can return

| Return | Meaning | What sima does |
|---|---|---|
| `Completed` | the candidate evaluated | commits its artifacts under the task key |
| `Failed` | transient — may not recur | retries, up to `max_attempts` |
| `Rejected` | definitive — this candidate cannot produce a result | never retries; the task does not commit |

A program that always succeeds returns `Completed` and never touches the other two. The distinction earns its keep when the work is expensive: returning `Failed` for something permanently broken pays `max_attempts` times for the same nothing.

A process that dies or raises is handled out of band — sima meets a broken pipe or a traceback, respawns the worker, and a saved checkpoint resumes the attempt rather than restarting it.

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
max_attempts = 3
checkpoint_interval_ms = 0

[[orchestrator.device]]
select = "example:cpu"
workers = 2

[domain."example.stepper.v1"]
binary = "./stepper.py"
env = ["PATH"]
sdk = "python"
```

- **`[run]`** is the identity: seed, format, and `segments = 3`, which cuts each candidate's work into a chain of 3 tasks, each continuing from the previous one's committed state.
- **`[run.generator]`** names the generator; the keys after `id` belong to your program and cross as TOML text.
- **`[run.params]`** belongs to your program too.
- **`[config]`** and **`[[orchestrator.device]]`** are operational — store location, retry ceiling, checkpoint cadence, which device class runs the work and with how many workers. Changing them changes no task key.
- **`[domain."example.stepper.v1"]`** is the registration: this format is answered by `./stepper.py`, resolved against the config file's directory, and written against the Python SDK.

### The environment a spawned program receives

`env` lists the **names** of variables that reach your program; their values come from the shell that launched sima. Everything else is cleared.

This is deliberate. A configured program is spawned with a cleared environment and a scratch working directory of its own, so:

- a credential the orchestrator holds is never inherited by third-party code,
- a program cannot quietly depend on where it was launched from.

The consequence for the example: `PATH` is required for the `#!/usr/bin/env python3` shebang to find an interpreter. Without it the spawn fails.

`import sima` needs no name here. `sdk = "python"` is what carries it: sima writes the package it holds under the config's own directory and puts that directory at the head of the interpreter's module path, ahead of anything the machine already has under the same name. The vended copy is the one that matches the binary's protocol, which is why it leads.

## Run it

```
cd examples/stepper-py
sima run search.toml
```

```
run bff61aa384aa94add7c1609ae6634ca0825ac9548b69712563af224e18b800ef
started: 6 tasks
committed 1/6  99edb1e25385
committed 2/6  6863796364eb
committed 3/6  27f7405a9ff1
committed 4/6  ee0f5a8cb965
committed 5/6  c28a080f85c9
committed 6/6  15fe1ae7fe0e
finalized: 6 tasks committed
```

Six tasks is `count = 2` candidates × `segments = 3`. The config decided that; the program did not.

The hex strings are **addresses, not sequence numbers** — each is the hash of what determined that task, stable across machines and reruns.

## Inspect the results

**`sima status`** reports the run's state:

```
run                  bff61aa384aa94add7c1609ae6634ca0825ac9548b69712563af224e18b800ef
state                finalized
tasks                6
committed            6
retried              0
rejected             0
faulted              0
lease expired        0
checkpoint degraded  0
devices              python host processor ×6
```

**`sima report`** reports the stats your program returned:

```
6 committed tasks
3  steps=5 acc=1346066267577507600
3  steps=5 acc=16081153144031734000
```

Two distinct values, three tasks each — one group per candidate, one task per segment. `sima report search.toml --all` prints one line per task, and `--timeline` reports the run's metrics over time.

Other queries:

- `sima status search.toml --task <prefix>` — one task's attempt timeline. Any unambiguous key prefix works.
- `sima status search.toml --failed` — a digest of the tasks that did not commit.
- `sima follow search.toml` — stream events live, useful in a second terminal.
- `sima tui search.toml` — drive the run in a full-screen terminal UI.

### Artifacts and stats are different things

The three segments of one candidate report the **same** `acc`, even though the accumulator really does advance from segment to segment. The reason is worth internalizing:

- **Artifacts** are the result: exact bytes, content-addressed, identity-bearing. Here the `state` artifact is 16 bytes of `(step, acc)`, and it is what the next segment consumed.
- **Stats** are observational `f64`. `acc` is a `u64` near $10^{18}$, where the gap between adjacent doubles is 256 or larger, so a per-segment difference of a few hundred disappears in the conversion.

Nothing was lost — the display cannot show what the artifact holds exactly. Stats are for grouping and filtering; artifacts are what you keep.

## Rerun it

```
sima run search.toml
```

Nothing recomputes. Every key is already answered, so there is no work to do. That is the resume property, and it is the reason keys are hashes rather than counters.

## Change the program

sima cannot see your source. Watch what that means.

Edit the step loop in `stepper.py` so the math differs:

```python
acc = (acc + increment) % WRAP        →        acc = (acc + 2 * increment) % WRAP
```

Then:

```
sima run search.toml
```

The run is **refused**: sima recorded the binary's digest when the run started, and the file no longer matches. That is a provenance guard — it exists so code cannot be swapped silently under a half-finished run. Override it deliberately:

```
sima run search.toml --accept-binary
```

Nothing recomputes. The run is finalized, every task key is already answered, and `sima report search.toml --all` shows the old values. **New code, old results.**

The keys did not change because a task key is derived from the spec, the params, and **the environment you declare** — never from your source file. Doubling the math changed the answer in reality and changed nothing sima was told about.

Now declare it. In `StepperDomain.environment()`:

```python
sima.EnvironmentComponent("example.stepper.executor", version="v1")   →   version="v2"
```

```
sima run search.toml
sima report search.toml --all
```

All six keys are different, all six tasks recompute, and the new values appear. The old results remain in the store under their own keys: both versions coexist, each addressed by what produced it.

This is the contract you take on by registering a format:

> **The environment declaration is your promise that results under this key came from this code.**

Everything else in sima follows from that promise being kept.

## Housekeeping

- `sima rm <config>` — delete the run and what only it references.
- `sima pack <store-dir>` — consolidate loose objects into packs.
- `sima pack <store-dir> --gc` — additionally delete everything outside the finalized runs' closures, which destroys the work of any run still in progress.

## Running on other machines

sima has two workflows for putting a run on other hardware.

- **Migrate** (`sima migrate <config>`) — the run's durable state travels to one declared host, a `sima run` process drives it there, and your machine holds the lock and follows. **Your program travels with it**, which is the rest of this section.
- **Fleet** (`sima run <config> --fleet`) — the store and orchestrator stay on your machine; declared or rented machines run workers only. It routes the formats sima carries in process; see `examples/gray-scott-search.toml`. Routing a `[domain.*]` format this way waits on installing your program where a worker runs, which the migration does for the destination and nothing yet does for a fleet member.

### Declaring what travels

A migration moves the run onto a machine that has never seen your program, so the program travels with it. Say what, on the entry:

```toml
[domain."example.stepper.v1"]
binary  = "./stepper.py"   # how it runs here
payload = "./stepper.py"   # what travels
env     = ["PATH"]         # names; the values are that machine's
sdk     = "python"         # travels as the declaration; the far binary vends it
```

`payload` is one file or one directory, resolved against the config file. A single file **is** the program: sima installs it as the entry point and nothing else is needed. A directory needs an `install` script, because which of its files runs is your decision:

```toml
payload = "./program"
install = "./install.sh"
```

An entry that states no `payload` describes a program this machine holds and no other, and `sima migrate` refuses it, naming the missing key.

The SDK is not part of the payload. `sdk = "python"` crosses as the declaration it is, and the destination's own `sima` writes the package: what a program imports there matches the binary driving it there. A third-party dependency is the payload's business — carry it in a directory payload and install it with the script below.

### The install contract

sima runs your script on the destination as `/bin/sh install.sh`, with two variables set:

- `SIMA_PAYLOAD_DIR` — where your payload's files were materialized. The path is stable, so a wrapper you write may point into it.
- `SIMA_INSTALL_DIR` — where to leave what you built.

Everything else is the destination's own environment. **Nothing is forwarded from your machine** — no credential, no `PATH` of yours — so build out of what is there. A script that needs to fetch or compile does it here.

When your script exits 0, `$SIMA_INSTALL_DIR/program` must exist and be executable. That is the whole contract: the entry point is found by convention, so the script reports no path. A script installing a Python program typically writes a wrapper:

```sh
#!/bin/sh
set -e
python3 -m venv "$SIMA_INSTALL_DIR/venv"
"$SIMA_INSTALL_DIR/venv/bin/pip" install -q -r "$SIMA_PAYLOAD_DIR/requirements.txt"
cat > "$SIMA_INSTALL_DIR/program" <<EOF
#!/bin/sh
exec "$SIMA_INSTALL_DIR/venv/bin/python" "$SIMA_PAYLOAD_DIR/stepper.py" "\$@"
EOF
chmod 755 "$SIMA_INSTALL_DIR/program"
```

A wrapper like that composes with the vended SDK: the interpreter it execs inherits the module path sima set, so `import sima` resolves inside the virtualenv too, and the requirements file carries everything else.

A script that exits non-zero, or leaves no entry point, fails the run on the destination — and `sima migrate` reports the machine and your script's own last lines, so you read the failure here.

### Moving the run

Name the destination and go:

```toml
[orchestrator]
migrate = "gpubox"

[host.gpubox]
workers = 4
```

```
sima migrate search.toml
```

The program's bytes travel through the store as content-addressed objects, so an unchanged program crosses the wire once: a second migration sends nothing and installs nothing. The destination's events stream back as they happen, and the results come home to your store. The run id is unchanged — where a format is answered from is operational — so the manifest is the one this machine would have written.

Ctrl-C winds the far run down, pulls what it computed, and leaves the run resumable. Re-run `sima migrate` to continue.

### Changing the program

Edit your payload and re-run `sima migrate`: the new manifest travels and the destination installs it. Then the far run stops, because its stored results and checkpoints came from the previous build and sima cannot tell whether the change was material. That is your call to make:

```
sima migrate search.toml --accept-binary
```

The flag travels to the run on the destination, which is where the comparison happens.

## Where to go next

- `docs/protocol.md` — the wire contract your own program implements: framing, canonical encoding, both message sets, and the obligations a program takes on.
- `docs/architecture.md` — the full design: store, scheduler, journals, sync, placement, migration.
- `examples/gray-scott-search.toml` — a built-in GPU domain, driven by the same commands.
- `examples/stepper-py/README.md` — what the example computes and the failure paths its own tests arm.
