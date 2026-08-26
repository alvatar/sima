# Tutorial: running your own program under sima

This tutorial takes a program of which sima knows nothing and conducts it through a complete run: we shall install the instrument, execute the work, inspect what it produced, alter the code, and come to understand precisely what such an alteration does to the results already resting in the store.

The worked example is `examples/stepper-py`, a Python program. It stands in for your own. Every command is issued from the repository root, unless a section declares otherwise.

**Scope.** Everything here proceeds upon one machine until the later sections, in which a whole run removes to another, and a company of machines is engaged to serve one. See [Running on other machines](#running-on-other-machines).

## What sima is

sima conducts **searches**. A generator proposes candidates, executors evaluate them, and every result is deposited in a content-addressed store under a **task key** derived from exactly those inputs which determined it. To run a config a second time is to resume it rather than to repeat it; and two machines which have performed the same work may prove their agreement to the byte.

The vocabulary:

- **Run** — one search, declared by a `sima.toml`. Its id is the hash of the config's identity-bearing fields.
- **Format** — an id naming what a candidate means and who may evaluate it, such as `example.stepper.v1`.
- **Domain** — the program-side object bound to a format: its devices, its environment, its config translation, and the executor it builds.
- **Generator** — one manner of proposing candidates for a format. A format has one executor and may have many generators.
- **Task key** — the hash of the spec, the params, the environment, and the position in a chain. It is the address of the result.
- **Store** — content-addressed objects together with a journal kept for each run. The store holds the results; the journal narrates how they were produced.

A format is answered either **in-process**, by a domain this build carries, or by an **external program**: a binary speaking the domain-service and worker protocols over pipes. `docs/protocol.md` publishes that contract; `sima-api` is the Rust SDK upon it, and the `sima` Python package — which the binary itself dispenses — the Python one. To speak the protocol is the sole requirement, and the program may therefore be written in any language whatever.

## Install

Build the CLI:

```
cargo build --release -p sima
export PATH="$PWD/target/release:$PATH"
sima
```

`sima` prints its list of commands.

The SDK arrives in the binary's own company. **sima never links your code**: it spawns your program and converses with it over pipes, and the SDK is the library which speaks that wire protocol — which is what preserves your program as an ordinary executable. The binary carries the package, so a config entry declaring `sdk = "python"` is what places it upon the interpreter's path — here, and upon any machine to which the run may remove.

Should you wish to read it, or to develop against it in an editor, write it out:

```
sima sdk python --out ./vendor
```

## What your program supplies

Open `examples/stepper-py/stepper.py`. It is one file holding three objects and one call.

**`StepperExecutor(sima.Executor)`** — `execute(task, context, checkpoint)` receives a candidate, performs the work, and returns an outcome. It never writes to the store; it returns values over a pipe, and sima does the writing.

**`StepperDomain(sima.Domain)`** — binds one format id:

- `format()` — the id for which this domain answers.
- `environment()` — the components upon which every result's identity depends.
- `enumerate_devices()` — the devices on which the work may run.
- `translate_config(toml, segmented)` — renders the `[run.params]` table into canonical bytes.
- `executor(device)` — builds the executor for a chosen device.

**`StepperCandidates(sima.Generator)`** — `id()`, `format()`, its own `translate_config`, and `generate(root_seed, params)`.

**`sima.serve(domain, [generators])`** — the point of entry. It reads its office from the process arguments, so that one file serves both as what sima interrogates concerning the format and as what runs within the workers.

### Why the executor is separate from the domain

The two are read in different processes, and frequently upon different machines.

- The **domain** is interrogated where the run is conducted: what devices exist, what environment identifies the results, how the config is to be translated. This must be had cheaply — no device, no loaded assets.
- The **executor** exists only within a worker, upon a chosen device. The orchestrator cannot build one, for the device is not known until a worker has been placed.

`Domain.executor(device)` is therefore a **constructor**. The domain answers questions anywhere; the executor performs work upon a device. Task keys include the environment, and this is why their derivation must demand no device: a laptop possessing no GPU may nonetheless derive every key in a run.

### How ids bind

An id is a string your program returns from a method. The matching is done by string, and examined before any work begins.

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
- `[run.generator] id = "..."` is matched against the `id()` of each generator served. Where no match is found, the config is in error, and the id is named in the complaint.
- The matched generator's `format()` must equal the run's format, or it is refused with both ids named.

To pass several generators to `serve` is the means by which one format acquires several strategies of search — a sweep, a random sample, a mutation of a former run's winners. The generator's id and its params bear identity, so two strategies keep their task keys apart.

### The outcomes an executor can return

| Return | Meaning | What sima does |
|---|---|---|
| `Completed` | the candidate evaluated | commits its artifacts under the task key |
| `Failed` | transient — may not recur | retries, up to `max_attempts` |
| `Rejected` | definitive — this candidate cannot produce a result | never retries; the task does not commit |

A program that always succeeds returns `Completed` and never troubles the other two. The distinction earns its keep where the work is dear: to return `Failed` for a thing permanently broken is to pay `max_attempts` times for the same nothing.

A process that dies or raises is dealt with out of band — sima meets a broken pipe or a traceback, spawns the worker anew, and a saved checkpoint resumes the attempt rather than beginning it afresh.

## The config

`examples/stepper-py/search.toml`:

```toml
[run]
root_seed = 1
format = "example.stepper.v1"
segments = 3

[run.generator]
id = "example.stepper.candidates"
count = 2

[run.params]
steps = 300000000

[config]
store = "./store"
max_attempts = 3
checkpoint_interval_ms = 2000

[[orchestrator.device]]
select = "example:cpu"
workers = 2

[domain."example.stepper.v1"]
binary = "./stepper.py"
env = ["PATH"]
sdk = "python"
```

- **`[run]`** is the identity: seed, format, and `segments = 3`, which divides each candidate's work into a chain of 3 tasks, each continuing from the committed state of the last.
- **`[run.generator]`** names the generator; the keys following `id` belong to your program and cross as TOML text.
- **`[run.params]`** belongs to your program likewise.
- **`[config]`** and **`[[orchestrator.device]]`** are operational — the store's location, the ceiling upon retries, the cadence of checkpoints, which class of device performs the work and with how many workers. To change them changes no task key.
- **`[domain."example.stepper.v1"]`** is the registration: this format is answered by `./stepper.py`, resolved against the config file's own directory, and written against the Python SDK.

### The environment a spawned program receives

`env` lists the **names** of the variables that reach your program; their values come from the shell that launched sima. All else is cleared.

This is by design. A configured program is spawned with a cleared environment and a scratch working directory of its own, so that:

- a credential held by the orchestrator is never inherited by third-party code,
- a program cannot come quietly to depend upon the place from which it was launched.

The consequence for the example: `PATH` is required, that the `#!/usr/bin/env python3` shebang may find an interpreter. Without it the spawn fails.

`import sima` wants no name here. `sdk = "python"` is what carries it: sima writes the package it holds beneath the config's own directory and sets that directory at the head of the interpreter's module path, before anything the machine may already keep under the same name. The dispensed copy is the one that matches the binary's protocol, and for that reason it leads.

## Run it

```
cd examples/stepper-py
sima run search.toml
```

```
run bff61aa384aa94add7c1609ae6634ca0825ac9548b69712563af224e18b800ef
started: 6 tasks
task 99edb1e25385 started (worker 0)
task 6863796364eb started (worker 1)
task 99edb1e25385 checkpointed (worker 0)
task 6863796364eb checkpointed (worker 1)
committed 1/6  99edb1e25385
committed 2/6  6863796364eb
…
finalized: 6 tasks committed
```

An attempt says when it begins and which worker took it; a task that saves checkpoints says so as it goes, at most once in ten seconds however often it saves. Between them the terminal shows a run computing rather than a run that has merely not finished. `--quiet` leaves the run, its start, its commits, and its ending.

Six tasks is `count = 2` candidates × `segments = 3`. The config decided that; the program did not. As shipped, the run computes for some couple of minutes; reduce `steps` by a few orders of magnitude if you wish merely to see the shape of the output.

The hexadecimal strings are **addresses, not numbers in a sequence** — each is the hash of what determined that task, constant across machines and repetitions.

## Inspect the results

**`sima status`** reports the state of the run:

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

Two distinct values, three tasks apiece — one group to each candidate, one task to each segment. `sima report search.toml --all` prints a line for every task, and `--timeline` reports the run's metrics as they unfolded in time.

Further inquiries:

- `sima status search.toml --task <prefix>` — the timeline of one task's attempts. Any unambiguous prefix of the key will serve.
- `sima status search.toml --failed` — a digest of the tasks that did not commit.
- `sima follow search.toml` — the events as a live stream, of use in a second terminal.
- `sima tui search.toml` — the run conducted in a full-screen terminal UI.

### Artifacts and stats are different things

The three segments of one candidate report the **same** `acc`, though the accumulator does in truth advance from segment to segment. The reason repays attention:

- **Artifacts** are the result: exact bytes, content-addressed, identity-bearing. Here the `state` artifact is 16 bytes of `(step, acc)`, and it is what the next segment consumed.
- **Stats** are observational `f64`. `acc` is a `u64` in the neighbourhood of $10^{18}$, where the interval between adjacent doubles is 256 or greater, so a difference of a few hundred between segments vanishes in the conversion.

Nothing has been lost — it is only that the display cannot show what the artifact holds exactly. Stats serve for grouping and filtering; artifacts are what you keep.

## Rerun it

```
sima run search.toml
```

Nothing recomputes. Every key is already answered, and there is accordingly no work to do. This is the resume property, and it is the reason the keys are hashes rather than counters.

## Change the program

sima cannot see your source. Observe what follows from that.

Edit the step loop in `stepper.py` so the arithmetic differs:

```python
acc = (acc + increment) % WRAP        →        acc = (acc + 2 * increment) % WRAP
```

Then:

```
sima run search.toml
```

The run is **refused**: sima recorded the binary's digest when the run began, and the file no longer answers to it. This is a guard upon provenance — it exists so that code cannot be exchanged silently beneath a half-finished run. Override it deliberately:

```
sima run search.toml --accept-binary
```

Nothing recomputes. The run is finalized, every task key is already answered, and `sima report search.toml --all` shows the old values. **New code, old results.**

The keys did not change, because a task key is derived from the spec, the params, and **the environment you declare** — never from your source file. Doubling the arithmetic changed the answer in fact, and changed nothing that sima had been told of.

Now declare it. In `StepperDomain.environment()`:

```python
sima.EnvironmentComponent("example.stepper.executor", version="v1")   →   version="v2"
```

```
sima run search.toml
sima report search.toml --all
```

All six keys are now different, all six tasks recompute, and the new values appear. The old results remain in the store under keys of their own: the two versions stand side by side, each addressed by what produced it.

This is the contract you take upon yourself in registering a format:

> **The environment declaration is your promise that the results under this key came from this code.**

Everything else in sima follows from that promise being kept.

## Housekeeping

- `sima rm <config>` — delete the run and whatever only it references.
- `sima pack <store-dir>` — consolidate loose objects into packs.
- `sima pack <store-dir> --gc` — delete, besides, everything outside the closures of the finalized runs; this destroys the work of any run still in progress.

## Running on other machines

sima has two workflows for placing a run upon other hardware.

- **Migrate** (`sima migrate <config>`) — the run's durable state travels to one declared host, a `sima run` process conducts it there, and your machine holds the lock and follows. **Your program travels with it**, which is the matter of the remainder of this section.
- **Fleet** (`sima run <config> --fleet`) — the store and the orchestrator remain upon your machine; declared or rented machines run workers only. Your program travels there as well: the same `payload` key sends it to every machine the fleet draws in, each installs it beneath its `root`, and its workers run what was installed. Both are driven below.

### Declaring what travels

A migration moves the run onto a machine that has never seen your program, and the program must therefore travel with it. Say what, upon the entry:

```toml
[domain."example.stepper.v1"]
binary  = "./stepper.py"   # how it runs here
payload = "./stepper.py"   # what travels
env     = ["PATH"]         # names; the values are that machine's
sdk     = "python"         # travels as the declaration; the far binary vends it
```

`payload` is one file or one directory, resolved against the config file. A single file **is** the program: sima installs it as the point of entry and nothing further is wanted. A directory requires an `install` script, for the question of which of its files runs is yours to decide:

```toml
payload = "./program"
install = "./install.sh"
```

An entry stating no `payload` describes a program which this machine holds and no other; both `sima migrate` and `sima run --fleet` refuse it, naming the missing key.

The SDK crosses as the declaration it is: `sdk = "python"` travels, and the destination's own `sima` writes the package, so that what a program imports there matches the binary conducting it there. A third-party dependency is the payload's affair — carry it in a directory payload and install it by the script below.

### The install contract

sima runs your script upon the destination as `/bin/sh install.sh`, with two variables set:

- `SIMA_PAYLOAD_DIR` — where your payload's files were laid out. The path is stable, so a wrapper of your writing may point into it.
- `SIMA_INSTALL_DIR` — where to leave what you have built.

All else is the destination's own environment. **Nothing is forwarded from your machine** — no credential, no `PATH` of yours — so build from what is found there. A script that must fetch or compile does so here.

When your script exits 0, `$SIMA_INSTALL_DIR/program` must exist and be executable. That is the whole of the contract: the entry point is found by convention, and the script reports no path. A script installing a Python program will commonly write a wrapper:

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

A wrapper of that kind composes with the dispensed SDK: the interpreter it execs inherits the module path sima set, so `import sima` resolves within the virtualenv also, and the requirements file carries all the rest.

A script that exits with any figure but zero, or leaves no entry point, fails the run upon the destination — and `sima migrate` reports the machine and the last lines of your script's own complaint, so that you read the failure here.

### The journey: one run, three machines

Everything to this point has run upon your machine. A real search outgrows one — and this section therefore takes the run upon the journey a real one makes: it begins upon your laptop, removes to a rented machine, computes there while you are absent, and comes home to finish. One run id the whole way, and no committed task is ever computed twice.

The example is already provisioned for the trip: `steps` gives each task minutes of computation rather than instants, and `checkpoint_interval_ms = 2000` means that every couple of seconds each task lays by state from which it may resume — which is what permits this run to change machines in the middle of a task.

**Start local.** `sima run search.toml`; suffer a couple of tasks to commit, then Ctrl-C. The run winds down — the attempts in flight are abandoned, their checkpoints standing where they were laid — and exits `130`: interrupted, resumable. Your store now holds a run that is partly done, and a re-run resumes each abandoned attempt from its checkpoint rather than from its beginning.

**Send it away.** The example's config already declares whither:

```toml
[orchestrator]
migrate = "cloudbox"   # the [host.*] entry the run moves onto

[host.cloudbox]
provider = "vast"      # rent from vast.ai; key comes from $VAST_API_KEY
disk_gb  = 32
root     = "~/sima-runs"
binary   = "sima"

[host.cloudbox.constraints]
max_price_usd_hour = 0.20   # take no offer above this
verified_only      = true

[budget]
max_spend_usd     = 2.0     # wind the run down if rental spend reaches this
max_wall_clock_ms = 0       # no ceiling; a rented destination is never sent one
```

Then, with your provider key in the environment:

```
sima migrate search.toml
```

sima rents the cheapest machine that satisfies the constraints, boots its image upon it, and ships the run — the store's objects, your program, the whole of it. Each phase names itself as it begins, so that the quiet minutes while the machine comes up are quiet for a stated reason:

```
run 4728422a…
renting: 1× RTX 4090 on 8127-a41 at $0.174/hr
waiting for the machine to come up (pulls the image; up to 600s)
sending the run: 214 objects
installing the program
starting the run
resuming: 2/6 committed, 4 outstanding
task 27f7405a9ff1 started (worker 0)
```

The far run states the ledger it inherits rather than a task count, which is how you see at a glance that it continued your work instead of beginning it again. The stream then resumes exactly where your laptop left off: the tasks committed locally are not computed again, for the far machine's store already holds them, and the commit counter carries on from what is already in it.

The program's bytes travel as content-addressed objects, so an unchanged program crosses the wire once: a second migration sends nothing and installs nothing. Every worker upon a machine that received your program answers with the digest from which that machine's own tree was built, and one answering anything else fails its spawn — so a machine running something other than what you sent stops the run, rather than filling your store with results from it.

**Walk away.** Ctrl-C. This no longer ends anything: the far run computes on, and the command exits telling you the two ways back. A closed laptop or a dropped connection does the same — nothing that befalls your machine ends the far run. To end it is a thing you must ask for by name.

**Come back.** `sima migrate search.toml` once more. Upon a run already living at the destination, the same command reattaches: what committed in your absence is replayed, and the live stream then continues.

**Bring it home.** Detach again, and this time:

```
sima recall search.toml
```

This is the destructive verb, and the only one. It winds the far run down, reads how it ended, draws everything it computed into your store, and takes the rented machine away:

```
migrated: run 4728422a… came home with 2 tasks outstanding
```

A rental bills by the hour and not by what it computes, so to stop the run early saves you nothing — the same bill arrives whether the machine computes or sits idle — and to tear the machine down requires the provider credential, which never leaves your machine. That is why recall does both at one stroke, and why nothing remote can do it in your stead. `sima reconcile search.toml` is the net beneath it all: it asks the provider what still stands rented under your key, and answers `nothing to reconcile` when the ledger is clean.

**Finish at home.** `sima run search.toml` one last time. The store knows what remains outstanding; the remaining tasks compute upon your laptop, and the run reports `finalized`. It began here, computed upon a machine that no longer exists, and ended here — and the manifest is the very one any single machine would have written.

**Settle the account.** `sima report search.toml --spend` states what the rental cost, and `sima report search.toml --machines` the reputation of the machines that served — a machine that misbehaved is blacklisted and not rented again. While a run lives elsewhere, `sima status`, `sima report`, `sima tui`, and `sima follow` all accept `--on <host>` to observe it upon its own machine; and where a migrated run's machine is to be destroyed without the ceremony of a recall — nothing upon it worth bringing home, the rental merely billing — `sima reconcile search.toml --hosted` takes that one down as well; plain `reconcile` spares it.

### Starting over

`root_seed` in `[run]` is the anchor of the identity, and your generator receives it as the first argument of `generate` — one seed always yields the same candidates. To rerun a finalized run is to find its commits and do nothing; to run the same search afresh, advance the seed — a new identity, every task under new addresses. To recompute under the *same* seed, clean up first (below).

### The fleet: machines that only lend workers

Migration moved everything to one machine. The fleet is the inverse: the store and the orchestrator remain before you, and other machines only lend workers. That is the shape for many machines at once — and for results landing in your local store the moment they commit.

The example already declares a fleet:

```toml
[host_class.cheap]
provider = "vast"      # two machines rented to one specification
count    = 2
disk_gb  = 32

[host_class.cheap.constraints]
max_price_usd_hour = 0.10   # cheapest end of the market
verified_only      = true

[fleet]
members = ["cheap"]
```

`[fleet]` lists what a run *may* draw upon; nothing is rented until you ask:

```
sima run search.toml --fleet
```

```
run b80a8ca…
renting cheap[0]: 1× GTX 1660 on 8127-a41 at $0.056/hr
renting cheap[1]: 1× RTX 3060 Ti on 5512-c09 at $0.048/hr
installing the program cheap[0]
installing the program cheap[1]
started: 6 tasks
instance online 48763646 on ssh8.vast.ai: 1× GTX 1660 at $0.056/hr
instance online 48763734 on ssh8.vast.ai: 1× RTX 3060 Ti at $0.048/hr
committed 1/6  ef16c6b54f49
…
finalized: 6 tasks committed
```

Each member says what it took and at what rate the moment its offer is taken, which is minutes before that machine is up; the online line marks the boot completed. Should a member fail to come up, it says so and states what your `fill` policy makes of it — the run stops under `strict`, and goes on with the machines that did come up under `best-effort`. Pass `--quiet` and none of it is printed: what remains is the run, its start, its commits, and its ending.

Your program went to both machines just as it traveled in the migration; each installed it, and its workers ran what was installed. When the run ends, the rentals are torn down of their own accord — `sima reconcile search.toml` confirms the ledger clean.

What has just happened deserves understanding in three particulars:

- **Parallelism is purchased with `count` in the generator, not with machines.** The segments of one candidate are sequential by construction — each begins from the committed state of the last — so at most one task per candidate is ever ready. With two candidates, two machines is already more than the run can feed; twenty candidates would keep every worker on all of them occupied.
- **No task is bound to any machine.** One ready queue lives at home, and a ready task goes to whichever free worker answers its device class. A chain's next segment may compute upon a different machine than its last.
- **State travels with the task, and the machines share nothing.** A dispatched task carries its input bytes from your store; its result comes home with the commit. Fleet machines hold no store and never address one another — which is why a chain that hops machines costs nothing extra: every task's input crosses the wire in the same manner, wherever it runs.

### Cleaning up

`sima rm` deletes one run — the run the config presently names — together with everything only it references:

```
sima rm search.toml
```

Mark that "presently names": the target is resolved through the config. A store accumulates the runs of every identity ever driven against it — an edited seed, a changed parameter — and what it holds is asked of the store rather than of a config:

```
sima runs ./store
```

```
run                                                               state         committed
b80a8ca384aa94add7c1609ae6634ca0825ac9548b69712563af224e18b800ef  finalized     6/6
4728422a1c0e4f8d90a1b3c5d7e9f0a2b4c6d8e0f2a4b6c8d0e2f4a6b8c0d2e4  interrupted   2/6
```

Any run of the listing is removed by its own id, whether or not the config still names it — any unambiguous prefix will serve:

```
sima rm search.toml --run 4728422a
```

Ask for a run the store never held, and it refuses by name:

```
sima: validation error: cannot remove run 7c19fe97…: run not found
```

`rm -rf store/` removes every run at once, the store being disposable in its entirety. After the cleaning, by either instrument, the same seed computes afresh from nothing — whereas without it, a rerun finds its commits and does nothing.

### A deadline where time is free

A local run and a machine of your own cost nothing by the hour, so there a deadline is worth stating:

```toml
[budget]
max_wall_clock_ms = 21600000   # six hours; 0 states no ceiling
```

The run interrupts itself after six hours, whether or not anything watches, and leaves the store resumable. The key does not travel to a rented destination.

### Changing the program

Edit your payload and run `sima migrate` again: the new manifest travels and the destination installs it. The far run then stops, for its stored results and checkpoints came of the previous build, and sima cannot judge whether the change was material. That judgment is yours to make:

```
sima migrate search.toml --accept-binary
```

The flag travels to the run upon the destination, which is where the comparison is made.

## Where to go next

- `docs/protocol.md` — the wire contract your own program implements: framing, canonical encoding, both message sets, and the obligations a program takes on.
- `docs/architecture.md` — the full design: store, scheduler, journals, sync, placement, migration.
- `examples/gray-scott-search.toml` — a built-in GPU domain, driven by the same commands.
- `examples/stepper-py/README.md` — what the example computes and the failure paths its own tests arm.
