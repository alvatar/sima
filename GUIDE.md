# Operating sima

Operational reference for driving sima from another project. Written for
agents: every section states what to run, what to expect, and what to do when
it fails. `TUTORIAL.md` is the narrated walk-through; `docs/architecture.md`
is the design; `docs/protocol.md` is the wire contract a program speaks.

## Rules that hold everywhere

- The store is the only durable state. Resume, crash recovery, and running
  again are the same command run again.
- `[search]` is the only hashed section. Changing any key in it changes every
  task key and starts a new search identity. Every other section is
  operational and changes nothing about results.
- Nothing is rented unless the invocation asks: `sima search --fleet`,
  `sima migrate` onto a rented host, or `sima exec`. A declared rented entry
  costs nothing by itself.
- The vast.ai key is read from `VAST_API_KEY` only. It is never a config key.
  Source it into the one command that needs it.
- A rented machine bills until something destroys it. After a hard crash
  (`SIGKILL`, power loss) run `sima reconcile <config>`.
- `sima pack <store> --gc` destroys the work of every unfinalized search in
  that store. Run it only when no search is in progress.
- Every write verb takes the search lock. Observation (`status`, `report`,
  `follow`, `tui`) takes no lock and writes nothing.

## Install

```sh
cargo build --release -p sima -p sima-worker
export PATH="$PWD/target/release:$PATH"
sima                                  # prints the usage block, exit 1
sima-worker --enumerate-devices       # one JSON line per device this build reaches
```

- `sima` finds `sima-worker` beside itself, or at `SIMA_WORKER`.
- Vulkan is the default backend: the loader and a device ICD must be present.
- CUDA is optional. The build vendors NVRTC 12.0.x beside the binaries; an
  offline build sets `SIMA_NVRTC_DIR` to a directory holding `libnvrtc.so`.
- Remote work needs `ssh` and `docker` or `podman` on every machine involved.

## Vocabulary

| Term | Meaning |
|---|---|
| search | One `sima.toml` driven to completion. Id: hash of `[search]`. |
| format | Id naming what a candidate means and who evaluates it, e.g. `example.stepper.v1`. |
| domain | The program-side object bound to a format: devices, environment, config translation, executor. |
| generator | One way of proposing candidates for a format. |
| task key | Hash of spec, params, environment, chain position. The address of a result. |
| store | Content-addressed objects plus one journal per search. Default `.sima/store` beside the config. |
| orchestrator | The machine the command was typed on. Holds the store and the lock. |
| host | One machine. Yours when it has no `provider`, rented when it has one. |
| fleet | The hosts a search may borrow workers from. Engaged only by `--fleet`. |
| migrate | Moving the store and the orchestrator to another machine. |
| exec | One opaque shell command on one rented host, with payload and outputs. |

## Commands

`<config>` is a path to a `.toml` file; the extension may be omitted. There
is no `--help` and no `--version`; any unmatched form prints usage to stderr
and exits 1.

| Command | Effect |
|---|---|
| `sima search <config>` | Drive the search here. |
| `sima search <config> --fleet` | Drive it here plus every `[fleet]` member. Rents what members declare. |
| `sima search <config> --accept-binary` | Continue although the program's build changed. |
| `sima search <config> --quiet` | Print only the search's own progress. |
| `sima tui <config> [--fleet]` | Full-screen live view. Keys: `s` start, `x` stop, `q` quit, `Q` force quit. Needs a TTY. |
| `sima follow <config>` | One line per event until the search ends. Pipeable. |
| `sima status <config>` | State and task counts. |
| `sima status <config> --task <key>` | One task's attempt timeline. `<key>` is any unique prefix. |
| `sima status <config> --failed` | Digest of tasks that did not commit. |
| `sima report <config>` | Committed tasks grouped by distinct stats value. |
| `sima report <config> --all` | One line per committed task. |
| `sima report <config> --task <key>` | One task's stats. |
| `sima report <config> --timeline` | Throughput, retries, per-worker table. |
| `sima report <config> --spend` | Rental ledger: closed, open, total. Local only. |
| `sima report <config> --machines` | Machine incidents and blacklist. Local only. |
| `sima migrate <config> [--accept-binary]` | Move the search onto `[orchestrator].migrate`. |
| `sima recall <config>` | Wind the migrated search down, bring results home, destroy the rental. |
| `sima exec <config>` | Run `[exec].command` on its rented host. Machine kept. |
| `sima exec <config> --attach` | Follow a running command. Rents nothing. |
| `sima exec <config> --one-shot` | Run, fetch, destroy the instance. |
| `sima exec <config> --end` | Stop, fetch, destroy the instance. |
| `sima exec <config> --fetch-to <dir>` | Fetch into `<dir>` relative to the current directory. |
| `sima exec <config> --quiet` | Print only the remote command's output. |
| `sima reconcile <config>` | Destroy worker rentals recorded by any config's store. Spares hosted rentals. |
| `sima reconcile <config> --hosted` | Also destroy hosted migration and exec rentals recorded by any config's store. |
| `sima rm <config>` | Delete the search and what only it references. |
| `sima rm <config> --search <id>` | Delete another search of the same store, by id prefix. |
| `sima searches <store-dir>` | List the searches a store holds. |
| `sima pack <store-dir>` | Consolidate loose objects into packs. |
| `sima pack <store-dir> --gc` | Also delete everything outside finalized searches. Destructive. |
| `sima sdk python --out <dir>` | Write the embedded Python SDK into `<dir>`. |

`--on <ssh-host>` composes with `status`, `report` (except `--spend` and
`--machines`), `tui`, and `follow`. The config path is then interpreted on
that host.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Finalized, a query answered, a migration detached, an exec detached or ended. |
| 1 | Config error, usage error, infrastructure fault, migrate or recall with tasks outstanding. |
| 2 | Definitive candidate failure. |
| 130 | Interrupted by Ctrl-C, SIGTERM, or SIGHUP. Store resumable. |
| n | `sima exec`: the remote command's exit code verbatim. |

## Configuration

Unknown keys are refused. Missing required sections are refused naming the
section.

### `[search]`, hashed

| Key | Required | Notes |
|---|---|---|
| `root_seed` | yes | Non-negative integer. |
| `format` | yes | 1 to 64 bytes of `[a-z0-9._-]`. |
| `segments` | no | Chain length, at least 1. Absent means one stateless task per candidate. |
| `[search.generator] id` | yes | Generator id. Other keys pass to the generator unread by sima. |
| `[search.params]` | no | Passed to the domain as TOML text. |

### `[config]`, operational

| Key | Required | Default |
|---|---|---|
| `max_attempts` | yes | Retries per task, at least 1. |
| `store` | no | `.sima/store` beside the config. |
| `attempt_timeout_ms` | no | No deadline. |
| `answer_timeout_ms` | no | No deadline. Bounds every program answer except `Generate`. |
| `checkpoint_interval_ms` | no | Off. |
| `checkpoint_interval_steps` | no | Off. At least 1. |

Either checkpoint key present turns checkpointing on.

### `[orchestrator]`, this machine

| Key | Notes |
|---|---|
| `workers` | Plain worker count. Exclusive with device tables. |
| `[[orchestrator.device]]` | `select` (substring of device name or `vendor:device` hex) and `workers`. |
| `migrate` | A `[host.*]` entry `sima migrate` moves the search onto. Never a class. |
| `image`, `runtime`, `run_args` | Run this machine's workers in a container. No default image; naming one asks for a container. |

Omitting a worker layout is allowed only when `--fleet` supplies workers.

### `[host.<name>]` and `[host_class.<name>]`

An entry is yours when it names no `provider` and rented when it does. Keys
of the wrong form are refused by name.

| Key | Yours | Rented | Default |
|---|---|---|---|
| `ssh` | yes | no | The entry name. A class takes a list. |
| `count` | class | class | Class size. Exclusive with an `ssh` list. |
| `workers` or `[[...device]]` | yes | no | Required on a host of yours. |
| `image` | yes | yes | `localhost/sima:latest` yours, `ghcr.io/alvatar/sima:latest` rented. |
| `runtime` | yes | no | `docker`. Or `podman`. |
| `run_args` | yes | no | `[]`. Verbatim container-run flags, e.g. `["--device", "nvidia.com/gpu=all"]`. |
| `provider` | no | yes | `vast` or `stub`. |
| `fill` | no | class | `strict`. `best-effort` runs on whatever came up. |
| `disk_gb` | no | yes | 32. |
| `env` | no | yes | `{}`. Environment set at instance creation. |
| `bootstrap_sima` | no | yes | `false`. Let exec upload sima onto an image lacking it. |
| `ready_timeout_ms`, `ready_poll_ms` | no | yes | 1200000, 5000. |
| `[...constraints]` | no | yes | See below. |
| `root` | yes | yes | `~/sima`. Where migrated searches and delivered programs live. |
| `binary` | yes | yes | `sima`. The sima binary on that machine. |

Class addressing: `count = 6` yields `lab1` to `lab6`; an `ssh` list is its
own count.

### `[host.<name>.constraints]`, rented only

`gpu_models` (list), `min_gpu_count`, `min_vram_mb`, `min_cuda`,
`max_price_usd_hour`, `min_reliability`, `verified_only`, `min_disk_gb`,
`min_bandwidth_mbps`. All optional. Machines with two or more recorded
incidents are excluded automatically.

### `[fleet]`

`members = [...]` names `[host.*]` and `[host_class.*]` entries. A member
listed twice, or two members reaching one ssh destination, is refused.

### `[budget]`

| Key | Meaning |
|---|---|
| `max_spend_usd` | Cumulative rental spend across every rental of the search. Assessed every ten seconds while attached. |
| `max_wall_clock_ms` | Compute ceiling from each execution's start. `0` is none. Never sent to a rented destination. |

### `[domain."<format>"]`, bring your own program

| Key | Required | Notes |
|---|---|---|
| `binary` | yes | Path resolved against the config. Spawned at config load. |
| `env` | no | Variable names forwarded from your shell. Everything else is cleared. Include `PATH` for a shebang. |
| `sdk` | no | `"python"`. Vends the package under `.sima/sdk/python/` and leads `PYTHONPATH`. |
| `payload` | no | One file or one directory. What travels on migrate and `--fleet`. |
| `install` | file: no, directory: yes | Script run as `/bin/sh install.sh` on the destination. |
| `payload_digest` | no | Written by a migration. Do not set by hand. |

Install contract: the script receives `SIMA_PAYLOAD_DIR` and
`SIMA_INSTALL_DIR`, runs under the destination's environment, and must leave
an executable `$SIMA_INSTALL_DIR/program` on exit 0.

### `[exec]`

| Key | Required | Notes |
|---|---|---|
| `host` | yes | A rented `[host.*]` entry. |
| `command` | yes | Run by the remote shell at the payload root. Put `cd` and env assignments in the string. |
| `payload` | yes | File or directory, resolved against the config. |
| `install` | directory: yes | Runs once per payload digest. |
| `outputs` | no | Shell globs at the payload root. `..` is refused. |
| `fetch_to` | no | `exec-outputs` beside the config. |

## Workflows

### Run a built-in search

```sh
sima search examples/gray-scott-search.toml
sima report examples/gray-scott-search.toml
```

Output: `search <id>`, `started: N tasks`, one line per task start and
commit, `finalized: N tasks committed`. Ctrl-C, SIGTERM, or SIGHUP winds down
with exit 130; the same command resumes.

### Bring your own program in Python

1. Write the SDK for reference: `sima sdk python --out ./vendor`.
2. Write one file with three classes and one call. Skeleton:

```python
#!/usr/bin/env python3
import sima

FORMAT, GENERATOR = "acme.thing.v1", "acme.thing.random"

class Exec(sima.Executor):
    def execute(self, task, context, checkpoint):
        # task.spec.candidate: bytes; task.params: bytes; task.seed: int
        # task.input_state: bytes | None (previous segment's "state" artifact)
        # checkpoint.resume() -> bytes | None; checkpoint.offer(lambda: bytes)
        result = b"..."
        return sima.Completed(
            artifacts=(sima.Artifact(name=sima.STATE_ARTIFACT, bytes=result),),
            stats=sima.Stats(scalars=(("score", 1.0),)),
        )
        # or sima.Failed(reason=...) transient, sima.Rejected(reason=...) definitive

class Dom(sima.Domain):
    def format(self): return FORMAT
    def environment(self):
        return sima.Environment((sima.EnvironmentComponent("acme.thing.executor", version="v1"),))
    def enumerate_devices(self):
        return [sima.DeviceInfo(clazz="acme:cpu", name="cpu", device_type=sima.DeviceType.CPU, member=0)]
    def translate_config(self, toml, segmented): return b""   # [search.params] text in, bytes out
    def executor(self, device): return Exec()
    def device_desc(self, device): return ("cpu", "acme v1")

class Gen(sima.Generator):
    def id(self): return GENERATOR
    def format(self): return FORMAT
    def translate_config(self, toml): return b""              # [search.generator] minus id
    def generate(self, root_seed, params):
        return [sima.Spec(format=FORMAT, candidate=bytes([i])) for i in range(8)]

if __name__ == "__main__":
    sima.serve(Dom(), [Gen()])
```

3. Config beside it:

```toml
[search]
root_seed = 1
format = "acme.thing.v1"
# segments = 3                      # chain; each segment must commit a "state" artifact

[search.generator]
id = "acme.thing.random"

[config]
max_attempts = 3
checkpoint_interval_ms = 2000

[[orchestrator.device]]
select = "acme:cpu"
workers = 2

[domain."acme.thing.v1"]
binary = "./program.py"
env = ["PATH"]
sdk = "python"
payload = "./program.py"           # needed for migrate and --fleet
```

4. `chmod +x program.py` and `sima search search.toml`.

Obligations: `generate` is deterministic per root seed; artifacts are a pure
function of spec, params, seed, environment, and input state; a segmented
search commits a `state` artifact from every segment; `translate_config`
refuses keys it does not read.

Iterating on the program:

- After editing the program, `sima search` is refused because the binary
  changed. Re-run with `--accept-binary`.
- Accepting the binary recomputes nothing. To recompute, bump the environment
  component version. Old results stay under their keys.
- To start over under the same code, change `root_seed`, or `sima rm`.

A directory payload with dependencies:

```toml
payload = "./program"
install = "./install.sh"
```

```sh
#!/bin/sh
set -e
python3 -m venv "$SIMA_INSTALL_DIR/venv"
"$SIMA_INSTALL_DIR/venv/bin/pip" install -r "$SIMA_PAYLOAD_DIR/requirements.txt"
cat > "$SIMA_INSTALL_DIR/program" <<EOF
#!/bin/sh
exec "$SIMA_INSTALL_DIR/venv/bin/python" "$SIMA_PAYLOAD_DIR/main.py" "\$@"
EOF
chmod +x "$SIMA_INSTALL_DIR/program"
```

### Inspect and observe

```sh
sima status search.toml                 # state, tasks, committed, retried, rejected, faulted
sima status search.toml --failed
sima status search.toml --task 4728
sima report search.toml --all
sima report search.toml --timeline
sima follow search.toml | grep committed
sima status search.toml --on gpubox     # config path interpreted on gpubox
```

Stats are `f64` and observational; artifacts are the exact bytes.

### Housekeeping

```sh
sima searches .sima/store
sima rm search.toml
sima rm search.toml --search 4728422a
sima pack .sima/store
rm -rf .sima                            # everything a config generated
```

A stderr line `store holds ~N loose objects; run sima pack ...` appears past
100 000 loose objects.

### Machines of yours

1. Build the image from the workspace root, in an interactive shell:
   `podman build -t localhost/sima:latest -f containers/sima/Containerfile .`
2. Deliver it: `podman save localhost/sima:latest | ssh gpubox docker load`.
3. Verify GPU reach:
   `ssh gpubox docker run --rm --device nvidia.com/gpu=all localhost/sima:latest --enumerate-devices <format>`.
   Intel and AMD use `--device /dev/dri`.
4. Declare and engage:

```toml
[host.gpubox]
ssh = "user@10.0.0.5"
run_args = ["--device", "nvidia.com/gpu=all"]
[[host.gpubox.device]]
select = "nvidia"
workers = 2

[fleet]
members = ["gpubox"]
```

```sh
sima search search.toml --fleet
```

Workers run as
`ssh -o BatchMode=yes <host> -- <runtime> run --rm -i <run_args> <image> sima-worker`.
Authentication is your ssh config. A password prompt fails fast.

A search over a program of yours needs `payload` on the domain entry; the
program is delivered to `<root>/programs/` on each member and installed there.

### Rented machines

```toml
[host_class.cheap]
provider = "vast"
count = 2
disk_gb = 32
[host_class.cheap.constraints]
max_price_usd_hour = 0.20
verified_only = true
min_reliability = 0.98

[fleet]
members = ["cheap"]

[budget]
max_spend_usd = 2.0
```

```sh
VAST_API_KEY="$(cat ~/.secrets/vast)" sima search search.toml --fleet
sima report search.toml --spend
sima reconcile search.toml              # after any hard crash
```

- Output during acquisition: `renting cheap[0]: 1x RTX 4090 on <machine> at $/hr`,
  `waiting for the machine cheap[0] (pulls the image; up to 1200s)`,
  `installing the program cheap[0]`, then task lines.
- `fill = "strict"` fails the search when a member is short; `best-effort`
  proceeds on survivors.
- Ctrl-C, SIGTERM, or SIGHUP during acquisition releases everything rented
  and exits 130.
- Rentals tear down at search end on every exit path except a hard crash.
- Budget exhaustion acts as an interrupt: wind-down, teardown, store resumable.
- A rental accepts the ssh keys registered on the vast account. ssh offers
  them from your agent (`SSH_AUTH_SOCK`) or default identity files. sima
  configures no key.
- The ssh channel to a rental accepts whatever host key answers. Treat specs,
  params, and results as exposed to an active network attacker.

### Migrate and recall

```toml
[orchestrator]
migrate = "cloudbox"

[host.cloudbox]
provider = "vast"
[host.cloudbox.constraints]
max_price_usd_hour = 0.20
```

```sh
VAST_API_KEY=... sima migrate search.toml
```

Phases printed: `renting`, `waiting for the machine to come up`,
`sending the search: N objects`, `installing the program`,
`starting the search`, `resuming: k/N committed`, then task lines.

- Ctrl-C, SIGTERM, or SIGHUP after `starting the search` detaches: exit 0, the
  far search keeps computing, and the rental keeps billing. Re-running
  `sima migrate` reattaches.
- The same signals before the start abandon: rental released, exit 130.
- `sima recall search.toml` winds the far search down, pulls results, destroys
  the rental. Exit 1 with tasks outstanding; `sima search` at home finishes them.
- `sima migrate --accept-binary` when the payload changed since the far search
  started.
- The far side lives at `<root>/<search-id>/` with `sima.toml`, `store/`,
  `search.log`, `search.pid`. A far-side load failure is reported with the last
  lines of `search.log`.
- Keep the backend the same on both ends. Vulkan to CUDA changes every task key.
- The destination of a machine of yours needs `sima` at its `binary` path and
  `sima-worker` inside its `image`.

### Exec

```toml
[exec]
host = "cloudbox"
command = "cargo test --release"
payload = "."
install = "./ci/install-remote.sh"
outputs = ["reports/*.html"]

[host.cloudbox]
provider = "vast"
image = "nvidia/cuda:12.4.1-devel-ubuntu22.04"
bootstrap_sima = true
[host.cloudbox.constraints]
min_cuda = 12.0
```

```sh
VAST_API_KEY=... sima exec ci.toml                 # machine kept for the next run
VAST_API_KEY=... sima exec ci.toml --attach        # follow a running command
VAST_API_KEY=... sima exec ci.toml --end           # stop, fetch, destroy
VAST_API_KEY=... sima exec ci.toml --one-shot      # run, fetch, destroy
```

- Exit code is the remote command's. `exec.log` lands beside the outputs.
- A plain invocation while a command runs is refused; use `--attach` or
  `--end`.
- Phase output includes `waiting for the machine to answer` when the first
  contact is refused while the machine comes up.
- Authentication follows the rental account-key rule above.
- Budget is assessed only while attached. A detached command bills until an
  attach, `--end`, or `sima reconcile --hosted`.
- `bootstrap_sima = true` uploads sima onto an image that lacks it. The upload
  source is `sima-static` beside the local `sima` executable. Run
  `scripts/build-sima-static.sh` to build the musl executable and place it at
  `target/release/sima-static`. Rebuild the artifact after any commit touching
  `crates/` or `Cargo.lock`. Without the flag on such an image, exec fails
  naming the key.
- Bootstrap is verified on `nvidia/cuda:12.4.1-devel-ubuntu22.04`.
  `nvidia/cuda:12.8.1-base-ubuntu24.04` rejects the account key.
- After an instance is destroyed outside sima, `sima exec <config> --end`
  clears its record and exits 0. `sima reconcile <config> --hosted` reaps a
  detached exec rental.
- The remote job tree is `<root>/exec/<owner16>/` with `payload/`, `exec.log`,
  `exec.pid`, `exec.status`. Untracked files such as build caches survive
  redelivery.

## Filesystem

Beside the config, one directory:

```
.sima/store                          the store (unless [config].store)
.sima/sdk/python/installed/sima/     vended SDK
.sima/program/<format>/installed/    installed payload tree
exec-outputs/                        exec fetches (unless [exec].fetch_to)
```

Store internals: `objects/`, `packs/`, `tasks/`, `instances/`, `spend/`,
`machines/`, `searches/<id>/{manifest.json,journal,orchestrator.lock,checkpoint/,placement/}`.

## Environment variables

| Variable | Read by | Meaning |
|---|---|---|
| `VAST_API_KEY` | any command renting from vast | Bearer token. Unset or empty is an error before any store mutation. |
| `SIMA_WORKER` | `sima` | Path to `sima-worker` when not beside the binary. |
| `SIMA_GPU_DEVICE` | workers | Device index override in the backend's numbering. |
| `SIMA_NVRTC_DIR` | build | Directory holding `libnvrtc.so` for an offline build. |
| `SIMA_VULKAN_VALIDATION` | workers | Requests the Khronos validation layer. |
| `SIMA_STUB_SSH` | `stub` provider | `user@host:port` for exercising the ssh path without vast. |

Set by sima for programs: `SIMA_PAYLOAD_DIR`, `SIMA_INSTALL_DIR` (install
script), `SIMA_PROGRAM_DIGEST` (workers, echoed at handshake), `PYTHONPATH`.

## Failures and remedies

| Symptom | Cause | Remedy |
|---|---|---|
| `[search] section is required for this command` | A search-only verb received another config form. | `sima exec` for `[exec]`, search verbs for `[search]`; reconcile accepts either. |
| Spawn of `./program.py` fails | `PATH` missing from `env`. | `env = ["PATH"]`. |
| `sima search` refused after editing the program | Binary digest changed. | `--accept-binary`. |
| Results unchanged after code change | Task keys ignore source. | Bump the environment component version. |
| Segmented search fails naming `state` | Program commits no `state` artifact. | Commit `sima.STATE_ARTIFACT` from every segment. |
| `the vast.ai API key is read from VAST_API_KEY` | Unset or empty. | Source it into the command. |
| Member short, search stopped | `fill = "strict"`. | `fill = "best-effort"` or fix constraints. |
| `the remote image has no sima binary; set bootstrap_sima = true` | Specialized image. | Set the key and run `scripts/build-sima-static.sh`. |
| `bootstrap_sima expects .../sima-static` | Artifact absent. | Run `scripts/build-sima-static.sh`; it places `target/release/sima-static`. |
| `cannot reach <host>: Permission denied (publickey)` | The image did not install the account key, or the account has no registered key. | Use a verified image; check the account's keys. |
| `cannot reach <host>: Connection refused` at the deadline | sshd did not come up within the readiness bound. | Raise `ready_timeout_ms` or change the image. |
| `an exec command is already running` | Plain invocation over a running command. | `--attach` or `--end`. |
| Machine still billing after crash | No code ran at death. | `sima reconcile <config>`; add `--hosted` if no migration is live. |
| `cannot remove search ...: search not found` | Prefix names no search. | `sima searches <store>` for the id. |
| Far search restarted from segment 0 | Backend differs between ends. | Same backend on both ends. |
| Migration refused naming `payload` | Domain entry has no payload. | Add `payload` (and `install` for a directory). |
| Container build fails at `newuidmap` | Rootless podman under an agent session. | Build from an interactive shell. |
| `tui` exits 1 | stdout is not a TTY. | Use `follow`. |
