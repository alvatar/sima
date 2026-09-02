//! [`LoadedConfig`]: a `sima.toml`, loaded and translated.
//!
//! A search declares the machines it can use by naming them once, and refers to
//! them by name everywhere else. A host is one machine; a host class is several
//! identical machines declared in one entry; `[fleet]` lists the members a search
//! may draw on; `[orchestrator]` is this machine.
//!
//! The file schema (this comment is the reference):
//!
//! ```toml
//! [search]                                   # the only hashed section
//! root_seed = 42
//! format    = "stub.v1"
//! segments  = 10                          # optional; absent = static batch, >= 1
//!
//! [search.generator]
//! id    = "stub.v1"
//! # remaining keys are generator-specific; stub.v1 takes:
//! behaviors = ["succeed", "flaky:2", "sleep:50", "reject", "panic"]
//!
//! [search.params]                            # domain-specific; stub.v1 takes:
//! hex = ""                                # optional hex string, default empty
//!
//! [config]                                # global settings, fully qualified
//! store                     = "./store"   # optional; resolved against this file's
//!                                         # directory, and .sima/store by default
//! max_attempts              = 3
//! attempt_timeout_ms        = 300000      # optional; absent disables the deadline
//! answer_timeout_ms         = 120000      # optional; absent disables the deadline
//! checkpoint_interval_ms    = 30000       # optional; wall-clock cadence
//! checkpoint_interval_steps = 500         # optional; step cadence, >= 1
//!
//! [exec]                                  # one opaque command on one rental
//! host     = "cloudbox"                    # a rented [host.*] entry
//! command  = "cargo test --release"        # interpreted by the remote shell
//! payload  = "."                           # file or directory, resolved here
//! install  = "./ci/install-remote.sh"      # required for a directory payload
//! outputs  = ["reports/*.html", "*.pfm"]   # globs at the payload tree root
//! fetch_to = "exec-outputs"                # optional; default as shown
//!
//! [host.gpubox]                           # reached at "gpubox"; image and runtime default
//! # ssh      = "user@10.0.0.5"            # override the address
//! # image    = "localhost/sima:latest"    # default as shown
//! # runtime  = "podman"                   # docker | podman; default docker
//! # run_args = ["--gpus", "all"]          # verbatim container-run flags
//! # workers  = 4                          # exclusive with the device tables below
//! # root     = "~/sima"              # a migrated search's directory and the programs delivered here; default as shown
//! # binary   = "sima"                     # the sima binary here; default as shown
//! [[host.gpubox.device]]
//! select  = "nvidia"
//! workers = 2
//!
//! [host.bigbox]                           # named one thing, reached at another
//! ssh     = "bigbox.dept.internal"
//! workers = 8
//!
//! [host.cloudbox]                        # one rented machine: a host, not a class
//! provider = "vast"
//! image    = "nvidia/cuda:12.4.1-devel-ubuntu22.04"
//! bootstrap_sima = true                  # upload sima-static when binary is absent
//! disk_gb  = 64
//! env = { NVIDIA_DRIVER_CAPABILITIES = "all" }
//! [host.cloudbox.constraints]
//! gpu_models  = ["RTX 4090"]
//! min_vram_mb = 16000
//! min_cuda    = 12.0
//!
//! [host_class.lab]                        # lab1 … lab6; raise count to grow
//! count   = 6
//! workers = 8
//!
//! [host_class.oldlab]                     # addresses that follow no pattern
//! ssh     = ["fermi", "pauli", "dirac"]
//! workers = 4
//!
//! [host_class.rtx4090]                    # four rented to one specification
//! provider = "vast"
//! count    = 4
//! fill     = "best-effort"                # strict | best-effort; default strict
//! disk_gb  = 32
//! [host_class.rtx4090.constraints]        # every key optional
//! gpu_models         = ["RTX 4090"]
//! min_gpu_count      = 1
//! min_vram_mb        = 16000
//! max_price_usd_hour = 0.45
//! min_reliability    = 0.95
//! verified_only      = true
//! min_disk_gb        = 32
//! min_bandwidth_mbps = 100
//!
//! [fleet]
//! members = ["gpubox", "lab", "rtx4090"]
//!
//! [budget]
//! max_spend_usd     = 20.0                # ceiling on the search's total rental
//!                                         # spend, assessed only while a
//!                                         # migration or a fleet search is attached
//! max_wall_clock_ms = 0                   # ceiling on how long the search may
//!                                         # compute, measured from the start of
//!                                         # each execution; 0 is no ceiling.
//!                                         # Kept on a local search and on a machine
//!                                         # of yours, never sent to a rental
//!
//! [orchestrator]                          # this machine
//! migrate = "cloudbox"                   # the host `sima migrate` moves the search onto
//! # image    = "localhost/sima:latest"    # run this machine's workers in a container
//! # runtime  = "podman"                   # docker | podman; default docker
//! # run_args = ["--gpus", "all"]          # verbatim container-run flags
//! # workers  = 4                          # exclusive with the device tables below
//! [[orchestrator.device]]
//! select  = "nvidia"
//! workers = 1
//!
//! [domain."acme.thing.v1"]                # a format served by its own program
//! binary  = "/opt/acme/worker"            # resolved against this file's directory
//! # env     = ["ACME_ASSETS"]             # optional; variable names it also receives
//! # sdk     = "python"                    # optional; the SDK this binary vends
//! # payload = "./program"                 # optional; what travels when the search migrates
//! # install = "./install.sh"              # optional for a file payload, required for a directory
//! # payload_digest = "<64 hex>"           # a migration writes this; the manifest to install here
//! ```
//!
//! ## Where a format is answered from
//!
//! A `[domain.<format>]` entry names the binary that answers for that format:
//! sima spawns it, asks it what the format binds, and spawns the search's workers
//! from it. Every format without an entry is answered by this build, in
//! process. The entry is read when the config loads, so a program that cannot
//! answer for the format it is declared under fails there.
//!
//! The program is spawned with an explicit environment and a scratch working
//! directory of its own. `env` names the variables it receives on top of that
//! baseline, by name alone: each value comes from the orchestrator's own
//! environment, and a name the orchestrator does not hold is simply absent in
//! the program.
//!
//! ### The SDK the program is written against
//!
//! `sdk` names the language whose package this binary carries, and `"python"`
//! is the one it vends. Any other value is refused at load, naming the config,
//! the format, the key, and the value.
//!
//! An entry declaring it has the package written under `<config-dir>/sdk/<sdk>/`
//! where the config resolves, once per binary version, and the installed
//! directory put at the head of the program's module path — `PYTHONPATH` for
//! Python — ahead of anything the machine holds under the same name. The
//! vended copy is the one that matches this binary's protocol, which is why it
//! leads. Every entry declaring one SDK shares its tree: what it holds is a
//! property of the binary rather than of any one program.
//!
//! The key is independent of `payload`, and a migration carries it as the
//! declaration it is: the destination's own binary vends the package there.
//! `sima sdk <language> --out <dir>` writes the same package by hand, for
//! developing a program outside a search.
//!
//! ### What travels when the search reaches another machine
//!
//! `binary` says how the program runs here; `payload` says what travels — one
//! file or one directory, resolved against this file's directory. It travels
//! the same way for both ways of using another machine: `sima migrate` sends it
//! to the destination, and `sima search --fleet` sends it to every machine the
//! fleet draws in, where it is installed under `<root>/programs/` and the
//! machine's workers are spawned from it. An entry that states none describes a
//! program this machine holds and no other, and both refuse it, naming the key.
//! A plain local `sima search` validates both keys and otherwise ignores them.
//!
//! `install` is the shell script the destination runs over the materialized
//! payload. It is optional for a single-file payload, which is its own entry
//! point, and required for a directory, where which file runs is what the
//! script decides. The contract:
//!
//! - it runs as `/bin/sh install.sh`, working directory the program tree, under
//!   the destination's own environment plus `SIMA_PAYLOAD_DIR` (the
//!   materialized payload) and `SIMA_INSTALL_DIR` (where to leave what it
//!   builds). Nothing is forwarded from the machine that sent the payload;
//! - after exit 0, `$SIMA_INSTALL_DIR/program` must exist and be executable.
//!   The entry point is found by convention, so the script reports no path;
//! - a non-zero exit, or an exit leaving no entry point, fails the load naming
//!   the script, its status, and its log.
//!
//! `payload_digest` is the far side of the same thing: the manifest object the
//! store already holds, which a migration writes into the config it
//! synthesizes. An entry carrying it has its program materialized and installed
//! where the config resolves, before the binary it names is spawned, and a
//! stamp makes a second load install nothing. It admits neither `payload` nor
//! `install`: the digest names one program, and the manifest carries its own
//! script.
//!
//! The digest has a second consumer past the install: it is what every worker
//! of the search answers at its handshake. Each process spawned from the program
//! is told it, echoes it back, and a worker answering anything else fails its
//! spawn — so the machines a search uses agree with it on which program they run.
//! On a machine the search delivered to, the value is read from that machine's own
//! stamp at exec time, so what it answers is that disk's claim.
//!
//! A config carrying `payload_digest` and no `[orchestrator]` worker layout is
//! what a migration onto a rented machine synthesizes: nothing there could say
//! where the search's work goes until the program was installed. Such a search
//! derives one worker per usable device from the program's own enumeration at
//! start. Every other config states its layout, as the refusal for one that
//! states neither says.
//!
//! ## Exec jobs
//!
//! An `[exec]` section declares one opaque remote shell command. Its `host`
//! names one rented `[host.*]` entry; owned hosts and host classes belong to
//! other contracts. `payload` and `install` follow the same path and manifest
//! rules as a domain payload. The command starts at the payload tree root, so
//! any working-directory change or environment assignment belongs in the
//! command string.
//!
//! `outputs` are remote shell globs anchored at that root. They and `exec.log`
//! are unpacked beneath `fetch_to`, which resolves against the config file and
//! defaults to `exec-outputs`. The payload tree is mutable: manifest files are
//! refreshed on a changed digest while untracked build caches remain. The
//! install script runs once per payload digest, before the command starts.
//!
//! A file may declare both `[search]` and `[exec]`. Search commands require
//! `[search]` and `[config]`; `sima exec` requires `[exec]` and derives the
//! default store beside the file when `[config]` is absent. For an exec, the
//! store holds the rental and spend ledgers plus the payload object cache.
//! Search journals, tasks, manifests, and catalog entries belong to a search.
//!
//! ## Addressing
//!
//! The entry's name is its ssh destination unless `ssh` says otherwise, so a
//! class scales by changing one number. A class appends the index to the name
//! with no separator and no padding, so a class of six is `lab1 … lab6`.
//!
//! | Entry | Addresses |
//! |---|---|
//! | `[host.<name>]` | `<name>` |
//! | `[host.<name>]` with `ssh = "…"` | as written |
//! | `[host_class.<name>]` with `count = N` | `<name>1` … `<name>N` |
//! | `[host_class.<name>]` with `ssh = […]` | as written; the list is the count, so `count` is rejected |
//! | any entry with `provider` | from the provider; `ssh` is rejected |
//!
//! ## Keys, by form
//!
//! An entry is **yours** when it names no `provider`, and **rented** when it
//! does. Keys of the other form are rejected, naming the key and the form.
//!
//! | Key | Yours | Rented | Meaning |
//! |---|---|---|---|
//! | `ssh` | yes | no | destination, or a list of them on a class |
//! | `count` | class only | class only | how many machines |
//! | `image` | yes | yes | the worker image |
//! | `runtime` | yes | no | `docker` or `podman` |
//! | `run_args` | yes | no | verbatim container-run flags |
//! | `workers` | yes | no | plain worker count, exclusive with device tables |
//! | `[[….device]]` | yes | no | device tables, exclusive with `workers` |
//! | `provider` | no | yes | `vast` or `stub` |
//! | `fill` | no | class only | `strict` or `best-effort`, default `strict` |
//! | `disk_gb` | no | yes | provisioned disk |
//! | `env` | no | yes | environment assigned when the provider creates the instance |
//! | `bootstrap_sima` | no | yes | allow exec to upload `sima-static` when `binary` is absent |
//! | `ready_timeout_ms`, `ready_poll_ms` | no | yes | readiness bounds |
//! | `[….constraints]` | no | yes | offer constraints, including `min_cuda` |
//! | `root` | yes | yes | where a migrated search lives on that machine, and where its `programs/` directory holds what searches deliver there |
//! | `binary` | yes | yes | the `sima` binary on that machine |
//!
//! A rented machine states no worker layout: it did not exist when the config
//! was written, so its devices come from the `sima-worker --enumerate-devices` probe.
//!
//! `[orchestrator]` is a machine of yours, implicitly this one, so it takes the
//! same worker-side keys an owned host does — `image`, `runtime`, `run_args`,
//! and either `workers` or device tables — plus `migrate`, which names a
//! `[host.*]` entry. It takes no `ssh` and no `provider`, being where the
//! command was typed, and no `root` or `binary`, the search already being here. Its
//! `runtime` and `run_args` describe the container `image` names, so both are
//! rejected without one.
//!
//! `[budget]` bounds the rentals owned by one config contract. A search may
//! draw on several rented classes under one ceiling; an exec applies it to its
//! single standing rental. Its two keys differ in where they are enforced:
//!
//! - `max_spend_usd` bounds what the contract's rentals cost, and enforcing it
//!   means destroying a machine — a provider-API call, which needs the
//!   credential that stays on this machine. It is therefore assessed only
//!   while something here is attached, and a section stating it alone is inert
//!   to a search that rents nothing.
//!   An attached exec assesses the same bound every ten seconds; detaching
//!   leaves assessment to a later attach, `--end`, or reconciliation.
//! - `max_wall_clock_ms` bounds how long the search may compute, measured from the
//!   start of each execution, and `0` states no ceiling at all — the same as
//!   omitting the key. It is kept where no bill runs against the time it
//!   bounds: a plain `sima search` interrupts itself on it, and so does a search
//!   migrated onto a machine of yours, which carries the key. The key does not
//!   travel to a rented destination, because a rental bills by the hour rather
//!   than by use — a search stopped early there saves nothing, and the machine
//!   bills on until `sima recall` or `sima reconcile` takes it down.
//!   An exec assesses this bound with the spend bound while attached, anchored
//!   at the job owner's first rental.
//!
//! ## What a search uses
//!
//! ```text
//! sima search           the orchestrator alone
//! sima search --fleet   the orchestrator plus every member of [fleet]
//! ```
//!
//! Declaring a machine says a search *may* use it; the invocation says it *does*.
//! Without `--fleet` no provider is constructed and no credential is read. A
//! declared host or class that no `[fleet] members` list names is valid and
//! unused.
//!
//! ## Identity and cadence
//!
//! The `[search]` section is canonicalized into [`SearchConfig`], so its fields define
//! the search id; every other section is operational and never hashed — a search
//! resumed with different parallelism, a different store path, or a different
//! set of machines keeps its id. The structural keys are strict: an unknown key
//! anywhere is rejected. The `[search.generator]` table (minus `id`) and the
//! `[search.params]` table pass opaquely to the generator and domain translations,
//! which own and validate their keys.
//!
//! The two checkpoint cadences are unioned: a save is due when either fires, and
//! either present enables checkpointing. With both absent, no checkpoint is ever
//! written.
//!
//! A device `select` names real hardware, so it resolves when a search starts and
//! never at load — reading a config needs no GPU.

/// The one directory a driven config writes into beside itself: its store
/// unless the config names another path, the SDK package this binary vends for
/// the programs it spawns here, and the tree each declared payload installs
/// into. One directory to ignore in a repository, and one to delete to reclaim
/// everything a config generated.
pub(crate) const GENERATED_DIR: &str = ".sima";

mod file;
mod load;
mod machines;
mod search;
mod settings;

pub use load::{ExecConfig, LoadedConfig, load, load_exec};
pub use machines::{
    Container, FillPolicy, Fleet, Host, HostClass, HostClassForm, HostForm, Orchestrator,
    OwnedClass, OwnedHost, Pool, ProviderId, Rented, RentedClass,
};

/// The readiness defaults a destination stating none falls back on. The
/// migration reads them on the non-test path alone — its suite overrides both
/// so it spends no wall clock — so the re-export follows the same gate.
#[cfg(not(test))]
pub(crate) use machines::{DEFAULT_READY_POLL_MS, DEFAULT_READY_TIMEOUT_MS};

#[cfg(test)]
pub(crate) use file::config_section_keys;
