//! A `--fleet` search whose format is served by a program of its own, on machines
//! of yours: the program is delivered to each machine, installed there, and the
//! machine's workers run it.
//!
//! A machine of yours is reached over ssh and its workers run in a container, so
//! this suite stands in for both. `ssh` and the container runtime are shell
//! scripts on the search's `PATH` that strip their own wrapping and run the command
//! they were handed here — which is exactly what the real pair do, minus the
//! network and the namespace. Every argv the pipeline builds is therefore the
//! real one, and every test runs in the ordinary gate.
//!
//! What each test fixes:
//!
//! - a routed entry that names no `payload` refuses before any machine is
//!   contacted, naming the format;
//! - a `--fleet` search of a routed format ingests the program's closure into its
//!   own store and delivers it to every machine of yours;
//! - a machine that cannot receive the program fails the search, naming it;
//! - the search finalizes with the machine's workers running what was installed
//!   there, each answering the digest that machine's own stamp carries;
//! - a machine whose installed program answers another digest fails its spawn,
//!   naming both;
//! - a search whose format this build carries contacts its machines exactly as
//!   before, delivering nothing;
//! - a rented machine receives the program the same way and its workers answer
//!   the same digest, over the stub provider's machines — which are this one,
//!   reached without a hop;
//! - a rented machine that cannot be given the program is excluded and
//!   replaced, its incident recorded, rather than failing the search;
//! - a config that states no worker layout and routes its format through a
//!   payload digest — the shape a migration onto a rented machine synthesizes —
//!   derives its workers from the program's own enumeration and drives to
//!   finalization, while one without that digest is still refused.

mod common;

use std::path::{Path, PathBuf};

use common::{IMAGE, executable, machine_stubs, sima_command, worker_binary, write_config_text};
use sima_core::Result;
use sima_pipeline::{Event, Record, load};
use sima_store::Store;

/// A `sima` command whose `PATH` leads with the machine stand-ins.
fn fleet_command(bin: &Path) -> std::process::Command {
    let mut command = sima_command();
    let path = std::env::var("PATH").unwrap_or_default();
    command.env("PATH", format!("{}:{path}", bin.display()));
    command
}

/// Writes the program that answers for `stub.v1` under `dir` — a directory
/// payload whose install script appends to `installs` — and answers the
/// `[domain.*]` entry declaring it.
fn program(dir: &Path, installs: &Path) -> String {
    executable(
        &dir.join("src/wrapper.sh"),
        &format!("#!/bin/sh\nexec {} \"$@\"\n", worker_binary().display()),
    );
    executable(
        &dir.join("install.sh"),
        &format!(
            "#!/bin/sh\n\
             set -e\n\
             echo ran >> {installs:?}\n\
             cp \"$SIMA_PAYLOAD_DIR/wrapper.sh\" \"$SIMA_INSTALL_DIR/program\"\n\
             chmod 755 \"$SIMA_INSTALL_DIR/program\"\n",
            installs = installs.display(),
        ),
    );
    "[domain.\"stub.v1\"]\nbinary = \"./src/wrapper.sh\"\n\
     payload = \"./src\"\ninstall = \"./install.sh\"\n"
        .to_string()
}

/// A config under `dir` with one machine of yours rooted at `root`, its format
/// served by whatever `entry` declares, and `local` workers of its own.
///
/// A search with none is carried entirely by the machine, so every worker it binds
/// is one spawned there — which is what makes an assertion about them
/// deterministic, the scheduler spawning a slot only when it has work for it.
fn config(dir: &Path, root: &Path, entry: &str, local: usize) -> PathBuf {
    write_config_text(
        dir,
        "sima.toml",
        &format!(
            r#"
        [search]
        root_seed = 21
        segments = 2
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["accumulate:2", "accumulate:2"]

        [config]
        store = "./store"
        max_attempts = 3

        [orchestrator]
        {local}

        [host.machine]
        ssh = "machine"
        image = "{IMAGE}"
        runtime = "docker"
        workers = 1
        root = {root:?}

        [fleet]
        members = ["machine"]

        {entry}
    "#,
            local = if local == 0 {
                String::new()
            } else {
                format!("workers = {local}")
            },
            root = root.to_string_lossy(),
        ),
    )
}

/// Runs `sima search <config> --fleet` and answers its exit code and stderr.
fn fleet_search(bin: &Path, config: &Path) -> (Option<i32>, String) {
    let output = fleet_command(bin)
        .args(["search", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima search");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn an_entry_that_names_no_payload_refuses_before_any_machine_is_contacted() {
    // Such an entry says the program stays where it is installed, so no machine
    // of the fleet could ever serve a worker for the search. The stand-in ssh is
    // absent from the PATH here: reaching a machine at all would fail with a
    // command that is not there, and the refusal names the format instead.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    executable(
        &dir.path().join("src/wrapper.sh"),
        &format!("#!/bin/sh\nexec {} \"$@\"\n", worker_binary().display()),
    );
    let config = config(
        dir.path(),
        far.path(),
        "[domain.\"stub.v1\"]\nbinary = \"./src/wrapper.sh\"\n",
        1,
    );
    let output = sima_command()
        .args(["search", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima search");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_ne!(output.status.code(), Some(0), "{stderr}");
    assert!(stderr.contains("stub.v1"), "names the format: {stderr}");
    assert!(stderr.contains("payload"), "names the key: {stderr}");
    assert!(
        !far.path().exists() || std::fs::read_dir(far.path()).into_iter().flatten().count() == 0,
        "nothing was put on the machine"
    );
}

#[test]
fn a_fleet_search_ingests_the_program_and_delivers_it_to_every_machine() -> Result<()> {
    // What the machine must hold before a pool of its own exists. The search's own
    // outcome is not this test's subject — the delivery is, and it is what
    // every later stage rests on.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("installs");
    let bin = machine_stubs(dir.path(), false);
    let config = config(dir.path(), far.path(), &program(dir.path(), &log), 1);
    fleet_search(&bin, &config);

    // The closure is in the search's own store, which is what a delivery sends
    // from and what a second search reuses.
    let loaded = load(&config)?;
    let store = Store::open(&loaded.store)?;
    let programs = far.path().join("programs");
    let digest = delivered(&programs);
    assert!(store.has(&digest)?, "the search's store holds what it sent");

    // The machine holds the tree, installed and stamped.
    let tree = programs.join(digest.to_string());
    assert!(tree.join("installed/program").is_file());
    assert_eq!(
        std::fs::read_to_string(tree.join("installed.digest")).expect("the stamp"),
        digest.to_string()
    );
    // A second search delivers nothing and installs nothing: the machine's store
    // holds every object and its stamp answers the install.
    let ran = installs(&log);
    fleet_search(&bin, &config);
    assert_eq!(
        installs(&log),
        ran,
        "both stamps answered the second search"
    );
    Ok(())
}

/// The payload digest the delivery under `programs` landed, found by the tree it
/// is keyed by.
fn delivered(programs: &Path) -> sima_core::Hash {
    std::fs::read_dir(programs)
        .expect("the machine holds a delivery")
        .filter_map(|entry| entry.ok().map(|entry| entry.file_name()))
        .find_map(|name| sima_core::Hash::from_hex(&name.to_string_lossy()).ok())
        .expect("a program tree keyed by payload digest")
}

/// How many times the install script under `path` ran, on either machine.
fn installs(path: &Path) -> usize {
    std::fs::read_to_string(path).map_or(0, |text| text.lines().count())
}

#[test]
fn a_machine_that_cannot_receive_the_program_fails_the_search_naming_it() {
    // A machine of yours was declared as a place this search executes, and without
    // the program it can serve no worker — so the search fails rather than
    // proceeding on whatever else it has.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("installs");
    let bin = machine_stubs(dir.path(), true);
    let config = config(dir.path(), far.path(), &program(dir.path(), &log), 1);

    let (code, stderr) = fleet_search(&bin, &config);
    assert_ne!(code, Some(0), "{stderr}");
    assert!(
        stderr.contains("machine"),
        "the failure names the machine: {stderr}"
    );
    assert!(
        stderr.contains("deliver"),
        "and what could not be done: {stderr}"
    );
}

#[test]
fn a_format_this_build_carries_delivers_nothing() {
    // The builtin path, untouched: the machine's workers are the image's own,
    // so nothing is sent and the machine's root stays empty.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let bin = machine_stubs(dir.path(), false);
    let config = config(dir.path(), far.path(), "", 1);

    let (code, stderr) = fleet_search(&bin, &config);
    assert_eq!(code, Some(0), "{stderr}");
    assert!(
        !far.path().join("programs").exists(),
        "a search that sends nothing puts nothing there"
    );
}

/// The program each worker of the search answered at its handshake, in the order
/// the journal bound them.
fn bound_programs(config: &Path) -> Result<Vec<Option<String>>> {
    let loaded = load(config)?;
    let store = Store::open(&loaded.store)?;
    Ok(store
        .journal(&loaded.search.id())?
        .iter()
        .filter_map(|line| Record::from_line(line).ok())
        .filter_map(|record| match record.event {
            Event::WorkerBound { program, .. } => Some(program),
            _ => None,
        })
        .collect())
}

#[test]
fn the_machine_s_workers_run_what_was_installed_there_and_answer_its_stamp() -> Result<()> {
    // The whole path: the program is delivered, installed, and spawned out of
    // the machine's own tree, and every worker it serves answers the digest
    // that machine's stamp carries.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("installs");
    let bin = machine_stubs(dir.path(), false);
    let config = config(dir.path(), far.path(), &program(dir.path(), &log), 0);

    let (code, stderr) = fleet_search(&bin, &config);
    assert_eq!(code, Some(0), "{stderr}");

    let digest = delivered(&far.path().join("programs")).to_string();
    let bound = bound_programs(&config)?;
    assert!(!bound.is_empty(), "the machine served the search");
    for program in &bound {
        assert_eq!(
            program.as_deref(),
            Some(digest.as_str()),
            "every worker answers the digest the machine's stamp carries: {bound:?}"
        );
    }
    Ok(())
}

#[test]
fn a_worker_the_search_sent_no_program_to_answers_none() -> Result<()> {
    // The orchestrator spawns the program where it already sits, so it sent
    // itself nothing and expects nothing back. Its own pool carries this search.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("installs");
    let bin = machine_stubs(dir.path(), false);
    let config = config(dir.path(), far.path(), &program(dir.path(), &log), 2);
    let text = std::fs::read_to_string(&config).expect("the config");
    std::fs::write(
        &config,
        text.replace("members = [\"machine\"]", "members = []"),
    )
    .expect("rewrite the config");

    let (code, stderr) = fleet_search(&bin, &config);
    assert_eq!(code, Some(0), "{stderr}");
    for program in bound_programs(&config)? {
        assert_eq!(program, None, "the orchestrator was sent no program");
    }
    Ok(())
}

#[test]
fn a_machine_holding_another_program_fails_its_spawn_naming_both_digests() {
    // The agreement is between the digest the search sent and the one the machine
    // reads off its own disk, so a tree that drifted is caught at the
    // handshake. The install script writing an entry point that states another
    // digest is how a drifted machine is produced without one.
    const DRIFTED: &str = "3333333333333333333333333333333333333333333333333333333333333333";
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let bin = machine_stubs(dir.path(), false);
    executable(
        &dir.path().join("src/wrapper.sh"),
        &format!("#!/bin/sh\nexec {} \"$@\"\n", worker_binary().display()),
    );
    // The machine's own install writes an entry point that overrides what the
    // shell read from the stamp; the orchestrator's install of the same payload
    // is never spawned through it, since its pool runs the config's `binary`.
    executable(
        &dir.path().join("install.sh"),
        &format!(
            "#!/bin/sh\n\
             set -e\n\
             printf '#!/bin/sh\\nSIMA_PROGRAM_DIGEST={DRIFTED} exec {worker} \"$@\"\\n' \
             > \"$SIMA_INSTALL_DIR/program\"\n\
             chmod 755 \"$SIMA_INSTALL_DIR/program\"\n",
            worker = worker_binary().display(),
        ),
    );
    let config = config(
        dir.path(),
        far.path(),
        "[domain.\"stub.v1\"]\nbinary = \"./src/wrapper.sh\"\n\
         payload = \"./src\"\ninstall = \"./install.sh\"\n",
        0,
    );

    let (code, stderr) = fleet_search(&bin, &config);
    assert_ne!(code, Some(0), "{stderr}");
    assert!(
        stderr.contains("program digest mismatch"),
        "the spawn is refused: {stderr}"
    );
    assert!(stderr.contains(DRIFTED), "names what answered: {stderr}");
    assert!(
        stderr.contains(&delivered(&far.path().join("programs")).to_string()),
        "and what the search sent: {stderr}"
    );
}

/// A config under `dir` renting `count` machines from the stub provider, each
/// rooted at `root`, its format served by whatever `entry` declares.
///
/// The orchestrator declares no workers, so the rentals carry the whole search and
/// every worker it binds is one of theirs.
fn rented_config(dir: &Path, root: &Path, entry: &str, count: usize, fill: &str) -> PathBuf {
    write_config_text(
        dir,
        "rented.toml",
        &format!(
            r#"
        [search]
        root_seed = 21
        segments = 2
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["accumulate:2", "accumulate:2"]

        [config]
        store = "./store"
        max_attempts = 3

        [orchestrator]

        [host_class.rented]
        provider = "stub"
        count = {count}
        fill = "{fill}"
        root = {root:?}
        binary = {binary:?}
        ready_timeout_ms = 30000
        ready_poll_ms = 20

        [fleet]
        members = ["rented"]

        {entry}
    "#,
            root = root.to_string_lossy(),
            binary = env!("CARGO_BIN_EXE_sima"),
        ),
    )
}

#[test]
fn a_rented_machine_receives_the_program_and_its_workers_answer_its_stamp() -> Result<()> {
    // The rented path, end to end: the machine answers a probe that names no
    // format, receives the program, says where its work can go through the
    // program's own session, and serves workers spawned out of the tree it
    // installed.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("installs");
    let config = rented_config(
        dir.path(),
        far.path(),
        &program(dir.path(), &log),
        1,
        "strict",
    );

    let output = sima_command()
        .args(["search", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima search");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(output.status.code(), Some(0), "{stderr}");

    let digest = delivered(&far.path().join("programs")).to_string();
    let bound = bound_programs(&config)?;
    assert!(!bound.is_empty(), "the rented machine served the search");
    for program in &bound {
        assert_eq!(
            program.as_deref(),
            Some(digest.as_str()),
            "every worker answers the digest the machine's stamp carries: {bound:?}"
        );
    }
    Ok(())
}

#[test]
fn a_rented_machine_that_cannot_be_given_the_program_is_replaced() -> Result<()> {
    // A rented machine is disposable, so one that cannot serve the search costs a
    // machine rather than the search: the incident is recorded against it, it is
    // excluded from the attempts that follow, and the next offer fills its
    // place. The install script fails on its first search and succeeds after, so
    // the first machine is refused and the second is not.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let once = dir.path().join("refused-once");
    executable(
        &dir.path().join("src/wrapper.sh"),
        &format!("#!/bin/sh\nexec {} \"$@\"\n", worker_binary().display()),
    );
    executable(
        &dir.path().join("install.sh"),
        &format!(
            "#!/bin/sh\n\
             set -e\n\
             if [ ! -f {once:?} ]; then : > {once:?}; echo 'the install refused' >&2; exit 7; fi\n\
             cp \"$SIMA_PAYLOAD_DIR/wrapper.sh\" \"$SIMA_INSTALL_DIR/program\"\n\
             chmod 755 \"$SIMA_INSTALL_DIR/program\"\n",
            once = once.display(),
        ),
    );
    let config = rented_config(
        dir.path(),
        far.path(),
        "[domain.\"stub.v1\"]\nbinary = \"./src/wrapper.sh\"\n\
         payload = \"./src\"\ninstall = \"./install.sh\"\n",
        // Two offers, so the refused machine has a successor to be replaced
        // by; best-effort, so the search proceeds on the one machine the
        // marketplace could still fill.
        2,
        "best-effort",
    );

    let output = sima_command()
        .args(["search", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima search");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(output.status.code(), Some(0), "{stderr}");
    assert!(once.is_file(), "the first machine's install ran and failed");

    // The refusal is recorded against the machine it was refused by, under its
    // own kind: a machine that answers but cannot be given the program is not
    // one that failed a probe.
    let report = sima_command()
        .args(["report", config.to_str().expect("utf-8 path"), "--machines"])
        .output()
        .expect("spawn sima report");
    let text = String::from_utf8_lossy(&report.stdout).into_owned();
    assert!(
        text.contains("install-failed 1"),
        "the incident names what could not be done: {text}"
    );
    Ok(())
}

#[test]
fn a_rented_machine_holding_another_program_fails_its_spawn_naming_both_digests() {
    // The agreement holds on a rented machine too: what the search sent against
    // what that machine's tree answers, whichever way the machine is reached.
    const DRIFTED: &str = "4444444444444444444444444444444444444444444444444444444444444444";
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    executable(
        &dir.path().join("src/wrapper.sh"),
        &format!("#!/bin/sh\nexec {} \"$@\"\n", worker_binary().display()),
    );
    executable(
        &dir.path().join("install.sh"),
        &format!(
            "#!/bin/sh\n\
             set -e\n\
             printf '#!/bin/sh\\nSIMA_PROGRAM_DIGEST={DRIFTED} exec {worker} \"$@\"\\n' \
             > \"$SIMA_INSTALL_DIR/program\"\n\
             chmod 755 \"$SIMA_INSTALL_DIR/program\"\n",
            worker = worker_binary().display(),
        ),
    );
    let config = rented_config(
        dir.path(),
        far.path(),
        "[domain.\"stub.v1\"]\nbinary = \"./src/wrapper.sh\"\n\
         payload = \"./src\"\ninstall = \"./install.sh\"\n",
        1,
        "strict",
    );

    let output = sima_command()
        .args(["search", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima search");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_ne!(output.status.code(), Some(0), "{stderr}");
    assert!(
        stderr.contains("program digest mismatch"),
        "the spawn is refused: {stderr}"
    );
    assert!(stderr.contains(DRIFTED), "names what answered: {stderr}");
    assert!(
        stderr.contains(&delivered(&far.path().join("programs")).to_string()),
        "and what the search sent: {stderr}"
    );
}

/// A config under `dir` with no `[orchestrator]` layout and no fleet, its
/// format routed through `digest` — the shape a migration onto a rented machine
/// synthesizes, driven here directly.
fn layoutless_config(dir: &Path, digest: Option<&str>) -> PathBuf {
    write_config_text(
        dir,
        "layoutless.toml",
        &format!(
            r#"
        [search]
        root_seed = 21
        segments = 2
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["accumulate:2"]

        [config]
        store = "./store"
        max_attempts = 3

        [orchestrator]

        [domain."stub.v1"]
        binary = "./src/wrapper.sh"
        {digest}
    "#,
            digest = match digest {
                Some(digest) => format!("payload_digest = {digest:?}"),
                None => String::new(),
            }
        ),
    )
}

#[test]
fn a_layoutless_config_over_a_delivered_program_derives_its_workers() -> Result<()> {
    // What a migration onto a rented machine leaves behind: no layout, because
    // nothing there could answer where the work goes until the program was
    // installed. The search derives one worker per usable device from the
    // program's own enumeration — the stub opens none, so one deviceless
    // worker — and drives to finalization.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("installs");
    let bin = machine_stubs(dir.path(), false);
    // A delivery is what puts a payload digest in a store, so one is made here
    // the same way and the digest it lands under is what the config states.
    let source = config(dir.path(), far.path(), &program(dir.path(), &log), 1);
    fleet_search(&bin, &source);
    let digest = delivered(&far.path().join("programs")).to_string();

    let config = layoutless_config(dir.path(), Some(&digest));
    let output = sima_command()
        .args(["search", config.to_str().expect("utf-8 path")])
        .output()
        .expect("spawn sima search");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(output.status.code(), Some(0), "{stderr}");
    assert!(
        !bound_programs(&config)?.is_empty(),
        "the derived pool bound a worker"
    );
    Ok(())
}

#[test]
fn a_layoutless_config_without_a_payload_digest_is_still_refused() {
    // The digest is what scopes the derivation to a config a migration wrote.
    // A hand-written one naming a program on this machine states its own
    // layout, as every config does, and nothing about it changes meaning.
    let dir = tempfile::tempdir().expect("temp dir");
    executable(
        &dir.path().join("src/wrapper.sh"),
        &format!("#!/bin/sh\nexec {} \"$@\"\n", worker_binary().display()),
    );
    let config = layoutless_config(dir.path(), None);
    let output = sima_command()
        .args(["search", config.to_str().expect("utf-8 path")])
        .output()
        .expect("spawn sima search");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_ne!(output.status.code(), Some(0), "{stderr}");
    assert!(
        stderr.contains("no workers and no devices"),
        "the refusal names what is missing: {stderr}"
    );
}
