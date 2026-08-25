//! A `--fleet` run whose format is served by a program of its own, on machines
//! of yours: the program is delivered to each machine, installed there, and the
//! machine's workers run it.
//!
//! A machine of yours is reached over ssh and its workers run in a container, so
//! this suite stands in for both. `ssh` and the container runtime are shell
//! scripts on the run's `PATH` that strip their own wrapping and run the command
//! they were handed here — which is exactly what the real pair do, minus the
//! network and the namespace. Every argv the pipeline builds is therefore the
//! real one, and every test runs in the ordinary gate.
//!
//! What each test fixes:
//!
//! - a routed entry that names no `payload` refuses before any machine is
//!   contacted, naming the format;
//! - a `--fleet` run of a routed format ingests the program's closure into its
//!   own store and delivers it to every machine of yours;
//! - a machine that cannot receive the program fails the run, naming it;
//! - the run finalizes with the machine's workers running what was installed
//!   there, each answering the digest that machine's own stamp carries;
//! - a machine whose installed program answers another digest fails its spawn,
//!   naming both;
//! - a run whose format this build carries contacts its machines exactly as
//!   before, delivering nothing;
//! - a rented machine receives the program the same way and its workers answer
//!   the same digest, over the stub provider's machines — which are this one,
//!   reached without a hop;
//! - a rented machine that cannot be given the program is excluded and
//!   replaced, its incident recorded, rather than failing the run.

mod common;

use std::path::{Path, PathBuf};

use common::{sima_command, worker_binary, write_config_text};
use sima_core::Result;
use sima_pipeline::{Event, Record, load};
use sima_store::Store;

/// The image name the stand-in runtime looks for to know where the container's
/// own flags end and its command begins.
const IMAGE: &str = "IMAGE";

/// Writes an executable file at `path`, creating its parents.
fn executable(path: &Path, text: &str) -> PathBuf {
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("create the parent");
    std::fs::write(path, text).expect("write the file");
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("make it executable");
    path.to_path_buf()
}

/// Writes the stand-ins a machine of yours is reached through, and answers the
/// directory holding them — to be put ahead of everything on the run's `PATH`.
///
/// `ssh` drops its own options and destination and runs the rest here. The
/// runtime answers `image inspect` and `kill`, and for `run` drops everything up
/// to and including the image name, then runs the command that follows. The
/// bind mount needs no honouring: the mount states the identical path on both
/// sides, and here both sides are one filesystem.
///
/// `run_fails` makes every `run` exit non-zero instead, which is what a machine
/// that cannot receive the program looks like.
fn machine_stubs(dir: &Path, run_fails: bool) -> PathBuf {
    let bin = dir.join("machine-bin");
    executable(
        &bin.join("ssh"),
        "#!/bin/sh\n\
         while [ $# -gt 0 ]; do\n\
         \x20 case \"$1\" in\n\
         \x20   -o|-p) shift 2 ;;\n\
         \x20   --) shift; break ;;\n\
         \x20   *) shift ;;\n\
         \x20 esac\n\
         done\n\
         exec \"$@\"\n",
    );
    executable(
        &bin.join("docker"),
        &format!(
            "#!/bin/sh\n\
             verb=$1; shift\n\
             case \"$verb\" in\n\
             \x20 image|kill) exit 0 ;;\n\
             \x20 run)\n\
             \x20   {fail}\n\
             \x20   while [ $# -gt 0 ]; do\n\
             \x20     if [ \"$1\" = \"{IMAGE}\" ]; then shift; break; fi\n\
             \x20     shift\n\
             \x20   done\n\
             \x20   exec \"$@\" ;;\n\
             esac\n\
             exit 1\n",
            fail = if run_fails {
                "echo 'the runtime refused' >&2; exit 3;"
            } else {
                ""
            }
        ),
    );
    // What an image carries, by the names they answer to on the PATH there: the
    // worker a builtin format's pool spawns, and the `sima` a delivery runs.
    for (built, name) in [
        (PathBuf::from(env!("CARGO_BIN_EXE_sima")), "sima"),
        (worker_binary(), "sima-worker"),
    ] {
        std::os::unix::fs::symlink(&built, bin.join(name)).expect("link the image's binary");
    }
    bin
}

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
/// A run with none is carried entirely by the machine, so every worker it binds
/// is one spawned there — which is what makes an assertion about them
/// deterministic, the scheduler spawning a slot only when it has work for it.
fn config(dir: &Path, root: &Path, entry: &str, local: usize) -> PathBuf {
    write_config_text(
        dir,
        "sima.toml",
        &format!(
            r#"
        [run]
        root_seed = 21
        segments = 2
        format = "stub.v1"

        [run.generator]
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

/// Runs `sima run <config> --fleet` and answers its exit code and stderr.
fn fleet_run(bin: &Path, config: &Path) -> (Option<i32>, String) {
    let output = fleet_command(bin)
        .args(["run", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima run");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn an_entry_that_names_no_payload_refuses_before_any_machine_is_contacted() {
    // Such an entry says the program stays where it is installed, so no machine
    // of the fleet could ever serve a worker for the run. The stand-in ssh is
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
        .args(["run", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima run");
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
fn a_fleet_run_ingests_the_program_and_delivers_it_to_every_machine() -> Result<()> {
    // What the machine must hold before a pool of its own exists. The run's own
    // outcome is not this test's subject — the delivery is, and it is what
    // every later stage rests on.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("installs");
    let bin = machine_stubs(dir.path(), false);
    let config = config(dir.path(), far.path(), &program(dir.path(), &log), 1);
    fleet_run(&bin, &config);

    // The closure is in the run's own store, which is what a delivery sends
    // from and what a second run reuses.
    let loaded = load(&config)?;
    let store = Store::open(&loaded.store)?;
    let programs = far.path().join("programs");
    let digest = delivered(&programs);
    assert!(store.has(&digest)?, "the run's store holds what it sent");

    // The machine holds the tree, installed and stamped.
    let tree = programs.join(digest.to_string());
    assert!(tree.join("installed/program").is_file());
    assert_eq!(
        std::fs::read_to_string(tree.join("installed.digest")).expect("the stamp"),
        digest.to_string()
    );
    // A second run delivers nothing and installs nothing: the machine's store
    // holds every object and its stamp answers the install.
    let ran = installs(&log);
    fleet_run(&bin, &config);
    assert_eq!(installs(&log), ran, "both stamps answered the second run");
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
fn a_machine_that_cannot_receive_the_program_fails_the_run_naming_it() {
    // A machine of yours was declared as a place this run executes, and without
    // the program it can serve no worker — so the run fails rather than
    // proceeding on whatever else it has.
    let dir = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("installs");
    let bin = machine_stubs(dir.path(), true);
    let config = config(dir.path(), far.path(), &program(dir.path(), &log), 1);

    let (code, stderr) = fleet_run(&bin, &config);
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

    let (code, stderr) = fleet_run(&bin, &config);
    assert_eq!(code, Some(0), "{stderr}");
    assert!(
        !far.path().join("programs").exists(),
        "a run that sends nothing puts nothing there"
    );
}

/// The program each worker of the run answered at its handshake, in the order
/// the journal bound them.
fn bound_programs(config: &Path) -> Result<Vec<Option<String>>> {
    let loaded = load(config)?;
    let store = Store::open(&loaded.store)?;
    Ok(store
        .journal(&loaded.run.id())?
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

    let (code, stderr) = fleet_run(&bin, &config);
    assert_eq!(code, Some(0), "{stderr}");

    let digest = delivered(&far.path().join("programs")).to_string();
    let bound = bound_programs(&config)?;
    assert!(!bound.is_empty(), "the machine served the run");
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
fn a_worker_the_run_sent_no_program_to_answers_none() -> Result<()> {
    // The orchestrator spawns the program where it already sits, so it sent
    // itself nothing and expects nothing back. Its own pool carries this run.
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

    let (code, stderr) = fleet_run(&bin, &config);
    assert_eq!(code, Some(0), "{stderr}");
    for program in bound_programs(&config)? {
        assert_eq!(program, None, "the orchestrator was sent no program");
    }
    Ok(())
}

#[test]
fn a_machine_holding_another_program_fails_its_spawn_naming_both_digests() {
    // The agreement is between the digest the run sent and the one the machine
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

    let (code, stderr) = fleet_run(&bin, &config);
    assert_ne!(code, Some(0), "{stderr}");
    assert!(
        stderr.contains("program digest mismatch"),
        "the spawn is refused: {stderr}"
    );
    assert!(stderr.contains(DRIFTED), "names what answered: {stderr}");
    assert!(
        stderr.contains(&delivered(&far.path().join("programs")).to_string()),
        "and what the run sent: {stderr}"
    );
}

/// A config under `dir` renting `count` machines from the stub provider, each
/// rooted at `root`, its format served by whatever `entry` declares.
///
/// The orchestrator declares no workers, so the rentals carry the whole run and
/// every worker it binds is one of theirs.
fn rented_config(dir: &Path, root: &Path, entry: &str, count: usize, fill: &str) -> PathBuf {
    write_config_text(
        dir,
        "rented.toml",
        &format!(
            r#"
        [run]
        root_seed = 21
        segments = 2
        format = "stub.v1"

        [run.generator]
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
        .args(["run", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima run");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(output.status.code(), Some(0), "{stderr}");

    let digest = delivered(&far.path().join("programs")).to_string();
    let bound = bound_programs(&config)?;
    assert!(!bound.is_empty(), "the rented machine served the run");
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
    // A rented machine is disposable, so one that cannot serve the run costs a
    // machine rather than the run: the incident is recorded against it, it is
    // excluded from the attempts that follow, and the next offer fills its
    // place. The install script fails on its first run and succeeds after, so
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
        // by; best-effort, so the run proceeds on the one machine the
        // marketplace could still fill.
        2,
        "best-effort",
    );

    let output = sima_command()
        .args(["run", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima run");
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
    // The agreement holds on a rented machine too: what the run sent against
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
        .args(["run", config.to_str().expect("utf-8 path"), "--fleet"])
        .output()
        .expect("spawn sima run");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_ne!(output.status.code(), Some(0), "{stderr}");
    assert!(
        stderr.contains("program digest mismatch"),
        "the spawn is refused: {stderr}"
    );
    assert!(stderr.contains(DRIFTED), "names what answered: {stderr}");
    assert!(
        stderr.contains(&delivered(&far.path().join("programs")).to_string()),
        "and what the run sent: {stderr}"
    );
}
