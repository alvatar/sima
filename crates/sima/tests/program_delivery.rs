//! Delivering a registered program to a machine, over the real binary: `sima
//! sync-serve <dir> --payload <D> [--sdk <S>]` as the far half, the pipeline's
//! own near half driving it.
//!
//! This is what a fleet machine receives before it can serve a worker for a search
//! whose format is a program rather than a build-in format. Every test here
//! searches in the ordinary gate: the far half is a subprocess on this machine,
//! with no ssh hop, no container, and no network.
//!
//! What each test fixes:
//!
//! - a delivery into an empty directory installs the program tree and stamps it
//!   with the digest that was sent;
//! - a second delivery of one digest moves no object and searches no install, which
//!   is what makes putting work on a machine twice cost nothing;
//! - the SDK lands byte-identical to the package the orchestrator's own build
//!   holds, since that build is the one the program speaks the wire to;
//! - a digest whose objects never arrived fails loudly, naming it;
//! - several deliveries into one directory at once install one tree between
//!   them.

mod common;

use std::io::{BufReader, BufWriter, Read};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use common::{sima_command, worker_binary, write_config_text};
use sima_core::{Hash, Result, hash_bytes};
use sima_pipeline::{ProgramDelivery, ingest_program, load};
use sima_store::{ObjectScope, Store, SyncReport, SyncRole};

/// Writes an executable file at `path`, creating its parents.
fn executable(path: &Path, text: &str) -> PathBuf {
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("create the parent");
    std::fs::write(path, text).expect("write the file");
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("make it executable");
    path.to_path_buf()
}

/// A config under `dir` whose `stub.v1` is served by a directory payload whose
/// install script appends a line to `installs` every time it searches.
///
/// The directory shape is what makes an install observable: a single-file
/// payload is its own entry point and searches no script at all.
fn config(dir: &Path, installs: &Path, sdk: bool) -> PathBuf {
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
        behaviors = ["accumulate:2"]

        [config]
        store = "./store"
        max_attempts = 3

        [orchestrator]
        workers = 1

        [domain."stub.v1"]
        binary = "./src/wrapper.sh"
        payload = "./src"
        install = "./install.sh"
        {sdk}
    "#,
            sdk = if sdk { "sdk = \"python\"" } else { "" }
        ),
    )
}

/// The delivery `config` sends, over the store the config names.
fn delivery(config: &Path) -> Result<(Store, ProgramDelivery)> {
    let loaded = load(config)?;
    let store = Store::open(&loaded.store)?;
    let delivery = ingest_program(&loaded, &store)?.expect("a routed format sends its program");
    Ok((store, delivery))
}

/// Sends `delivery` to `sima sync-serve <dir> …` and answers what moved.
fn deliver(store: &Store, delivery: &ProgramDelivery, dir: &Path) -> Result<SyncReport> {
    let mut argv = vec![env!("CARGO_BIN_EXE_sima").to_string()];
    argv.extend(delivery.args(dir.to_str().expect("utf-8 path")));
    delivery.send(store, &argv)
}

/// How many times the install script under `installs` ran.
fn installs(path: &Path) -> usize {
    std::fs::read_to_string(path).map_or(0, |text| text.lines().count())
}

/// The stamp the tree for `digest` under `dir` carries.
fn stamp(dir: &Path, digest: &Hash) -> String {
    std::fs::read_to_string(dir.join(digest.to_string()).join("installed.digest"))
        .expect("the tree is stamped")
}

#[test]
fn a_delivery_installs_the_program_and_stamps_it() -> Result<()> {
    let near = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = near.path().join("installs");
    let config = config(near.path(), &log, false);
    let (store, delivery) = delivery(&config)?;

    let report = deliver(&store, &delivery, far.path())?;
    assert!(report.objects_sent > 0, "the program's objects crossed");

    let tree = far.path().join(delivery.payload().to_string());
    assert!(
        tree.join("installed/program").is_file(),
        "the install left the entry point a spawn searches"
    );
    assert_eq!(
        stamp(far.path(), delivery.payload()),
        delivery.payload().to_string()
    );
    assert_eq!(installs(&log), 1);
    // The store the objects landed in is shared across searches, so it sits beside
    // the trees rather than inside one.
    assert!(far.path().join("store").is_dir());
    Ok(())
}

#[test]
fn a_second_delivery_of_one_digest_moves_no_object_and_runs_no_install() -> Result<()> {
    // What makes putting work on a machine twice cost nothing: the sync's own
    // negotiation skips what is held, and the stamp answers the install.
    let near = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = near.path().join("installs");
    let config = config(near.path(), &log, false);
    let (store, delivery) = delivery(&config)?;

    deliver(&store, &delivery, far.path())?;
    let again = deliver(&store, &delivery, far.path())?;
    assert_eq!(
        again.objects_sent, 0,
        "the machine already held every object"
    );
    assert_eq!(again.objects_received, 0);
    assert_eq!(installs(&log), 1, "the stamp answered the second delivery");
    Ok(())
}

#[test]
fn a_delivery_lands_the_sdk_the_orchestrator_holds() -> Result<()> {
    // The package ships from the orchestrator's build, because the program that
    // imports it speaks the wire to the orchestrator: a machine vending its own
    // could vend one built against another protocol.
    let near = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = near.path().join("installs");
    let config = config(near.path(), &log, true);
    let (store, delivery) = delivery(&config)?;

    let args = delivery.args("unused");
    let sdk = args
        .iter()
        .position(|arg| arg == "--sdk")
        .map(|at| args[at + 1].clone())
        .expect("an entry declaring an SDK names it");
    deliver(&store, &delivery, far.path())?;

    let installed = far.path().join("sdk").join(&sdk).join("installed");
    let vended = tempfile::tempdir().expect("temp dir");
    let vend = sima_command()
        .args([
            "sdk",
            "python",
            "--out",
            vended.path().to_str().expect("utf-8 path"),
        ])
        .output()
        .expect("vend the SDK");
    assert!(vend.status.success(), "{vend:?}");
    for entry in walk(&vended.path().join("sima")) {
        let name = entry.file_name().expect("a file name");
        assert_eq!(
            std::fs::read(installed.join("sima").join(name)).expect("the delivered file"),
            std::fs::read(&entry).expect("the vended file"),
            "{}",
            name.display()
        );
    }
    Ok(())
}

/// Every file directly under `dir`.
fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read the directory")
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| path.is_file())
        .collect();
    files.sort();
    assert!(!files.is_empty(), "{} holds files", dir.display());
    files
}

#[test]
fn a_delivery_whose_objects_never_arrived_fails_naming_the_digest() {
    // A torn delivery, or one asking for a program this side never sent: the
    // far half installs nothing and says which digest it was left without,
    // rather than stamping an empty tree.
    let far = tempfile::tempdir().expect("temp dir");
    let near = tempfile::tempdir().expect("temp dir");
    let store = Store::open(near.path()).expect("open store");
    let absent = hash_bytes(b"a program nobody sent");

    let mut child = sima_command()
        .args([
            "sync-serve",
            far.path().to_str().expect("utf-8 path"),
            "--payload",
            &absent.to_string(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sima sync-serve");
    let mut writer = BufWriter::new(child.stdin.take().expect("piped stdin"));
    let mut reader = BufReader::new(child.stdout.take().expect("piped stdout"));
    // The session itself succeeds: this side advertises nothing and wants
    // nothing. The install is what has no objects to work from.
    let _ = store.sync(
        &[],
        ObjectScope::Named(&[]),
        &mut reader,
        &mut writer,
        SyncRole::Initiator,
    );
    drop(writer);
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("piped stderr")
        .read_to_string(&mut stderr)
        .expect("read the diagnostics");
    let status = child.wait().expect("reap sima sync-serve");

    assert!(!status.success(), "the far half exits nonzero");
    assert!(
        stderr.contains(&absent.to_string()),
        "the diagnostic names the program that never arrived: {stderr}"
    );
    assert!(
        !far.path()
            .join(absent.to_string())
            .join("installed.digest")
            .exists(),
        "nothing claims a tree that was never built"
    );
}

#[test]
fn concurrent_deliveries_into_one_directory_install_once() -> Result<()> {
    // Several searches putting work on one machine at once: the trees are built
    // through the same lock-and-stamp choreography a load uses, so they build
    // one tree between them rather than one each.
    let near = tempfile::tempdir().expect("temp dir");
    let far = tempfile::tempdir().expect("temp dir");
    let log = near.path().join("installs");
    let config = config(near.path(), &log, false);
    let (store, delivery) = delivery(&config)?;

    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                deliver(&store, &delivery, far.path()).expect("deliver the program");
            });
        }
    });
    assert_eq!(installs(&log), 1, "one install between every delivery");
    assert_eq!(
        stamp(far.path(), delivery.payload()),
        delivery.payload().to_string()
    );
    Ok(())
}

#[test]
fn a_format_this_build_carries_sends_nothing() -> Result<()> {
    // Every machine's own worker answers for a built-in format, so there is no
    // program to deliver and no delivery to make.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write_config_text(
        dir.path(),
        "sima.toml",
        r#"
        [search]
        root_seed = 21
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["accumulate:2"]

        [config]
        store = "./store"
        max_attempts = 3

        [orchestrator]
        workers = 1
    "#,
    );
    let loaded = load(&config)?;
    let store = Store::open(&loaded.store)?;
    assert!(ingest_program(&loaded, &store)?.is_none());
    Ok(())
}

#[test]
fn an_entry_that_names_no_payload_is_refused_before_any_machine_is_contacted() -> Result<()> {
    // Such an entry says the program stays where it is installed. A machine
    // that never receives it cannot serve a worker for the search, so the refusal
    // comes from the ingest rather than from a failed handshake later.
    let dir = tempfile::tempdir().expect("temp dir");
    executable(
        &dir.path().join("program.sh"),
        &format!("#!/bin/sh\nexec {} \"$@\"\n", worker_binary().display()),
    );
    let config = write_config_text(
        dir.path(),
        "sima.toml",
        r#"
        [search]
        root_seed = 21
        format = "stub.v1"

        [search.generator]
        id = "stub.v1"
        behaviors = ["accumulate:2"]

        [config]
        store = "./store"
        max_attempts = 3

        [orchestrator]
        workers = 1

        [domain."stub.v1"]
        binary = "./program.sh"
    "#,
    );
    let loaded = load(&config)?;
    let store = Store::open(&loaded.store)?;
    let error = ingest_program(&loaded, &store).expect_err("nothing to send");
    let message = error.to_string();
    assert!(message.contains("stub.v1"), "names the format: {message}");
    assert!(message.contains("payload"), "names the key: {message}");
    Ok(())
}
