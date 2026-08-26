//! The program a run carries with it: what a config declares travels, the
//! content-addressed form it travels as, and how a destination gets it back.
//!
//! A `[domain.*]` entry routes a format to a program on this machine. Moving
//! that run onto another machine therefore has to move the program too, and
//! the payload is how: the files the entry names become ordinary store
//! objects, one manifest object names them, and the manifest's hash is the
//! digest the far config states. The sync that already carries a run's closure
//! carries these objects with it, so nothing is published and no image is
//! rebuilt.
//!
//! ```text
//!    payload dir              objects in the store
//!    ───────────              ────────────────────
//!    stepper.py    ──blake3──►  H₁ (file bytes)
//!    assets/w.bin  ──blake3──►  H₂ (file bytes)
//!                               │
//!    manifest ──────────────►   M = hash(manifest bytes)   ◄── payload_digest
//!      entries (sorted by path):
//!        ("assets/w.bin", exec=false, H₂)
//!        ("stepper.py",   exec=true,  H₁)
//!      install: the script text, when one is declared
//! ```
//!
//! The manifest is identity-bearing, so it encodes through `Enc`/`Dec` like
//! every other hashed value, and the ingest is deterministic: the entries are
//! sorted by path, so one tree always ingests to one digest and a payload the
//! destination already holds costs the sync nothing.
//!
//! The module sits in the pipeline because a payload is a config-driven
//! concept; the store below it stays generic bytes.

use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use sima_core::{Codec, Dec, Enc, Error, Hash, MAX_PAYLOAD, Result, own_process_group};
use sima_store::Store;

use crate::config::GENERATED_DIR;
use crate::stamped_tree::{
    EXECUTABLE_MODE, REGULAR_MODE, STAMP_FILE, build_once, create_dir, executable, read_file,
    remove_dir, validate_path, write_file,
};

/// The directory every installed program hangs off, under the generated
/// directory beside the config file.
const PROGRAM_DIR: &str = "program";
/// Where the manifest's files are materialized. Stable, so a wrapper an
/// install script writes may point into it.
const PAYLOAD_DIR: &str = "payload";
/// Where the install script puts what it built.
const INSTALLED_DIR: &str = "installed";
/// The entry point the config's `binary` names, found by convention inside
/// [`INSTALLED_DIR`].
const ENTRY_POINT: &str = "program";
/// The install script, written out of the manifest.
const SCRIPT_FILE: &str = "install.sh";
/// The last install's combined stdout and stderr.
const LOG_FILE: &str = "install.log";
/// How much of the install log a failure carries inline. The script's own last
/// words are what say why it failed; the whole log is at the path the error
/// also names.
const LOG_TAIL_LINES: usize = 20;

/// What a `[domain.*]` entry declares travels: the payload itself, and the
/// script that turns it into a program on the destination.
///
/// Both paths are resolved against the config file's directory, the rule every
/// path in a config follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadSpec {
    /// One file or one directory. A single file is the program; a directory is
    /// whatever `install` makes of it.
    pub(crate) payload: PathBuf,
    /// The shell script the destination runs over the materialized payload.
    /// `None` for a single-file payload, which is its own entry point.
    pub(crate) install: Option<PathBuf>,
}

/// One file of a payload: where it sits in the tree, whether it runs, and the
/// object holding its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileEntry {
    /// Relative to the payload root, `/`-separated, with no `.` or `..`
    /// component — a path that means the same thing on any machine.
    pub(crate) path: String,
    /// Whether the file carried an execute bit where it was ingested.
    pub(crate) executable: bool,
    /// The object holding the file's bytes.
    pub(crate) object: Hash,
}

/// A payload, named: every file it comprises and the script that installs
/// them. The hash of its canonical bytes is the payload digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Manifest {
    /// Strictly ascending by path bytes, so one tree always encodes one way.
    entries: Vec<FileEntry>,
    /// The install script's text, when the entry declared one.
    install: Option<String>,
}

impl Manifest {
    /// The manifest `entries` and `install` describe, validated: paths are
    /// relative and machine-independent, the order is canonical, and a payload
    /// of several files names the script that turns them into a program.
    ///
    /// The constructor is where both sides meet — the ingest builds through it
    /// and the decode reads through it — so a manifest that came off the wire
    /// is held to exactly what one built here satisfies.
    fn new(mut entries: Vec<FileEntry>, install: Option<String>) -> Result<Manifest> {
        entries.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        if entries.is_empty() {
            return Err(Error::Validation(
                "a payload manifest names no file, so it names no program".to_string(),
            ));
        }
        if entries.len() > 1 && install.is_none() {
            return Err(Error::Validation(format!(
                "a payload manifest of {} files names no install script; \
                 which of them is the program is what the script decides",
                entries.len()
            )));
        }
        for pair in entries.windows(2) {
            if pair[0].path == pair[1].path {
                return Err(Error::Validation(format!(
                    "a payload manifest names {:?} twice",
                    pair[0].path
                )));
            }
        }
        for entry in &entries {
            validate_path(&entry.path)?;
        }
        Ok(Manifest { entries, install })
    }

    /// The install script's text, when the payload carries one.
    pub(crate) fn install(&self) -> Option<&str> {
        self.install.as_deref()
    }

    /// Every object this manifest names, in the manifest's own order. With the
    /// manifest's own hash these are the payload's whole closure — what a push
    /// has to advertise for the destination to be able to install it.
    pub(crate) fn objects(&self) -> impl Iterator<Item = Hash> + '_ {
        self.entries.iter().map(|entry| entry.object)
    }

    /// The one file a payload of a single file comprises, and `None` for a
    /// payload of several — which names an install script instead.
    pub(crate) fn lone_file(&self) -> Option<&FileEntry> {
        match self.entries.as_slice() {
            [entry] => Some(entry),
            _ => None,
        }
    }
}

impl Codec for Manifest {
    fn encode(&self, enc: &mut Enc) {
        // A u32 count: a payload of four billion files is not a payload, and
        // the ingest's own refusals bound it long before this.
        enc.u32(self.entries.len() as u32);
        for entry in &self.entries {
            enc.str(&entry.path)
                .flag(entry.executable)
                .hash(&entry.object);
        }
        match &self.install {
            None => enc.flag(false),
            Some(script) => enc.flag(true).str(script),
        };
    }

    fn decode(dec: &mut Dec<'_>) -> Result<Manifest> {
        let count = dec.u32()?;
        let mut entries = Vec::new();
        for _ in 0..count {
            let path = dec.str()?.to_string();
            let executable = dec.flag()?;
            let object = dec.hash()?;
            entries.push(FileEntry {
                path,
                executable,
                object,
            });
        }
        // Ascending order is part of the canonical form, so a re-sorted
        // manifest would encode back to different bytes under the same hash.
        for pair in entries.windows(2) {
            if pair[0].path.as_bytes() >= pair[1].path.as_bytes() {
                return Err(Error::Encoding(format!(
                    "payload manifest entries are out of order: {:?} does not precede {:?}",
                    pair[0].path, pair[1].path
                )));
            }
        }
        let install = dec
            .flag()?
            .then(|| dec.str().map(str::to_string))
            .transpose()?;
        Manifest::new(entries, install)
    }
}

/// Ingests what `spec` declares into `store` and answers the manifest's hash,
/// which is the payload digest a far config states.
///
/// The walk is deterministic — directory entries are visited in sorted order
/// and the manifest is sorted again on construction — so ingesting one tree
/// twice puts the same objects and yields the same digest. That is what makes
/// the sync's negotiation skip a payload the destination already holds.
pub(crate) fn ingest(store: &Store, spec: &PayloadSpec) -> Result<Hash> {
    let install = match &spec.install {
        None => None,
        Some(path) => Some(String::from_utf8(read_file(path)?).map_err(|e| {
            Error::Validation(format!(
                "install script {} is not UTF-8: {e}",
                path.display()
            ))
        })?),
    };
    let root = &spec.payload;
    let metadata = std::fs::symlink_metadata(root).map_err(|source| Error::Io {
        path: root.clone(),
        source,
    })?;
    let mut entries = Vec::new();
    if metadata.is_dir() {
        collect(store, root, "", &mut entries)?;
        if entries.is_empty() {
            return Err(Error::Validation(format!(
                "payload {} holds no file",
                root.display()
            )));
        }
    } else {
        // A single-file payload keeps its own name, so the tree the
        // destination materializes reads like the one that was declared.
        let name = file_name(root)?;
        entries.push(ingest_file(store, root, name, &metadata)?);
    }
    store.put(&Manifest::new(entries, install)?.to_bytes())
}

/// Walks `dir`, appending one entry per file under it. `prefix` is the path
/// `dir` sits at inside the payload, empty at the root.
fn collect(store: &Store, dir: &Path, prefix: &str, entries: &mut Vec<FileEntry>) -> Result<()> {
    let mut children: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|source| Error::Io {
            path: dir.to_path_buf(),
            source,
        })?
        .map(|entry| {
            entry.map(|entry| entry.path()).map_err(|source| Error::Io {
                path: dir.to_path_buf(),
                source,
            })
        })
        .collect::<Result<_>>()?;
    // Sorted, so the walk visits one tree in one order whatever the directory
    // happened to return.
    children.sort();
    for child in children {
        let name = file_name(&child)?;
        let path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };
        // `symlink_metadata`, so a link is seen as a link: what it points at is
        // this machine's business, and the destination has no such file.
        let metadata = std::fs::symlink_metadata(&child).map_err(|source| Error::Io {
            path: child.clone(),
            source,
        })?;
        if metadata.is_dir() {
            collect(store, &child, &path, entries)?;
        } else {
            entries.push(ingest_file(store, &child, &path, &metadata)?);
        }
    }
    Ok(())
}

/// Stores one file's bytes and describes it, refusing anything that is not a
/// regular file and anything too large to cross the sync.
fn ingest_file(
    store: &Store,
    file: &Path,
    path: &str,
    metadata: &std::fs::Metadata,
) -> Result<FileEntry> {
    if !metadata.is_file() {
        return Err(Error::Validation(format!(
            "payload entry {} is not a regular file; a payload carries files and \
             directories, and what a link points at is this machine's alone",
            file.display()
        )));
    }
    // The sync frames an object whole, so a file above the frame cap could be
    // stored here and never reach the destination. Refusing it at the ingest
    // states that where the file is named.
    if metadata.len() > u64::from(MAX_PAYLOAD) {
        return Err(Error::Validation(format!(
            "payload entry {} is {} bytes, above the {MAX_PAYLOAD} byte cap a \
             transferred object may reach",
            file.display(),
            metadata.len()
        )));
    }
    Ok(FileEntry {
        path: path.to_string(),
        executable: metadata.permissions().mode() & 0o111 != 0,
        object: store.put(&read_file(file)?)?,
    })
}

/// Every object a destination needs before it can install the payload
/// `digest` names: the manifest itself and each file it names.
///
/// This is the whole closure — the manifest names the files by hash, so a
/// destination holding these can materialize and install without asking for
/// anything else. A push appends it to the objects it advertises, and the
/// sync's own negotiation is what skips the ones the destination already has.
pub(crate) fn closure(store: &Store, digest: &Hash) -> Result<Vec<Hash>> {
    let manifest = Manifest::from_bytes(&store.get(digest)?)?;
    Ok(std::iter::once(*digest).chain(manifest.objects()).collect())
}

/// Materializes the payload `digest` names into `dest`, restoring each file's
/// path and execute bit, and answers the manifest it worked from — which is
/// also where the install script comes from.
///
/// A digest the store does not hold is [`Error::MissingObject`] naming it: on
/// the destination that is a payload the push did not carry.
pub(crate) fn materialize(store: &Store, digest: &Hash, dest: &Path) -> Result<Manifest> {
    let manifest = Manifest::from_bytes(&store.get(digest)?)?;
    create_dir(dest)?;
    for entry in &manifest.entries {
        let path = dest.join(&entry.path);
        if let Some(parent) = path.parent() {
            create_dir(parent)?;
        }
        write_file(&path, &store.get(&entry.object)?, mode_of(entry.executable))?;
    }
    Ok(manifest)
}

/// Where the program answering for one format is installed on the machine
/// that runs it.
///
/// ```text
///   <config-dir>/program/<format>/
///       .lock              held while installing
///       payload/           the manifest's files, materialized
///       install.sh         the manifest's script, when it carries one
///       install.log        the last install's combined stdout and stderr
///       installed/         what the script filled
///       installed/program  the entry point the config's binary names
///       installed.digest   the manifest digest this tree was built from
/// ```
///
/// The tree hangs off the config file's own directory, which on a machine a
/// run migrated onto is the run's directory — so two runs on one machine
/// install into two trees. It is keyed by format below that, so a config
/// routing two formats to two programs installs two programs rather than
/// overwriting one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProgramTree {
    root: PathBuf,
}

impl ProgramTree {
    /// The tree for `format` under the directory `config` sits in.
    ///
    /// The format id is a path component here, and the id rule admits `.` and
    /// `..`, so it is held to the same rule a manifest path is: a name that
    /// would mean a directory other than this one is refused.
    pub(crate) fn new(config_dir: &Path, format: &str) -> Result<ProgramTree> {
        validate_path(format)?;
        Ok(ProgramTree::at(
            config_dir
                .join(GENERATED_DIR)
                .join(PROGRAM_DIR)
                .join(format),
        ))
    }

    /// The tree rooted at `root`, whatever named it.
    ///
    /// A fleet machine keys its trees by payload digest rather than by format,
    /// because what lands there is one program per digest shared across every
    /// run that sends it, so the root is handed in whole.
    pub(crate) fn at(root: PathBuf) -> ProgramTree {
        ProgramTree { root }
    }

    /// Where the manifest's files are materialized.
    fn payload(&self) -> PathBuf {
        self.root.join(PAYLOAD_DIR)
    }

    /// Where the install script puts what it built.
    fn installed(&self) -> PathBuf {
        self.root.join(INSTALLED_DIR)
    }

    /// The entry point the config's `binary` names.
    pub(crate) fn entry_point(&self) -> PathBuf {
        self.installed().join(ENTRY_POINT)
    }

    /// The digest the tree was built from, as the machine holding it recorded
    /// it. A spawn on that machine reads this file to state which program it is
    /// running, so what the run is answered is the disk's own claim.
    pub(crate) fn stamp(&self) -> PathBuf {
        self.root.join(STAMP_FILE)
    }
}

/// The entry point of `format`'s installed program, relative to the directory
/// the config file sits in — which is what a synthesized far config writes as
/// its `binary`.
///
/// Stated here beside the tree it names, so the path a config points at and the
/// path an install fills have one definition between them.
pub(crate) fn relative_entry_point(format: &str) -> String {
    format!("./{GENERATED_DIR}/{PROGRAM_DIR}/{format}/{INSTALLED_DIR}/{ENTRY_POINT}")
}

/// Installs the payload `digest` names into `tree`, so the entry point is on
/// this machine by the time the config's `binary` is spawned.
///
/// The tree is stamped with the digest it was built from, so a load whose stamp
/// already names it reads one file — what makes a reattach, a status query, and
/// a follow attach cost nothing, and what makes a changed payload reinstall
/// exactly once. [`crate::stamped_tree`] carries that choreography; what is
/// this module's is what a program tree holds and when it is complete, which is
/// when its entry point is executable.
pub(crate) fn install(store: &Store, digest: &Hash, tree: &ProgramTree) -> Result<()> {
    build_once(
        &tree.root,
        digest,
        &|| executable(&tree.entry_point()),
        &|| build(store, digest, tree),
    )
}

/// Fills the tree: the previous trees are removed, the payload is materialized,
/// and the install runs, leaving the entry point the config's `binary` names.
///
/// Called with the tree's lock held and its stamp already removed, so what a
/// failure here leaves behind is a tree nothing claims.
fn build(store: &Store, digest: &Hash, tree: &ProgramTree) -> Result<()> {
    remove_dir(&tree.payload())?;
    remove_dir(&tree.installed())?;

    let manifest = materialize(store, digest, &tree.payload())?;
    create_dir(&tree.installed())?;
    match manifest.install() {
        Some(script) => run_install(tree, script)?,
        // With no script the payload is one file, which is the program: the
        // convention puts it where the config's `binary` looks.
        None => {
            let lone = manifest
                .lone_file()
                .expect("a manifest with no install script names one file");
            let bytes =
                std::fs::read(tree.payload().join(&lone.path)).map_err(|source| Error::Io {
                    path: tree.payload().join(&lone.path),
                    source,
                })?;
            write_file(&tree.entry_point(), &bytes, EXECUTABLE_MODE)?;
        }
    }
    if !executable(&tree.entry_point())? {
        return Err(Error::Validation(format!(
            "the install left no executable {}; a payload's install script \
             leaves the program it built at $SIMA_INSTALL_DIR/{ENTRY_POINT}",
            tree.entry_point().display()
        )));
    }
    Ok(())
}

/// Writes the manifest's script out and runs it over the materialized payload.
///
/// The script runs under this machine's own environment plus the two variables
/// that tell it where to read from and where to leave what it builds. Nothing
/// is forwarded from the machine that sent the payload: an installed program is
/// built out of what the destination has.
fn run_install(tree: &ProgramTree, script: &str) -> Result<()> {
    let script_path = tree.root.join(SCRIPT_FILE);
    write_file(&script_path, script.as_bytes(), EXECUTABLE_MODE)?;
    let log_path = tree.root.join(LOG_FILE);
    let log = File::create(&log_path).map_err(|source| Error::Io {
        path: log_path.clone(),
        source,
    })?;
    // One file for both streams, so the script's output reads in the order it
    // was produced.
    let errors = log.try_clone().map_err(|source| Error::Io {
        path: log_path.clone(),
        source,
    })?;
    // Absolute, because the script may `cd` anywhere it likes.
    let payload = absolute(&tree.payload())?;
    let installed = absolute(&tree.installed())?;
    let status = own_process_group(&mut Command::new("/bin/sh"))
        .arg(&script_path)
        .current_dir(&tree.root)
        .env("SIMA_PAYLOAD_DIR", &payload)
        .env("SIMA_INSTALL_DIR", &installed)
        .stdin(std::process::Stdio::null())
        .stdout(log)
        .stderr(errors)
        .status()
        .map_err(|source| Error::Io {
            path: script_path.clone(),
            source,
        })?;
    if status.success() {
        return Ok(());
    }
    Err(Error::Validation(format!(
        "the install script {} exited with {status}; the whole log is at {}, \
         and its last lines are:\n{}",
        script_path.display(),
        log_path.display(),
        log_tail(&log_path),
    )))
}

/// The last lines of the install log, for the error that names the failure.
/// A log that cannot be read says so rather than replacing the failure it was
/// meant to explain.
fn log_tail(path: &Path) -> String {
    let Ok(text) = std::fs::read_to_string(path) else {
        return format!("({} could not be read)", path.display());
    };
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(LOG_TAIL_LINES)..].join("\n")
}

/// A path from the filesystem root, naming it on failure.
fn absolute(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// The mode a materialized file is given: what it carried where it was
/// ingested, reduced to the one bit that matters.
fn mode_of(executable: bool) -> u32 {
    if executable {
        EXECUTABLE_MODE
    } else {
        REGULAR_MODE
    }
}

/// One file's name, as a manifest may carry it.
fn file_name(path: &Path) -> Result<&str> {
    path.file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| {
            Error::Validation(format!(
                "payload entry {} has no name a manifest can carry; \
                 a payload's file names are UTF-8",
                path.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sima_core::hash_bytes;

    use super::*;

    /// A store in a fresh temporary directory.
    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(dir.path()).expect("open store");
        (dir, store)
    }

    /// Writes `bytes` at `path` under `dir`, creating parents, and answers the
    /// file's path.
    fn write(dir: &Path, path: &str, bytes: &[u8], mode: u32) -> PathBuf {
        let full = dir.join(path);
        create_dir(full.parent().expect("a parent")).expect("create the parent");
        write_file(&full, bytes, mode).expect("write the file");
        full
    }

    /// A payload tree of two files under a fresh directory: one executable
    /// script and one plain asset.
    fn tree() -> (tempfile::TempDir, PayloadSpec) {
        let dir = tempfile::tempdir().expect("temp dir");
        write(dir.path(), "payload/run.sh", b"#!/bin/sh\ntrue\n", 0o755);
        write(dir.path(), "payload/assets/w.bin", b"weights", 0o644);
        let install = write(dir.path(), "install.sh", b"#!/bin/sh\nexit 0\n", 0o755);
        let spec = PayloadSpec {
            payload: dir.path().join("payload"),
            install: Some(install),
        };
        (dir, spec)
    }

    /// A payload of one executable file.
    fn lone(bytes: &[u8], mode: u32) -> (tempfile::TempDir, PayloadSpec) {
        let dir = tempfile::tempdir().expect("temp dir");
        let payload = write(dir.path(), "program.sh", bytes, mode);
        let spec = PayloadSpec {
            payload,
            install: None,
        };
        (dir, spec)
    }

    /// The manifest `digest` names in `store`.
    fn manifest(store: &Store, digest: &Hash) -> Manifest {
        Manifest::from_bytes(&store.get(digest).expect("the manifest object"))
            .expect("the manifest decodes")
    }

    /// The validation message `outcome` was refused with.
    fn refusal<T: std::fmt::Debug>(outcome: Result<T>) -> String {
        match outcome {
            Err(Error::Validation(message)) => message,
            other => panic!("expected a validation error, got {other:?}"),
        }
    }

    // ---- The codec ----

    #[test]
    fn a_manifest_round_trips_through_its_canonical_bytes() -> Result<()> {
        let built = Manifest::new(
            vec![
                FileEntry {
                    path: "run.sh".to_string(),
                    executable: true,
                    object: hash_bytes(b"script"),
                },
                FileEntry {
                    path: "assets/w.bin".to_string(),
                    executable: false,
                    object: hash_bytes(b"weights"),
                },
            ],
            Some("#!/bin/sh\ninstall\n".to_string()),
        )?;
        assert_eq!(Manifest::from_bytes(&built.to_bytes())?, built);
        // Sorted on construction, whatever order the caller offered.
        assert_eq!(
            built.entries.iter().map(|e| &e.path).collect::<Vec<_>>(),
            ["assets/w.bin", "run.sh"]
        );
        Ok(())
    }

    #[test]
    fn a_manifest_without_a_script_round_trips_too() -> Result<()> {
        let built = Manifest::new(
            vec![FileEntry {
                path: "program".to_string(),
                executable: true,
                object: hash_bytes(b"program"),
            }],
            None,
        )?;
        assert_eq!(built.install(), None);
        assert_eq!(Manifest::from_bytes(&built.to_bytes())?, built);
        Ok(())
    }

    #[test]
    fn a_manifest_naming_no_file_is_refused() {
        assert!(refusal(Manifest::new(Vec::new(), None)).contains("no file"));
    }

    #[test]
    fn several_files_without_a_script_are_refused_on_both_sides() -> Result<()> {
        // Which file is the program is what the script decides, so a payload
        // of several without one names no entry point.
        let entries = vec![
            FileEntry {
                path: "a".to_string(),
                executable: true,
                object: hash_bytes(b"a"),
            },
            FileEntry {
                path: "b".to_string(),
                executable: false,
                object: hash_bytes(b"b"),
            },
        ];
        assert!(refusal(Manifest::new(entries.clone(), None)).contains("install script"));
        // The decode holds a manifest off the wire to the same rule: the bytes
        // are written by hand, since nothing here would build them.
        let mut enc = Enc::new();
        enc.u32(2);
        for entry in &entries {
            enc.str(&entry.path)
                .flag(entry.executable)
                .hash(&entry.object);
        }
        enc.flag(false);
        assert!(refusal(Manifest::from_bytes(&enc.finish())).contains("install script"));
        Ok(())
    }

    #[test]
    fn a_manifest_whose_entries_are_out_of_order_fails_to_decode() {
        // The order is part of the canonical form: a re-sorted manifest would
        // encode to different bytes under the same digest.
        let mut enc = Enc::new();
        enc.u32(2);
        for path in ["b", "a"] {
            enc.str(path).flag(false).hash(&hash_bytes(path.as_bytes()));
        }
        enc.flag(true).str("install");
        match Manifest::from_bytes(&enc.finish()) {
            Err(Error::Encoding(message)) => assert!(message.contains("out of order"), "{message}"),
            other => panic!("expected an encoding error, got {other:?}"),
        }
    }

    #[test]
    fn a_path_that_means_something_else_elsewhere_is_refused() {
        for path in [
            "",
            "/etc/passwd",
            "../escape",
            "a/../b",
            "./a",
            "a//b",
            "a\\b",
        ] {
            let entry = FileEntry {
                path: path.to_string(),
                executable: false,
                object: hash_bytes(b"x"),
            };
            let message = refusal(Manifest::new(vec![entry], None));
            assert!(message.contains("payload path"), "{path:?}: {message}");
        }
    }

    // ---- The ingest ----

    #[test]
    fn one_tree_ingests_to_one_digest_however_often_it_is_ingested() -> Result<()> {
        let (_dir, store) = store();
        let (_payload, spec) = tree();
        assert_eq!(ingest(&store, &spec)?, ingest(&store, &spec)?);
        Ok(())
    }

    #[test]
    fn the_digest_moves_with_content_with_path_and_with_the_execute_bit() -> Result<()> {
        let (_dir, store) = store();
        let (payload, spec) = tree();
        let base = ingest(&store, &spec)?;

        // Content.
        write(payload.path(), "payload/assets/w.bin", b"other", 0o644);
        let changed = ingest(&store, &spec)?;
        assert_ne!(base, changed, "content enters the digest");

        // Path.
        std::fs::rename(
            payload.path().join("payload/assets/w.bin"),
            payload.path().join("payload/assets/v.bin"),
        )
        .expect("rename");
        assert_ne!(changed, ingest(&store, &spec)?, "the path enters it too");

        // The execute bit.
        let renamed = payload.path().join("payload/assets/v.bin");
        let moved = ingest(&store, &spec)?;
        std::fs::set_permissions(&renamed, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
        assert_ne!(moved, ingest(&store, &spec)?, "so does the execute bit");
        Ok(())
    }

    #[test]
    fn the_install_script_enters_the_digest() -> Result<()> {
        let (_dir, store) = store();
        let (payload, spec) = tree();
        let base = ingest(&store, &spec)?;
        write(payload.path(), "install.sh", b"#!/bin/sh\nexit 1\n", 0o755);
        assert_ne!(base, ingest(&store, &spec)?);
        Ok(())
    }

    #[test]
    fn a_single_file_payload_is_a_manifest_of_one_entry_named_after_it() -> Result<()> {
        let (_dir, store) = store();
        let (_payload, spec) = lone(b"#!/bin/sh\ntrue\n", 0o755);
        let manifest = manifest(&store, &ingest(&store, &spec)?);
        let lone = manifest.lone_file().expect("one file");
        assert_eq!(lone.path, "program.sh");
        assert!(lone.executable);
        assert_eq!(manifest.install(), None);
        Ok(())
    }

    #[test]
    fn nested_directories_carry_their_paths() -> Result<()> {
        let (_dir, store) = store();
        let dir = tempfile::tempdir().expect("temp dir");
        write(dir.path(), "p/a/b/c/deep.txt", b"deep", 0o644);
        write(dir.path(), "p/top.txt", b"top", 0o644);
        let install = write(dir.path(), "install.sh", b"true\n", 0o755);
        let manifest = manifest(
            &store,
            &ingest(
                &store,
                &PayloadSpec {
                    payload: dir.path().join("p"),
                    install: Some(install),
                },
            )?,
        );
        assert_eq!(
            manifest.entries.iter().map(|e| &e.path).collect::<Vec<_>>(),
            ["a/b/c/deep.txt", "top.txt"]
        );
        Ok(())
    }

    #[test]
    fn the_walk_order_is_corrected_to_the_canonical_one() -> Result<()> {
        // The walk visits a directory before the sibling files that sort after
        // its name, and `/` sorts after `.`, so `a/z` comes out of the walk
        // ahead of `a.txt` while the canonical order is the reverse. The
        // manifest sorts on construction, which is what makes the two agree.
        let (_dir, store) = store();
        let dir = tempfile::tempdir().expect("temp dir");
        write(dir.path(), "p/a/z.txt", b"nested", 0o644);
        write(dir.path(), "p/a.txt", b"sibling", 0o644);
        let install = write(dir.path(), "install.sh", b"true\n", 0o755);
        let digest = ingest(
            &store,
            &PayloadSpec {
                payload: dir.path().join("p"),
                install: Some(install),
            },
        )?;
        assert_eq!(
            manifest(&store, &digest)
                .entries
                .iter()
                .map(|e| &e.path)
                .collect::<Vec<_>>(),
            ["a.txt", "a/z.txt"],
            "the manifest decodes, which the canonical order is what allows"
        );
        Ok(())
    }

    #[test]
    fn a_symlink_in_a_payload_is_refused_naming_it() -> Result<()> {
        // What a link points at is this machine's business; the destination
        // has no such file.
        let (_dir, store) = store();
        let (payload, spec) = tree();
        std::os::unix::fs::symlink("/etc/hostname", payload.path().join("payload/link"))
            .expect("link");
        let message = refusal(ingest(&store, &spec));
        assert!(message.contains("link"), "names the entry: {message}");
        assert!(message.contains("regular file"), "{message}");
        Ok(())
    }

    #[test]
    fn a_file_name_that_is_not_utf_8_is_refused_naming_it() -> Result<()> {
        use std::os::unix::ffi::OsStrExt;

        let (_dir, store) = store();
        let (payload, spec) = tree();
        let name = std::ffi::OsStr::from_bytes(b"\xff\xfe");
        write_file(&payload.path().join("payload").join(name), b"bytes", 0o644)?;
        assert!(refusal(ingest(&store, &spec)).contains("UTF-8"));
        Ok(())
    }

    #[test]
    fn an_empty_directory_payload_is_refused_naming_it() -> Result<()> {
        let (_dir, store) = store();
        let dir = tempfile::tempdir().expect("temp dir");
        let empty = dir.path().join("empty");
        create_dir(&empty)?;
        // Nested empty directories hold no file either: a payload is its
        // files, and a tree of none names no program.
        create_dir(&empty.join("still/empty"))?;
        let install = write(dir.path(), "install.sh", b"true\n", 0o755);
        let message = refusal(ingest(
            &store,
            &PayloadSpec {
                payload: empty.clone(),
                install: Some(install),
            },
        ));
        assert!(
            message.contains(&empty.display().to_string()),
            "names the payload: {message}"
        );
        Ok(())
    }

    #[test]
    fn a_file_too_large_to_cross_the_sync_is_refused_naming_it() -> Result<()> {
        // A sparse file, so the refusal is reached without writing 256 MiB.
        let (_dir, store) = store();
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("huge.bin");
        let file = std::fs::File::create(&path).expect("create");
        file.set_len(u64::from(MAX_PAYLOAD) + 1).expect("grow it");
        drop(file);
        let message = refusal(ingest(
            &store,
            &PayloadSpec {
                payload: path,
                install: None,
            },
        ));
        assert!(message.contains("huge.bin"), "names the entry: {message}");
        assert!(message.contains("cap"), "{message}");
        Ok(())
    }

    #[test]
    fn several_ingested_files_without_a_script_are_refused() -> Result<()> {
        let (_dir, store) = store();
        let (_payload, mut spec) = tree();
        spec.install = None;
        assert!(refusal(ingest(&store, &spec)).contains("install script"));
        Ok(())
    }

    #[test]
    fn an_install_script_that_is_not_utf_8_is_refused() -> Result<()> {
        let (_dir, store) = store();
        let (payload, spec) = tree();
        write(payload.path(), "install.sh", b"\xff\xfe", 0o755);
        assert!(refusal(ingest(&store, &spec)).contains("UTF-8"));
        Ok(())
    }

    // ---- Materialization ----

    /// Every file under `dir`, by relative path, with its bytes and mode.
    fn walk(dir: &Path, prefix: &str, out: &mut BTreeMap<String, (Vec<u8>, u32)>) {
        for entry in std::fs::read_dir(dir).expect("read the directory") {
            let entry = entry.expect("a directory entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            let metadata = entry.metadata().expect("metadata");
            if metadata.is_dir() {
                walk(&entry.path(), &path, out);
            } else {
                out.insert(
                    path,
                    (
                        std::fs::read(entry.path()).expect("read"),
                        metadata.permissions().mode() & 0o777,
                    ),
                );
            }
        }
    }

    /// The tree under `dir`, by relative path.
    fn contents(dir: &Path) -> BTreeMap<String, (Vec<u8>, u32)> {
        let mut out = BTreeMap::new();
        walk(dir, "", &mut out);
        out
    }

    #[test]
    fn a_materialized_payload_is_the_tree_that_was_ingested() -> Result<()> {
        let (_dir, store) = store();
        let (payload, spec) = tree();
        let digest = ingest(&store, &spec)?;
        let dest = tempfile::tempdir().expect("temp dir");
        let manifest = materialize(&store, &digest, dest.path())?;

        assert_eq!(
            contents(dest.path()),
            contents(&payload.path().join("payload"))
        );
        assert_eq!(manifest.install(), Some("#!/bin/sh\nexit 0\n"));
        Ok(())
    }

    #[test]
    fn the_execute_bit_survives_the_round_trip() -> Result<()> {
        let (_dir, store) = store();
        let (_payload, spec) = lone(b"#!/bin/sh\ntrue\n", 0o755);
        let dest = tempfile::tempdir().expect("temp dir");
        materialize(&store, &ingest(&store, &spec)?, dest.path())?;
        let mode = std::fs::metadata(dest.path().join("program.sh"))
            .expect("the file")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, EXECUTABLE_MODE);

        // And a plain file stays plain, so the bit says something.
        let (_payload2, spec2) = lone(b"data\n", 0o644);
        let dest2 = tempfile::tempdir().expect("temp dir");
        materialize(&store, &ingest(&store, &spec2)?, dest2.path())?;
        let mode = std::fs::metadata(dest2.path().join("program.sh"))
            .expect("the file")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, REGULAR_MODE);
        Ok(())
    }

    #[test]
    fn materializing_a_digest_the_store_lacks_names_the_digest() -> Result<()> {
        let (_dir, store) = store();
        let absent = hash_bytes(b"a payload nobody pushed");
        let dest = tempfile::tempdir().expect("temp dir");
        match materialize(&store, &absent, dest.path()) {
            Err(Error::MissingObject(named)) => assert_eq!(named, absent),
            other => panic!("expected a missing object, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn the_closure_is_the_manifest_and_every_file_it_names() -> Result<()> {
        // What a push has to advertise: a destination holding these can
        // install without asking for anything else.
        let (_dir, store) = store();
        let (_payload, spec) = tree();
        let digest = ingest(&store, &spec)?;
        let closure = closure(&store, &digest)?;
        assert_eq!(closure.len(), 3, "the manifest and its two files");
        assert_eq!(closure[0], digest, "the manifest leads");
        for object in &closure {
            assert!(store.has(object)?, "{object} is in the store");
        }
        // Materializing from a store holding exactly the closure works, which
        // is the property the push rests on.
        let elsewhere = tempfile::tempdir().expect("temp dir");
        let far = Store::open(elsewhere.path()).expect("open store");
        for object in &closure {
            far.put(&store.get(object)?)?;
        }
        let dest = tempfile::tempdir().expect("temp dir");
        materialize(&far, &digest, dest.path())?;
        Ok(())
    }

    #[test]
    fn the_manifest_names_every_object_the_payload_needs() -> Result<()> {
        // What a push has to advertise: the destination materializes from the
        // manifest, so a file object it lacks is a file it cannot write.
        let (_dir, store) = store();
        let (_payload, spec) = tree();
        let digest = ingest(&store, &spec)?;
        let manifest = manifest(&store, &digest);
        let objects: Vec<Hash> = manifest.objects().collect();
        assert_eq!(objects.len(), 2);
        for object in objects {
            assert!(store.has(&object)?, "{object} is in the store");
        }
        Ok(())
    }
}
