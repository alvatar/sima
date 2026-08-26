//! The SDK this binary vends: the package a registered program imports to
//! speak sima's protocol.
//!
//! A program is only self-contained down to the wire — everything above it, the
//! framing, the codecs, the model types, and the serve loop, is the SDK's. That
//! makes the SDK a dependency of every program written in the language it
//! serves, and a dependency the machine the program lands on has no way to
//! install: a migration carries the program's own files and nothing else.
//!
//! So the binary carries it. The package's source is embedded at build time and
//! the entry that needs it says so with `sdk = "python"`. A machine gets the
//! package one of two ways, and both leave the same shape of tree:
//!
//! - [`materialize`] — the machine writes its own copy out of its own binary,
//!   under `<config-dir>/.sima/sdk/python/`. This is what a load does for the
//!   program it is about to spawn here.
//! - [`ingest`](Sdk::ingest) and [`install`] — the package crosses as store
//!   objects and the receiving machine writes them out, keyed by digest. This
//!   is what a fleet machine gets, because the program there speaks the wire to
//!   the orchestrator and must import the orchestrator's build, not its own.
//!
//! ```text
//!   <root>/sdk/<name-or-digest>/
//!       .lock                    held while writing
//!       installed/sima/*.py      the package
//!       installed.digest         the digest of the package that was written
//! ```
//!
//! `installed/` is what goes on the interpreter's path, ahead of anything the
//! machine already has: the vended copy is the one that matches the protocol
//! the program is spoken to over, which is the point of vending it.
//!
//! The digest is over the embedded files alone, so one binary produces one
//! digest on any machine and an upgraded binary restamps exactly once. It is
//! the hash of the manifest [`ingest`](Sdk::ingest) stores, so a delivered
//! package has one name rather than two. The tree is shared by every entry
//! declaring the SDK, since what it holds is a property of the binary rather
//! than of any one program.

use std::path::{Path, PathBuf};

use sima_core::{Dec, Enc, Error, Hash, Result, hash_bytes};
use sima_store::Store;

use crate::config::GENERATED_DIR;
use crate::stamped_tree::{
    REGULAR_MODE, build_once, create_dir, remove_dir, validate_path, write_file,
};

/// The directory the vended SDKs hang off, under the directory the config file
/// itself sits in.
const SDK_DIR: &str = "sdk";
/// Where one SDK's package is written, and what goes on the interpreter's path.
const INSTALLED_DIR: &str = "installed";

/// The Python package, path by path, as this binary holds it. The paths are
/// what an interpreter looks for under the directory on its path.
const PYTHON_PACKAGE: [(&str, &str); 5] = [
    (
        "sima/__init__.py",
        include_str!("../../../python/sima/__init__.py"),
    ),
    (
        "sima/encode.py",
        include_str!("../../../python/sima/encode.py"),
    ),
    (
        "sima/frame.py",
        include_str!("../../../python/sima/frame.py"),
    ),
    (
        "sima/model.py",
        include_str!("../../../python/sima/model.py"),
    ),
    (
        "sima/serve.py",
        include_str!("../../../python/sima/serve.py"),
    ),
];

/// An SDK a program is written against, which is to say a language this binary
/// vends a package for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Sdk {
    /// The `sima` Python package, put on `PYTHONPATH`.
    Python,
}

impl Sdk {
    /// Every SDK this binary vends, which is what a value naming none is
    /// refused against.
    pub const ALL: [Sdk; 1] = [Sdk::Python];

    /// The SDK `value` names, and `None` for a value naming none.
    pub fn parse(value: &str) -> Option<Sdk> {
        Sdk::ALL.into_iter().find(|sdk| sdk.as_str() == value)
    }

    /// The name a config and the vend verb write.
    pub fn as_str(self) -> &'static str {
        match self {
            Sdk::Python => "python",
        }
    }

    /// Every name this binary vends, quoted, for a refusal to list.
    pub fn accepted() -> String {
        Sdk::ALL
            .iter()
            .map(|sdk| format!("{:?}", sdk.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The path-list variable the interpreter reads its modules from, which is
    /// what the vended directory is prepended to.
    pub(crate) fn path_variable(self) -> &'static str {
        match self {
            Sdk::Python => "PYTHONPATH",
        }
    }

    /// The package's files, each as a path under the vend destination and the
    /// text this binary holds for it.
    fn files(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Sdk::Python => &PYTHON_PACKAGE,
        }
    }

    /// The file an import resolves through, whose presence is what says the
    /// package a stamp claims is really there.
    fn entry_file(self) -> &'static str {
        match self {
            Sdk::Python => "sima/__init__.py",
        }
    }

    /// Writes this SDK's package under `dest`, so `dest` is what an interpreter
    /// puts on its path.
    ///
    /// Directories are created and files overwritten, so vending twice into one
    /// directory leaves what vending once leaves.
    pub fn vend(self, dest: &Path) -> Result<()> {
        for (path, text) in self.files() {
            let file = dest.join(path);
            create_dir(file.parent().ok_or_else(|| {
                Error::Validation(format!(
                    "the SDK cannot be written to {}, which names no directory",
                    dest.display()
                ))
            })?)?;
            write_file(&file, text.as_bytes(), REGULAR_MODE)?;
        }
        Ok(())
    }

    /// The digest of the package this binary holds: the hash of the manifest
    /// [`manifest`](Sdk::manifest) encodes, which names every file's path and
    /// the hash of its text.
    ///
    /// It is a property of the binary alone, so two machines running one build
    /// compute one digest and stamp one tree, and an upgraded binary restamps
    /// exactly once.
    pub(crate) fn digest(self) -> Hash {
        hash_bytes(&self.manifest())
    }

    /// The manifest of the package this binary holds: a `u32` count, then each
    /// file's path beside the hash of its text, in the order the package
    /// declares them.
    ///
    /// These bytes are what [`Sdk::ingest`] stores, so the manifest object's
    /// content address is the digest itself and the package has one name
    /// rather than two.
    fn manifest(self) -> Vec<u8> {
        let files = self.files();
        let mut enc = Enc::new();
        // A u32 count: the package is a fixed list in this binary, and the
        // count is here so a shorter package cannot encode as a prefix of a
        // longer one.
        enc.u32(files.len() as u32);
        for (path, text) in files {
            enc.str(path).hash(&hash_bytes(text.as_bytes()));
        }
        enc.finish()
    }

    /// Puts this binary's package into `store` — every file's bytes as an
    /// object, then the manifest naming them — and answers the manifest's
    /// digest, which is what a delivery sends and a stamp records.
    ///
    /// The package ships from the orchestrator's own build rather than from the
    /// destination's, because the program that imports it speaks the wire
    /// directly to the orchestrator: a machine vending its own copy could vend
    /// one built against another protocol. Ingesting is idempotent by content
    /// addressing, so a store that already holds the package gains nothing.
    pub(crate) fn ingest(self, store: &Store) -> Result<Hash> {
        for (_, text) in self.files() {
            store.put(text.as_bytes())?;
        }
        let digest = store.put(&self.manifest())?;
        // The manifest object addresses itself by the same value the stamp
        // carries; nothing downstream keeps a second identity in step.
        debug_assert_eq!(digest, self.digest());
        Ok(digest)
    }
}

/// The package a `digest` manifest names, decoded: each file's path and the
/// object holding its text, in the manifest's own order.
///
/// The paths come off the wire, so each is held to the rule a materialized path
/// follows — nothing that would mean a directory other than the tree it is
/// written into.
fn entries(store: &Store, digest: &Hash) -> Result<Vec<(String, Hash)>> {
    let bytes = store.get(digest)?;
    let mut dec = Dec::new(&bytes);
    let count = dec.u32()?;
    let mut entries = Vec::new();
    for _ in 0..count {
        let path = dec.str()?.to_string();
        validate_path(&path)?;
        let object = dec.hash()?;
        entries.push((path, object));
    }
    dec.finish()?;
    Ok(entries)
}

/// Where an interpreter reads the SDK from, under the tree at `root`. Stated
/// here so a caller naming the path a spawn puts on the module search path and
/// the install that fills it agree.
pub(crate) fn installed(root: &Path) -> PathBuf {
    root.join(INSTALLED_DIR)
}

/// Every file object the SDK `digest` names. With the manifest's own hash these
/// are the package's whole closure — what a delivery has to advertise for the
/// receiving machine to be able to install it.
pub(crate) fn objects(store: &Store, digest: &Hash) -> Result<Vec<Hash>> {
    Ok(entries(store, digest)?
        .into_iter()
        .map(|(_, object)| object)
        .collect())
}

/// Installs the SDK `digest` names from `store` into the tree at `root`, and
/// answers the directory an interpreter reads it from.
///
/// The counterpart of [`materialize`] for a machine that was sent the package
/// rather than one running the binary that holds it: the files come out of the
/// store instead of out of this build, and the stamp choreography is the same,
/// so a second delivery of one digest writes nothing.
pub(crate) fn install(store: &Store, digest: &Hash, root: &Path) -> Result<PathBuf> {
    let installed = installed(root);
    // The first listed file is what says the package a stamp claims is really
    // there; the manifest is read to learn which, so an empty tree under a
    // stamp is rebuilt rather than imported from.
    let complete = || match entries(store, digest) {
        Ok(entries) => Ok(entries
            .first()
            .is_some_and(|(path, _)| installed.join(path).is_file())),
        // A store that no longer holds the manifest cannot say what complete
        // means, so the tree is rebuilt — where the missing object is named.
        Err(_) => Ok(false),
    };
    build_once(root, digest, &complete, &|| {
        // A package left by another build may name modules this one does not,
        // and an import would find them.
        remove_dir(&installed)?;
        for (path, object) in entries(store, digest)? {
            let file = installed.join(&path);
            create_dir(file.parent().ok_or_else(|| {
                Error::Validation(format!(
                    "the SDK cannot be written to {}, which names no directory",
                    installed.display()
                ))
            })?)?;
            write_file(&file, &store.get(&object)?, REGULAR_MODE)?;
        }
        Ok(())
    })?;
    Ok(installed)
}

/// Materializes `sdk` under `config_dir` and answers the directory an
/// interpreter reads it from.
///
/// Every entry declaring the same SDK shares the tree, and the stamp is what
/// makes a load that has nothing to write read one file.
pub(crate) fn materialize(config_dir: &Path, sdk: Sdk) -> Result<PathBuf> {
    let root = config_dir
        .join(GENERATED_DIR)
        .join(SDK_DIR)
        .join(sdk.as_str());
    let installed = root.join(INSTALLED_DIR);
    build_once(
        &root,
        &sdk.digest(),
        &|| Ok(installed.join(sdk.entry_file()).is_file()),
        &|| {
            // A package left by another build may name modules this one does
            // not, and an import would find them.
            remove_dir(&installed)?;
            sdk.vend(&installed)
        },
    )?;
    Ok(installed)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// A store under a fresh temporary directory.
    fn store() -> (tempfile::TempDir, sima_store::Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = sima_store::Store::open(dir.path()).expect("open store");
        (dir, store)
    }

    #[test]
    fn the_ingested_manifest_is_the_digest_itself() -> Result<()> {
        // The one identity: the manifest object's content address is what
        // `digest` answers, so a delivery names the package by the same value
        // the stamp carries and there is no second name to keep in step.
        let (_dir, store) = store();
        assert_eq!(Sdk::Python.ingest(&store)?, Sdk::Python.digest());
        for (_, text) in PYTHON_PACKAGE {
            assert!(
                store.has(&hash_bytes(text.as_bytes()))?,
                "each file's bytes are an object of their own"
            );
        }
        Ok(())
    }

    #[test]
    fn ingesting_twice_moves_the_same_objects() -> Result<()> {
        // Content addressing, so a machine that already holds the package
        // holds it under the same digest and the sync moves nothing.
        let (_dir, store) = store();
        assert_eq!(Sdk::Python.ingest(&store)?, Sdk::Python.ingest(&store)?);
        Ok(())
    }

    #[test]
    fn an_installed_package_holds_what_this_binary_embeds() -> Result<()> {
        let (_dir, store) = store();
        let digest = Sdk::Python.ingest(&store)?;
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("sdk").join(digest.to_string());
        let installed = install(&store, &digest, &root)?;

        for (path, text) in PYTHON_PACKAGE {
            assert_eq!(
                std::fs::read_to_string(installed.join(path)).expect("the installed file"),
                text,
                "{path}"
            );
        }
        assert_eq!(
            std::fs::read_to_string(root.join("installed.digest")).expect("the stamp"),
            digest.to_string()
        );
        Ok(())
    }

    #[test]
    fn a_second_install_reads_the_stamp_and_writes_nothing() -> Result<()> {
        let (_dir, store) = store();
        let digest = Sdk::Python.ingest(&store)?;
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("tree");
        let installed = install(&store, &digest, &root)?;

        let untouched = installed.join("sima/marker");
        write_file(&untouched, b"left by the first install", REGULAR_MODE)?;
        assert_eq!(install(&store, &digest, &root)?, installed);
        assert!(untouched.is_file(), "the second install wrote nothing");
        Ok(())
    }

    #[test]
    fn an_sdk_the_store_never_received_fails_naming_the_digest() {
        // What a torn delivery leaves: the digest was named but its objects
        // never arrived, so the install says which package is missing rather
        // than leaving an empty tree behind.
        let (_dir, store) = store();
        let absent = hash_bytes(b"an SDK this store never received");
        let dir = tempfile::tempdir().expect("temp dir");
        let error = install(&store, &absent, &dir.path().join("tree"))
            .expect_err("a package with no objects");
        assert!(
            error.to_string().contains(&absent.to_string()),
            "{error}, which names the package that is missing"
        );
    }

    /// The `python/sima/` directory this crate's package is embedded from.
    fn package_source() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../python/sima")
            .canonicalize()
            .expect("the Python package is in the repository")
    }

    #[test]
    fn the_embedded_python_package_is_the_one_on_disk() {
        // The embed list is written by hand, so a file added to the package
        // without a line here would be missing from every machine the SDK is
        // vended to — and the program that imports it would fail there and
        // nowhere else.
        let on_disk: BTreeSet<String> = std::fs::read_dir(package_source())
            .expect("read the package directory")
            .map(|entry| entry.expect("a directory entry").file_name())
            .filter_map(|name| name.to_str().map(str::to_string))
            .filter(|name| name.ends_with(".py"))
            .map(|name| format!("sima/{name}"))
            .collect();
        let embedded: BTreeSet<String> = PYTHON_PACKAGE
            .iter()
            .map(|(path, _)| (*path).to_string())
            .collect();
        assert_eq!(
            embedded, on_disk,
            "PYTHON_PACKAGE names exactly the .py files under python/sima/"
        );
    }

    #[test]
    fn every_embedded_file_holds_what_the_file_on_disk_holds() {
        // The other half of the drift check: the names agreeing says nothing
        // about the bytes, and `include_str!` is what makes them agree.
        for (path, text) in PYTHON_PACKAGE {
            let name = path.strip_prefix("sima/").expect("a package path");
            let on_disk =
                std::fs::read_to_string(package_source().join(name)).expect("read the source file");
            assert_eq!(text, on_disk, "{path}");
        }
    }

    #[test]
    fn a_name_this_binary_vends_parses_and_any_other_does_not() {
        assert_eq!(Sdk::parse("python"), Some(Sdk::Python));
        assert_eq!(Sdk::parse("rust"), None);
        assert_eq!(Sdk::parse("Python"), None);
        assert_eq!(Sdk::parse(""), None);
        assert!(Sdk::accepted().contains("\"python\""));
    }

    #[test]
    fn the_digest_covers_every_file_s_path_and_text() {
        // What the stamp rests on: the digest is a property of the package
        // this binary holds, so it is the same on every machine running one
        // build and different on a build whose package differs.
        let package = Sdk::Python.digest();
        let mut enc = Enc::new();
        enc.u32(PYTHON_PACKAGE.len() as u32);
        for (path, text) in PYTHON_PACKAGE {
            enc.str(path).hash(&hash_bytes(text.as_bytes()));
        }
        assert_eq!(package, hash_bytes(&enc.finish()));

        // A changed text and a changed path are both changed digests.
        let mut changed = Enc::new();
        changed.u32(PYTHON_PACKAGE.len() as u32);
        for (path, text) in PYTHON_PACKAGE {
            changed
                .str(path)
                .hash(&hash_bytes(format!("{text}# edited\n").as_bytes()));
        }
        assert_ne!(package, hash_bytes(&changed.finish()));
    }

    #[test]
    fn vending_writes_the_package_where_an_interpreter_looks_for_it() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        Sdk::Python.vend(dir.path())?;
        for (path, text) in PYTHON_PACKAGE {
            assert_eq!(
                std::fs::read_to_string(dir.path().join(path)).expect("the vended file"),
                text,
                "{path}"
            );
        }
        // Twice into one directory leaves what once leaves: the verb is a
        // write, not an install.
        Sdk::Python.vend(dir.path())?;
        assert_eq!(
            std::fs::read_to_string(dir.path().join("sima/__init__.py")).expect("the vended file"),
            PYTHON_PACKAGE[0].1
        );
        Ok(())
    }

    #[test]
    fn a_config_directory_materializes_one_tree_however_often_it_is_loaded() -> Result<()> {
        // The stamp end to end: the second load reads it and writes nothing,
        // which a file left inside the tree is what proves.
        let dir = tempfile::tempdir().expect("temp dir");
        let installed = materialize(dir.path(), Sdk::Python)?;
        assert_eq!(installed, dir.path().join(".sima/sdk/python/installed"));
        assert!(installed.join("sima/__init__.py").is_file());
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".sima/sdk/python/installed.digest"))
                .expect("the stamp"),
            Sdk::Python.digest().to_string()
        );

        let untouched = installed.join("sima/marker");
        write_file(&untouched, b"left by the first load", REGULAR_MODE)?;
        assert_eq!(materialize(dir.path(), Sdk::Python)?, installed);
        assert!(
            untouched.is_file(),
            "the second load read the stamp and wrote nothing"
        );
        Ok(())
    }

    #[test]
    fn a_tree_left_by_another_build_is_replaced_rather_than_merged() -> Result<()> {
        // An upgraded binary restamps, and what it leaves is its own package:
        // a module the previous build carried would otherwise still import.
        let dir = tempfile::tempdir().expect("temp dir");
        let installed = materialize(dir.path(), Sdk::Python)?;
        let stale = installed.join("sima/removed.py");
        write_file(&stale, b"# a module an older build carried\n", REGULAR_MODE)?;
        // The stamp is what a differing build differs by, so writing another
        // one is what an upgrade looks like from this tree's side.
        write_file(
            &dir.path().join(".sima/sdk/python/installed.digest"),
            hash_bytes(b"another build").to_string().as_bytes(),
            REGULAR_MODE,
        )?;

        materialize(dir.path(), Sdk::Python)?;
        assert!(!stale.exists(), "the previous package was removed");
        assert!(installed.join("sima/__init__.py").is_file());
        Ok(())
    }
}
