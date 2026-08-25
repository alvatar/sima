//! Getting a registered program onto the machines a run puts work on.
//!
//! A `[domain.*]` entry routes the run's format to a program on the
//! orchestrator. A fleet machine has no such program, so before it can serve a
//! worker the program has to be there — the payload's objects delivered, the
//! install run, the tree stamped.
//!
//! The delivery is the far half of a store sync plus an install, which is why
//! it needs no verb of its own: `sima sync-serve` gains a second form.
//!
//! ```text
//!  orchestrator                              machine
//!  ────────────                              ───────
//!  ingest payload closure    ─┐
//!  ingest SDK objects         │              <root>/programs/
//!                             │              ├── store/          objects land here
//!  sima sync-serve <dir>      └────────────► ├── <payload>/       program tree
//!    --payload D --sdk S                     │     payload/
//!    (Store::sync initiator,                 │     installed/program
//!     ObjectScope::Named)                    │     installed.digest = D
//!                                            └── sdk/<S>/installed/
//! ```
//!
//! Two properties make repeat delivery cheap rather than merely correct. The
//! store under `<dir>` is shared across runs, so the sync's own have/want
//! negotiation moves an unchanged program's bytes once, ever. And both trees
//! are built through [`crate::stamped_tree`], so a machine that already holds a
//! digest runs no install and several runs delivering at once build one tree
//! between them.
//!
//! The SDK ships from the orchestrator's build rather than from the machine's
//! own binary. The program on that machine speaks the wire directly to the
//! orchestrator — frames tunnel through ssh and the container runtime untouched
//! — so the package it imports must match the orchestrator's protocol, and a
//! machine vending its own could vend one built against another.

use std::path::{Path, PathBuf};

use sima_core::{Error, Hash, Result};
use sima_store::{ObjectScope, Store, SyncReport};

use crate::config::LoadedConfig;
use crate::payload::{self, ProgramTree};
use crate::sdk;
use crate::sync_session::sync_against;

/// The store the delivered objects land in, under the delivery directory.
/// Shared across every run that delivers to this machine.
const STORE_DIR: &str = "store";
/// The directory the SDK trees hang off, keyed by SDK digest below it. A digest
/// directory is 64 hex characters, so this name cannot collide with one.
const SDK_DIR: &str = "sdk";

/// What a run sends a machine so that machine can serve the run's format: the
/// program's payload, and the SDK the program imports when its entry declares
/// one.
///
/// Both are content addresses of objects in the run's own store, so what
/// crosses the wire is decided by what the machine already holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgramDelivery {
    /// The payload manifest: the digest every worker of this run answers at its
    /// handshake.
    payload: Hash,
    /// The SDK manifest, for an entry declaring one.
    sdk: Option<Hash>,
}

impl ProgramDelivery {
    /// The payload digest this delivery installs, which is what the run expects
    /// back from every worker it spawns on a machine that received it.
    pub fn payload(&self) -> &Hash {
        &self.payload
    }

    /// The arguments naming this delivery to the far half, appended to a `sima`
    /// invocation: `sync-serve <dir> --payload <D> [--sdk <S>]`.
    ///
    /// `dir` is a path on the receiving machine, travelling unresolved — a
    /// tilde is that machine's shell's to expand.
    pub fn args(&self, dir: &str) -> Vec<String> {
        let mut args = vec![
            "sync-serve".to_string(),
            dir.to_string(),
            "--payload".to_string(),
            self.payload.to_string(),
        ];
        if let Some(sdk) = &self.sdk {
            args.extend(["--sdk".to_string(), sdk.to_string()]);
        }
        args
    }

    /// Sends this delivery to the far half `argv` spawns, and answers what the
    /// session moved.
    ///
    /// The key set is empty and the scope is [`ObjectScope::Named`] over the
    /// closure: no task record names a program, so the objects are advertised
    /// by hand and nothing else about the run crosses.
    pub fn send(&self, store: &Store, argv: &[String]) -> Result<SyncReport> {
        let closure = self.closure(store)?;
        sync_against(store, &[], ObjectScope::Named(&closure), argv)
    }

    /// Every object a machine needs before it can install what this delivery
    /// names: the payload's whole closure, and the SDK's manifest with the
    /// files it names.
    fn closure(&self, store: &Store) -> Result<Vec<Hash>> {
        let mut closure = payload::closure(store, &self.payload)?;
        if let Some(digest) = &self.sdk {
            closure.push(*digest);
            closure.extend(sdk::objects(store, digest)?);
        }
        Ok(closure)
    }
}

/// Ingests what `config`'s run sends its machines into `store`, and answers it.
///
/// `None` for a run whose format this build carries: every machine's own worker
/// answers for it, so nothing travels.
///
/// A routed format whose entry declares neither `payload` nor `payload_digest`
/// is refused here, naming the format and the key. Such an entry says the
/// program stays on the machine it is installed on, and a machine that never
/// receives it cannot serve a worker for the run — so the refusal comes before
/// any machine is contacted.
///
/// Ingesting is idempotent by content addressing: a config carrying
/// `payload_digest` was written by a migration whose sync already delivered
/// these objects, so the ingest re-derives the same digest over objects the
/// store holds.
pub fn ingest_program(config: &LoadedConfig, store: &Store) -> Result<Option<ProgramDelivery>> {
    let format = &config.run.format;
    let Some(routed) = config.domains.routed(format) else {
        return Ok(None);
    };
    let payload = match (routed.payload, routed.payload_digest) {
        (Some(spec), _) => payload::ingest(store, spec)?,
        // A migrated config states the digest without the files: the tree on
        // this machine was installed from objects the migration delivered, and
        // those objects are what this store holds.
        (None, Some(digest)) => *digest,
        (None, None) => {
            return Err(Error::Validation(format!(
                "the program declared for format {:?} cannot reach another machine: \
                 its entry names no `payload`, so there is nothing to send",
                format.as_str()
            )));
        }
    };
    let sdk = routed.sdk.map(|sdk| sdk.ingest(store)).transpose()?;
    Ok(Some(ProgramDelivery { payload, sdk }))
}

/// Receives one delivery into `dir` and installs what it names: the program
/// tree at `<dir>/<payload>`, and the SDK tree at `<dir>/sdk/<sdk>`.
///
/// The far half of `sima sync-serve <dir> --payload <D> [--sdk <S>]`. Nothing
/// but protocol frames may reach `output`: the caller wires it to stdout, and
/// every diagnostic goes to stderr, which ssh keeps on its own channel.
///
/// It addresses a directory and two digests rather than a config, for the
/// reason the `--run` form does: loading a config resolves its `[domain.*]`
/// entries, which spawns the very program this session is delivering.
pub fn receive_program(
    dir: &Path,
    payload: &Hash,
    sdk: Option<&Hash>,
    input: &mut dyn std::io::Read,
    output: &mut dyn std::io::Write,
) -> Result<SyncReport> {
    let store = Store::open(dir.join(STORE_DIR))?;
    // What this side advertises is what it already holds of the two digests
    // named, so a repeat delivery negotiates down to nothing. What it receives
    // is the initiator's whole closure regardless: a want is over what the peer
    // advertised, not over this scope.
    let named: Vec<Hash> = std::iter::once(*payload).chain(sdk.copied()).collect();
    let report = store.sync(
        &[],
        ObjectScope::Named(&named),
        input,
        output,
        sima_store::SyncRole::Responder,
    )?;
    payload::install(&store, payload, &program_tree(dir, payload))?;
    if let Some(sdk) = sdk {
        sdk::install(&store, sdk, &sdk_tree(dir, sdk))?;
    }
    Ok(report)
}

/// The program tree the payload `digest` names installs into, under the
/// delivery directory `dir`.
pub(crate) fn program_tree(dir: &Path, digest: &Hash) -> ProgramTree {
    ProgramTree::at(dir.join(digest.to_string()))
}

/// The SDK tree the `digest` names installs into, under the delivery directory
/// `dir`.
pub(crate) fn sdk_tree(dir: &Path, digest: &Hash) -> PathBuf {
    dir.join(SDK_DIR).join(digest.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A digest that names nothing, for the path-shape assertions.
    fn digest(text: &str) -> Hash {
        sima_core::hash_bytes(text.as_bytes())
    }

    #[test]
    fn the_arguments_name_the_directory_and_the_digests() {
        let delivery = ProgramDelivery {
            payload: digest("a program"),
            sdk: Some(digest("a package")),
        };
        assert_eq!(
            delivery.args("~/sima-runs/programs"),
            [
                "sync-serve",
                "~/sima-runs/programs",
                "--payload",
                &delivery.payload.to_string(),
                "--sdk",
                &delivery.sdk.expect("an SDK").to_string(),
            ]
        );
    }

    #[test]
    fn an_entry_declaring_no_sdk_names_none() {
        let delivery = ProgramDelivery {
            payload: digest("a program"),
            sdk: None,
        };
        let args = delivery.args("programs");
        assert!(!args.iter().any(|arg| arg == "--sdk"), "{args:?}");
    }

    #[test]
    fn the_two_trees_and_the_store_cannot_collide() {
        // Digest directories are 64 hex characters, so neither reserved name is
        // one; a delivery of any digest lands beside them rather than on them.
        let dir = Path::new("/srv/programs");
        let payload = digest("a program");
        assert_eq!(
            program_tree(dir, &payload).entry_point(),
            dir.join(payload.to_string())
                .join("installed")
                .join("program")
        );
        assert_eq!(
            sdk_tree(dir, &payload),
            dir.join(SDK_DIR).join(payload.to_string())
        );
        assert_eq!(payload.to_string().len(), 64);
        for reserved in [STORE_DIR, SDK_DIR] {
            assert_ne!(reserved.len(), 64);
        }
    }
}
