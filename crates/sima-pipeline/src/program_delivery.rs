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
use sima_transport::container::{ContainerRun, once_argv};
use sima_transport::protocol::PROGRAM_DIGEST_VAR;

use crate::config::LoadedConfig;
use crate::fleet::OwnedMachine;
use crate::payload::{self, ProgramTree};
use crate::sdk::{self, Sdk};
use crate::sync_session::sync_against;

/// The `sima` binary inside a worker image, by its name on the `PATH` there.
/// A delivery's far half runs there rather than on the machine itself, so the
/// install builds in the environment the program will run in.
const IMAGE_BINARY: &str = "sima";
/// The directory a machine's delivered programs hang off, under the `root` its
/// entry names. Every run delivering to that machine shares it, which is what
/// makes an unchanged program cross the wire once.
const PROGRAMS_DIR: &str = "programs";
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramDelivery {
    /// The payload manifest: the digest every worker of this run answers at its
    /// handshake.
    payload: Hash,
    /// The SDK the entry declared and the manifest it ingested to, for an entry
    /// declaring one. The two travel together because the receiving machine
    /// needs both: the digest names the tree, and the language names the path
    /// variable that tree leads.
    sdk: Option<(Sdk, Hash)>,
    /// The variable names the entry declared. They cross as names alone — each
    /// value is read on the machine the program runs on — so a credential never
    /// reaches a command line or the wire.
    env: Vec<String>,
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
        if let Some((_, digest)) = &self.sdk {
            args.extend(["--sdk".to_string(), digest.to_string()]);
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

    /// What a container on a machine rooted at `root` runs to reach this
    /// program, with `args` handed to it.
    ///
    /// Three things make the container's program the same program it would be
    /// here:
    ///
    /// - the delivery directory is mounted at the identical path on both sides,
    ///   so the stamp the install wrote names the same file the shell reads;
    /// - the declared variables are forwarded by name, so each value is the
    ///   machine's own and none appears on a command line;
    /// - the shell states exactly what the local `Explicit` policy would — the
    ///   digest assigned, the SDK's directory leading the interpreter's path —
    ///   before it execs the program.
    ///
    /// The digest is read from the machine's own stamp at exec time rather than
    /// written in, so what the worker answers at its handshake is that disk's
    /// claim about what is installed there, and the run's comparison is against
    /// a value it never wrote.
    ///
    /// Paths reach the shell unquoted, so a `root` naming `~` expands there —
    /// the rule every far-side path in a config follows.
    pub(crate) fn container_run(&self, root: &str, args: &[&str]) -> ContainerRun {
        let programs = programs_dir(root);
        let tree = program_tree(Path::new(&programs), &self.payload);
        let mut script = vec![format!(
            "{PROGRAM_DIGEST_VAR}=$(cat {})",
            tree.stamp().display()
        )];
        let mut exported = vec![PROGRAM_DIGEST_VAR.to_string()];
        if let Some((sdk, digest)) = &self.sdk {
            // Leading, not replacing: the vended package is the one matching
            // the protocol this run speaks, and whatever the machine already
            // has still resolves behind it.
            let variable = sdk.path_variable();
            let installed = sdk::installed(&sdk_tree(Path::new(&programs), digest));
            script.push(format!(
                "{variable}={}${{{variable}:+:${variable}}}",
                installed.display()
            ));
            exported.push(variable.to_string());
        }
        script.push(format!("export {}", exported.join(" ")));
        script.push(format!(
            "exec {}{}",
            tree.entry_point().display(),
            args.iter().map(|arg| format!(" {arg}")).collect::<String>()
        ));
        ContainerRun::program(
            vec![format!("{programs}:{programs}")],
            self.env.clone(),
            vec!["sh".to_string(), "-c".to_string(), script.join("\n")],
        )
    }

    /// Every object a machine needs before it can install what this delivery
    /// names: the payload's whole closure, and the SDK's manifest with the
    /// files it names.
    fn closure(&self, store: &Store) -> Result<Vec<Hash>> {
        let mut closure = payload::closure(store, &self.payload)?;
        if let Some((_, digest)) = &self.sdk {
            closure.push(*digest);
            closure.extend(sdk::objects(store, digest)?);
        }
        Ok(closure)
    }
}

/// Where a machine whose entry names `root` keeps the programs runs deliver to
/// it.
///
/// The path travels unresolved — a tilde is the receiving machine's shell's to
/// expand — so it is composed as text rather than as a [`Path`], which would
/// resolve it against this machine.
pub(crate) fn programs_dir(root: &str) -> String {
    format!("{}/{PROGRAMS_DIR}", root.trim_end_matches('/'))
}

/// Refuses a run whose format is a program that cannot travel.
///
/// Such an entry declares neither `payload` nor `payload_digest`, which is what
/// says the program stays on the machine it is installed on. A fleet machine
/// would never receive it and could serve no worker for the run, and no
/// machine's answer could change that — so a run that engages one is refused
/// before any machine is contacted.
pub(crate) fn sendable(config: &LoadedConfig) -> Result<()> {
    let format = &config.run.format;
    let Some(routed) = config.domains.routed(format) else {
        return Ok(());
    };
    if routed.payload.is_none() && routed.payload_digest.is_none() {
        return Err(Error::Validation(format!(
            "the program declared for format {:?} cannot reach another machine: its \
             [domain] entry names no `payload`, so a machine of the fleet never \
             receives it and can serve no worker for this run",
            format.as_str()
        )));
    }
    Ok(())
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
    sendable(config)?;
    let Some(routed) = config.domains.routed(&config.run.format) else {
        return Ok(None);
    };
    let payload = match (routed.payload, routed.payload_digest) {
        (Some(spec), _) => payload::ingest(store, spec)?,
        // A migrated config states the digest without the files: the tree on
        // this machine was installed from objects the migration delivered, and
        // those objects are what this store holds.
        (None, Some(digest)) => *digest,
        // `sendable` above refused this entry.
        (None, None) => unreachable!("an entry with nothing to send is refused"),
    };
    let sdk = routed
        .sdk
        .map(|sdk| sdk.ingest(store).map(|digest| (sdk, digest)))
        .transpose()?;
    Ok(Some(ProgramDelivery {
        payload,
        sdk,
        env: routed.env.to_vec(),
    }))
}

/// Delivers `delivery` to every machine of yours the fleet drew in, so each
/// holds the program before its pool is constructed.
///
/// The far half runs in the image the machine's workers run in, with the
/// delivery directory bind-mounted at the identical path on both sides. Both
/// follow from what an install is: a script that builds the program has to
/// build it in the environment the program will run in, and the stamp it writes
/// has to name the same file to the spawn that reads it later.
///
/// A machine that cannot be delivered to fails the run, naming it. It was
/// declared as a place this run executes, and without the program it can serve
/// no worker.
pub(crate) fn deliver_to_owned(
    machines: &[OwnedMachine<'_>],
    store: &Store,
    delivery: &ProgramDelivery,
) -> Result<()> {
    for machine in machines {
        let programs = programs_dir(machine.root);
        let mut command = vec![IMAGE_BINARY.to_string()];
        command.extend(delivery.args(&programs));
        let argv = once_argv(
            Some(machine.ssh),
            &machine.container.runtime,
            &machine.container.image,
            &machine.container.run_args,
            // The delivery itself forwards nothing: it runs sima's own verb,
            // and what the program needs to see is stated where the program
            // runs.
            &ContainerRun::program(vec![format!("{programs}:{programs}")], Vec::new(), command),
        );
        delivery.send(store, &argv).map_err(|e| {
            Error::Transport(format!(
                "cannot deliver the program to {:?}: {e}",
                machine.ssh
            ))
        })?;
    }
    Ok(())
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

    /// A delivery of one program, with an SDK when `sdk` says so and the given
    /// declared variable names.
    fn delivery(sdk: bool, env: &[&str]) -> ProgramDelivery {
        ProgramDelivery {
            payload: digest("a program"),
            sdk: sdk.then(|| (Sdk::Python, digest("a package"))),
            env: env.iter().map(|name| (*name).to_string()).collect(),
        }
    }

    #[test]
    fn the_arguments_name_the_directory_and_the_digests() {
        let delivery = delivery(true, &[]);
        let (_, sdk) = delivery.sdk.expect("an SDK");
        assert_eq!(
            delivery.args("~/sima-runs/programs"),
            [
                "sync-serve",
                "~/sima-runs/programs",
                "--payload",
                &delivery.payload.to_string(),
                "--sdk",
                &sdk.to_string(),
            ]
        );
    }

    #[test]
    fn an_entry_declaring_no_sdk_names_none() {
        let args = delivery(false, &[]).args("programs");
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

    #[test]
    fn the_programs_directory_hangs_off_the_root_however_it_was_written() {
        assert_eq!(programs_dir("~/sima-runs"), "~/sima-runs/programs");
        assert_eq!(programs_dir("/srv/sima/"), "/srv/sima/programs");
    }

    #[test]
    fn a_worker_reads_the_machine_s_own_stamp_before_it_execs_the_program() {
        // The digest is read where the program runs, not written in from here,
        // so what the worker answers is that disk's claim about what is
        // installed on it.
        let delivery = delivery(false, &[]);
        let run = delivery.container_run("~/sima-runs", &[]);
        let tree = format!("~/sima-runs/programs/{}", delivery.payload);
        assert_eq!(
            run,
            ContainerRun::program(
                vec!["~/sima-runs/programs:~/sima-runs/programs".to_string()],
                Vec::new(),
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!(
                        "SIMA_PROGRAM_DIGEST=$(cat {tree}/installed.digest)\n\
                         export SIMA_PROGRAM_DIGEST\n\
                         exec {tree}/installed/program"
                    ),
                ],
            )
        );
    }

    #[test]
    fn an_entry_declaring_an_sdk_leads_the_module_path_with_it() {
        // Leading rather than replacing: the delivered package matches the
        // protocol this run speaks, and whatever the machine already installed
        // still resolves behind it.
        let delivery = delivery(true, &[]);
        let (_, sdk) = delivery.sdk.expect("an SDK");
        let run = delivery.container_run("/srv", &[]);
        let script = format!("{run:?}");
        assert!(
            script.contains(&format!(
                "PYTHONPATH=/srv/programs/sdk/{sdk}/installed${{PYTHONPATH:+:$PYTHONPATH}}"
            )),
            "{script}"
        );
        assert!(
            script.contains("export SIMA_PROGRAM_DIGEST PYTHONPATH"),
            "{script}"
        );
    }

    #[test]
    fn the_declared_variables_are_forwarded_by_name_and_the_arguments_reach_the_program() {
        let delivery = delivery(false, &["HF_TOKEN", "CACHE_DIR"]);
        let run = delivery.container_run("/srv", &["--serve-domain", "stub.v1"]);
        assert_eq!(
            run,
            ContainerRun::program(
                vec!["/srv/programs:/srv/programs".to_string()],
                vec!["HF_TOKEN".to_string(), "CACHE_DIR".to_string()],
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!(
                        "SIMA_PROGRAM_DIGEST=$(cat /srv/programs/{payload}/installed.digest)\n\
                         export SIMA_PROGRAM_DIGEST\n\
                         exec /srv/programs/{payload}/installed/program --serve-domain stub.v1",
                        payload = delivery.payload
                    ),
                ],
            )
        );
    }
}
