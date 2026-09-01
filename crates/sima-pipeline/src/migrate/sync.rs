//! Store synchronization over a destination: the two halves of a `Store::sync`
//! session joined by a spawned process.
//!
//! The sync engine is symmetric — both sides advertise what they hold within a
//! task key set, compute `want = theirs − mine`, and stream the difference — so
//! a push and a pull are the same call at two moments:
//!
//! ```text
//!    push                                   pull
//!    local:  segments 0,1                   local:  segments 0,1
//!    far:    (empty)                        far:    segments 0..5
//!    ──────────────────────────             ──────────────────────────
//!    local advertises 0,1                   local advertises 0,1
//!    far advertises nothing                 far advertises 0..5
//!    far wants 0,1 → receives               local wants 2..5 → receives
//!    local wants nothing                    far wants nothing
//! ```
//!
//! **The key set is derived independently on each side** from (config, store
//! state), the same rule the scheduler's frontier follows. No key list crosses
//! the wire and the sync protocol is unchanged. It converges because whichever
//! side holds more also derives more keys, and it advertises the records the
//! other lacks.
//!
//! What differs between the two directions is the object scope, which is the
//! caller's to choose: a push names the frontier states, a pull advertises
//! everything its records reference. This module does not know which it is
//! serving.

use std::path::Path;

use sima_core::Result;
use sima_model::SearchId;
use sima_store::{ObjectScope, Store, SyncReport, SyncRole};
use sima_transport::{SpawnMode, SshDestination};

use crate::sync_session::sync_against;
use crate::task_keys::journaled_keys;

/// The far half: serves one sync session over `input` and `output`, against
/// the store at `store` and the search `search`.
///
/// **It addresses the store and the search directly, never a config.** A config
/// load resolves the `[domain.*]` entries, which installs and spawns the
/// program the search is served by — and on the destination of a migration that
/// program is what this very session is delivering. So the two values a config
/// would have given are passed instead: the initiator knows both, deriving the
/// search id locally and the store path from the search's own directory.
///
/// The key set therefore comes from the search's journal rather than from the
/// scheduler's derivation. It is the same set for this purpose: a record or a
/// checkpoint exists only for a task the search journaled, so every key with
/// state here is named there.
///
/// The search lock is held for the session's duration — a sync writes records and
/// objects, and the store admits one writer.
///
/// Nothing but protocol frames may reach `output`: the caller wires it to
/// stdout, and every diagnostic goes to stderr, which ssh keeps on its own
/// channel.
///
/// The scope is [`ObjectScope::Referenced`] in both directions, because this
/// side advertises what it holds and holds only what it was sent.
pub fn sync_serve(
    store: &Path,
    search: &SearchId,
    input: &mut dyn std::io::Read,
    output: &mut dyn std::io::Write,
) -> Result<SyncReport> {
    let store = Store::open(store)?;
    let _lock = store.acquire_search_lock(search)?;
    let keys = journaled_keys(&store, search)?;
    store.sync(
        &keys,
        ObjectScope::Referenced,
        input,
        output,
        SyncRole::Responder,
    )
}

/// The near half: spawns `sima sync-serve` on the destination and runs one
/// session against it.
///
/// One function serves both directions. The caller chooses the scope — a push
/// names the frontier states, a pull advertises everything its records
/// reference — and the key set is this side's own derivation, never the far
/// side's.
pub(crate) fn sync_over(
    store: &Store,
    keys: &[sima_model::TaskKey],
    scope: ObjectScope<'_>,
    reach: &Reach,
    far_store: &str,
    far_search: &SearchId,
) -> Result<SyncReport> {
    sync_against(
        store,
        keys,
        scope,
        &reach.sync_serve_argv(far_store, far_search),
    )
}

/// How a far-side command is reached: over ssh, or as a program on this
/// machine.
///
/// The distinction is [`SpawnMode`]'s, for a different program: a worker is
/// launched the same two ways, and the stub provider's testing path is a local
/// spawn in both cases, so every layer above exercises identically with no
/// network.
#[derive(Debug, Clone)]
pub(crate) enum Reach {
    /// Over ssh at this destination; the far side's own `sima` runs the verb.
    Ssh {
        /// Where the command lands.
        destination: SshDestination,
        /// The `sima` binary on that machine.
        binary: String,
    },
    /// A `sima` binary on this machine, for a destination reached without a
    /// hop.
    Here(std::path::PathBuf),
}

impl Reach {
    /// How a destination's `binary` is reached under `mode`.
    pub(crate) fn new(mode: &SpawnMode, destination: &SshDestination, binary: &str) -> Reach {
        match mode {
            SpawnMode::Ssh => Reach::Ssh {
                destination: destination.clone(),
                binary: binary.to_string(),
            },
            SpawnMode::Local(_) => Reach::Here(std::path::PathBuf::from(binary)),
        }
    }

    /// The argv that serves one sync session over the far side's `store` — a
    /// path there, travelling unresolved — and the search it holds.
    ///
    /// The verb addresses the store rather than a config: loading the config
    /// on the far side would spawn the program the session exists to deliver.
    pub(crate) fn sync_serve_argv(&self, far_store: &str, far_search: &SearchId) -> Vec<String> {
        self.verb_argv(&["sync-serve", far_store, "--search", &far_search.to_string()])
    }

    /// The argv that serves the live follow stream of the search `far_config`
    /// names.
    pub(crate) fn follow_serve_argv(&self, far_config: &str) -> Vec<String> {
        self.verb_argv(&["follow-serve", far_config])
    }

    /// The argv that reads the journal of the search `far_config` names once and
    /// exits, for a caller that wants what the far search ended as rather than a
    /// stream of what it is doing.
    pub(crate) fn follow_serve_once_argv(&self, far_config: &str) -> Vec<String> {
        self.verb_argv(&["follow-serve", far_config, "--once"])
    }

    /// The argv that runs a shell on the far side, reading its script from
    /// stdin.
    ///
    /// Feeding the script over stdin is what keeps the far-side operations free
    /// of quoting: an ssh command line is re-parsed by the far shell, so a
    /// script passed as an argument would have to survive two rounds of word
    /// splitting, while one arriving on stdin is read verbatim.
    pub(crate) fn shell_argv(&self) -> Vec<String> {
        match self {
            Reach::Ssh { destination, .. } => {
                let mut argv = destination.prefix();
                argv.push("sh".to_string());
                argv
            }
            Reach::Here(_) => vec!["/bin/sh".to_string()],
        }
    }

    /// The argv that runs one shell script while leaving stdin available for
    /// arbitrary bytes, as exec bootstrap requires.
    pub(crate) fn shell_script_argv(&self, script: &str) -> Vec<String> {
        match self {
            Reach::Ssh { destination, .. } => {
                let mut argv = destination.prefix();
                argv.extend(["sh".to_string(), "-c".to_string(), script.to_string()]);
                argv
            }
            Reach::Here(_) => vec!["/bin/sh".to_string(), "-c".to_string(), script.to_string()],
        }
    }

    /// The `sima` binary that drives the search on the far side.
    pub(crate) fn binary(&self) -> String {
        match self {
            Reach::Ssh { binary, .. } => binary.clone(),
            Reach::Here(binary) => binary.to_string_lossy().into_owned(),
        }
    }

    /// How the destination is named in an error: the ssh destination, or the
    /// binary standing in for a far side reached without a hop.
    pub(crate) fn label(&self) -> String {
        match self {
            Reach::Ssh { destination, .. } => destination.host().to_string(),
            Reach::Here(binary) => binary.to_string_lossy().into_owned(),
        }
    }

    /// The argv that runs `sima <args…>` on the destination.
    pub(crate) fn verb_argv(&self, args: &[&str]) -> Vec<String> {
        let mut argv = match self {
            Reach::Ssh {
                destination,
                binary,
            } => {
                let mut argv = destination.prefix();
                argv.push(binary.clone());
                argv
            }
            Reach::Here(binary) => vec![binary.to_string_lossy().into_owned()],
        };
        argv.extend(args.iter().map(|arg| (*arg).to_string()));
        argv
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The search every argv test addresses.
    fn search() -> SearchId {
        SearchId::from_hash(sima_core::hash_bytes(b"a migrated search"))
    }

    #[test]
    fn a_local_reach_runs_the_binary_directly() {
        let reach = Reach::new(
            &SpawnMode::Local("/tmp/sima".into()),
            &SshDestination::known("unused"),
            "/build/sima",
        );
        assert_eq!(
            reach.sync_serve_argv("far/store", &search()),
            [
                "/build/sima",
                "sync-serve",
                "far/store",
                "--search",
                &search().to_string(),
            ]
        );
    }

    #[test]
    fn an_ssh_reach_wraps_the_far_binary_in_the_destination_s_invocation() {
        let reach = Reach::new(
            &SpawnMode::Ssh,
            &SshDestination::rented("203.0.113.7", 41022, "root"),
            "sima",
        );
        assert_eq!(
            reach.sync_serve_argv("~/sima/abc/store", &search()),
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                "-o",
                "ConnectTimeout=10",
                "-p",
                "41022",
                "root@203.0.113.7",
                "--",
                "sima",
                "sync-serve",
                "~/sima/abc/store",
                "--search",
                &search().to_string(),
            ]
        );
    }

    #[test]
    fn the_far_store_path_travels_unresolved() {
        // It names a path on the far side, and the far side is what interprets
        // it: a tilde is the far shell's to expand.
        let reach = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        let argv = reach.sync_serve_argv("~/sima/abc/store", &search());
        assert!(argv.contains(&"~/sima/abc/store".to_string()), "{argv:?}");
    }

    #[test]
    fn the_sync_verb_addresses_a_store_and_a_search_rather_than_a_config() {
        // Loading a config on the far side resolves its `[domain.*]` entries,
        // which spawns the program a session may be there to deliver. The two
        // values a config would have given travel instead.
        let reach = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        let argv = reach.sync_serve_argv("~/sima/abc/store", &search());
        let verb = argv
            .iter()
            .position(|arg| arg == "sync-serve")
            .expect("the verb");
        assert_eq!(
            &argv[verb..],
            [
                "sync-serve",
                "~/sima/abc/store",
                "--search",
                &search().to_string(),
            ]
        );
        assert!(
            !argv.iter().any(|arg| arg.ends_with(".toml")),
            "no config travels: {argv:?}"
        );
    }

    #[test]
    fn a_shell_argv_reaches_a_far_shell_that_reads_its_script_from_stdin() {
        // No script on the command line, in either form: the far-side
        // operations write theirs to the shell's stdin, so nothing is quoted
        // for the hop.
        let over_ssh = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        assert_eq!(
            over_ssh.shell_argv(),
            ["ssh", "-o", "BatchMode=yes", "gpubox", "--", "sh"]
        );
        let here = Reach::new(
            &SpawnMode::Local("/tmp/sima-worker".into()),
            &SshDestination::known("unused"),
            "/build/sima",
        );
        assert_eq!(here.shell_argv(), ["/bin/sh"]);
    }

    #[test]
    fn the_binary_and_the_label_read_the_same_two_forms() {
        let over_ssh = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        assert_eq!(over_ssh.binary(), "sima");
        assert_eq!(over_ssh.label(), "gpubox");
        let here = Reach::new(
            &SpawnMode::Local("/tmp/sima-worker".into()),
            &SshDestination::known("unused"),
            "/build/sima",
        );
        assert_eq!(here.binary(), "/build/sima");
        // A far side reached without a hop has no destination to name, so its
        // binary is what an error identifies it by.
        assert_eq!(here.label(), "/build/sima");
    }

    #[test]
    fn a_follow_argv_serves_the_search_the_far_config_names() {
        let reach = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        let argv = reach.follow_serve_argv("~/sima/abc/sima.toml");
        let binary = argv.iter().position(|a| a == "sima").expect("the binary");
        assert_eq!(
            &argv[binary..],
            ["sima", "follow-serve", "~/sima/abc/sima.toml"]
        );
    }

    #[test]
    fn a_one_shot_follow_argv_asks_the_far_side_for_the_journal_and_an_exit() {
        // What a recall reads the far search's final state over: the same verb the
        // live follow uses, told to write the journal once and stop.
        let reach = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        let argv = reach.follow_serve_once_argv("~/sima/abc/sima.toml");
        let binary = argv.iter().position(|a| a == "sima").expect("the binary");
        assert_eq!(
            &argv[binary..],
            ["sima", "follow-serve", "~/sima/abc/sima.toml", "--once"]
        );
    }

    #[test]
    fn a_verb_argv_carries_every_argument_after_the_binary() {
        let reach = Reach::new(&SpawnMode::Ssh, &SshDestination::known("gpubox"), "sima");
        let argv = reach.verb_argv(&["search", "sima.toml"]);
        let binary = argv.iter().position(|a| a == "sima").expect("the binary");
        assert_eq!(&argv[binary..], ["sima", "search", "sima.toml"]);
    }
}
