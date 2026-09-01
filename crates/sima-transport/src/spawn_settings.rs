//! [`SpawnSettings`]: what every worker spawn of one pool shares.

use std::num::NonZeroU64;
use std::time::Duration;

use sima_model::FormatId;

use crate::protocol::Hello;
use crate::spawn_policy::SpawnPolicy;

/// What every worker spawn of one pool shares: the surface the child is
/// spawned with, how long its handshake answer is awaited, and the handshake
/// frame it receives.
///
/// One value carried by each transport, so a bare local child, a container
/// client, and an ssh client are spawned and greeted alike.
#[derive(Debug, Clone)]
pub struct SpawnSettings {
    /// The environment and working directory the child receives.
    pub(crate) policy: SpawnPolicy,
    /// How long the spawn waits for `Ready`. [`Duration::MAX`] waits for as
    /// long as the child lives.
    pub(crate) answer_timeout: Duration,
    /// The search's settings, with the worker id and device left unbound: they
    /// vary per worker, so each spawn sets them on a copy of this frame.
    pub(crate) hello: Hello,
    /// The digest of the program this search sent to the machine these workers
    /// run on, which each of them answers back at the handshake; `None` for a
    /// format this build answers in process, where no program travelled.
    pub(crate) program_digest: Option<String>,
}

impl SpawnSettings {
    /// The settings a search over `format` spawns its workers under, with the
    /// given checkpoint cadence ([`Duration::MAX`] and `None` disable an
    /// axis) and the given deadline on the handshake answer.
    pub fn new(
        policy: SpawnPolicy,
        answer_timeout: Duration,
        format: FormatId,
        checkpoint_interval: Duration,
        checkpoint_interval_steps: Option<NonZeroU64>,
    ) -> SpawnSettings {
        SpawnSettings {
            policy,
            answer_timeout,
            hello: Hello::for_search(format, checkpoint_interval, checkpoint_interval_steps),
            program_digest: None,
        }
    }

    /// The same settings expecting `digest` from every worker's handshake.
    ///
    /// `Some` names the program this search sent to the machine the workers run
    /// on, so a worker answering anything else fails its spawn; `None` expects
    /// none, and a worker naming a program fails its spawn just the same. What
    /// the digest identifies is the caller's business — this side compares.
    pub fn expecting_program(self, digest: Option<String>) -> SpawnSettings {
        SpawnSettings {
            program_digest: digest,
            ..self
        }
    }
}
