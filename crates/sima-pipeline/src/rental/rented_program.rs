//! [`RentedProgram`]: what a rented machine runs for a run, and everything that
//! follows from it.
//!
//! A rented machine is asked four things between being acquired and serving its
//! first task: is it up, can it be given what the run needs, what devices can
//! the run's work go on there, and how is a worker started. All four answers
//! differ by whether the run's format is one the machine's image carries or a
//! program that has to reach it first, so they are answered together here
//! rather than as four flags threaded through the acquisition.

use std::time::Duration;

use sima_core::Result;
use sima_domains::devices::DeviceInfo;
use sima_model::FormatId;
use sima_scheduler::ExecutionConfig;
use sima_store::Store;
use sima_transport::domain_service::DomainService;
use sima_transport::serve::serve_domain_args;
use sima_transport::{
    DeviceProbe, RemoteCommand, SpawnMode, SpawnPolicy, SpawnSettings, SshDestination,
};

use crate::program_delivery::{ProgramDelivery, programs_dir};

/// What a rented machine runs for one run.
///
/// The image's own worker for a format this build carries: nothing is delivered
/// there, its readiness probe asks about the format, and its workers answer no
/// program. A delivered program otherwise: the machine is only up when it
/// answers at all, the program reaches it before any slot is derived, and every
/// worker there answers the digest that machine's stamp carries.
pub(crate) enum RentedProgram<'a> {
    /// The image answers for the run's format itself.
    Image,
    /// A program delivered to the machine before it serves anything.
    Delivered {
        /// What the run sends.
        delivery: &'a ProgramDelivery,
        /// The `sima` on that machine, which runs the delivery's far half.
        binary: &'a str,
        /// Where that machine keeps what runs deliver to it.
        root: &'a str,
    },
}

impl RentedProgram<'_> {
    /// What the readiness probe asks the machine.
    ///
    /// A format the image carries is what its worker is asked about, and the
    /// answer is also where the machine's slots come from. A format that is a
    /// program cannot be resolved by that worker at all, so the probe asks
    /// about no format and its answer states only that the machine is up.
    pub(crate) fn readiness<'f>(&self, format: &'f FormatId) -> DeviceProbe<'f> {
        match self {
            RentedProgram::Image => DeviceProbe::Format(format),
            RentedProgram::Delivered { .. } => DeviceProbe::EveryBackend,
        }
    }

    /// Puts what the run needs on the machine, once it has answered.
    ///
    /// Nothing for a format the image carries. Otherwise the payload's objects
    /// and the SDK's, followed by the install the far half runs — after which
    /// the machine holds a stamped tree its workers are spawned out of.
    pub(crate) fn install(
        &self,
        store: &Store,
        mode: &SpawnMode,
        target: &SshDestination,
    ) -> Result<()> {
        let RentedProgram::Delivered {
            delivery,
            binary,
            root,
        } = self
        else {
            return Ok(());
        };
        // ssh lands inside the machine's own container, so the far half is that
        // machine's `sima` rather than an image's; reached without a hop, the
        // machine is this one and the binary is a path here.
        let mut argv = match mode {
            SpawnMode::Ssh => target.prefix(),
            SpawnMode::Local(_) => Vec::new(),
        };
        argv.push((*binary).to_string());
        argv.extend(delivery.args(&programs_dir(root)));
        delivery.send(store, &argv)?;
        Ok(())
    }

    /// The devices this run's work can be placed on there.
    ///
    /// The image's worker already answered for a format it carries, so its
    /// enumeration is reused. A delivered program is asked itself, over the
    /// domain service it answers on that machine: only the program knows which
    /// devices its own backend opens.
    pub(crate) fn devices(
        &self,
        answered: Vec<DeviceInfo>,
        mode: &SpawnMode,
        target: &SshDestination,
        format: &FormatId,
        answer_timeout: Duration,
    ) -> Result<Vec<DeviceInfo>> {
        let RentedProgram::Delivered { delivery, root, .. } = self else {
            return Ok(answered);
        };
        let role = serve_domain_args(format);
        // The session ends with this expression: its drop says goodbye, closes
        // the pipe, and reaps whatever the spawn started.
        match mode {
            SpawnMode::Ssh => {
                let argv = delivery
                    .ssh_command(root, &[&role[0], &role[1]])
                    .argv(target);
                DomainService::spawn_argv(&argv, answer_timeout)?.enumerate_devices(format)
            }
            SpawnMode::Local(_) => {
                let (binary, policy) = delivery.local_spawn(root)?;
                DomainService::spawn(&binary, format, &policy, answer_timeout)?
                    .enumerate_devices(format)
            }
        }
    }

    /// How one of the machine's workers is spawned: what to run there, and the
    /// settings it is greeted under.
    ///
    /// Over ssh the spawn is a shell on that machine, and sima's process here
    /// is only the ssh client — which keeps its own environment, since it needs
    /// the ambient one to reach anything. Reached without a hop the machine is
    /// this one, so the program is spawned directly under the explicit policy
    /// every configured program gets here.
    pub(crate) fn spawn(
        &self,
        mode: &SpawnMode,
        format: &FormatId,
        exec: &ExecutionConfig,
    ) -> Result<(SpawnMode, RemoteCommand, SpawnSettings)> {
        let settings = |policy, expected| {
            SpawnSettings::new(
                policy,
                exec.answer_timeout,
                format.clone(),
                exec.checkpoint_interval,
                exec.checkpoint_interval_steps,
            )
            .expecting_program(expected)
        };
        let RentedProgram::Delivered { delivery, root, .. } = self else {
            return Ok((
                mode.clone(),
                RemoteCommand::worker(),
                settings(SpawnPolicy::Inherit, None),
            ));
        };
        let expected = Some(delivery.payload().to_string());
        match mode {
            SpawnMode::Ssh => Ok((
                SpawnMode::Ssh,
                delivery.ssh_command(root, &[]),
                settings(SpawnPolicy::Inherit, expected),
            )),
            SpawnMode::Local(_) => {
                let (binary, policy) = delivery.local_spawn(root)?;
                Ok((
                    SpawnMode::Local(binary),
                    RemoteCommand::worker(),
                    settings(policy, expected),
                ))
            }
        }
    }
}
