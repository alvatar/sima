//! [`serve`]: the process entry point of a program that hosts a domain.
//!
//! One binary answers both roles a run needs of a program, and the arguments
//! it was spawned with say which: bare, it hosts the format's executor over
//! the worker protocol; under `--serve-domain <format>`, it answers what the
//! format binds over the domain service. A program is then its two plugs plus
//! this call.

use sima_contracts::{DomainPlug, Executor, GeneratorPlug};
use sima_core::{Error, Result};
use sima_model::FormatId;

use crate::domain_service::host::served;
use crate::{domain_service, host};

/// The flag that asks a program for the domain-service role, followed by the
/// format id it is asked about.
const SERVE_DOMAIN: &str = "--serve-domain";

/// What a program was spawned to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// Host the format's executor over the worker protocol.
    Execute,
    /// Answer what the named format binds over the domain service.
    ServeDomain(FormatId),
}

impl Role {
    /// The role `args` — a process's whole argument vector, program name first
    /// — asks for. Arguments the role vocabulary does not name are a wrapper's
    /// own, so anything without the flag is the executor role.
    pub fn from_args(args: impl Iterator<Item = String>) -> Result<Role> {
        let mut args = args.skip_while(|arg| arg != SERVE_DOMAIN);
        if args.next().is_none() {
            return Ok(Role::Execute);
        }
        let Some(format) = args.next() else {
            return Err(Error::Validation(format!(
                "{SERVE_DOMAIN} takes the format id the domain service is asked about"
            )));
        };
        Ok(Role::ServeDomain(FormatId::new(format)?))
    }
}

/// Hosts a domain: the executor role when the parent hands over a task, and
/// the domain service role when it asks for the format's metadata,
/// configuration translation, or a batch of specs.
///
/// The role is read from the process arguments, so one binary answers both.
/// Returns `Ok` when the parent closes the pipe or says goodbye; an `Err` is a
/// handshake refusal, a frame violation, or a broken pipe, which the caller
/// maps to a nonzero exit.
pub fn serve(domain: &dyn DomainPlug, generators: &[&dyn GeneratorPlug]) -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    match Role::from_args(std::env::args())? {
        Role::Execute => {
            // Panic messages and backtraces latch for the serve loop's
            // correlated diagnostics; the process's own hook still runs after
            // the capture.
            host::capture_panics();
            host::serve(stdin.lock(), stdout.lock(), &|format, device| {
                served(domain, format)?;
                let executor: Box<dyn Executor> = domain.executor(device)?;
                let (name, driver) = domain.device_desc(device)?;
                Ok((executor, name, driver))
            })
        }
        Role::ServeDomain(format) => {
            served(domain, &format)?;
            domain_service::serve(stdin.lock(), stdout.lock(), domain, generators)
        }
    }
}

#[cfg(test)]
mod tests {
    use sima_core::{Error, Result};

    use super::*;

    /// The arguments a process sees, program name first.
    fn args(rest: &[&str]) -> Vec<String> {
        std::iter::once("/opt/acme/worker".to_string())
            .chain(rest.iter().map(|arg| (*arg).to_string()))
            .collect()
    }

    #[test]
    fn a_bare_invocation_is_the_executor_role() {
        // The orchestrator spawns a worker with no arguments, so the executor
        // role is what a program does by default.
        assert!(matches!(
            Role::from_args(args(&[]).into_iter()),
            Ok(Role::Execute)
        ));
    }

    #[test]
    fn the_flag_names_the_format_the_domain_service_is_asked_for() -> Result<()> {
        let Role::ServeDomain(format) =
            Role::from_args(args(&["--serve-domain", "acme.thing.v1"]).into_iter())?
        else {
            panic!("expected the domain-service role");
        };
        assert_eq!(format.as_str(), "acme.thing.v1");
        Ok(())
    }

    #[test]
    fn the_flag_without_a_format_names_what_it_takes() {
        let Err(error) = Role::from_args(args(&["--serve-domain"]).into_iter()) else {
            panic!("expected a validation error");
        };
        assert!(error.to_string().contains("--serve-domain"), "{error}");
    }

    #[test]
    fn a_format_outside_the_rule_is_rejected_at_the_flag() {
        assert!(matches!(
            Role::from_args(args(&["--serve-domain", "Bad Name"]).into_iter()),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn arguments_the_role_does_not_name_leave_the_executor_role() {
        // A wrapper may hand a worker arguments of its own; the role is decided
        // by the flag it finds rather than by the shape of the whole vector.
        assert!(matches!(
            Role::from_args(args(&["--assets", "/opt/acme/assets"]).into_iter()),
            Ok(Role::Execute)
        ));
    }
}
