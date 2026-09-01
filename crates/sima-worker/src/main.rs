//! The `sima-worker` binary: the in-tree domains, hosted.
//!
//! Its role is what the arguments say:
//!
//! - bare, it is an executor host, one per worker slot. The orchestrator
//!   spawns one of these per worker and converses over its stdin/stdout in the
//!   transport's wire protocol; stderr is captured by the parent and journaled
//!   as correlated diagnostics.
//! - under `--serve-domain <format>`, it answers what that format binds over
//!   the domain service, through the same contracts a program outside the
//!   workspace is written against.
//! - under `--enumerate-devices <format>`, it prints the devices that format's
//!   work can search on and exits; under `--enumerate-devices` alone, every device
//!   every compiled backend reaches, which states that the machine is up and
//!   what hardware it has.
//!
//! The process is pure compute by construction — it is never given a store
//! path, so the "executors are pure compute" invariant is OS-enforced. All
//! logic lives in [`sima_transport`]; this binary only wires the streams, the
//! domain resolver, and the exit code, plus the orphan protection.

use sima_contracts::{DeviceBinding, Executor, Generator};
use sima_core::Result;
use sima_model::FormatId;
use sima_transport::serve::Role;

/// Resolves the executor for the handshake's format id through the domain
/// registry, bound to the handshake's device, and describes the device it
/// opened as `(name, driver version)`.
///
/// All three happen here, before the parent is told the worker is ready, so a
/// binding naming a device this machine does not have fails the handshake.
fn resolver(
    format: &FormatId,
    device: Option<&DeviceBinding>,
) -> Result<(Box<dyn Executor>, String, String)> {
    let domain = sima_domains::domain_for(format)?;
    let executor = domain.executor(device)?;
    let (device_name, driver) = domain.device_desc(device)?;
    Ok((executor, device_name, driver))
}

/// Enumerates compute devices and prints one JSON object per device to stdout,
/// one per line, then exits. The output is the serde form of
/// [`sima_domains::devices::DeviceInfo`] — human-readable, never
/// identity-bearing.
///
/// The two forms answer two questions:
///
/// - `Some(format)` — the devices the program bound to that format can search on.
///   The format selects the backend to ask, so the answer is where this search's
///   work can be placed rather than every device present; a machine commonly
///   has devices only one backend reaches. The orchestrator searches this over ssh
///   at search start to resolve a remote's device selectors.
/// - `None` — every device every compiled backend reaches. The readiness probe
///   for a search whose format is a program outside this build: nothing here can
///   resolve that format, so the answer states that the machine is up and what
///   hardware it has, and the program's own enumeration decides placement.
fn enumerate_devices(format: Option<&str>) -> Result<()> {
    let devices = match format {
        Some(format) => sima_domains::devices::enumerate_devices(&FormatId::new(format)?)?,
        None => sima_domains::devices::enumerate_all_devices()?,
    };
    for device in devices {
        let line = serde_json::to_string(&device)
            .map_err(|e| sima_core::Error::Encoding(format!("device to JSON: {e}")))?;
        println!("{line}");
    }
    Ok(())
}

/// Answers the domain service for `format` over stdin/stdout: what its
/// environment is, what devices its work searches on, how its configuration
/// translates, and what specs its generators produce.
///
/// The in-tree formats are reached through the same two contracts a program
/// outside the workspace implements, so this role proves those contracts carry
/// everything a search needs.
fn serve_domain(format: &FormatId) -> Result<()> {
    let domain = sima_domains::domain_for(format)?;
    let generators = sima_domains::generators_for(format)?;
    let generators: Vec<&dyn Generator> = generators.iter().map(AsRef::as_ref).collect();
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    sima_transport::domain_service::serve(stdin.lock(), stdout.lock(), domain.as_ref(), &generators)
}

/// Exit codes: 0 on the parent closing the pipe (clean end-of-stream), 1
/// with a stderr line on a protocol refusal or a serve error.
fn main() {
    // The one-shot enumeration probe: no protocol, no store, no orphan
    // protection — enumerate, print, exit. It searches before anything else so a
    // probe never spawns the handshake machinery. A format id following the
    // flag decides which backend is asked; without one, every backend is.
    let mut args = std::env::args().skip_while(|arg| arg != "--enumerate-devices");
    if args.next().is_some() {
        if let Err(e) = enumerate_devices(args.next().as_deref()) {
            eprintln!("sima-worker: {e}");
            std::process::exit(1);
        }
        return;
    }
    // Orphan protection, before anything else: if the orchestrator dies
    // without closing this process's stdin — SIGKILL, OOM kill — the kernel
    // delivers SIGKILL here. The stdin end-of-stream exit is the second,
    // graceful layer. Safety: PR_SET_PDEATHSIG with a valid signal number
    // has no memory-safety conditions.
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
    }
    // The death signal only arrives for deaths after the prctl; a parent
    // that died in between already reparented this process, so exit now.
    if unsafe { libc::getppid() } == 1 {
        eprintln!("sima-worker: orphaned before startup");
        std::process::exit(1);
    }
    // The domain-service role: one session for the search, answering what a
    // format binds. It searches under the same orphan protection as a worker,
    // its session being just as long-lived.
    let role = match Role::from_args(std::env::args()) {
        Ok(role) => role,
        Err(e) => {
            eprintln!("sima-worker: {e}");
            std::process::exit(1);
        }
    };
    if let Role::ServeDomain(format) = role {
        if let Err(e) = serve_domain(&format) {
            eprintln!("sima-worker: {e}");
            std::process::exit(1);
        }
        return;
    }
    // Latch panic messages and backtraces for the serve loop's correlated
    // diagnostics; the default hook still prints to stderr after the capture.
    sima_transport::host::capture_panics();
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    if let Err(e) = sima_transport::host::serve(stdin.lock(), stdout.lock(), &resolver) {
        eprintln!("sima-worker: {e}");
        std::process::exit(1);
    }
}
