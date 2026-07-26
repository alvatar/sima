//! The `sima-worker` binary: an executor host, one per worker slot.
//!
//! The orchestrator spawns one of these per worker and converses over its
//! stdin/stdout in the transport's wire protocol; stderr is captured by the
//! parent and journaled as correlated diagnostics. The process is pure
//! compute by construction —
//! it is never given a store path, so the "executors are pure compute"
//! invariant is OS-enforced. All logic lives in [`sima_transport::host`];
//! this binary only wires the streams, the domain resolver, and the exit
//! code, plus the orphan protection.

use sima_contracts::{DeviceBinding, Executor};
use sima_core::Result;
use sima_model::FormatId;

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
    let executor = (domain.executor)(device)?;
    let (device_name, driver) = (domain.device_desc)(device)?;
    Ok((executor, device_name, driver))
}

/// Enumerates the compute devices the program bound to `format` can run on and
/// prints one JSON object per device to stdout, one per line, then exits. The
/// remote-resolution probe: the orchestrator runs this over ssh at run start to
/// resolve a remote's device selectors against its actual hardware. The output
/// is the serde form of [`sima_domains::devices::DeviceInfo`] — human-readable,
/// never identity-bearing.
///
/// The format is what selects the backend to ask, so the answer is the devices
/// this run's work can be placed on rather than every device present. A machine
/// commonly has devices only one backend reaches.
fn enumerate(format: &str) -> Result<()> {
    let format = FormatId::new(format)?;
    for device in sima_domains::devices::enumerate_devices(&format)? {
        let line = serde_json::to_string(&device)
            .map_err(|e| sima_core::Error::Encoding(format!("device to JSON: {e}")))?;
        println!("{line}");
    }
    Ok(())
}

/// Exit codes: 0 on the parent closing the pipe (clean end-of-stream), 1
/// with a stderr line on a protocol refusal or a serve error.
fn main() {
    // The one-shot enumeration probe: no protocol, no store, no orphan
    // protection — enumerate, print, exit. It runs before anything else so a
    // probe never spawns the handshake machinery. The format id follows the
    // flag and decides which backend is asked.
    let mut args = std::env::args().skip_while(|arg| arg != "--enumerate");
    if args.next().is_some() {
        let Some(format) = args.next() else {
            eprintln!("sima-worker: --enumerate takes the run's format id");
            std::process::exit(1);
        };
        if let Err(e) = enumerate(&format) {
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
