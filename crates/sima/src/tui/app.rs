//! The tui runtime: the subcommand entry, the terminal guard, and the event
//! loop that folds observations into the display.

use std::io::IsTerminal;
use std::path::Path;
use std::process::ExitCode;

/// `sima tui <config>`: opens the terminal frontend over the configured run.
///
/// A full-screen UI needs a terminal to draw into and read keys from; when
/// stdout is not a TTY — piped or redirected — there is nothing to drive, so
/// the command refuses rather than corrupt a non-terminal stream.
pub fn tui_command(config: &Path) -> ExitCode {
    if !std::io::stdout().is_terminal() {
        eprintln!("sima tui requires a terminal");
        return ExitCode::from(crate::EXIT_ERROR);
    }
    session(config)
}

/// Drives one terminal session over the run `config` describes. The
/// interactive runtime is assembled by the later tui tasks; until then a
/// present terminal has nothing to drive.
fn session(_config: &Path) -> ExitCode {
    eprintln!("sima tui is not yet wired to a runtime");
    ExitCode::from(crate::EXIT_ERROR)
}
