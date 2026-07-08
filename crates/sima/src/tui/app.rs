//! The tui runtime: the subcommand entry, the terminal guard and panic hook,
//! the key mapping, and the event loop that folds observations into the
//! display and drives the run on a background thread.

use std::any::Any;
use std::cell::Cell;
use std::io::{self, IsTerminal, Stdout};
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::thread;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use sima_core::Error;
use sima_pipeline::{LifecycleEvent, LoadedConfig, RunControl, RunStatus, load, orchestrate};

use super::state::{KeyAction, Msg, TuiState};
use super::view;

thread_local! {
    /// Set on the orchestrate thread so the panic hook knows a panic there is
    /// caught and reported as a fault by [`spawn_run`], and must not touch the
    /// terminal the UI thread owns.
    static ON_RUN_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// How long the loop waits for a key before ticking, in milliseconds. Short
/// enough that a redraw after a folded event feels immediate.
const TICK_MS: u64 = 50;

/// The channel bound between the run thread and the UI loop. Generous enough
/// that the observer never blocks in practice; the loop drains it fully each
/// tick.
const CHANNEL_BOUND: usize = 1024;

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
    let loaded = match load(config) {
        Ok(loaded) => loaded,
        Err(e) => {
            eprintln!("sima: {e}");
            return ExitCode::from(crate::EXIT_ERROR);
        }
    };
    // Seed before entering the terminal so a store fault surfaces on the
    // normal screen, exactly as `sima status` would report it.
    let status = match seed_status(&loaded) {
        Ok(status) => status,
        Err(e) => {
            eprintln!("sima: {e}");
            return ExitCode::from(crate::EXIT_ERROR);
        }
    };
    match run_session(loaded, status) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            // The guard has restored the terminal by now, so the message
            // lands on a clean screen.
            eprintln!("sima tui: {e}");
            ExitCode::from(crate::EXIT_ERROR)
        }
    }
}

/// Maps a key event to its action, or `None` for an unbound key. The
/// keybindings follow mprocs: `s` starts, `x` stops, `q` quits, `Q` force
/// quits, and Ctrl-C stops — in raw mode Ctrl-C arrives as a key, not a
/// signal, so it is handled here rather than through a SIGINT flag.
fn key_action(key: KeyEvent) -> Option<KeyAction> {
    match key.code {
        // Ctrl-C reads as a key in raw mode; treat it as a graceful stop.
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(KeyAction::Stop)
        }
        KeyCode::Char('s') => Some(KeyAction::Start),
        KeyCode::Char('x') => Some(KeyAction::Stop),
        KeyCode::Char('q') => Some(KeyAction::Quit),
        KeyCode::Char('Q') => Some(KeyAction::ForceQuit),
        _ => None,
    }
}

/// Seeds the display from any existing journal for `config`'s run, folding it
/// through the shared accumulator so a resumed run shows its prior progress.
/// A store that does not exist yet, or a run never driven, seeds a zeroed
/// status; a corrupt journal or an I/O fault is a real problem `sima status`
/// reports, so it surfaces here rather than hiding behind a blank screen.
fn seed_status(config: &LoadedConfig) -> sima_core::Result<RunStatus> {
    match sima_pipeline::status(config) {
        Ok(status) => Ok(status),
        Err(Error::Validation(_)) => Ok(RunStatus::new(config.run.id())),
        Err(other) => Err(other),
    }
}

/// Runs one terminal session over `config`, returning its exit code. Sets up
/// the terminal, folds keys and run events into the state, drives the run on a
/// background thread, and tears the terminal down on return.
fn run_session(config: LoadedConfig, status: RunStatus) -> io::Result<u8> {
    let workers = config.execution.workers;
    let mut state = TuiState::new(status, workers);
    let config = Arc::new(config);

    install_panic_hook();
    let mut guard = TerminalGuard::enter()?;

    let (tx, rx) = mpsc::sync_channel::<Msg>(CHANNEL_BOUND);
    // The interrupt flag of the run currently in flight, shared with the run
    // thread; a fresh run gets a fresh flag.
    let mut interrupt: Option<Arc<AtomicBool>> = None;

    loop {
        guard
            .terminal
            .draw(|frame| view::draw(frame, &state.view()))?;

        // A key press that maps to an action folds in; releases, repeats, and
        // unbound keys are ignored. `read` runs only after `poll` reports one.
        if event::poll(Duration::from_millis(TICK_MS))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && let Some(action) = key_action(key)
        {
            state.handle(Msg::Key(action));
        }
        // Drain everything the run thread has sent since the last tick.
        while let Ok(msg) = rx.try_recv() {
            state.handle(msg);
        }

        if state.take_start() {
            let flag = Arc::new(AtomicBool::new(false));
            interrupt = Some(Arc::clone(&flag));
            spawn_run(Arc::clone(&config), tx.clone(), flag);
        }
        if state.take_stop()
            && let Some(flag) = &interrupt
        {
            flag.store(true, Ordering::Relaxed);
        }
        if state.should_exit() {
            break;
        }
    }
    Ok(state.exit_code())
}

/// Spawns the orchestrate thread for one run: its observer forwards every
/// lifecycle event into the channel, the shared flag carries interrupts in,
/// and its return arrives as [`Msg::Finished`].
fn spawn_run(config: Arc<LoadedConfig>, tx: SyncSender<Msg>, interrupt: Arc<AtomicBool>) {
    thread::spawn(move || {
        ON_RUN_THREAD.with(|flag| flag.set(true));
        let events = tx.clone();
        let observer = move |event: &LifecycleEvent| {
            let _ = events.send(Msg::Event(event.clone()));
        };
        // `orchestrate` can unwind rather than return `Err` — the scheduler
        // re-raises a worker or journal-sink panic at its scope join. Catch it
        // so the UI loop always receives a return: an unwinding run thread
        // would otherwise never send `Finished` and leave the session hung.
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let control = RunControl {
                observer: &observer,
                interrupt: &interrupt,
            };
            orchestrate(&config, &control)
        }))
        .unwrap_or_else(|payload| Err(panic_fault(payload)));
        let _ = tx.send(Msg::Finished(outcome));
    });
}

/// Renders a caught panic payload as an error, so a run-thread panic reaches
/// the UI loop as a fault outcome.
fn panic_fault(payload: Box<dyn Any + Send>) -> Error {
    let text = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown cause".to_string());
    Error::Validation(format!("the run thread panicked: {text}"))
}

/// Restores the terminal on a UI-thread panic before the default hook prints,
/// so a panic never leaves the shell in raw mode on the alternate screen. A
/// panic on the run thread is left to [`spawn_run`], which catches it and
/// reports it as a fault, so the hook neither touches the terminal — the UI
/// thread owns it — nor prints from there.
fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if ON_RUN_THREAD.with(Cell::get) {
            return;
        }
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default(info);
    }));
}

/// An RAII terminal guard: raw mode and the alternate screen on construction,
/// both restored on drop however the session leaves — a return, an error, or
/// the unwinding a caught panic starts.
struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    /// Enters raw mode and the alternate screen, returning the drawing
    /// terminal.
    fn enter() -> io::Result<TerminalGuard> {
        enable_raw_mode()?;
        let mut out = io::stdout();
        execute!(out, EnterAlternateScreen)?;
        let terminal = Terminal::new(CrosstermBackend::new(out))?;
        Ok(TerminalGuard { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        // Best effort: nothing actionable remains if restoring the terminal
        // itself fails while leaving.
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn press(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn keys_map_to_their_actions() {
        assert_eq!(
            key_action(press(KeyCode::Char('s'), KeyModifiers::NONE)),
            Some(KeyAction::Start)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('x'), KeyModifiers::NONE)),
            Some(KeyAction::Stop)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('q'), KeyModifiers::NONE)),
            Some(KeyAction::Quit)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('Q'), KeyModifiers::SHIFT)),
            Some(KeyAction::ForceQuit)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Some(KeyAction::Stop)
        );
    }

    #[test]
    fn unbound_keys_map_to_nothing() {
        assert_eq!(
            key_action(press(KeyCode::Char('z'), KeyModifiers::NONE)),
            None
        );
        assert_eq!(key_action(press(KeyCode::Esc, KeyModifiers::NONE)), None);
        // A bare 'c' without control is not the interrupt.
        assert_eq!(
            key_action(press(KeyCode::Char('c'), KeyModifiers::NONE)),
            None
        );
    }
}
