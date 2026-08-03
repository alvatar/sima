//! The tui runtime: the subcommand entry, the terminal guard and panic hook,
//! the key mapping, and the event loop that applies observations to the
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
use sima_pipeline::{
    BinaryChange, Engagement, LoadedConfig, LocalFeed, Record, RemoteFeed, RunControl, RunFeed,
    RunStatus, load, orchestrate,
};

use crate::Target;

use super::state::{KeyAction, LockView, Msg, TuiState};
use super::view;

thread_local! {
    /// Set on the UI thread that owns the terminal, so the panic hook restores
    /// the terminal only for a panic there. Every other thread — the scheduler
    /// worker threads that run executors, the orchestrate thread — leaves the
    /// marker unset, so the hook does nothing on their panics.
    static ON_UI_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// How long the loop waits for a key before ticking, in milliseconds. Short
/// enough that a redraw after an applied event feels immediate.
const TICK_MS: u64 = 50;

/// The channel bound between the run thread and the UI loop. Generous enough
/// that the observer never blocks in practice; the loop drains it fully each
/// tick.
const CHANNEL_BOUND: usize = 1024;

/// How many ticks between run-lock probes: with the tick at [`TICK_MS`], the
/// observer probes about once per second, so the probe — which briefly
/// acquires a lock it finds free to prove it free — stays rare.
const PROBE_TICKS: u32 = 20;

/// `sima tui <config>`: opens the terminal frontend over the configured run.
///
/// A full-screen UI needs a terminal to draw into and read keys from; when
/// stdout is not a TTY — piped or redirected — there is nothing to drive, so
/// the command refuses rather than corrupt a non-terminal stream.
///
/// Mode selection is automatic: a run lock held by another process means a
/// foreign orchestrator drives this run, and the session observes it; a free
/// lock enters the drive session. A run on another host is observed and never
/// driven — driving happens where the hardware is.
pub fn tui_command(target: &Target, engagement: Engagement) -> ExitCode {
    if !io::stdout().is_terminal() {
        eprintln!("sima tui requires a terminal");
        return ExitCode::from(crate::EXIT_ERROR);
    }
    match target {
        Target::Local(config) => local_command(config, engagement),
        Target::Remote { host, config } => remote_command(host, config),
    }
}

/// The session over a run on this machine: observe while a foreign
/// orchestrator holds it, otherwise drive it.
fn local_command(config: &Path, engagement: Engagement) -> ExitCode {
    let loaded = match load(config) {
        Ok(loaded) => loaded,
        Err(e) => return crate::report(e),
    };
    // Probe before entering the terminal so a store fault surfaces on the
    // normal screen, as the seed below does for the drive session.
    match observed_holder(&loaded) {
        Ok(Some((feed, holder))) => {
            return finish(observe_session(
                Box::new(feed),
                Some(holder),
                Some(loaded),
                engagement,
            ));
        }
        Ok(None) => {}
        Err(e) => return crate::report(e),
    }
    // Seed before entering the terminal so a store fault surfaces on the
    // normal screen, exactly as `sima status` would report it.
    let status = match crate::seed_status(&loaded) {
        Ok(status) => status,
        Err(e) => return crate::report(e),
    };
    finish(run_session(loaded, status, engagement))
}

/// The session over a run on another host: always an observation, whatever
/// its lock says, since this machine cannot drive it.
fn remote_command(host: &str, config: &str) -> ExitCode {
    // Open before entering the terminal so an unreachable host, a version
    // gap, or a run never started there surfaces on the normal screen.
    let feed = match RemoteFeed::open(host, config) {
        Ok(feed) => feed,
        Err(e) => return crate::report(e),
    };
    let holder = match feed.holder() {
        Ok(holder) => holder,
        Err(e) => return crate::report(e),
    };
    // A run on another host is never driven from here, so the engagement it
    // would take never reaches an orchestrator.
    finish(observe_session(
        Box::new(feed),
        holder,
        None,
        Engagement::Orchestrator,
    ))
}

/// Maps a session's return to the process exit code. The terminal guard has
/// restored the screen by the time an error surfaces here, so the message
/// lands on a clean screen.
fn finish(result: io::Result<u8>) -> ExitCode {
    match result {
        Ok(code) => ExitCode::from(code),
        // Through the one reporter, so a terminal failure reads as every other
        // failure this binary prints rather than as a second format.
        Err(e) => crate::report(Error::System(format!("the terminal session failed: {e}"))),
    }
}

/// The run's holder when another process drives it: the opened feed and the
/// recorded holder line, or `None` when the lock is free. A store that does
/// not exist yet, and a run never started in it, have nothing to observe —
/// and a query must not create the store — so both read as free and the drive
/// session proceeds.
fn observed_holder(config: &LoadedConfig) -> sima_core::Result<Option<(LocalFeed, String)>> {
    let Some(feed) = LocalFeed::opened(config)? else {
        return Ok(None);
    };
    Ok(feed.holder()?.map(|holder| (feed, holder)))
}

/// Maps a key event to its action, or `None` for an unbound key. The
/// keybindings follow mprocs: `s` starts, `x` stops, `q` quits, `Q` force
/// quits, `?` opens help, and Ctrl-C stops. The plain letters require no
/// modifier, so a chord like Ctrl-S or Alt-x is not one of them; `Q` carries
/// the shift its capital implies, `?` arrives with shift on many layouts so it
/// matches under any modifier, and Ctrl-C arrives as a key in raw mode rather
/// than a signal, so it is handled here rather than through a SIGINT flag.
fn key_action(key: KeyEvent) -> Option<KeyAction> {
    match key.code {
        // Ctrl-C reads as a key in raw mode; treat it as a graceful stop.
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(KeyAction::Stop)
        }
        KeyCode::Char('s') if key.modifiers.is_empty() => Some(KeyAction::Start),
        KeyCode::Char('x') if key.modifiers.is_empty() => Some(KeyAction::Stop),
        KeyCode::Char('q') if key.modifiers.is_empty() => Some(KeyAction::Quit),
        KeyCode::Char('Q') => Some(KeyAction::ForceQuit),
        KeyCode::Char('?') => Some(KeyAction::Help),
        _ => None,
    }
}

/// Runs one drive session over `config`, returning its exit code: sets up
/// the terminal and hands the seeded state to the drive loop.
fn run_session(config: LoadedConfig, status: RunStatus, engagement: Engagement) -> io::Result<u8> {
    // Mark this as the UI thread that owns the terminal, so the panic hook
    // restores it only for a panic here and stays inert on worker and
    // orchestrate threads.
    ON_UI_THREAD.with(|flag| flag.set(true));
    install_panic_hook();
    let mut guard = TerminalGuard::enter()?;
    drive_loop(&mut guard, config, status, false, engagement)
}

/// The drive loop: applies keys and run events to the state, drives the run
/// on a background thread, and returns the session's exit code. The observer
/// session enters here on take-over, reusing its live terminal; its `start`
/// arms an immediate start, so the freed run continues without a second key
/// press.
fn drive_loop(
    guard: &mut TerminalGuard,
    config: LoadedConfig,
    status: RunStatus,
    start: bool,
    engagement: Engagement,
) -> io::Result<u8> {
    let workers = config.execution.workers;
    let mut state = TuiState::new(status, workers);
    if start {
        state.handle(Msg::Key(KeyAction::Start));
    }
    let config = Arc::new(config);

    let (tx, rx) = mpsc::sync_channel::<Msg>(CHANNEL_BOUND);
    // The interrupt flag of the run currently in flight, shared with the run
    // thread; a fresh run gets a fresh flag.
    let mut interrupt: Option<Arc<AtomicBool>> = None;
    // The display is push-driven: redraw only after a message changed the
    // state, never on the bare keyboard tick. The first frame is the initial
    // screen.
    let mut dirty = true;

    loop {
        if dirty {
            guard
                .terminal
                .draw(|frame| view::draw(frame, &state.view()))?;
            dirty = false;
        }

        dirty |= apply_key(&mut state)?;
        // Drain everything the run thread has sent since the last tick.
        while let Ok(msg) = rx.try_recv() {
            state.handle(msg);
            dirty = true;
        }

        if state.take_start() {
            let flag = Arc::new(AtomicBool::new(false));
            interrupt = Some(Arc::clone(&flag));
            spawn_run(Arc::clone(&config), tx.clone(), flag, engagement);
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

/// What ends an observer loop: the session leaves with its exit code, or the
/// user takes the freed run over into the drive session, carrying the config
/// to drive it from — which only a session over a local run holds.
enum ObserveEnd {
    Exit(u8),
    TakeOver(Box<LoadedConfig>),
}

/// Runs one observer session over a run another orchestrator holds: sets up
/// the terminal and tails the run through `observer`. On take-over — `s`
/// once the lock is free — the run continues through the normal resume path:
/// a fresh seed (stale leases cleared, exactly as the drive session seeds),
/// then the drive loop on the same terminal with the start armed.
fn observe_session(
    feed: Box<dyn RunFeed>,
    holder: Option<String>,
    takeover: Option<LoadedConfig>,
    engagement: Engagement,
) -> io::Result<u8> {
    ON_UI_THREAD.with(|flag| flag.set(true));
    install_panic_hook();
    let mut guard = TerminalGuard::enter()?;
    match observe_loop(&mut guard, feed, holder, takeover)? {
        ObserveEnd::Exit(code) => Ok(code),
        ObserveEnd::TakeOver(config) => {
            let status = crate::seed_status(&config).map_err(io::Error::other)?;
            drive_loop(&mut guard, *config, status, true, engagement)
        }
    }
}

/// The observer loop: each tick polls the journal and applies every new
/// event through the same path drive events take — the first batch replays
/// the run's history and seeds the display — and every [`PROBE_TICKS`] ticks
/// probes the run lock for liveness. A terminal journal event presents the
/// ended run; a freed lock without one presents the run as resumable.
fn observe_loop(
    guard: &mut TerminalGuard,
    mut feed: Box<dyn RunFeed>,
    holder: Option<String>,
    mut takeover: Option<LoadedConfig>,
) -> io::Result<ObserveEnd> {
    let mut state = TuiState::new(RunStatus::new(feed.info().run), feed.info().workers);
    if takeover.is_none() {
        state.observe_only();
    }
    let mut lock = lock_view(holder);
    state.observe(lock.clone());
    let mut dirty = true;
    let mut ticks_to_probe = PROBE_TICKS;

    loop {
        if dirty {
            guard
                .terminal
                .draw(|frame| view::draw(frame, &state.view()))?;
            dirty = false;
        }

        dirty |= apply_key(&mut state)?;
        // Tail the journal: every line the foreign orchestrator appended
        // since the last poll, applied in append order. A read or parse
        // fault ends the session as a real error.
        for record in feed.poll().map_err(io::Error::other)? {
            state.handle(Msg::Event(record));
            dirty = true;
        }
        ticks_to_probe -= 1;
        if ticks_to_probe == 0 {
            ticks_to_probe = PROBE_TICKS;
            let probed = lock_view(feed.holder().map_err(io::Error::other)?);
            if probed != lock {
                lock = probed;
                state.observe(lock.clone());
                dirty = true;
            }
        }

        // The state requests a start only in a session that may take over,
        // which is exactly a session holding the config to drive from.
        if state.take_start()
            && let Some(config) = takeover.take()
        {
            return Ok(ObserveEnd::TakeOver(Box::new(config)));
        }
        if state.should_exit() {
            return Ok(ObserveEnd::Exit(state.exit_code()));
        }
    }
}

/// The lock state a probed holder reads as.
fn lock_view(holder: Option<String>) -> LockView {
    match holder {
        Some(holder) => LockView::Held(holder),
        None => LockView::Free,
    }
}

/// Reads at most one key within the tick timeout and applies it, reporting
/// whether the state changed. A key press is handled; releases and repeats
/// are ignored, and `read` runs only after `poll` reports an event. A bound
/// key applies its action — which, behind the help overlay, the state
/// consumes to close it. An unbound key is ignored, except that it too
/// closes an open overlay.
fn apply_key(state: &mut TuiState) -> io::Result<bool> {
    if event::poll(Duration::from_millis(TICK_MS))?
        && let Event::Key(key) = event::read()?
        && key.kind == KeyEventKind::Press
    {
        if let Some(action) = key_action(key) {
            state.handle(Msg::Key(action));
            return Ok(true);
        }
        return Ok(state.dismiss_help_if_open());
    }
    Ok(false)
}

/// Spawns the orchestrate thread for one run: its observer forwards every
/// journal record into the channel, the shared flag carries interrupts in,
/// and its return arrives as [`Msg::Finished`].
fn spawn_run(
    config: Arc<LoadedConfig>,
    tx: SyncSender<Msg>,
    interrupt: Arc<AtomicBool>,
    engagement: Engagement,
) {
    thread::spawn(move || {
        let events = tx.clone();
        let observer = move |record: &Record| {
            let _ = events.send(Msg::Event(record.clone()));
        };
        // `orchestrate` can unwind rather than return `Err` — the scheduler
        // re-raises a worker or collector panic at its scope join. Catch it
        // so the UI loop always receives a return: an unwinding orchestrate
        // thread would otherwise never send `Finished` and leave the session
        // hung. The panic hook is inert off the UI thread, so it does not touch
        // the terminal from here. Catching is this caller's own boundary
        // decision, made because a hung session inside a raw-mode alternate
        // screen is worse than a lost backtrace; `sima run` makes the opposite
        // choice and dies with the panic. `catch_unwind` intercepts only
        // unwinding panics: under `panic = "abort"` this catch is unreachable
        // and the session dies with the process — a crash the store's
        // recovery guarantee covers, so nothing here depends on unwinding
        // for correctness.
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let control = RunControl {
                observer: &observer,
                interrupt: &interrupt,
                on_start: None,
            };
            orchestrate(&config, &control, engagement, BinaryChange::Refuse)
        }))
        .unwrap_or_else(|payload| Err(panic_fault(payload)));
        let _ = tx.send(Msg::Finished(outcome));
    });
}

/// Renders a caught panic payload as an error, so an orchestrate-thread panic
/// reaches the UI loop as a fault outcome.
fn panic_fault(payload: Box<dyn Any + Send>) -> Error {
    let text = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown cause".to_string());
    Error::System(format!("the run thread panicked: {text}"))
}

/// Restores the terminal on a UI-thread panic before the default hook prints,
/// so a panic never leaves the shell in raw mode on the alternate screen. The
/// hook is inert on every other thread: a worker-thread executor panic is
/// caught around the executor call and surfaces as a rejection or fault line,
/// and a scheduler-bug panic re-raises at the scope join into the orchestrate
/// thread, where [`spawn_run`] catches it as `Finished(Err)`. Restoring the
/// terminal from one of those threads — while the UI loop keeps drawing —
/// would corrupt the live session, so only the UI thread's panic acts here.
fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if !ON_UI_THREAD.with(Cell::get) {
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
        // `?` opens help under any modifier, since it arrives with shift on
        // many layouts.
        assert_eq!(
            key_action(press(KeyCode::Char('?'), KeyModifiers::NONE)),
            Some(KeyAction::Help)
        );
        assert_eq!(
            key_action(press(KeyCode::Char('?'), KeyModifiers::SHIFT)),
            Some(KeyAction::Help)
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

    #[test]
    fn a_modifier_on_an_action_letter_maps_to_nothing() {
        // The plain-letter actions require no modifier, so a chord over them
        // is not the action.
        assert_eq!(
            key_action(press(KeyCode::Char('s'), KeyModifiers::CONTROL)),
            None
        );
        assert_eq!(
            key_action(press(KeyCode::Char('q'), KeyModifiers::CONTROL)),
            None
        );
        assert_eq!(
            key_action(press(KeyCode::Char('x'), KeyModifiers::ALT)),
            None
        );
    }

    #[test]
    fn a_caught_panic_renders_its_cause_as_a_fault() {
        // A panic carries its message as either a &str or a String; both
        // reach the fault so the run thread's cause is not lost.
        let from_str: Box<dyn Any + Send> = Box::new("boom");
        assert!(panic_fault(from_str).to_string().contains("boom"));
        let from_string: Box<dyn Any + Send> = Box::new("kaboom".to_string());
        assert!(panic_fault(from_string).to_string().contains("kaboom"));
    }
}
