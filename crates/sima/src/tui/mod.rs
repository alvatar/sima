//! `sima tui <config>`: a full-screen terminal frontend over one search.
//!
//! The subcommand opens an alternate-screen UI whose display is built live
//! from the same lifecycle event stream `sima search` observes: an idle screen
//! lists the configured workers, `s` starts the search, the worker rows and
//! counters update as events arrive, and `x`/`q`/`Q` wind the search down or
//! leave. The search itself is unchanged — orchestration stays in `sima-pipeline`,
//! and this module only drives a terminal and applies each observation to the
//! display.
//!
//! Pointed at a search another process is driving — the search lock is held — the
//! session observes it instead: the journal is tailed through
//! [`sima_pipeline::SearchObserver`], every replayed and live event feeds the
//! same display path, the header names the holder, and once the lock frees
//! `s` continues the search through the ordinary drive session. Observation is
//! read-only: the session never takes the search lock and never writes the
//! store.

mod app;
mod state;
mod view;

pub use app::tui_command;
