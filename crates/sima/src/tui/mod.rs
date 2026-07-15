//! `sima tui <config>`: a full-screen terminal frontend over one run.
//!
//! The subcommand opens an alternate-screen UI whose display is built live
//! from the same lifecycle event stream `sima run` observes: an idle screen
//! lists the configured workers, `s` starts the run, the worker rows and
//! counters update as events arrive, and `x`/`q`/`Q` wind the run down or
//! leave. The run itself is unchanged — orchestration stays in `sima-pipeline`,
//! and this module only drives a terminal and applies each observation to the
//! display.
//!
//! Pointed at a run another process is driving — the run lock is held — the
//! session observes it instead: the journal is tailed through
//! [`sima_pipeline::RunObserver`], every replayed and live event feeds the
//! same display path, the header names the holder, and once the lock frees
//! `s` continues the run through the ordinary drive session. Observation is
//! read-only: the session never takes the run lock and never writes the
//! store.

mod app;
mod state;
mod view;

pub use app::tui_command;
