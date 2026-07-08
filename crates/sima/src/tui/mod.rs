//! `sima tui <config>`: a full-screen terminal frontend over one run.
//!
//! The subcommand opens an alternate-screen UI whose display folds live from
//! the same lifecycle event stream `sima run` observes: an idle screen lists
//! the configured workers, `s` starts the run, the worker rows and counters
//! update as events arrive, and `x`/`q`/`Q` wind the run down or leave. The
//! run itself is unchanged — orchestration stays in `sima-pipeline`, and this
//! module only drives a terminal and folds observations into a display.

mod app;
mod state;
mod view;

pub use app::tui_command;
