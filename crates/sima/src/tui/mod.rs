//! `sima tui <config>`: a full-screen terminal frontend over one run.
//!
//! The subcommand opens an alternate-screen UI whose display folds live from
//! the same lifecycle event stream `sima run` observes: an idle screen lists
//! the configured workers, `s` starts the run, the worker rows and counters
//! update as events arrive, and `x`/`q`/`Q` wind the run down or leave. The
//! run itself is unchanged — orchestration stays in `sima-pipeline`, and this
//! module only drives a terminal and folds observations into a display.

mod app;
// The runtime consumes the state machine in a following step; until it does,
// the module is reached only from its own tests.
#[allow(dead_code)]
mod state;

pub use app::tui_command;
