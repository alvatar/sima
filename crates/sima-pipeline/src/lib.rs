//! Pipeline layer: the human-facing configuration in, a driven run out.
//!
//! A `sima.toml` is loaded and translated into the identity-bearing
//! [`sima_model::RunConfig`] plus the operational execution settings; the
//! format id dispatches through [`sima_domains`] to the executor that
//! evaluates the format's specs, the environment that enters task identity,
//! and the translation of the domain-owned params section, and the generator
//! id dispatches to a generator with its own config translation. The pipeline
//! routes configuration sections to the domain and generator code that owns
//! them; it never interprets their content.
//!
//! Beside the driven run sit the read-only queries over what a run left
//! behind. Each folds the run's journal and touches no store object:
//! [`status`] and [`task_history`] project execution — the run's state, and
//! one task's attempts — [`failures`] names the tasks that did not commit,
//! and [`report`] and [`report_task`] render the results committed tasks
//! produced. The queries return data; rendering it is the caller's.

mod config;
mod devices;
mod feed;
#[cfg(test)]
mod fixtures;
mod journal;
mod observe;
mod orchestrate;
mod remove;
mod report;
mod status;
mod task_history;

pub use config::{LoadedConfig, RemoteConfig, load};
pub use devices::DeviceSelector;
pub use feed::{
    FOLLOW_PROTOCOL_VERSION, FeedInfo, FollowFrame, LocalFeed, RemoteFeed, RunFeed, follow_serve,
    remote_snapshot,
};
pub use observe::RunObserver;
pub use orchestrate::orchestrate;
pub use remove::remove;
pub use report::{ReportRow, report, report_records, report_task, report_task_records};
// The run identity a query names, re-exported with the rest of the surface a
// caller reads a run through.
pub use sima_model::RunId;
pub use sima_store::RemovalReport;
// The scheduler types a caller drives and observes runs through, re-exported
// so the CLI consumes one coherent surface.
pub use sima_scheduler::{Event, Level, Record, RunControl, RunOutcome};
pub use status::{Occupancy, RunState, RunStatus, status, status_records};
pub use task_history::{
    Attempt, AttemptResult, TaskHistory, TaskOutcome, failures, failures_records, task_history,
    task_history_records,
};
