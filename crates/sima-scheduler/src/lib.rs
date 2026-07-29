//! The scheduler: it runs a search from `(RunConfig, store state)`.
//!
//! A task source derives the runnable frontier — the tasks the store does not
//! yet answer — and the driver hands each to a pool of worker processes over
//! the transport ([`sima_transport`]), commits successes through the store,
//! retries transient failures, and stops on a definitive one. It is the layer
//! that bridges pure executor output into durable store state, so the
//! executor trust boundary lives on the worker seam: the executor returns
//! values from its own process, and only the parent-side worker writes to
//! the store.
//!
//! Determinism is the correctness criterion: the same config run twice into
//! two fresh stores yields byte-identical manifests. Worker completion order
//! never reaches identity — the manifest sorts by task key at finalize, and
//! the journal, whose event order does vary between runs, is observational and
//! excluded from every equality criterion.

mod config;
mod control;
mod coordinator;
mod driver;
mod placement;
mod segment_chain;
mod static_batch;
mod task_source;
mod worker;
mod worker_pool;

pub use config::{DeviceEntry, ExecutionConfig};
pub use control::RunControl;
pub use driver::{RunOutcome, run, run_keys, worker_slots};
pub use segment_chain::SegmentChain;
// The event vocabulary and journal line type are the trace facade's; the
// scheduler re-exports them as the emitting layer consumers import from.
pub use sima_trace::{Event, Level, Record, StatScalar};
pub use static_batch::StaticBatch;
pub use task_source::{RunnableTask, TaskSource};
pub use worker_pool::WorkerPool;
