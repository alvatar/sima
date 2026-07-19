//! The structured-event facade: one typed vocabulary every layer can emit.
//!
//! The crate sits directly above `sima-core`, so any layer — scheduler,
//! transport, worker host — can emit without an upward edge. Three pieces:
//!
//! - [`Event`] — the typed vocabulary: the run-lifecycle variants plus a
//!   correlated [`Diagnostic`](Event::Diagnostic) line.
//! - [`Record`] — one journal line: the event plus the wall-clock stamp the
//!   collector applied at append time.
//! - [`Collector`] / [`Emitter`] — the funnel: emitters send events over an
//!   `mpsc` channel; one collector thread stamps each event, appends its
//!   line through a [`DurableSink`], and hands the record to the observer.
//!
//! Events are observational — they record what happened, never run identity —
//! so the stream is excluded from every equality criterion, and its
//! serialization world is serde, never the canonical encoding. Causality
//! context is the natural keys: events carry `run`, `task`, `attempt`,
//! `worker`, and `host` directly as fields.

mod collector;
mod event;
mod record;

pub use collector::{Collector, DurableSink, Emitter, Subscriber};
pub use event::{Event, Level};
pub use record::Record;
