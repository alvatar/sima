//! The workspace's home for end-to-end tests of real domains through the full
//! spine.
//!
//! Concrete domains — `ca_evolution` and the families that follow — are exercised
//! through the pipeline API here, so no infrastructure crate accretes domain
//! names in its own test tree. The suites live under `tests/`; this crate
//! ships no code.
