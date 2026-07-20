//! The feed: a run's records and lock state, from the host that drives it.

mod protocol;

pub use protocol::{FOLLOW_PROTOCOL_VERSION, FollowFrame};
