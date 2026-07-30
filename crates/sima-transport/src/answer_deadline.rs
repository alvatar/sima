//! Waiting for a peer's answer under the configured deadline.
//!
//! Both protocols wait the same way: a reader thread decodes the peer's
//! frames into a channel, and the caller takes one message from it. What
//! `[config] answer_timeout_ms` bounds is that take — startup plus answer
//! latency, never computation.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

/// Takes one message from `answers`, waiting at most `within`.
///
/// [`Duration::MAX`] waits for as long as the peer lives, on `recv` rather
/// than on `recv_timeout`: a timeout that far out is an instant the platform
/// cannot represent, and computing it overflows.
///
/// A peer that died before answering is [`RecvTimeoutError::Disconnected`],
/// which the caller reports as the peer ending rather than as an expiry.
pub(crate) fn receive_within<T>(
    answers: &Receiver<T>,
    within: Duration,
) -> Result<T, RecvTimeoutError> {
    if within == Duration::MAX {
        answers.recv().map_err(|_| RecvTimeoutError::Disconnected)
    } else {
        answers.recv_timeout(within)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use super::*;

    #[test]
    fn an_answer_already_queued_is_taken_under_either_bound() {
        for within in [Duration::MAX, Duration::from_secs(30)] {
            let (sender, answers) = channel();
            sender.send(7).expect("queue an answer");
            assert_eq!(receive_within(&answers, within), Ok(7));
        }
    }

    #[test]
    fn an_unbounded_wait_over_a_dead_peer_reports_the_disconnection() {
        // The sender is dropped at once: the peer died before answering, and
        // the unbounded wait ends there rather than hanging.
        let (sender, answers) = channel::<u8>();
        drop(sender);
        assert_eq!(
            receive_within(&answers, Duration::MAX),
            Err(RecvTimeoutError::Disconnected)
        );
    }

    #[test]
    fn a_bounded_wait_over_a_silent_peer_expires() {
        let (_sender, answers) = channel::<u8>();
        assert_eq!(
            receive_within(&answers, Duration::from_millis(10)),
            Err(RecvTimeoutError::Timeout)
        );
    }
}
