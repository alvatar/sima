//! [`follow_serve`]: the far half of the follow transport — a run's journal
//! and lock state, written to a byte stream as [`FollowFrame`]s.
//!
//! It runs on the host the run's orchestrator runs on, where the config
//! resolves to a real store, the journal is a real file, and the run lock is
//! an advisory lock the local kernel holds. It takes no lock and writes
//! nothing: it is a [`RunObserver`] with a wire on its output, so serving a
//! run cannot perturb it.

use std::io::Write;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use sima_core::{Result, write_frame};

use crate::config::load;
use crate::feed::protocol::{FOLLOW_PROTOCOL_VERSION, FollowFrame};
use crate::journal;
use crate::observe::RunObserver;

/// How long the live loop waits between journal reads.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How many polls between lock probes: with [`POLL_INTERVAL`] at 100 ms, the
/// stream probes about once per second, so the probe — which briefly acquires
/// a lock it finds free to prove it free — stays rare.
const PROBE_POLLS: u32 = 10;

/// Serves the run `config` describes as a [`FollowFrame`] stream on `out`.
///
/// The config is loaded on this host, so the run id, the store path, and the
/// lock are all this host's. The stream opens with a `Hello` carrying the run
/// metadata the reader renders through, then a `Records` frame carrying the
/// journal's current contents. `once` closes there with a `Complete`; a live
/// stream tails the journal until the reader closes the pipe.
///
/// Everything that goes wrong on this host — a config that does not load, a
/// run never started, a corrupt journal — is written as a single `Fault`
/// frame, so the reader renders the cause instead of an empty stream.
pub fn follow_serve(config: &Path, once: bool, out: &mut impl Write) -> Result<()> {
    match serve(config, once, out) {
        Ok(()) => Ok(()),
        Err(e) => frame(out, &FollowFrame::Fault(e.to_string())),
    }
}

/// The stream proper: everything that can fail returns here, and the caller
/// turns it into the `Fault` frame that carries it across the wire.
fn serve(config: &Path, once: bool, out: &mut impl Write) -> Result<()> {
    let loaded = load(config)?;
    // The same guard the one-shot queries apply: a store that does not exist
    // and a run never started there are errors, not empty streams.
    journal::followable(&loaded)?;
    let mut observer = RunObserver::new(&loaded)?;
    let mut holder = observer.holder()?;
    frame(
        out,
        &FollowFrame::Hello {
            protocol: FOLLOW_PROTOCOL_VERSION,
            run: loaded.run.id(),
            format: loaded.run.format.clone(),
            workers: loaded.execution.workers as u32,
            holder: holder.clone(),
        },
    )?;
    // The first `Records` frame carries the run's history and is always sent,
    // empty or not. A reader cannot distinguish "nothing journalled yet" from
    // "the history has not arrived yet" by waiting, so the frame itself marks
    // the point the history is complete, and the reader blocks for it.
    frame(out, &FollowFrame::Records(observer.poll_lines()?))?;
    if once {
        return frame(out, &FollowFrame::Complete);
    }
    let mut polls_to_probe = PROBE_POLLS;
    loop {
        let lines = observer.poll_lines()?;
        if !lines.is_empty() {
            frame(out, &FollowFrame::Records(lines))?;
        }
        polls_to_probe -= 1;
        if polls_to_probe == 0 {
            polls_to_probe = PROBE_POLLS;
            let probed = observer.holder()?;
            if probed != holder {
                holder = probed;
                frame(out, &FollowFrame::Holder(holder.clone()))?;
            }
        }
        sleep(POLL_INTERVAL);
    }
}

/// Writes one frame to the stream. A write failure is the reader closing the
/// pipe — the near side decides when a follow ends — and it ends the loop
/// that is writing.
fn frame(out: &mut impl Write, frame: &FollowFrame) -> Result<()> {
    write_frame(out, &frame.encode())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sima_core::{Result, read_frame};
    use sima_scheduler::{Event, Record};
    use sima_store::Store;

    use crate::feed::FOLLOW_PROTOCOL_VERSION;
    use crate::fixtures::{served_config, served_run};

    /// A `Committed` record for `task`.
    fn committed(task: &str) -> Record {
        Record {
            ts_ms: 0,
            event: Event::Committed {
                task: task.to_string(),
                record: "11".repeat(32),
                stats: Vec::new(),
                stats_blob_hex: String::new(),
            },
        }
    }

    /// Every frame of a served stream, decoded.
    fn frames(bytes: &[u8]) -> Result<Vec<FollowFrame>> {
        let mut reader = bytes;
        let mut frames = Vec::new();
        while let Some(payload) = read_frame(&mut reader)? {
            frames.push(FollowFrame::decode(&payload)?);
        }
        Ok(frames)
    }

    /// Serves `config` in snapshot mode into a byte buffer and decodes it.
    fn snapshot(config: &std::path::Path) -> Result<Vec<FollowFrame>> {
        let mut out = Vec::new();
        follow_serve(config, true, &mut out)?;
        frames(&out)
    }

    #[test]
    fn a_snapshot_serves_hello_then_the_journal_then_complete() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let records = [committed("aa"), committed("bb")];
        let (config, loaded) = served_run(dir.path(), &records)?;
        let lines = records
            .iter()
            .map(|record| record.to_line())
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            snapshot(&config)?,
            vec![
                FollowFrame::Hello {
                    protocol: FOLLOW_PROTOCOL_VERSION,
                    run: loaded.run.id(),
                    format: loaded.run.format.clone(),
                    workers: loaded.execution.workers as u32,
                    holder: None,
                },
                FollowFrame::Records(lines),
                FollowFrame::Complete,
            ]
        );
        Ok(())
    }

    #[test]
    fn a_run_never_started_is_served_as_a_fault() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = served_config(dir.path());
        Store::open(dir.path().join("store"))?;
        let served = snapshot(&config)?;
        assert!(
            matches!(served.as_slice(), [FollowFrame::Fault(message)] if message.contains("never started")),
            "{served:?}"
        );
        Ok(())
    }

    #[test]
    fn a_config_that_does_not_load_is_served_as_a_fault() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let served = snapshot(&dir.path().join("absent.toml"))?;
        assert!(
            matches!(served.as_slice(), [FollowFrame::Fault(_)]),
            "{served:?}"
        );
        Ok(())
    }

    #[test]
    fn the_opening_frame_carries_the_run_lock_holder() -> Result<()> {
        let dir = tempfile::tempdir().expect("temp dir");
        let (config, loaded) = served_run(dir.path(), &[committed("aa")])?;
        let store = Store::open(&loaded.store)?;
        let lock = store.acquire_run_lock(&loaded.run.id())?;
        let served = snapshot(&config)?;
        assert!(
            matches!(
                &served[0],
                FollowFrame::Hello {
                    holder: Some(_),
                    ..
                }
            ),
            "{:?}",
            served[0]
        );
        drop(lock);
        // With the lock released the same snapshot reports a free run.
        assert!(matches!(
            &snapshot(&config)?[0],
            FollowFrame::Hello { holder: None, .. }
        ),);
        Ok(())
    }
}
