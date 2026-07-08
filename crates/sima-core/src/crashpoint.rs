//! Crash-injection points for durability testing.
//!
//! Store and scheduler code plants named points at durability-critical
//! spots by calling [`crashpoint`]. A test harness arms one point through
//! the `SIMA_CRASHPOINT` environment variable and the process SIGKILLs
//! itself the moment execution reaches it — an unmaskable death, the
//! ground truth crash-recovery must survive.
//!
//! The facility is gated behind the `crash-injection` cargo feature.
//! Without the feature every call compiles to an empty inline function,
//! so production builds carry no trace of it.

/// With the crash-injection feature: SIGKILLs the process when the
/// `SIMA_CRASHPOINT` env var arms this point. Armed form: `name` or
/// `name:k` — the k-th hit fires (k >= 1, default 1). Without the
/// feature this is a no-op that compiles to nothing.
#[cfg(feature = "crash-injection")]
pub fn crashpoint(name: &str) {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The armed point, parsed from the env var once per process.
    static ARMED: OnceLock<Option<Armed>> = OnceLock::new();
    /// Hits of the armed point's name so far, process-wide.
    static HITS: AtomicU64 = AtomicU64::new(0);

    let armed = ARMED.get_or_init(|| std::env::var("SIMA_CRASHPOINT").ok().map(|s| parse(&s)));
    let Some(armed) = armed else { return };
    if armed.name != name {
        return;
    }
    let hit = HITS.fetch_add(1, Ordering::Relaxed) + 1;
    if fires(armed, name, hit) {
        // SIGKILL to our own pid: unmaskable, no destructors, no unwinding —
        // exactly the death the crash-recovery guarantees are stated against.
        unsafe {
            libc::kill(libc::getpid(), libc::SIGKILL);
        }
    }
}

/// With the crash-injection feature: SIGKILLs the process when the
/// `SIMA_CRASHPOINT` env var arms this point. Armed form: `name` or
/// `name:k` — the k-th hit fires (k >= 1, default 1). Without the
/// feature this is a no-op that compiles to nothing.
#[cfg(not(feature = "crash-injection"))]
#[inline(always)]
pub fn crashpoint(_name: &str) {}

/// A parsed `SIMA_CRASHPOINT` value: the armed point's name and the hit
/// count at which it fires.
#[cfg(feature = "crash-injection")]
struct Armed {
    name: String,
    k: u64,
}

/// Parses the value of the `SIMA_CRASHPOINT` environment variable —
/// `name` or `name:k` — into an [`Armed`]. A malformed or zero suffix
/// falls back to k = 1: arming is a test-harness act, and dying on the
/// first hit is the least surprising reading of a bad spec.
#[cfg(feature = "crash-injection")]
fn parse(spec: &str) -> Armed {
    let (name, suffix) = match spec.split_once(':') {
        Some((name, suffix)) => (name, Some(suffix)),
        None => (spec, None),
    };
    let k = suffix
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&k| k >= 1)
        .unwrap_or(1);
    Armed {
        name: name.to_string(),
        k,
    }
}

/// Whether the `hit`-th hit (1-based) of point `name` fires the armed
/// point: names match and the hit count is exactly k.
#[cfg(feature = "crash-injection")]
fn fires(armed: &Armed, name: &str, hit: u64) -> bool {
    armed.name == name && hit == armed.k
}

#[cfg(test)]
mod tests {
    /// The call is available in every feature configuration; without the
    /// env var armed it must return normally.
    #[test]
    fn unarmed_call_is_a_no_op() {
        super::crashpoint("any.point");
    }

    #[cfg(feature = "crash-injection")]
    mod armed {
        use crate::crashpoint::{fires, parse};

        #[test]
        fn bare_name_parses_with_k_one() {
            let armed = parse("commit.after-object");
            assert_eq!(armed.name, "commit.after-object");
            assert_eq!(armed.k, 1);
        }

        #[test]
        fn suffixed_name_parses_the_hit_count() {
            let armed = parse("object.mid-write:3");
            assert_eq!(armed.name, "object.mid-write");
            assert_eq!(armed.k, 3);
        }

        #[test]
        fn zero_suffix_falls_back_to_k_one() {
            let armed = parse("lease.held:0");
            assert_eq!(armed.name, "lease.held");
            assert_eq!(armed.k, 1);
        }

        #[test]
        fn non_numeric_suffix_falls_back_to_k_one() {
            let armed = parse("lease.held:x");
            assert_eq!(armed.name, "lease.held");
            assert_eq!(armed.k, 1);
        }

        #[test]
        fn mismatched_name_never_fires() {
            let armed = parse("finalize.pre-write");
            assert!(!fires(&armed, "commit.after-object", 1));
            assert!(!fires(&armed, "commit.after-object", u64::MAX));
        }

        #[test]
        fn fires_exactly_on_the_kth_hit() {
            let armed = parse("object.mid-write:3");
            assert!(!fires(&armed, "object.mid-write", 1));
            assert!(!fires(&armed, "object.mid-write", 2));
            assert!(fires(&armed, "object.mid-write", 3));
            assert!(!fires(&armed, "object.mid-write", 4));
        }

        #[test]
        fn default_k_fires_on_the_first_hit() {
            let armed = parse("lease.held");
            assert!(fires(&armed, "lease.held", 1));
            assert!(!fires(&armed, "lease.held", 2));
        }
    }
}
