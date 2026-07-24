//! Rendering observational stats into one human-readable line.

use sima_scheduler::StatScalar;

/// Renders observational stats into one line: each `name=value` pair joined by
/// a single space, values through `f64`'s `Display`, and — when the family blob
/// is non-empty — a trailing `blob=<len>B` naming its byte length. Empty stats
/// render as the empty string.
///
/// The renderer is generic over every domain: infrastructure knows only names,
/// numbers, and a byte count. `blob_hex` is the family blob as hex, so its byte
/// length is half the string length.
pub(crate) fn render_stats(scalars: &[StatScalar], blob_hex: &str) -> String {
    let mut parts: Vec<String> = scalars
        .iter()
        .map(|scalar| format!("{}={}", scalar.name, scalar.value))
        .collect();
    let blob_len = blob_hex.len() / 2;
    if blob_len > 0 {
        parts.push(format!("blob={blob_len}B"));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar(name: &str, value: f64) -> StatScalar {
        StatScalar {
            name: name.to_string(),
            value,
        }
    }

    #[test]
    fn scalars_render_as_space_joined_name_value_pairs() {
        let line = render_stats(&[scalar("population", 0.5), scalar("activity", 1.0e-4)], "");
        assert_eq!(line, "population=0.5 activity=0.0001");
    }

    #[test]
    fn a_non_empty_blob_appends_its_byte_length() {
        // Four hex characters are two bytes.
        let line = render_stats(&[scalar("population", 0.5)], "aabb");
        assert_eq!(line, "population=0.5 blob=2B");
    }

    #[test]
    fn empty_stats_render_as_the_empty_string() {
        assert_eq!(render_stats(&[], ""), "");
    }

    #[test]
    fn a_non_finite_value_renders_through_display() {
        // A diverged candidate: the value read back from the journal is NaN.
        let line = render_stats(&[scalar("c0.max", f64::NAN)], "");
        assert_eq!(line, "c0.max=NaN");
    }
}
