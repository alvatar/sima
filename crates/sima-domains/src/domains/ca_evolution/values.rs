//! The validation a model's own scalar values go through.
//!
//! A genome and an ignition are both bundles of `f32` a config states and a
//! content id is taken over, so what they admit is one rule rather than two:
//! the value must be finite, and it must carry a positive sign.

use sima_core::{Error, Result};

/// Validates a scalar: finite with positive sign.
///
/// Admits `+0.0` and rejects NaN, both infinities, negatives, and `-0.0`.
/// Rejecting `-0.0` is what keeps one value to one byte image: `-0.0 == 0.0`
/// numerically while their bit patterns differ, so admitting both would give
/// numerically identical values distinct content ids.
///
/// `subject` names what the value belongs to and `name` names the field, so a
/// refusal says which of a model's values was wrong.
pub(crate) fn finite_sign_positive(subject: &str, name: &str, value: f32) -> Result<f32> {
    if value.is_finite() && value.is_sign_positive() {
        Ok(value)
    } else {
        Err(Error::Validation(format!(
            "{subject} {name} must be a finite value with positive sign, got {value}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_finite_positive_value_including_zero_is_admitted() {
        for value in [0.0, 1.0, f32::MIN_POSITIVE, f32::MAX] {
            assert_eq!(
                finite_sign_positive("subject", "field", value).expect("admitted"),
                value
            );
        }
    }

    #[test]
    fn negative_zero_is_refused_so_one_value_has_one_byte_image() {
        // `-0.0 == 0.0` numerically and their bit patterns differ, so admitting
        // both would give numerically identical values distinct content ids.
        assert!(finite_sign_positive("subject", "field", -0.0).is_err());
    }

    #[test]
    fn nothing_non_finite_or_negative_is_admitted() {
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -1.0] {
            assert!(
                finite_sign_positive("subject", "field", value).is_err(),
                "{value} must be refused"
            );
        }
    }

    #[test]
    fn a_refusal_names_the_subject_and_the_field() {
        let Err(Error::Validation(message)) =
            finite_sign_positive("gray_scott genome", "feed", f32::NAN)
        else {
            panic!("expected a validation error");
        };
        assert!(message.contains("gray_scott genome"), "{message}");
        assert!(message.contains("feed"), "{message}");
    }
}
