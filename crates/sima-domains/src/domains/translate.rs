//! Shared helpers for the domains' TOML config translations.

use sima_core::{Error, Result};

/// Rejects any key of `table` outside `known`, naming the key, the domain
/// `id`, and the `section` it appeared in.
pub(crate) fn reject_unknown_keys(
    id: &str,
    table: &toml::Table,
    known: &[&str],
    section: &str,
) -> Result<()> {
    for key in table.keys() {
        if !known.contains(&key.as_str()) {
            return Err(Error::Validation(format!(
                "{id} {section} config does not define the key {key:?}"
            )));
        }
    }
    Ok(())
}
