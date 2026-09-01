//! The identity translation: `[search]` into the [`SearchConfig`] whose hash is the
//! search id.
//!
//! This is the only section that enters a search's identity, so it is the only one
//! whose translation can change what a search is. Everything it produces comes
//! from the file: the format and generator ids as written, and each domain's
//! own section translated by the source that owns its keys.

use std::num::NonZeroU64;
use std::path::Path;

use sima_core::{Error, Result};
use sima_model::{FormatId, GeneratorConfig, GeneratorId, SearchConfig};

use super::file::SearchSection;
use crate::domain_registry::{DomainRegistry, section_text};

/// Translates the `[search]` section into the canonical [`SearchConfig`] whose hash is
/// the search id, dispatching the generator and domain translations that own the
/// opaque tables.
pub(super) fn resolve_search(
    path: &Path,
    section: SearchSection,
    domains: &DomainRegistry,
) -> Result<SearchConfig> {
    let root_seed = u64::try_from(section.root_seed).map_err(|_| {
        Error::Validation(format!(
            "{}: root_seed must be non-negative, got {}",
            path.display(),
            section.root_seed
        ))
    })?;
    let segments = section
        .segments
        .map(|value| {
            u64::try_from(value)
                .ok()
                .and_then(NonZeroU64::new)
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "{}: segments must be at least 1, got {value}",
                        path.display()
                    ))
                })
        })
        .transpose()?;
    let format = FormatId::new(section.format)?;
    let generator_id = GeneratorId::new(section.generator.id)?;
    // Identity flows through the code the ids name: the generator and the
    // domain turn their sections into the canonical bytes the model hashes.
    // Each section crosses as its own text, so a program outside this
    // workspace parses it with a TOML of its own.
    let source = domains.source(&format);
    let generator_params = source
        .generator(&generator_id, &format)?
        .translate_config(&section_text(&section.generator.rest)?)?;
    let params =
        source.translate_config(&format, &section_text(&section.params)?, segments.is_some())?;
    Ok(SearchConfig {
        root_seed,
        segments,
        format,
        generator: GeneratorConfig {
            id: generator_id,
            params: generator_params,
        },
        params,
    })
}
