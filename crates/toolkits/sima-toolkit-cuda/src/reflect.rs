//! What a committed PTX module declares about an entry point, read from the
//! text itself.
//!
//! The driver's module loader answers for none of this: it hands back a
//! function handle and takes whatever arguments a launch pushes, so a launch
//! with the wrong number of them is undefined behaviour rather than an error.
//! PTX states both facts in plain text — the parameter list, and the block
//! dimensions `__launch_bounds__` fixed — so reading them here is what lets a
//! kernel build and a dispatch check themselves, and it needs no device.
//!
//! This is the CUDA counterpart of the WGSL toolkit's reflection over a parsed
//! naga module: same two questions, answered from the artifact each backend
//! actually loads.

use sima_core::{Error, Result};

/// What an entry point declares.
pub(crate) struct EntrySignature {
    /// Parameters the entry point takes, in declaration order. Every kernel
    /// here declares pointers, so this is the buffer count a launch binds.
    pub params: usize,
    /// The block dimensions `.maxntid` fixes, present when the source declared
    /// `__launch_bounds__`.
    pub max_block: Option<[u32; 3]>,
}

/// Reads what `entry` declares out of `ptx`.
pub(crate) fn entry_signature(ptx: &str, entry: &str) -> Result<EntrySignature> {
    let declaration = declaration_of(ptx, entry).ok_or_else(|| {
        Error::Backend(format!(
            "no entry point '{entry}' is declared in this PTX module"
        ))
    })?;
    // A parameter list holds no nested parentheses, so the first closing one
    // ends it.
    let open = declaration.find('(');
    let close = declaration.find(')');
    let (Some(open), Some(close)) = (open, close) else {
        return Err(Error::Backend(format!(
            "the declaration of entry point '{entry}' carries no parameter list"
        )));
    };
    if close < open {
        return Err(Error::Backend(format!(
            "the declaration of entry point '{entry}' has an unbalanced parameter list"
        )));
    }
    Ok(EntrySignature {
        params: declaration[open..close].matches(".param").count(),
        max_block: max_block(&declaration[close..], entry)?,
    })
}

/// The text of `entry`'s declaration, from its name to the opening brace of its
/// body.
fn declaration_of<'a>(ptx: &'a str, entry: &str) -> Option<&'a str> {
    let mut rest = ptx;
    loop {
        let at = rest.find(".entry")?;
        let after = &rest[at + ".entry".len()..];
        // The name must match whole: `main` is not the declaration of
        // `main_kernel`.
        if let Some(tail) = after.trim_start().strip_prefix(entry)
            && tail.starts_with(|c: char| c == '(' || c.is_whitespace())
        {
            return Some(match tail.find('{') {
                Some(body) => &tail[..body],
                None => tail,
            });
        }
        rest = after;
    }
}

/// The three dimensions of a `.maxntid` directive in `text`, if it carries one.
fn max_block(text: &str, entry: &str) -> Result<Option<[u32; 3]>> {
    let Some(at) = text.find(".maxntid") else {
        return Ok(None);
    };
    let line = text[at + ".maxntid".len()..]
        .lines()
        .next()
        .unwrap_or_default();
    let dimensions: Vec<u32> = line
        .split(',')
        .filter_map(|field| field.trim().parse().ok())
        .collect();
    match dimensions[..] {
        [x, y, z] => Ok(Some([x, y, z])),
        _ => Err(Error::Backend(format!(
            "entry point '{entry}' declares `.maxntid{line}`, which is not three dimensions"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped smoke kernel's committed PTX: three pointer parameters and a
    /// declared block width of 64.
    const SMOKE_PTX: &str = include_str!("../kernels/smoke.ptx");

    #[test]
    fn the_committed_ptx_declares_three_parameters_and_its_block_width() {
        let signature = entry_signature(SMOKE_PTX, "main_kernel").expect("the entry point");
        assert_eq!(signature.params, 3);
        assert_eq!(signature.max_block, Some([64, 1, 1]));
    }

    #[test]
    fn an_entry_point_the_module_does_not_declare_is_rejected() {
        assert!(matches!(
            entry_signature(SMOKE_PTX, "no_such_entry"),
            Err(Error::Backend(_))
        ));
    }

    #[test]
    fn a_name_is_matched_whole() {
        // `main` is a prefix of the shipped entry point's name; matching on the
        // prefix would reflect the wrong declaration and read a parameter count
        // that belongs to another kernel.
        assert!(matches!(
            entry_signature(SMOKE_PTX, "main"),
            Err(Error::Backend(_))
        ));
    }

    #[test]
    fn an_entry_point_taking_nothing_declares_no_parameters() {
        let ptx = ".visible .entry idle()\n.maxntid 32, 1, 1\n{\n\tret;\n}\n";
        let signature = entry_signature(ptx, "idle").expect("the entry point");
        assert_eq!(signature.params, 0);
        assert_eq!(signature.max_block, Some([32, 1, 1]));
    }

    #[test]
    fn an_entry_point_without_launch_bounds_declares_no_block() {
        // `__launch_bounds__` is optional in CUDA C, so the directive can be
        // absent; the kernel build then has only the device limit to check.
        let ptx = ".visible .entry idle(\n\t.param .u64 idle_param_0\n)\n{\n\tret;\n}\n";
        let signature = entry_signature(ptx, "idle").expect("the entry point");
        assert_eq!(signature.params, 1);
        assert_eq!(signature.max_block, None);
    }

    #[test]
    fn a_module_with_no_entry_point_at_all_is_rejected() {
        assert!(matches!(
            entry_signature("this is not PTX", "main_kernel"),
            Err(Error::Backend(_))
        ));
    }
}
