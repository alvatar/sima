//! Fetching files from a remote command tree over a tar stream.

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use sima_core::{Error, Result, own_process_group};

/// Fetches `patterns` from the remote payload tree and its log into `local`.
/// The remote shell expands each pattern; unmatched patterns warn on stderr.
pub(crate) fn fetch_over(
    shell_argv: &[String],
    remote_root: &str,
    patterns: &[String],
    local: &Path,
    narration: &mut dyn FnMut(&str),
) -> Result<()> {
    std::fs::create_dir_all(local).map_err(|source| Error::Io {
        path: local.to_path_buf(),
        source,
    })?;
    let (program, args) = shell_argv.split_first().expect("a shell argv");
    let mut remote = own_process_group(&mut Command::new(program))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::Transport(format!("cannot start remote fetch shell: {e}")))?;
    remote
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(remote_script(remote_root, patterns).as_bytes())
        .map_err(|e| Error::Transport(format!("cannot send remote fetch command: {e}")))?;
    let stream = remote.stdout.take().expect("piped stdout");
    let mut errors = remote.stderr.take().expect("piped stderr");
    let diagnostics = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        errors.read_to_end(&mut bytes).map(|_| bytes)
    });
    let status = own_process_group(&mut Command::new("tar"))
        .args(["-xf", "-", "-C"])
        .arg(local)
        .stdin(Stdio::from(stream))
        .status()
        .map_err(|e| Error::Transport(format!("cannot unpack fetched files: {e}")))?;
    let remote_status = remote
        .wait()
        .map_err(|e| Error::Transport(format!("cannot reap remote fetch shell: {e}")))?;
    let diagnostics = diagnostics
        .join()
        .map_err(|_| Error::Transport("remote fetch diagnostic reader panicked".to_string()))?
        .map_err(|e| Error::Transport(format!("cannot read remote fetch diagnostics: {e}")))?;
    let diagnostics = String::from_utf8_lossy(&diagnostics);
    if !remote_status.success() {
        return Err(Error::Transport(format!(
            "remote output archive exited with {remote_status}{}",
            if diagnostics.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", diagnostics.trim())
            }
        )));
    }
    if !status.success() {
        return Err(Error::Transport(format!(
            "unpacking remote output archive exited with {status}"
        )));
    }
    for line in diagnostics.lines() {
        narration(line);
    }
    Ok(())
}

/// The remote script that expands output globs and writes one tar archive.
fn remote_script(remote_root: &str, patterns: &[String]) -> String {
    let mut script = format!("set -e\njob={}\ncd \"$job/payload\"\nset --\n", remote_root);
    for pattern in patterns {
        script.push_str(&format!(
            "pattern={}\nmatched=\nfor file in $pattern; do\n  [ -e \"$file\" ] || continue\n  set -- \"$@\" \"./$file\"\n  matched=yes\ndone\n[ -n \"$matched\" ] || echo \"warning: output glob matched nothing: $pattern\" >&2\n",
            shell_quote(pattern)
        ));
    }
    script.push_str("tar -cf - -C \"$job/payload\" \"$@\" -C \"$job\" exec.log\n");
    script
}

/// Quotes one string as one POSIX shell word.
pub(crate) fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn fetch_script_expands_each_glob_at_the_payload_root_and_always_adds_the_log() {
        let script = remote_script("~/sima/exec/job", &["reports/*.html".to_string()]);
        assert!(script.contains("cd \"$job/payload\""));
        assert!(script.contains("reports/*.html"));
        assert!(script.contains("\"./$file\""));
        assert!(script.contains("exec.log"));
    }

    #[test]
    fn tar_fetch_keeps_payload_relative_paths_and_the_exec_log() -> sima_core::Result<()> {
        let remote = tempfile::tempdir().expect("remote");
        let payload = remote.path().join("payload/reports");
        fs::create_dir_all(&payload).expect("payload dirs");
        fs::write(payload.join("index.html"), "report").expect("report");
        fs::write(remote.path().join("payload/other.bin"), "other").expect("other");
        fs::write(remote.path().join("exec.log"), "command log").expect("log");
        let local = tempfile::tempdir().expect("local");
        let mut warnings = Vec::new();
        fetch_over(
            &["/bin/sh".to_string()],
            remote.path().to_str().expect("utf8 path"),
            &["reports/*.html".to_string(), "missing/*.pfm".to_string()],
            local.path(),
            &mut |line| warnings.push(line.to_string()),
        )?;
        assert_eq!(
            fs::read_to_string(local.path().join("reports/index.html")).expect("fetched report"),
            "report"
        );
        assert_eq!(
            fs::read_to_string(local.path().join("exec.log")).expect("fetched log"),
            "command log"
        );
        assert!(!local.path().join("other.bin").exists());
        assert_eq!(
            warnings,
            ["warning: output glob matched nothing: missing/*.pfm"]
        );
        Ok(())
    }
}
