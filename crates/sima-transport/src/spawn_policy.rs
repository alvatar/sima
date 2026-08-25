//! [`SpawnPolicy`]: what environment and working directory a spawned child
//! receives.
//!
//! Two processes are spawned across the boundary a configured program owns:
//! its domain service and its workers. Both run code sima does not build, so
//! both start from an explicit surface rather than from whatever the
//! orchestrator happens to hold: the baseline environment the platform needs
//! plus the variables the program's config entry names, and an empty working
//! directory of its own. On top of that, a path list may be prepended to, which
//! is how a directory sima put on the machine is read ahead of anything else
//! under that name.
//!
//! Every sima-owned process — a builtin worker, a container runtime client,
//! an ssh client — keeps the orchestrator's environment and working
//! directory: they run in the orchestrator's own trust domain, and the
//! clients need the ambient environment to reach anything.

use std::ffi::OsString;
use std::process::Command;

use sima_core::{Error, Result};
use tempfile::TempDir;

/// Environment variables an explicit surface carries by exact name.
///
/// The program search path, the user's identity and scratch space, and the
/// locale settings a library reads at startup.
const BASELINE_NAMES: [&str; 7] = ["PATH", "HOME", "USER", "LOGNAME", "TMPDIR", "LANG", "TZ"];

/// Environment variable prefixes an explicit surface carries.
///
/// The dynamic loader (`LD_`), the remaining locale settings (`LC_`), the
/// user's directory layout and caches (`XDG_`), and the three GPU stacks a
/// domain program computes on: NVIDIA (`CUDA_`, `NVIDIA_`, `__GL_`), AMD
/// (`ROCM_`, `ROCR_`, `HIP_`, `HSA_`), and Mesa/Vulkan (`VK_`, `MESA_`).
const BASELINE_PREFIXES: [&str; 12] = [
    "LC_", "XDG_", "LD_", "CUDA_", "NVIDIA_", "VK_", "MESA_", "ROCM_", "ROCR_", "HIP_", "HSA_",
    "__GL_",
];

/// How a spawned child's environment and working directory are prepared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpawnPolicy {
    /// The child inherits the parent's environment and working directory.
    /// The policy of every sima-owned process.
    Inherit,
    /// The child receives the baseline allowlist plus the named variables,
    /// and starts in a fresh scratch directory removed at reap.
    /// The policy of every config-routed binary.
    Explicit {
        passthrough: Vec<String>,
        /// Path lists the child reads a directory of sima's from first: each
        /// name's value leads whatever `passthrough` forwarded under it,
        /// joined with `:`. What a directory holds is the caller's business —
        /// this side knows only that a path list is a path list.
        prepend: Vec<(String, OsString)>,
    },
}

impl SpawnPolicy {
    /// Prepares `command` under this policy, drawing the parent's environment
    /// from `vars`. Returns the scratch directory the child runs in, which the
    /// caller holds for exactly as long as the child: dropping it removes the
    /// directory.
    ///
    /// [`SpawnPolicy::Inherit`] leaves `command` untouched and yields no
    /// directory, so an inheriting spawn is the plain one it always was — and
    /// `vars` goes uncalled, so the environment is read only where it is read
    /// from.
    pub(crate) fn apply<I>(
        &self,
        command: &mut Command,
        vars: impl FnOnce() -> I,
    ) -> Result<Option<TempDir>>
    where
        I: IntoIterator<Item = (OsString, OsString)>,
    {
        let SpawnPolicy::Explicit {
            passthrough,
            prepend,
        } = self
        else {
            return Ok(None);
        };
        // Clear first: what the child sees is what this loop puts back, so a
        // credential the orchestrator holds is dropped by never being copied.
        command.env_clear();
        // What each prepended name carried over, so the loop below can put the
        // directory ahead of it rather than replacing it.
        let mut led: Vec<Option<OsString>> = vec![None; prepend.len()];
        for (name, value) in vars() {
            // The names this policy forwards are text, so a name that is not
            // text matches none of them and is dropped like any other name the
            // policy does not forward. Values are forwarded as the bytes they
            // are: what a variable holds is the program's business.
            let Some(name) = name.to_str() else {
                continue;
            };
            if !forwarded(name, passthrough) {
                continue;
            }
            if let Some(index) = prepend.iter().position(|(prepended, _)| prepended == name) {
                led[index] = Some(value.clone());
            }
            command.env(name, value);
        }
        // After the loop, so the prepended directory leads whatever the loop
        // put there — and is the whole value where it put nothing.
        for ((name, directory), led) in prepend.iter().zip(led) {
            let mut value = directory.clone();
            if let Some(led) = led {
                value.push(":");
                value.push(led);
            }
            command.env(name, value);
        }
        let scratch = TempDir::with_prefix("sima-scratch-").map_err(|e| {
            Error::Transport(format!(
                "creating the scratch working directory of a spawned program failed: {e}"
            ))
        })?;
        command.current_dir(scratch.path());
        Ok(Some(scratch))
    }
}

/// Whether an explicit surface carries the variable named `name`: the
/// baseline the platform needs, or a name the program's config entry
/// declared.
fn forwarded(name: &str, passthrough: &[String]) -> bool {
    BASELINE_NAMES.contains(&name)
        || BASELINE_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        || passthrough.iter().any(|declared| declared == name)
}

/// The fixture the two spawn sites share: a program that reports the
/// directory it was spawned in, so a test can look for that directory once
/// the process is reaped.
#[cfg(test)]
pub(crate) mod fixture {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{Duration, Instant};

    /// The argument that makes the program exit without reporting, so the
    /// fixture can exec it harmlessly while it waits for it to become
    /// runnable.
    const PROBE: &str = "--probe";

    /// Writes an executable program named `name` under `dir` whose whole
    /// conversation is the shell `body`, and returns it ready to spawn.
    pub(crate) fn program(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\n\
                 [ \"$1\" = {PROBE} ] && exit 0\n\
                 {body}\n"
            ),
        )
        .expect("write the program");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("make the program executable");
        }
        await_runnable(&path);
        path
    }

    /// Writes an executable program under `dir` that records its working
    /// directory at `report` and exits at once. Exiting is what the caller
    /// wants: the spawn fails at the handshake, which is the path that reaps
    /// the child, and by then the report is written.
    pub(crate) fn cwd_reporting_program(dir: &Path, report: &Path) -> PathBuf {
        program(
            dir,
            "report-cwd.sh",
            &format!("pwd > {}\nexit 0", report.display()),
        )
    }

    /// Waits until the just-written program can be executed.
    ///
    /// Writing a program and running it within one multithreaded process
    /// races the kernel's rule against exec'ing a file open for writing: a
    /// spawn on another thread carries the fresh descriptor across its own
    /// fork, and every exec of the file is refused with `ETXTBSY` until that
    /// child has exec'd. Probing until one exec succeeds is what makes the
    /// fixture deterministic. Only a test writes the program it spawns; the
    /// run path spawns programs that were already on disk.
    fn await_runnable(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match Command::new(path).arg(PROBE).status() {
                Ok(status) if status.success() => return,
                outcome => assert!(
                    Instant::now() < deadline,
                    "{} never became runnable: {outcome:?}",
                    path.display()
                ),
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The directory the program recorded at `report`.
    pub(crate) fn reported_cwd(report: &Path) -> PathBuf {
        PathBuf::from(
            std::fs::read_to_string(report)
                .expect("the program reported its working directory")
                .trim(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};

    use super::*;

    /// A parent environment holding one credential, one baseline name, one
    /// prefixed name, and two names an entry may declare.
    fn parent_env() -> Vec<(OsString, OsString)> {
        [
            ("VAST_API_KEY", "secret"),
            ("PATH", "/usr/bin"),
            ("CUDA_VISIBLE_DEVICES", "0"),
            ("ACME_ASSETS", "/opt/acme/assets"),
            ("ACME_SECRET", "unnamed"),
        ]
        .into_iter()
        .map(|(name, value)| (OsString::from(name), OsString::from(value)))
        .collect()
    }

    /// The environment `policy` leaves on a fresh command over
    /// [`parent_env`], as name to value; a removal is a name mapped to `None`.
    fn applied_env(policy: &SpawnPolicy) -> BTreeMap<String, Option<String>> {
        let mut command = Command::new("/bin/true");
        policy
            .apply(&mut command, parent_env)
            .expect("apply the policy");
        command
            .get_envs()
            .map(|(name, value)| {
                (
                    name.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect()
    }

    /// An explicit-surface policy declaring `passthrough`.
    fn explicit(passthrough: &[&str]) -> SpawnPolicy {
        SpawnPolicy::Explicit {
            passthrough: passthrough.iter().map(|name| name.to_string()).collect(),
            prepend: Vec::new(),
        }
    }

    /// An explicit-surface policy declaring `passthrough` and prepending
    /// `prepend` to the path lists it names.
    fn prepending(passthrough: &[&str], prepend: &[(&str, &str)]) -> SpawnPolicy {
        SpawnPolicy::Explicit {
            passthrough: passthrough.iter().map(|name| name.to_string()).collect(),
            prepend: prepend
                .iter()
                .map(|(name, value)| ((*name).to_string(), OsString::from(value)))
                .collect(),
        }
    }

    /// The environment `policy` leaves on a fresh command over `vars`.
    fn applied_over(
        policy: &SpawnPolicy,
        vars: Vec<(OsString, OsString)>,
    ) -> BTreeMap<String, Option<String>> {
        let mut command = Command::new("/bin/true");
        policy
            .apply(&mut command, || vars)
            .expect("apply the policy");
        command
            .get_envs()
            .map(|(name, value)| {
                (
                    name.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect()
    }

    /// A parent environment holding `PYTHONPATH`, the path list a vended SDK is
    /// prepended to.
    fn parent_with_python_path() -> Vec<(OsString, OsString)> {
        vec![
            (OsString::from("PYTHONPATH"), OsString::from("/site")),
            (OsString::from("PATH"), OsString::from("/usr/bin")),
        ]
    }

    /// The working directory `policy` sets, with the scratch directory kept
    /// alive so the path still exists when the caller looks at it.
    fn applied_cwd(policy: &SpawnPolicy) -> (Option<PathBuf>, Option<TempDir>) {
        let mut command = Command::new("/bin/true");
        let scratch = policy
            .apply(&mut command, parent_env)
            .expect("apply the policy");
        (command.get_current_dir().map(Path::to_path_buf), scratch)
    }

    #[test]
    fn an_inheriting_spawn_touches_neither_the_environment_nor_the_directory() {
        // The sima-owned path is the plain spawn it always was: nothing is
        // cleared, nothing is set, and the child starts where the parent is.
        // The environment is not even read — the panicking source proves it,
        // since an inheriting spawn has no use for a copy of it.
        let mut command = Command::new("/bin/true");
        let scratch = SpawnPolicy::Inherit
            .apply(&mut command, || -> Vec<(OsString, OsString)> {
                panic!("an inheriting spawn reads no environment")
            })
            .expect("apply the policy");
        assert!(scratch.is_none(), "an inheriting spawn needs no scratch");
        assert_eq!(command.get_envs().count(), 0);
        assert_eq!(command.get_current_dir(), None);
    }

    #[test]
    fn an_explicit_spawn_drops_a_credential_the_orchestrator_holds() {
        // The measure the explicit surface exists for: a provider credential
        // in the orchestrator's environment matches nothing on the baseline,
        // so a foreign program never sees it. The baseline name beside it is
        // what makes the absence mean something — an environment that
        // forwarded nothing at all would drop the credential too.
        let env = applied_env(&explicit(&[]));
        assert!(!env.contains_key("VAST_API_KEY"), "{env:?}");
        assert!(env.contains_key("PATH"), "{env:?}");
    }

    #[test]
    fn an_explicit_spawn_forwards_the_baseline() {
        let env = applied_env(&explicit(&[]));
        assert_eq!(
            env.get("PATH"),
            Some(&Some("/usr/bin".to_string())),
            "{env:?}"
        );
        assert_eq!(
            env.get("CUDA_VISIBLE_DEVICES"),
            Some(&Some("0".to_string())),
            "the GPU stack the program computes on: {env:?}"
        );
    }

    #[test]
    fn every_baseline_name_and_prefix_crosses() {
        // The whole list, checked as a list: a name dropped from it silently
        // would leave a program without the platform it runs on.
        let names: Vec<String> = BASELINE_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .chain(
                BASELINE_PREFIXES
                    .iter()
                    .map(|prefix| format!("{prefix}SOMETHING")),
            )
            .collect();
        let vars: Vec<(OsString, OsString)> = names
            .iter()
            .map(|name| (OsString::from(name), OsString::from("value")))
            .collect();
        let mut command = Command::new("/bin/true");
        explicit(&[])
            .apply(&mut command, || vars)
            .expect("apply the policy");
        let forwarded: Vec<String> = command
            .get_envs()
            .filter_map(|(name, value)| value.map(|_| name.to_string_lossy().into_owned()))
            .collect();
        for name in &names {
            assert!(forwarded.contains(name), "{name} crosses: {forwarded:?}");
        }
    }

    #[test]
    fn an_explicit_spawn_forwards_a_declared_name_and_drops_an_undeclared_one() {
        // What the entry names crosses; what it does not name is a variable
        // the program has no claim on.
        let env = applied_env(&explicit(&["ACME_ASSETS"]));
        assert_eq!(
            env.get("ACME_ASSETS"),
            Some(&Some("/opt/acme/assets".to_string())),
            "{env:?}"
        );
        assert!(!env.contains_key("ACME_SECRET"), "{env:?}");
    }

    #[test]
    fn a_declared_name_absent_from_the_parent_is_absent_in_the_child() {
        // Naming a variable forwards it; it does not invent it. The program
        // owns its own defaults.
        let env = applied_env(&explicit(&["ACME_LICENSE_PATH", "ACME_ASSETS"]));
        assert!(!env.contains_key("ACME_LICENSE_PATH"), "{env:?}");
        // The second declared name, which the parent does hold, crossed: what
        // the entry declares is honoured, and only the absent one is absent.
        assert!(env.contains_key("ACME_ASSETS"), "{env:?}");
    }

    #[test]
    fn a_prepended_path_leads_the_value_the_policy_forwards() {
        // The contract a vended package rests on: the child reads the
        // prepended directory first, and what the machine already had is still
        // behind it. `:` is the separator every path list on this platform
        // uses.
        let env = applied_over(
            &prepending(&["PYTHONPATH"], &[("PYTHONPATH", "/vended")]),
            parent_with_python_path(),
        );
        assert_eq!(
            env.get("PYTHONPATH"),
            Some(&Some("/vended:/site".to_string())),
            "{env:?}"
        );
    }

    #[test]
    fn a_prepended_path_with_nothing_to_lead_is_the_whole_value() {
        // Nothing forwarded the name — the entry does not declare it, or the
        // parent does not hold it — so the child's path list is the prepended
        // directory alone, with no separator and no empty component.
        let undeclared = applied_over(
            &prepending(&[], &[("PYTHONPATH", "/vended")]),
            parent_with_python_path(),
        );
        assert_eq!(
            undeclared.get("PYTHONPATH"),
            Some(&Some("/vended".to_string())),
            "a name the entry does not declare carries nothing over: {undeclared:?}"
        );

        let unheld = applied_over(
            &prepending(&["PYTHONPATH"], &[("PYTHONPATH", "/vended")]),
            vec![(OsString::from("PATH"), OsString::from("/usr/bin"))],
        );
        assert_eq!(
            unheld.get("PYTHONPATH"),
            Some(&Some("/vended".to_string())),
            "and neither does a name the parent does not hold: {unheld:?}"
        );
    }

    #[test]
    fn prepending_leaves_every_other_variable_as_it_was() {
        // The prepend is one name's business: the baseline crosses beside it,
        // and a credential the policy drops stays dropped.
        let env = applied_over(
            &prepending(&["ACME_ASSETS"], &[("PYTHONPATH", "/vended")]),
            parent_env(),
        );
        assert_eq!(
            env.get("PATH"),
            Some(&Some("/usr/bin".to_string())),
            "{env:?}"
        );
        assert_eq!(
            env.get("ACME_ASSETS"),
            Some(&Some("/opt/acme/assets".to_string())),
            "{env:?}"
        );
        assert!(!env.contains_key("VAST_API_KEY"), "{env:?}");
    }

    #[cfg(unix)]
    #[test]
    fn a_forwarded_value_that_is_not_text_is_prepended_to_as_the_bytes_it_is() {
        // A path list is bytes, like every other value: what the machine holds
        // is carried across untouched behind the prepended directory.
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let opaque = OsString::from_vec(vec![b'/', 0xff]);
        let mut command = Command::new("/bin/true");
        prepending(&["PYTHONPATH"], &[("PYTHONPATH", "/vended")])
            .apply(&mut command, || {
                vec![(OsString::from("PYTHONPATH"), opaque.clone())]
            })
            .expect("apply the policy");
        let value = command
            .get_envs()
            .find(|(name, _)| *name == OsStr::new("PYTHONPATH"))
            .and_then(|(_, value)| value)
            .expect("the prepended name is set");
        let mut expected = b"/vended:".to_vec();
        expected.extend(opaque.as_bytes());
        assert_eq!(value.as_bytes(), expected);
    }

    #[test]
    fn an_explicit_spawn_starts_in_a_fresh_empty_directory() {
        let (cwd, scratch) = applied_cwd(&explicit(&[]));
        let scratch = scratch.expect("an explicit spawn has a scratch directory");
        let cwd = cwd.expect("an explicit spawn sets its working directory");
        assert_eq!(cwd, scratch.path());
        assert_eq!(
            std::fs::read_dir(&cwd)
                .expect("read the scratch directory")
                .count(),
            0,
            "a relative read finds nothing: {}",
            cwd.display()
        );
    }

    #[test]
    fn two_explicit_spawns_start_in_two_directories() {
        // A respawn gets a fresh directory, so nothing one process left
        // behind is visible to the next.
        let (first, first_scratch) = applied_cwd(&explicit(&[]));
        let (second, second_scratch) = applied_cwd(&explicit(&[]));
        assert_ne!(first, second);
        drop((first_scratch, second_scratch));
    }

    #[test]
    fn the_scratch_directory_is_removed_when_it_is_dropped() {
        // The holder's lifetime is the child's: the directory the program ran
        // in is gone once the process is reaped.
        let (cwd, scratch) = applied_cwd(&explicit(&[]));
        let cwd = cwd.expect("an explicit spawn sets its working directory");
        assert!(cwd.is_dir());
        drop(scratch);
        assert!(!cwd.exists(), "{} outlived its holder", cwd.display());
    }

    #[test]
    fn a_name_is_matched_whole_never_as_a_prefix_of_a_baseline_name() {
        // `PATH` is an exact name, so a variable merely starting with it is
        // not the platform's search path — while `PATH` itself still crosses.
        let vars = [
            (OsString::from("PATHOLOGICAL"), OsString::from("value")),
            (OsString::from("PATH"), OsString::from("/usr/bin")),
        ];
        let mut command = Command::new("/bin/true");
        explicit(&[])
            .apply(&mut command, || vars)
            .expect("apply the policy");
        let names: Vec<String> = command
            .get_envs()
            .map(|(name, _)| name.to_string_lossy().into_owned())
            .collect();
        assert!(!names.contains(&"PATHOLOGICAL".to_string()), "{names:?}");
        assert!(names.contains(&"PATH".to_string()), "{names:?}");
    }

    #[cfg(unix)]
    #[test]
    fn a_name_that_is_not_text_is_skipped_and_the_spawn_goes_on() {
        // An environment is bytes, and nothing obliges the orchestrator's own
        // to be text. Such a name matches neither the baseline nor a declared
        // name, both of which are text, so it is dropped like any other name
        // the policy does not forward — and the variables beside it still
        // cross, which is what makes the drop a decision rather than a stop.
        use std::os::unix::ffi::OsStringExt;

        let vars = vec![
            (
                OsString::from_vec(vec![0xff, 0xfe]),
                OsString::from("value"),
            ),
            (OsString::from("PATH"), OsString::from("/usr/bin")),
        ];
        let mut command = Command::new("/bin/true");
        explicit(&[])
            .apply(&mut command, || vars)
            .expect("apply the policy");
        let forwarded: Vec<(OsString, Option<OsString>)> = command
            .get_envs()
            .map(|(name, value)| (name.to_os_string(), value.map(OsStr::to_os_string)))
            .collect();
        assert_eq!(
            forwarded,
            vec![(OsString::from("PATH"), Some(OsString::from("/usr/bin")))],
            "the untranslatable name is the only one missing"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_value_that_is_not_text_crosses_as_the_bytes_it_is() {
        // Only the name is matched against the policy's lists, so a forwarded
        // variable carries whatever it holds. A path on a filesystem that
        // never promised an encoding is the ordinary case.
        use std::os::unix::ffi::OsStringExt;

        let opaque = OsString::from_vec(vec![b'/', 0xff, b'/', 0xfe]);
        let vars = vec![(OsString::from("PATH"), opaque.clone())];
        let mut command = Command::new("/bin/true");
        explicit(&[])
            .apply(&mut command, || vars)
            .expect("apply the policy");
        let value = command
            .get_envs()
            .find(|(name, _)| *name == OsStr::new("PATH"))
            .and_then(|(_, value)| value);
        assert_eq!(value, Some(opaque.as_os_str()));
    }
}
