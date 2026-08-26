//! How a child sima spawns stands relative to the terminal's signals.

use std::process::Command;

/// Puts the child `command` spawns in a process group of its own.
///
/// A terminal delivers Ctrl-C to every process in its foreground group, so a
/// child left in sima's group is signalled directly and dies where it stands:
/// an ssh transport ends mid-frame and its stream reads as a fault, a worker
/// dies mid-attempt, and a program prints its own interruption over the
/// operator's terminal. sima is the one interrupt handler — it winds every
/// child down itself, through the paths that drain and commit — so each child
/// is spawned into a group the terminal does not reach.
///
/// Every child of this process goes through here, whatever it is: a worker, a
/// program serving a domain, an ssh, a container runtime client, an install
/// script.
pub fn own_process_group(command: &mut Command) -> &mut Command {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Zero makes the child's own pid its group id, so it leads a group
        // nothing else is in.
        command.process_group(0);
    }
    command
}

// What the rule states holds on unix and nowhere else, so what exercises it is
// compiled there: everything below asks the system for a process group.
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::Stdio;

    /// The process group `pid` belongs to, as the system reports it.
    fn group_of(pid: u32) -> u32 {
        let group = unsafe { libc::getpgid(pid as libc::pid_t) };
        assert!(group >= 0, "the process table knows pid {pid}");
        group as u32
    }

    /// Spawns a child that stays up long enough to be looked at, optionally
    /// through [`own_process_group`], and answers its pid and the group it
    /// landed in.
    fn spawned_into(own_group: bool) -> (u32, u32) {
        // A read of a piped stdin nothing writes to: the child waits without
        // spending anything, and closing the pipe is what ends it.
        let mut command = Command::new("/bin/cat");
        command.stdin(Stdio::piped()).stdout(Stdio::null());
        if own_group {
            own_process_group(&mut command);
        }
        let mut child = command.spawn().expect("spawn the child");
        let placed = (child.id(), group_of(child.id()));
        drop(child.stdin.take());
        child.wait().expect("reap the child");
        placed
    }

    #[test]
    fn a_child_spawned_through_it_leads_a_group_of_its_own() {
        // Its group id is its own pid, so a signal to the group this process
        // is in reaches nothing of the child's.
        let (pid, group) = spawned_into(true);
        assert_eq!(group, pid, "the child leads its own group");
        assert_ne!(
            group,
            group_of(std::process::id()),
            "which is not this process's"
        );
    }

    #[test]
    fn a_child_spawned_without_it_joins_this_process_s_group() {
        // The default, and the reason the rule has to be stated at all: a
        // plain spawn puts the child where the terminal's signal lands.
        let (_, group) = spawned_into(false);
        assert_eq!(group, group_of(std::process::id()));
    }
}
