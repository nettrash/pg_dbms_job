//! Process management helpers for daemonization and signals.

use crate::constants::PROGRAM;
use crate::dlog;
use crate::logging::reset_logger_after_fork;
use crate::model::Config;
use crate::util::die;
use nix::sys::signal::{Signal, kill};
use nix::unistd::{ForkResult, Pid, fork, setsid};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::process::{self, Command};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

/// Fork and detach the scheduler from the controlling terminal.
pub fn daemonize(config: &Config) {
    match unsafe { fork() } {
        Ok(ForkResult::Parent { .. }) => process::exit(0),
        Ok(ForkResult::Child) => {}
        Err(err) => die(&format!("FATAL: Couldn't fork: {err}")),
    }

    // The parent's logger thread did not survive fork; discard the stale
    // sender so the next log call spawns a fresh writer in this child.
    reset_logger_after_fork();

    if let Err(err) = setsid() {
        die(&format!("Can't detach: {err}"));
    }
    dlog!(
        config,
        "DEBUG",
        "Detach from terminal with pid: {}",
        process::id()
    );

    let _ = OpenOptions::new()
        .read(true)
        .open("/dev/null")
        .and_then(|f| nix::unistd::dup2(f.as_raw_fd(), 0).map_err(io::Error::other));
    let _ = OpenOptions::new()
        .write(true)
        .open("/dev/null")
        .and_then(|f| nix::unistd::dup2(f.as_raw_fd(), 1).map_err(io::Error::other));
}

/// Write the current process id to a pid file.
pub fn write_pidfile(path: &str) {
    let mut file = File::create(path).unwrap_or_else(|err| {
        die(&format!("FATAL: can't create pid file {}, {}", path, err));
    });
    let _ = writeln!(file, "{}", process::id());
}

/// Send a signal to the running daemon using the pid file.
pub fn signal_handling(pidfile: &str, sig: Signal) {
    let pid = read_pid_from_file(pidfile).or_else(read_pid_from_ps);
    let pid = match pid {
        Some(pid) => pid,
        None => die(&format!(
            "ERROR: can't find a pid to send SIG{:?}, is {PROGRAM} running?",
            sig
        )),
    };
    if let Err(err) = kill(Pid::from_raw(pid), sig) {
        eprintln!("FATAL: failed to execute: {err}");
    } else {
        println!("OK: {PROGRAM} exited");
    }
    process::exit(0);
}

/// Read a pid from a file path.
fn read_pid_from_file(path: &str) -> Option<i32> {
    let mut buf = String::new();
    File::open(path).ok()?.read_to_string(&mut buf).ok()?;
    buf.trim().parse::<i32>().ok()
}

/// Fallback to `ps` when a pid file is missing.
fn read_pid_from_ps() -> Option<i32> {
    let output = Command::new("ps")
        .args(["h", "-opid", "-Cpg_dbms_job"])
        .output()
        .ok()?;
    let pid_str = String::from_utf8_lossy(&output.stdout);
    pid_str.split_whitespace().next()?.parse::<i32>().ok()
}

/// Reap completed worker threads and remove them from the active set.
pub fn reap_children(running: &mut HashMap<u64, JoinHandle<()>>) {
    let finished_ids: Vec<u64> = running
        .iter()
        .filter_map(|(id, handle)| handle.is_finished().then_some(*id))
        .collect();

    for id in finished_ids {
        if let Some(handle) = running.remove(&id) {
            let _ = handle.join();
        }
    }
}

/// Wait until all tracked worker threads have exited.
pub fn wait_all_children(running: &mut HashMap<u64, JoinHandle<()>>) {
    while !running.is_empty() {
        reap_children(running);
        thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::{read_pid_from_file, reap_children, wait_all_children, write_pidfile};
    use std::collections::HashMap;
    use std::fs;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn temp_path(prefix: &str) -> std::path::PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{now}"))
    }

    #[test]
    fn write_pidfile_creates_file() {
        let path = temp_path("pg_dbms_job_pid.pid");
        write_pidfile(path.to_str().unwrap());
        let content = fs::read_to_string(&path).expect("read pidfile");
        assert_eq!(content.trim(), std::process::id().to_string());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_pid_from_file_valid() {
        let path = temp_path("pg_dbms_job_rpid.pid");
        fs::write(&path, "12345\n").expect("write pid");
        let pid = read_pid_from_file(path.to_str().unwrap());
        assert_eq!(pid, Some(12345));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_pid_from_file_invalid_content() {
        let path = temp_path("pg_dbms_job_rpid_bad.pid");
        fs::write(&path, "notanumber\n").expect("write pid");
        let pid = read_pid_from_file(path.to_str().unwrap());
        assert_eq!(pid, None);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_pid_from_file_missing() {
        let pid = read_pid_from_file("/nonexistent/file.pid");
        assert_eq!(pid, None);
    }

    #[test]
    fn reap_children_empty_map() {
        let mut running = HashMap::new();
        reap_children(&mut running);
        assert!(running.is_empty());
    }

    #[test]
    fn reap_children_removes_finished_threads() {
        let mut running = HashMap::new();
        let handle = thread::spawn(|| {});
        running.insert(1, handle);
        // Give thread time to finish
        thread::sleep(Duration::from_millis(50));
        reap_children(&mut running);
        assert!(running.is_empty());
    }

    #[test]
    fn wait_all_children_empty_map() {
        let mut running = HashMap::new();
        wait_all_children(&mut running);
        assert!(running.is_empty());
    }

    #[test]
    fn wait_all_children_waits_for_threads() {
        let mut running = HashMap::new();
        let handle = thread::spawn(|| {
            thread::sleep(Duration::from_millis(50));
        });
        running.insert(1, handle);
        wait_all_children(&mut running);
        assert!(running.is_empty());
    }

    #[test]
    fn read_pid_from_file_with_whitespace() {
        let path = temp_path("pg_dbms_job_rpid_ws.pid");
        fs::write(&path, "  99999  \n").expect("write pid");
        let pid = read_pid_from_file(path.to_str().unwrap());
        assert_eq!(pid, Some(99999));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_pid_from_file_empty() {
        let path = temp_path("pg_dbms_job_rpid_empty.pid");
        fs::write(&path, "").expect("write pid");
        let pid = read_pid_from_file(path.to_str().unwrap());
        assert_eq!(pid, None);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_pid_from_file_negative_pid() {
        let path = temp_path("pg_dbms_job_rpid_neg.pid");
        fs::write(&path, "-1\n").expect("write pid");
        let pid = read_pid_from_file(path.to_str().unwrap());
        assert_eq!(pid, Some(-1));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_pidfile_overwrites_existing() {
        let path = temp_path("pg_dbms_job_pid_overwrite.pid");
        fs::write(&path, "old content").expect("write old");
        write_pidfile(path.to_str().unwrap());
        let content = fs::read_to_string(&path).expect("read pidfile");
        assert_eq!(content.trim(), std::process::id().to_string());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_pidfile_contains_trailing_newline() {
        let path = temp_path("pg_dbms_job_pid_nl.pid");
        write_pidfile(path.to_str().unwrap());
        let content = fs::read_to_string(&path).expect("read pidfile");
        assert!(content.ends_with('\n'));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reap_children_keeps_running_threads() {
        let mut running = HashMap::new();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let b = barrier.clone();
        let handle = thread::spawn(move || {
            b.wait();
        });
        running.insert(1, handle);
        // Thread is blocked on barrier, should not be reaped
        reap_children(&mut running);
        assert_eq!(running.len(), 1);
        // Release the thread
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        reap_children(&mut running);
        assert!(running.is_empty());
    }

    #[test]
    fn reap_children_multiple_finished() {
        let mut running = HashMap::new();
        for i in 0..5 {
            running.insert(i, thread::spawn(|| {}));
        }
        thread::sleep(Duration::from_millis(50));
        reap_children(&mut running);
        assert!(running.is_empty());
    }

    #[test]
    fn wait_all_children_multiple_threads() {
        let mut running = HashMap::new();
        for i in 0..5 {
            let ms = i * 10;
            running.insert(
                i,
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(ms));
                }),
            );
        }
        wait_all_children(&mut running);
        assert!(running.is_empty());
    }

    #[test]
    fn reap_children_mixed_finished_and_running() {
        let mut running = HashMap::new();
        // Two threads that finish immediately
        running.insert(1, thread::spawn(|| {}));
        running.insert(2, thread::spawn(|| {}));
        // One thread that blocks
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let b = barrier.clone();
        running.insert(
            3,
            thread::spawn(move || {
                b.wait();
            }),
        );
        thread::sleep(Duration::from_millis(50));
        reap_children(&mut running);
        // Only the blocked thread should remain
        assert_eq!(running.len(), 1);
        assert!(running.contains_key(&3));
        // Release and clean up
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        reap_children(&mut running);
        assert!(running.is_empty());
    }
}
