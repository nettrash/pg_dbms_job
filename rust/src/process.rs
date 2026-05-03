//! Process management helpers for daemonization and signals.

use crate::constants::PROGRAM;
use crate::dlog;
use crate::logging::reset_logger_after_fork;
use crate::model::Config;
use crate::util::die;
use fs2::FileExt;
use nix::sys::signal::{Signal, kill};
use nix::unistd::{ForkResult, Pid, fork, setsid};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::process::{self, Command};
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

/// Holds the open pidfile (with its advisory lock) for the lifetime of the
/// daemon. The lock is released automatically when this process exits, so a
/// crash leaves the pidfile locked-but-unowned and the next start can detect
/// staleness via `try_lock_exclusive`.
static PIDFILE_GUARD: Mutex<Option<File>> = Mutex::new(None);

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

/// Acquire the pid file atomically and write the current process id.
///
/// Uses `O_CREAT|O_EXCL` for atomic creation, falling back to opening an
/// existing file and grabbing an exclusive advisory lock so concurrent
/// startups can't both succeed. A stale pidfile from a crashed previous run
/// is detected by the lock being free, and is then truncated and reused.
///
/// The opened file is held in a process-wide static so its advisory lock
/// outlives this function; the lock is released when the process exits.
pub fn write_pidfile(path: &str) -> Result<(), String> {
    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|err| format!("can't open pid file {path}: {err}"))?;
            FileExt::try_lock_exclusive(&f)
                .map_err(|_| format!("another {PROGRAM} process holds the pid file {path}"))?;
            f.set_len(0)
                .map_err(|err| format!("can't truncate stale pid file {path}: {err}"))?;
            use std::io::Seek;
            (&f).seek(std::io::SeekFrom::Start(0))
                .map_err(|err| format!("can't rewind stale pid file {path}: {err}"))?;
            f
        }
        Err(e) => return Err(format!("can't create pid file {path}: {e}")),
    };

    // For the freshly-created path, also acquire the lock so subsequent
    // startups see this process as alive.
    let _ = FileExt::try_lock_exclusive(&file);

    writeln!(file, "{}", process::id()).map_err(|e| format!("can't write pid file {path}: {e}"))?;

    // Drop any previously held pidfile cleanly (explicit unlock before close)
    // before installing the new one. See `release_pidfile` for why the
    // explicit unlock matters.
    release_pidfile();

    if let Ok(mut g) = PIDFILE_GUARD.lock() {
        *g = Some(file);
    }
    Ok(())
}

/// Release the pidfile lock and drop the file handle. Call before unlinking
/// the pidfile path on shutdown.
///
/// We explicitly `unlock()` before letting the file drop. Relying on close()
/// alone is enough on Linux, but on macOS the kernel can briefly retain the
/// advisory-lock bookkeeping for an inode after the last fd closes — long
/// enough for an immediately-following `flock(LOCK_EX | LOCK_NB)` from the
/// same process to spuriously fail. Calling `unlock()` first sidesteps that
/// window.
pub fn release_pidfile() {
    if let Ok(mut g) = PIDFILE_GUARD.lock()
        && let Some(file) = g.take()
    {
        let _ = FileExt::unlock(&file);
        drop(file);
    }
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
    use super::{
        read_pid_from_file, reap_children, release_pidfile, wait_all_children, write_pidfile,
    };
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, MutexGuard};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn temp_path(prefix: &str) -> std::path::PathBuf {
        // SystemTime::now().as_nanos() collides ~95% of the time on macOS
        // for back-to-back calls; pair it with a process-wide counter so
        // every call really is unique.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{prefix}_{now}_{n}"))
    }

    /// Serializes tests that read or mutate the global `PIDFILE_GUARD` so
    /// they don't observe each other's intermediate state under cargo's
    /// default parallel runner.
    static PIDFILE_TEST_LOCK: Mutex<()> = Mutex::new(());

    /// Acquire the serialization lock, recovering from a poisoned mutex left
    /// behind by an earlier panicking test.
    fn pidfile_test_guard() -> MutexGuard<'static, ()> {
        match PIDFILE_TEST_LOCK.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[test]
    fn write_pidfile_creates_file() {
        let _g = pidfile_test_guard();
        let path = temp_path("pg_dbms_job_pid.pid");
        write_pidfile(path.to_str().unwrap()).expect("write pidfile");
        let content = fs::read_to_string(&path).expect("read pidfile");
        assert_eq!(content.trim(), std::process::id().to_string());
        release_pidfile();
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
    fn write_pidfile_reuses_unlocked_stale_file() {
        // Simulate a previous run that crashed without removing its pidfile:
        // the file exists but no process holds the advisory lock, so we
        // should be able to truncate and reuse it.
        let _g = pidfile_test_guard();
        let path = temp_path("pg_dbms_job_pid_overwrite.pid");
        fs::write(&path, "old content").expect("write old");
        write_pidfile(path.to_str().unwrap()).expect("reuse stale pidfile");
        let content = fs::read_to_string(&path).expect("read pidfile");
        assert_eq!(content.trim(), std::process::id().to_string());
        release_pidfile();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_pidfile_contains_trailing_newline() {
        let _g = pidfile_test_guard();
        let path = temp_path("pg_dbms_job_pid_nl.pid");
        write_pidfile(path.to_str().unwrap()).expect("write pidfile");
        let content = fs::read_to_string(&path).expect("read pidfile");
        assert!(content.ends_with('\n'));
        release_pidfile();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_pidfile_rejects_locked_file() {
        // A live peer process is simulated by an explicit fs2 lock on the
        // pidfile. write_pidfile must not stomp on it.
        use super::FileExt;
        let _g = pidfile_test_guard();
        let path = temp_path("pg_dbms_job_pid_locked.pid");
        let live = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("open peer pidfile");
        FileExt::lock_exclusive(&live).expect("peer lock");
        let result = super::write_pidfile(path.to_str().unwrap());
        assert!(
            result.is_err(),
            "expected write_pidfile to refuse a locked pidfile, got {result:?}"
        );
        FileExt::unlock(&live).ok();
        drop(live);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn release_pidfile_when_empty_is_noop() {
        // Called on shutdown after a startup that never reached write_pidfile,
        // or twice in a row — must not panic.
        let _g = pidfile_test_guard();
        release_pidfile();
        release_pidfile();
    }

    #[test]
    fn release_pidfile_frees_advisory_lock() {
        // After release_pidfile, the advisory lock obtained by write_pidfile
        // is gone — an external observer can take its own exclusive lock.
        use super::FileExt;
        let _g = pidfile_test_guard();
        let path = temp_path("pg_dbms_job_pid_release.pid");
        write_pidfile(path.to_str().unwrap()).expect("write pidfile");

        // While the daemon "holds" the pidfile, another locker must fail.
        let peer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open peer handle");
        assert!(
            FileExt::try_lock_exclusive(&peer).is_err(),
            "lock should be held by write_pidfile while guard is alive"
        );

        release_pidfile();

        FileExt::try_lock_exclusive(&peer)
            .expect("lock should be releasable after release_pidfile");
        FileExt::unlock(&peer).ok();
        drop(peer);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_pidfile_reacquires_after_release() {
        // The expected daemon-restart path: write the pidfile, release it,
        // then write again. The second write reuses the now-stale file.
        let _g = pidfile_test_guard();
        let path = temp_path("pg_dbms_job_pid_reacq.pid");
        write_pidfile(path.to_str().unwrap()).expect("first write");
        release_pidfile();
        write_pidfile(path.to_str().unwrap()).expect("re-acquire after release");
        let content = fs::read_to_string(&path).expect("read pidfile");
        assert_eq!(content.trim(), std::process::id().to_string());
        release_pidfile();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_pidfile_two_paths_back_to_back() {
        // Simulates a SIGHUP-triggered pidfile rename: the daemon writes a
        // new pidfile path; the old guard is implicitly dropped so its lock
        // is released and a third party can reclaim the original path.
        use super::FileExt;
        let _g = pidfile_test_guard();
        let first = temp_path("pg_dbms_job_pid_first.pid");
        let second = temp_path("pg_dbms_job_pid_second.pid");

        write_pidfile(first.to_str().unwrap()).expect("first write");
        write_pidfile(second.to_str().unwrap()).expect("second write");

        // Second pidfile is locked; first is now free.
        let first_peer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&first)
            .expect("open first");
        FileExt::try_lock_exclusive(&first_peer)
            .expect("first pidfile must be unlocked once second is taken");
        FileExt::unlock(&first_peer).ok();
        drop(first_peer);

        let second_peer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&second)
            .expect("open second");
        assert!(
            FileExt::try_lock_exclusive(&second_peer).is_err(),
            "second pidfile must still be locked"
        );
        drop(second_peer);

        release_pidfile();
        let _ = fs::remove_file(first);
        let _ = fs::remove_file(second);
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
