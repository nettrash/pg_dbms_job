//! Logging utilities.
//!
//! A dedicated writer thread receives pre-formatted log lines through a
//! channel, writes them via a persistent [`BufWriter`], and flushes once
//! per batch.  This avoids per-line open/close syscalls and eliminates
//! interleaved output from concurrent worker threads.

use crate::constants::LOG_CHANNEL_CAPACITY;
use crate::model::Config;
use chrono::Local;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

/// Command sent from any thread to the dedicated log writer.
enum LogCmd {
    /// A fully-formatted log line together with target file metadata.
    Line {
        line: String,
        fname: String,
        truncate_on_rotation: bool,
    },
    /// Flush all pending writes and send an ack (does not stop the thread).
    #[cfg(test)]
    Flush(mpsc::Sender<()>),
    /// Drop the persistent file handle so the next write re-opens the path.
    /// Used after external log rotation (e.g. logrotate + SIGHUP).
    Reopen,
    /// Request a clean shutdown; the writer flushes and sends an ack.
    Shutdown(mpsc::Sender<()>),
}

/// Sender + the pid of the process that spawned the writer thread.
/// After `fork()` only the calling thread survives in the child, so a sender
/// inherited across fork points at a dead receiver. We detect this by
/// comparing `pid` against `process::id()` and respawn when they differ.
struct LoggerState {
    pid: u32,
    tx: mpsc::SyncSender<LogCmd>,
}

static LOG_STATE: Mutex<Option<LoggerState>> = Mutex::new(None);

/// Obtain a sender valid for the current process, spawning the writer thread
/// if needed (first call, or first call in a forked child).
///
/// Returns `None` when the writer thread cannot be spawned (resource
/// exhaustion, ulimit, post-fork failure). Callers must treat that case as a
/// signal to write to stderr directly rather than panicking — losing logs is
/// recoverable; killing the scheduler is not.
fn with_sender<R>(f: impl FnOnce(&mpsc::SyncSender<LogCmd>) -> R) -> Option<R> {
    let mut guard = match LOG_STATE.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let my_pid = process::id();
    let need_spawn = match guard.as_ref() {
        None => true,
        Some(state) => state.pid != my_pid,
    };
    if need_spawn {
        // Bounded so a logging burst applies backpressure to producers rather
        // than buffering unbounded — under heavy async load with debug logging
        // this queue was a primary driver of load-correlated memory growth.
        let (tx, rx) = mpsc::sync_channel(LOG_CHANNEL_CAPACITY);
        match std::thread::Builder::new()
            .name("logger".into())
            .spawn(move || log_writer_thread(rx))
        {
            Ok(_) => {
                *guard = Some(LoggerState { pid: my_pid, tx });
            }
            Err(err) => {
                eprintln!("ERROR: failed to spawn logger thread ({err}); falling back to stderr");
                return None;
            }
        }
    }
    guard.as_ref().map(|s| f(&s.tx))
}

/// Drop any sender inherited from a parent process. Call this in the child
/// immediately after `fork()` so the next log call spawns a fresh writer
/// thread owned by the child.
pub fn reset_logger_after_fork() {
    let mut guard = match LOG_STATE.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

/// Background thread that owns the log file handle and serialises writes.
fn log_writer_thread(rx: mpsc::Receiver<LogCmd>) {
    let mut current_fname = String::new();
    let mut writer: Option<BufWriter<std::fs::File>> = None;
    // `(dev, ino)` of the file `writer` currently has open. We use it to
    // notice external rotation (logrotate's rename+create, or an outright
    // `rm`) that reached us without a SIGHUP — see the check at the top of
    // each batch below.
    let mut open_id: Option<(u64, u64)> = None;
    // Whether we have ever opened a real log file. Used to tell a genuine
    // rotation (truncate the destination if asked) from the very first open
    // or a return from stderr-only mode (leave a pre-existing file alone).
    // `current_fname` can't carry this: it goes back to "" whenever `logfile`
    // is unset, so it would mis-classify the next real open as a "first" one.
    let mut opened_real_logfile = false;

    loop {
        // Block until the first message arrives.
        let first = match rx.recv() {
            Ok(cmd) => cmd,
            Err(_) => break, // channel disconnected
        };

        // Drain any additional queued messages so we can batch-flush.
        let mut batch = vec![first];
        while let Ok(cmd) = rx.try_recv() {
            batch.push(cmd);
        }

        // If the file we hold open has been rotated out from under us — the
        // path now resolves to a different inode (logrotate rename+create) or
        // no longer exists at all — drop the handle so the first Line below
        // re-opens the configured path. This is a safety net for setups whose
        // logrotate config forgets the `postrotate kill -HUP`; the SIGHUP path
        // (`reopen_logger`) still handles the prompt, intended case. A plain
        // truncate (`copytruncate`) keeps the same inode, so it is left alone:
        // our handle is opened `O_APPEND`, so writes still land at byte 0.
        if let Some((dev, ino)) = open_id {
            let stale = match fs::metadata(&current_fname) {
                Ok(m) => m.dev() != dev || m.ino() != ino,
                Err(_) => true,
            };
            if stale {
                if let Some(ref mut w) = writer {
                    let _ = w.flush();
                }
                writer = None;
                open_id = None;
            }
        }

        let mut shutdown_ack: Option<mpsc::Sender<()>> = None;
        #[cfg(test)]
        let mut flush_acks: Vec<mpsc::Sender<()>> = Vec::new();

        for cmd in batch {
            match cmd {
                LogCmd::Line {
                    line,
                    fname,
                    truncate_on_rotation,
                } => {
                    // Handle file rotation / truncation.
                    if fname != current_fname {
                        // Flush the old writer before switching files.
                        if let Some(ref mut w) = writer {
                            let _ = w.flush();
                        }
                        writer = None;
                        open_id = None;

                        // Truncate iff this is a real rotation (we have opened
                        // a real log file before) and the operator opted in.
                        // The first time we open a log file is not a rotation,
                        // so a pre-existing file there is preserved.
                        if truncate_on_rotation && opened_real_logfile && Path::new(&fname).exists()
                        {
                            let _ = fs::remove_file(&fname);
                        }
                        current_fname = fname;
                    }

                    if current_fname.is_empty() {
                        // No log file configured — write to stderr.
                        eprint!("{line}");
                        continue;
                    }

                    // Open the file lazily (persistent handle).
                    if writer.is_none() {
                        match OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&current_fname)
                        {
                            Ok(f) => {
                                opened_real_logfile = true;
                                open_id = f.metadata().ok().map(|m| (m.dev(), m.ino()));
                                writer = Some(BufWriter::new(f));
                            }
                            Err(_) => {
                                eprintln!("ERROR: can't write to log file {current_fname}");
                                eprint!("{line}");
                                continue;
                            }
                        }
                    }

                    if let Some(ref mut w) = writer {
                        let _ = w.write_all(line.as_bytes());
                    }
                }
                #[cfg(test)]
                LogCmd::Flush(ack) => {
                    flush_acks.push(ack);
                }
                LogCmd::Reopen => {
                    if let Some(ref mut w) = writer {
                        let _ = w.flush();
                    }
                    writer = None;
                    open_id = None;
                }
                LogCmd::Shutdown(ack) => {
                    shutdown_ack = Some(ack);
                }
            }
        }

        // One flush per batch.
        if let Some(ref mut w) = writer {
            let _ = w.flush();
        }

        // Acknowledge all flush requests.
        #[cfg(test)]
        for ack in flush_acks {
            let _ = ack.send(());
        }

        if let Some(ack) = shutdown_ack {
            let _ = ack.send(());
            break;
        }
    }

    // Final flush on exit.
    if let Some(ref mut w) = writer {
        let _ = w.flush();
    }
}

/// Drop the persistent log file handle so the next write opens the configured
/// path fresh. Call this after receiving SIGHUP so logrotate-style rotation
/// (rename + create) starts writing to the new file instead of the old inode.
///
/// The writer thread also detects rotation on its own (it checks the open
/// file's inode at the start of every batch), so a missed SIGHUP only delays
/// the re-open until the next write rather than wedging the daemon on the old
/// inode forever.
pub fn reopen_logger() {
    with_sender(|tx| {
        let _ = tx.send(LogCmd::Reopen);
    });
}

/// Flush all pending messages and stop the writer thread.
pub fn shutdown_logger() {
    let guard = match LOG_STATE.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let my_pid = process::id();
    if let Some(state) = guard.as_ref()
        && state.pid == my_pid
    {
        let (ack_tx, ack_rx) = mpsc::channel();
        if state.tx.send(LogCmd::Shutdown(ack_tx)).is_ok() {
            let _ = ack_rx.recv_timeout(Duration::from_secs(5));
        }
    }
}

/// Block until all previously sent log messages have been written and flushed.
#[cfg(test)]
pub fn flush_logger() {
    let (ack_tx, ack_rx) = mpsc::channel();
    let sent = with_sender(|tx| tx.send(LogCmd::Flush(ack_tx)).is_ok()).unwrap_or(false);
    if sent {
        let _ = ack_rx.recv_timeout(Duration::from_secs(5));
    }
}

/// Write a log line based on config and severity level.
///
/// The line is fully formatted in the caller's thread (no allocation under
/// a lock) and then sent to the dedicated writer thread via a channel.
pub fn dprint(config: &Config, level: &str, msg: &str) {
    if level.eq_ignore_ascii_case("DEBUG") && !config.debug {
        return;
    }

    // Pre-format the complete line outside any lock.
    let t = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let line = format!("{t} [{}]: {level}: {msg}\n", process::id());

    let fname = if config.logfile.contains('%') {
        Local::now().format(&config.logfile).to_string()
    } else {
        config.logfile.clone()
    };

    let dispatched = with_sender(|tx| {
        tx.send(LogCmd::Line {
            line: line.clone(),
            fname,
            truncate_on_rotation: config.log_truncate_on_rotation,
        })
        .is_ok()
    })
    .unwrap_or(false);
    if !dispatched {
        // Writer thread missing or its channel is closed — make sure the line
        // still surfaces somewhere instead of silently disappearing.
        eprint!("{line}");
    }
}

/// Convenience macro that defers `format!` so DEBUG messages skip the
/// allocation entirely when debug logging is disabled.
///
/// ```ignore
/// dlog!(config, "DEBUG", "connecting to job {}", job.job);
/// dlog!(config, "ERROR", "notification error: {err}");
/// ```
#[macro_export]
macro_rules! dlog {
    ($config:expr, "DEBUG", $($arg:tt)+) => {
        if $config.debug {
            $crate::logging::dprint($config, "DEBUG", &format!($($arg)+))
        }
    };
    ($config:expr, $level:expr, $($arg:tt)+) => {
        $crate::logging::dprint($config, $level, &format!($($arg)+))
    };
}

#[cfg(test)]
mod tests {
    use super::{dprint, flush_logger, reopen_logger};
    use crate::model::Config;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_log_path() -> std::path::PathBuf {
        // SystemTime::now().as_nanos() collides ~95% of the time on macOS
        // for back-to-back calls; pair it with a process-wide counter so
        // every call really is unique.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pg_dbms_job_log_{now}_{n}.log"))
    }

    fn test_config(path: &std::path::Path, debug: bool) -> Config {
        Config {
            debug,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: path.to_string_lossy().to_string(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 1000,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 1.0,
            stats_interval: 0,
            job_run_details: crate::model::JobRunDetails::All,
            stale_job_timeout: 3600.0,
        }
    }

    #[test]
    fn dprint_writes_to_logfile() {
        let path = temp_log_path();
        let config = test_config(&path, true);
        dprint(&config, "LOG", "test message");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("test message"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn bounded_channel_drains_high_volume_without_loss() {
        // Send well past LOG_CHANNEL_CAPACITY so the bounded sync_channel is
        // forced to apply backpressure (send blocks until the writer drains).
        // The writer runs concurrently, so this must neither deadlock nor drop
        // lines: every message sent has to land in the file.
        use crate::constants::LOG_CHANNEL_CAPACITY;
        let path = temp_log_path();
        let config = test_config(&path, false);
        let count = LOG_CHANNEL_CAPACITY + 5000;
        for i in 0..count {
            dprint(&config, "LOG", &format!("bulk message {i}"));
        }
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        let lines = content
            .lines()
            .filter(|l| l.contains("bulk message"))
            .count();
        assert_eq!(lines, count, "bounded channel must not drop log lines");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dprint_skips_debug_when_disabled() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dprint(&config, "DEBUG", "debug message");
        flush_logger();
        assert!(!path.exists());
    }

    #[test]
    fn dprint_writes_debug_when_enabled() {
        let path = temp_log_path();
        let config = test_config(&path, true);
        dprint(&config, "DEBUG", "visible debug");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("visible debug"));
        assert!(content.contains("DEBUG"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dprint_log_format_contains_level_and_pid() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dprint(&config, "WARNING", "warn msg");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("WARNING"));
        assert!(content.contains("warn msg"));
        assert!(content.contains(&format!("[{}]", std::process::id())));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dprint_empty_logfile_no_crash() {
        let config = Config {
            debug: true,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 1000,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 1.0,
            stats_interval: 0,
            job_run_details: crate::model::JobRunDetails::All,
            stale_job_timeout: 3600.0,
        };
        // Should print to stderr without crashing
        dprint(&config, "LOG", "stderr fallback");
        flush_logger();
    }

    #[test]
    fn dprint_multiple_messages_appended() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dprint(&config, "LOG", "first");
        dprint(&config, "LOG", "second");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("first"));
        assert!(content.contains("second"));
        // Should be on separate lines
        assert!(content.lines().count() >= 2);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dprint_line_ends_with_newline() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dprint(&config, "LOG", "newline check");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.ends_with('\n'));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dprint_timestamp_format() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dprint(&config, "LOG", "timestamp check");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        // Should match YYYY-MM-DD HH:MM:SS pattern
        let line = content.lines().next().unwrap();
        assert!(line.len() >= 19, "line too short for timestamp: {line}");
        let ts = &line[..19];
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[7..8], "-");
        assert_eq!(&ts[10..11], " ");
        assert_eq!(&ts[13..14], ":");
        assert_eq!(&ts[16..17], ":");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dlog_macro_debug_skips_when_disabled() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dlog!(&config, "DEBUG", "should not appear {}", 42);
        flush_logger();
        assert!(!path.exists());
    }

    #[test]
    fn dlog_macro_debug_writes_when_enabled() {
        let path = temp_log_path();
        let config = test_config(&path, true);
        dlog!(&config, "DEBUG", "visible via macro {}", 99);
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("visible via macro 99"));
        assert!(content.contains("DEBUG"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dlog_macro_non_debug_always_writes() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dlog!(&config, "ERROR", "macro error {}", "msg");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("ERROR"));
        assert!(content.contains("macro error msg"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dprint_rotates_when_logfile_path_changes() {
        let path_a = temp_log_path();
        let path_b = temp_log_path();
        let mut cfg = test_config(&path_a, false);
        dprint(&cfg, "LOG", "first file");
        flush_logger();

        // Switch to a different log file — next write should land there.
        cfg.logfile = path_b.to_string_lossy().to_string();
        dprint(&cfg, "LOG", "second file");
        flush_logger();

        let content_a = fs::read_to_string(&path_a).expect("read first log");
        let content_b = fs::read_to_string(&path_b).expect("read second log");
        assert!(content_a.contains("first file"));
        assert!(!content_a.contains("second file"));
        assert!(content_b.contains("second file"));
        assert!(!content_b.contains("first file"));
        let _ = fs::remove_file(path_a);
        let _ = fs::remove_file(path_b);
    }

    #[test]
    fn dprint_expands_strftime_tokens_in_logfile() {
        // A logfile path with a '%' specifier should be expanded through
        // chrono's formatter so rotation by date works.
        let dir = std::env::temp_dir();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let template = dir.join(format!("pg_dbms_job_rot_{now}_%Y.log"));
        let cfg = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: template.to_string_lossy().to_string(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 1000,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 1.0,
            stats_interval: 0,
            job_run_details: crate::model::JobRunDetails::All,
            stale_job_timeout: 3600.0,
        };
        dprint(&cfg, "LOG", "date formatted");
        flush_logger();

        // The actual on-disk filename contains the current year, not '%Y'.
        let year = chrono::Local::now().format("%Y").to_string();
        let expected = dir.join(format!("pg_dbms_job_rot_{now}_{year}.log"));
        let content = fs::read_to_string(&expected).expect("read expanded log path");
        assert!(content.contains("date formatted"));
        // And the literal path with '%Y' must NOT exist.
        assert!(!template.exists(), "unexpanded template path was created");
        let _ = fs::remove_file(expected);
    }

    #[test]
    fn dprint_truncates_on_rotation_when_enabled() {
        let path_a = temp_log_path();
        let path_b = temp_log_path();

        let mut cfg = test_config(&path_a, false);
        cfg.log_truncate_on_rotation = true;
        dprint(&cfg, "LOG", "initial a");
        flush_logger();

        // Pre-populate path_b so rotation has something to truncate.
        fs::write(&path_b, "stale content\n").expect("seed old log");

        cfg.logfile = path_b.to_string_lossy().to_string();
        dprint(&cfg, "LOG", "fresh b");
        flush_logger();

        let content_b = fs::read_to_string(&path_b).expect("read rotated log");
        // Stale content must be gone; only the new line should remain.
        assert!(
            !content_b.contains("stale content"),
            "content_b={content_b:?}"
        );
        assert!(content_b.contains("fresh b"), "content_b={content_b:?}");
        let _ = fs::remove_file(path_a);
        let _ = fs::remove_file(path_b);
    }

    #[test]
    fn reopen_logger_writes_to_new_file_after_external_rename() {
        // Simulate logrotate: write to path, rename it aside, then reopen.
        // The next write must land at the original path (a fresh file),
        // not in the renamed-aside file.
        let path = temp_log_path();
        let rotated = {
            let mut p = path.clone();
            let name = format!("{}.1", path.file_name().unwrap().to_string_lossy());
            p.set_file_name(name);
            p
        };

        let config = test_config(&path, false);
        dprint(&config, "LOG", "before rotation");
        flush_logger();

        // External rotation: rename the active log out of the way.
        fs::rename(&path, &rotated).expect("rename log aside");

        // Without reopen, the daemon's persistent FD still points at `rotated`.
        reopen_logger();

        dprint(&config, "LOG", "after rotation");
        flush_logger();

        let new_content = fs::read_to_string(&path).expect("read new log file");
        let rotated_content = fs::read_to_string(&rotated).expect("read rotated log");
        assert!(
            new_content.contains("after rotation"),
            "post-rotation line should be in the new file"
        );
        assert!(
            !new_content.contains("before rotation"),
            "new file must not contain pre-rotation line"
        );
        assert!(
            rotated_content.contains("before rotation"),
            "rotated file should still contain pre-rotation line"
        );
        assert!(
            !rotated_content.contains("after rotation"),
            "rotated file must not receive post-rotation lines"
        );
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&rotated);
    }

    #[test]
    fn auto_reopens_after_external_rename_without_signal() {
        // logrotate that forgets the `postrotate kill -HUP`: the active log
        // is renamed aside (and, here, not even re-created) without any
        // notification to the daemon. The writer must still detect that its
        // open file no longer lives at the configured path and re-open it on
        // the next write — landing in a fresh file, not the renamed-aside one.
        let path = temp_log_path();
        let rotated = {
            let mut p = path.clone();
            let name = format!("{}.1", path.file_name().unwrap().to_string_lossy());
            p.set_file_name(name);
            p
        };

        let config = test_config(&path, false);
        dprint(&config, "LOG", "before rotation");
        flush_logger();

        // External rotation, no SIGHUP, no reopen_logger() call.
        fs::rename(&path, &rotated).expect("rename log aside");

        dprint(&config, "LOG", "after rotation");
        flush_logger();

        let new_content = fs::read_to_string(&path).expect("read new log file");
        let rotated_content = fs::read_to_string(&rotated).expect("read rotated log");
        assert!(
            new_content.contains("after rotation"),
            "post-rotation line should be in the freshly re-opened file"
        );
        assert!(
            !new_content.contains("before rotation"),
            "new file must not contain the pre-rotation line"
        );
        assert!(
            rotated_content.contains("before rotation"),
            "rotated file should still contain the pre-rotation line"
        );
        assert!(
            !rotated_content.contains("after rotation"),
            "rotated file must not receive post-rotation lines"
        );
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&rotated);
    }

    #[test]
    fn copytruncate_keeps_writing_to_same_inode() {
        // logrotate `copytruncate`: the file is copied aside and then
        // truncated in place — same path, same inode. The daemon must keep
        // its handle (an O_APPEND fd writes at the new EOF, i.e. byte 0), so
        // post-truncate lines land in the same file without a sparse hole.
        let path = temp_log_path();
        let config = test_config(&path, false);
        dprint(&config, "LOG", "before truncate");
        flush_logger();

        // Simulate `copytruncate`: copy aside, then truncate in place.
        let copied = fs::read(&path).expect("read log");
        let aside = {
            let mut p = path.clone();
            p.set_file_name(format!("{}.1", path.file_name().unwrap().to_string_lossy()));
            p
        };
        fs::write(&aside, &copied).expect("copy aside");
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open for truncate")
            .set_len(0)
            .expect("truncate in place");

        dprint(&config, "LOG", "after truncate");
        flush_logger();

        let content = fs::read_to_string(&path).expect("read log after truncate");
        assert!(
            content.contains("after truncate"),
            "post-truncate line must be present, got: {content:?}"
        );
        assert!(
            !content.contains("before truncate"),
            "truncated file should not retain the pre-truncate line"
        );
        // No leading NUL padding: the first byte must be the timestamp digit.
        assert!(
            content.as_bytes().first().is_some_and(u8::is_ascii_digit),
            "file starts with unexpected bytes (sparse hole?): {:?}",
            &content.as_bytes()[..content.len().min(8)]
        );
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&aside);
    }

    #[test]
    fn reopen_logger_is_safe_before_any_write() {
        // Calling reopen before any log line has been emitted (e.g. SIGHUP
        // arrives during the initial startup window) must not panic and must
        // not block. The writer thread may not even be spawned yet.
        reopen_logger();
        reopen_logger();
        // A subsequent write still has to land in the configured file.
        let path = temp_log_path();
        let cfg = test_config(&path, false);
        dprint(&cfg, "LOG", "after early reopen");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log");
        assert!(content.contains("after early reopen"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reopen_logger_flushes_pending_writes() {
        // A reopen must flush queued data into the *current* file before
        // dropping the handle — otherwise lines emitted just before SIGHUP
        // could be silently lost or appear in the wrong file.
        let path = temp_log_path();
        let cfg = test_config(&path, false);
        dprint(&cfg, "LOG", "pre-reopen line");
        // No flush_logger() here on purpose: rely on Reopen to flush.
        reopen_logger();
        // Round-trip a no-op flush so we know the writer thread has drained.
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log");
        assert!(
            content.contains("pre-reopen line"),
            "reopen must flush pending data, got: {content:?}"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reopen_logger_idempotent_back_to_back() {
        // Several reopens in a row (e.g. multiple SIGHUPs queued together)
        // must not corrupt subsequent output.
        let path = temp_log_path();
        let cfg = test_config(&path, false);
        dprint(&cfg, "LOG", "first");
        flush_logger();
        reopen_logger();
        reopen_logger();
        reopen_logger();
        dprint(&cfg, "LOG", "second");
        flush_logger();
        let content = fs::read_to_string(&path).expect("read log");
        assert!(content.contains("first"));
        assert!(content.contains("second"));
        assert_eq!(
            content.lines().count(),
            2,
            "expected exactly two lines, got {content:?}"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reopen_logger_with_empty_logfile_does_not_panic() {
        // When `logfile` is empty the writer falls back to stderr and never
        // opens a file. SIGHUP-triggered reopen must still be a no-op.
        let cfg = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 1000,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 1.0,
            stats_interval: 0,
            job_run_details: crate::model::JobRunDetails::All,
            stale_job_timeout: 3600.0,
        };
        dprint(&cfg, "LOG", "stderr fallback before reopen");
        reopen_logger();
        dprint(&cfg, "LOG", "stderr fallback after reopen");
        flush_logger();
    }

    #[test]
    fn dprint_concurrent_no_interleave() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let path = temp_log_path();
        let config = Arc::new(test_config(&path, false));
        let num_threads = 8;
        let msgs_per_thread = 20;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let cfg = Arc::clone(&config);
                let b = Arc::clone(&barrier);
                thread::spawn(move || {
                    b.wait();
                    for i in 0..msgs_per_thread {
                        dprint(&cfg, "LOG", &format!("thread{t}_msg{i}"));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        flush_logger();

        let content = fs::read_to_string(&path).expect("read log file");
        // Every line must be a complete log entry
        for line in content.lines() {
            assert!(
                line.contains("LOG") && line.contains("thread"),
                "interleaved or malformed line: {line}"
            );
        }
        assert_eq!(
            content.lines().count(),
            num_threads * msgs_per_thread,
            "expected {} lines",
            num_threads * msgs_per_thread
        );
        let _ = fs::remove_file(path);
    }
}
