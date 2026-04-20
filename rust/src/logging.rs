//! Logging utilities.
//!
//! A dedicated writer thread receives pre-formatted log lines through a
//! channel, writes them via a persistent [`BufWriter`], and flushes once
//! per batch.  This avoids per-line open/close syscalls and eliminates
//! interleaved output from concurrent worker threads.

use crate::model::Config;
use chrono::Local;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
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
    /// Request a clean shutdown; the writer flushes and sends an ack.
    Shutdown(mpsc::Sender<()>),
}

/// Sender + the pid of the process that spawned the writer thread.
/// After `fork()` only the calling thread survives in the child, so a sender
/// inherited across fork points at a dead receiver. We detect this by
/// comparing `pid` against `process::id()` and respawn when they differ.
struct LoggerState {
    pid: u32,
    tx: mpsc::Sender<LogCmd>,
}

static LOG_STATE: Mutex<Option<LoggerState>> = Mutex::new(None);

/// Obtain a sender valid for the current process, spawning the writer thread
/// if needed (first call, or first call in a forked child).
fn with_sender<R>(f: impl FnOnce(&mpsc::Sender<LogCmd>) -> R) -> Option<R> {
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
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("logger".into())
            .spawn(move || log_writer_thread(rx))
            .expect("spawn logger thread");
        *guard = Some(LoggerState { pid: my_pid, tx });
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
    let mut old_log_name = String::new();

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

                        if truncate_on_rotation
                            && !old_log_name.is_empty()
                            && Path::new(&fname).exists()
                        {
                            let _ = fs::remove_file(&fname);
                        }
                        old_log_name = current_fname.clone();
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
                            Ok(f) => writer = Some(BufWriter::new(f)),
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

    with_sender(|tx| {
        let _ = tx.send(LogCmd::Line {
            line,
            fname,
            truncate_on_rotation: config.log_truncate_on_rotation,
        });
    });
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
    use super::{dprint, flush_logger};
    use crate::model::Config;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_log_path() -> std::path::PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("pg_dbms_job_log_{now}.log"))
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
        assert!(!content_b.contains("stale content"));
        assert!(content_b.contains("fresh b"));
        let _ = fs::remove_file(path_a);
        let _ = fs::remove_file(path_b);
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
