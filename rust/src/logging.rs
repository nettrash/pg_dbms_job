//! Logging utilities.

use crate::model::Config;
use chrono::Local;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process;
use std::sync::{Mutex, OnceLock};

/// Write a log line based on config and severity level.
pub fn dprint(config: &Config, level: &str, msg: &str) {
    if level.eq_ignore_ascii_case("DEBUG") && !config.debug {
        return;
    }
    let t = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let fname = if config.logfile.contains('%') {
        Local::now().format(&config.logfile).to_string()
    } else {
        config.logfile.clone()
    };

    if config.log_truncate_on_rotation {
        // Track previous log file name to support truncate-on-rotation.
        static OLD_LOG_FILE: OnceLock<Mutex<String>> = OnceLock::new();
        let old = OLD_LOG_FILE.get_or_init(|| Mutex::new(String::new()));
        if let Ok(mut old_name) = old.lock() {
            if !old_name.is_empty() && *old_name != fname && Path::new(&fname).exists() {
                let _ = fs::remove_file(&fname);
            }
            *old_name = fname.clone();
        }
    }

    if !fname.is_empty() {
        if let Ok(mut out) = OpenOptions::new().append(true).create(true).open(&fname) {
            let _ =
                writeln!(out, "{t} [{}]: {level}: {msg}", process::id()).and_then(|_| out.flush());
        } else {
            eprintln!("ERROR: can't write to log file {fname}");
            eprintln!("{t} [{}]: {level}:  {msg}", process::id());
        }
    } else {
        eprintln!("{t} [{}]: {level}:  {msg}", process::id());
    }
}

#[cfg(test)]
mod tests {
    use super::dprint;
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
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("test message"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dprint_skips_debug_when_disabled() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dprint(&config, "DEBUG", "debug message");
        assert!(!path.exists());
    }

    #[test]
    fn dprint_writes_debug_when_enabled() {
        let path = temp_log_path();
        let config = test_config(&path, true);
        dprint(&config, "DEBUG", "visible debug");
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
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 1.0,
        };
        // Should print to stderr without crashing
        dprint(&config, "LOG", "stderr fallback");
    }

    #[test]
    fn dprint_multiple_messages_appended() {
        let path = temp_log_path();
        let config = test_config(&path, false);
        dprint(&config, "LOG", "first");
        dprint(&config, "LOG", "second");
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("first"));
        assert!(content.contains("second"));
        // Should be on separate lines
        assert!(content.lines().count() >= 2);
        let _ = fs::remove_file(path);
    }
}
