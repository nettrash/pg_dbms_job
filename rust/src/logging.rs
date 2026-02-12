//! Logging utilities.

use crate::model::Config;
use chrono::Local;
use nix::libc;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process;
use std::sync::{Mutex, OnceLock};

/// Write a log line based on config and severity level.
pub fn dprint(config: &Config, level: &str, msg: &str) {
    if level.eq_ignore_ascii_case("DEBUG") && !config.debug {
        return;
    }
    let t = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let mut fname = config.logfile.clone();
    if fname.contains('%') {
        fname = Local::now().format(&fname).to_string();
    }

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
            let _ = out
                .write_all(format!("{t} [{}]: {level}: {msg}\n", process::id()).as_bytes())
                .and_then(|_| out.flush());
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

    #[test]
    fn dprint_writes_to_logfile() {
        let path = temp_log_path();
        let config = Config {
            debug: true,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: path.to_string_lossy().to_string(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 1000,
            nap_time: 0.1,
        };
        dprint(&config, "LOG", "test message");
        let content = fs::read_to_string(&path).expect("read log file");
        assert!(content.contains("test message"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dprint_skips_debug_when_disabled() {
        let path = temp_log_path();
        let config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: path.to_string_lossy().to_string(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 1000,
            nap_time: 0.1,
        };
        dprint(&config, "DEBUG", "debug message");
        assert!(!path.exists());
    }
}
