//! Configuration file parsing and application.

use crate::dlog;
use crate::logging::dprint;
use crate::model::{Config, DbInfo};
use crate::util::die;
use std::fs;

/// Read and apply configuration from a file path.
///
/// When `nodie` is true, missing files are logged instead of aborting.
pub fn read_config(config_file: &str, config: &mut Config, dbinfo: &mut DbInfo, nodie: bool) {
    let content = fs::read_to_string(config_file);
    if content.is_err() {
        if !nodie {
            die(&format!(
                "FATAL: can not find the configuration file {config_file}"
            ));
        } else {
            dlog!(
                config,
                "ERROR",
                "can not find the configuration file {config_file}"
            );
            return;
        }
    }
    let content = content.unwrap();

    // Load logfile first so subsequent logs go to the configured location.
    for line in content.lines() {
        if let Some((var, val)) = parse_config_line(line)
            && var == "logfile"
            && config.logfile != val
        {
            config.logfile = val;
            dlog!(
                config,
                "LOG",
                "Setting logfile from configuration file to {}",
                config.logfile
            );
        }
    }

    // Apply remaining settings and database connection information.
    for line in content.lines() {
        if let Some((var, val)) = parse_config_line(line) {
            match var.as_str() {
                "pidfile" => {
                    if config.pidfile != val {
                        config.pidfile = val;
                        dlog!(
                            config,
                            "LOG",
                            "Setting pidfile from configuration file to {}",
                            config.pidfile
                        );
                    }
                }
                "debug" => {
                    let debug_val = val.parse::<i32>().unwrap_or(0) != 0;
                    if config.debug != debug_val {
                        config.debug = debug_val;
                        dlog!(
                            config,
                            "LOG",
                            "Setting debug from configuration file to {}",
                            config.debug as i32
                        );
                    }
                }
                "job_queue_interval" => {
                    apply_positive_float(config, "job_queue_interval", &val, |c| {
                        &mut c.job_queue_interval
                    });
                }
                "job_queue_processes" => {
                    if let Ok(v) = val.parse::<isize>() {
                        // Number of processes must be positive
                        if v > 0 {
                            config.job_queue_processes =
                                v.try_into().unwrap_or(config.job_queue_processes);
                            dlog!(
                                config,
                                "LOG",
                                "Setting job_queue_processes from configuration file to {}",
                                config.job_queue_processes
                            );
                        } else {
                            dlog!(
                                config,
                                "ERROR",
                                "Invalid job_queue_processes value {} in configuration file, must be positive. Ignoring. Actual value remains {}",
                                val,
                                config.job_queue_processes
                            );
                        }
                    }
                }
                "pool_size" => {
                    if let Ok(v) = val.parse::<isize>() {
                        if v > 0 {
                            config.pool_size = v.try_into().unwrap_or(config.pool_size);
                            dlog!(
                                config,
                                "LOG",
                                "Setting pool_size from configuration file to {}",
                                config.pool_size
                            );
                        } else {
                            dlog!(
                                config,
                                "ERROR",
                                "Invalid pool_size value {} in configuration file, must be positive. Ignoring. Actual value remains {}",
                                val,
                                config.pool_size
                            );
                        }
                    }
                }
                "nap_time" => {
                    apply_positive_float(config, "nap_time", &val, |c| &mut c.nap_time);
                }
                "startup_delay" => {
                    apply_positive_float(config, "startup_delay", &val, |c| &mut c.startup_delay);
                }
                "error_delay" => {
                    apply_positive_float(config, "error_delay", &val, |c| &mut c.error_delay);
                }
                "host" => {
                    dbinfo.host = val;
                    dlog!(
                        config,
                        "LOG",
                        "Setting host from configuration file to {}",
                        dbinfo.host
                    );
                }
                "database" => {
                    dbinfo.database = val;
                    dlog!(
                        config,
                        "LOG",
                        "Setting database from configuration file to {}",
                        dbinfo.database
                    );
                }
                "user" => {
                    dbinfo.user = val;
                    dlog!(
                        config,
                        "LOG",
                        "Setting user from configuration file to {}",
                        dbinfo.user
                    );
                }
                "passwd" => {
                    dbinfo.passwd = val;
                    dprint(
                        config,
                        "LOG",
                        "Setting passwd from configuration file to ****",
                    );
                }
                "port" => {
                    if let Ok(v) = val.parse::<u16>() {
                        if v > 0 {
                            dbinfo.port = v;
                            dlog!(
                                config,
                                "LOG",
                                "Setting port from configuration file to {}",
                                dbinfo.port
                            );
                        } else {
                            dlog!(
                                config,
                                "ERROR",
                                "Invalid port value {} in configuration file, must be a positive integer. Ignoring. Actual value remains {}",
                                val,
                                dbinfo.port
                            );
                        }
                    }
                }
                "log_truncate_on_rotation" => {
                    config.log_truncate_on_rotation = val.parse::<i32>().unwrap_or(0) != 0;
                }
                _ => {}
            }
        }
    }
}

/// Parse a configuration value as a finite, strictly positive `f64` and store
/// it via `field`. On invalid input the existing field value is preserved and
/// an error line is logged; on success a confirmation line is logged.
///
/// Pulled out so the four time-interval settings (job_queue_interval,
/// nap_time, startup_delay, error_delay) share one validation path.
fn apply_positive_float(
    config: &mut Config,
    name: &str,
    raw: &str,
    field: impl FnOnce(&mut Config) -> &mut f64,
) {
    let parsed = match raw.parse::<f64>() {
        Ok(v) => v,
        Err(_) => return,
    };
    if parsed > 0.0 && parsed.is_finite() {
        *field(config) = parsed;
        dlog!(
            config,
            "LOG",
            "Setting {} from configuration file to {}",
            name,
            parsed
        );
    } else {
        let current = *field(config);
        dlog!(
            config,
            "ERROR",
            "Invalid {} value {} in configuration file, must be positive and finite. Ignoring. Actual value remains {}",
            name,
            raw,
            current
        );
    }
}

/// Parse a single configuration line into `key=value` components.
fn parse_config_line(line: &str) -> Option<(String, String)> {
    let mut l = line.replace('\r', "");
    if let Some(idx) = l.find('#') {
        l = l[..idx].to_string();
    }
    let l = l.trim();
    if l.is_empty() {
        return None;
    }
    let parts: Vec<&str> = l.splitn(2, '=').collect();
    if parts.len() != 2 {
        return None;
    }
    let var = parts[0].trim().to_lowercase();
    let val = parts[1].trim().to_string();
    Some((var, val))
}

#[cfg(test)]
mod tests {
    use super::{apply_positive_float, parse_config_line, read_config};
    use crate::model::{Config, DbInfo};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

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

    /// Build a Config seeded with sentinel values that make it easy to detect
    /// which field a test wrote to.
    fn float_test_config() -> Config {
        Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 7.0,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 11.0,
            startup_delay: 13.0,
            error_delay: 17.0,
        }
    }

    #[test]
    fn parse_config_line_basic() {
        let parsed = parse_config_line("host = localhost");
        assert_eq!(parsed, Some(("host".to_string(), "localhost".to_string())));
    }

    #[test]
    fn parse_config_line_ignores_comments() {
        assert_eq!(parse_config_line("# just a comment"), None);
        let parsed = parse_config_line("logfile=/tmp/test.log # rotate");
        assert_eq!(
            parsed,
            Some(("logfile".to_string(), "/tmp/test.log".to_string()))
        );
    }

    #[test]
    fn parse_config_line_empty() {
        assert_eq!(parse_config_line(""), None);
        assert_eq!(parse_config_line("   "), None);
        assert_eq!(parse_config_line("  \t  "), None);
    }

    #[test]
    fn parse_config_line_no_equals() {
        assert_eq!(parse_config_line("no_equals_here"), None);
    }

    #[test]
    fn parse_config_line_strips_carriage_return() {
        let parsed = parse_config_line("host = myhost\r");
        assert_eq!(parsed, Some(("host".to_string(), "myhost".to_string())));
    }

    #[test]
    fn parse_config_line_value_with_equals() {
        let parsed = parse_config_line("passwd = a=b=c");
        assert_eq!(parsed, Some(("passwd".to_string(), "a=b=c".to_string())));
    }

    #[test]
    fn parse_config_line_case_insensitive_key() {
        let parsed = parse_config_line("HOST = myhost");
        assert_eq!(parsed, Some(("host".to_string(), "myhost".to_string())));
    }

    #[test]
    fn read_config_updates_values() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: "".to_string(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: "".to_string(),
            database: "".to_string(),
            user: "".to_string(),
            passwd: "".to_string(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_test.conf");
        let content = r#"
pidfile=/tmp/pg_dbms_job_test.pid
debug=1
job_queue_interval=7.5
job_queue_processes=50
nap_time=0.2
host=127.0.0.1
database=testdb
user=tester
passwd=secret
port=5433
log_truncate_on_rotation=1
"#;
        fs::write(&path, content).expect("write temp config");

        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);

        assert_eq!(config.pidfile, "/tmp/pg_dbms_job_test.pid");
        assert!(config.debug);
        assert_eq!(config.job_queue_interval, 7.5);
        assert_eq!(config.job_queue_processes, 50);
        assert_eq!(config.nap_time, 0.2);
        assert!(config.log_truncate_on_rotation);
        assert_eq!(dbinfo.host, "127.0.0.1");
        assert_eq!(dbinfo.database, "testdb");
        assert_eq!(dbinfo.user, "tester");
        assert_eq!(dbinfo.passwd, "secret");
        assert_eq!(dbinfo.port, 5433);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_missing_file_nodie() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        // Should not panic when nodie=true
        read_config("/nonexistent/path.conf", &mut config, &mut dbinfo, true);
        // Values should remain unchanged
        assert_eq!(config.pidfile, "/tmp/pg_dbms_job.pid");
        assert_eq!(dbinfo.port, 5432);
    }

    #[test]
    fn read_config_invalid_numeric_values_ignored() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_invalid.conf");
        let content = r#"
job_queue_interval=-1.0
job_queue_processes=-5
nap_time=0
startup_delay=-0.5
error_delay=NaN
port=notanumber
"#;
        fs::write(&path, content).expect("write temp config");

        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);

        // All values should remain at defaults since the config values are invalid
        assert_eq!(config.job_queue_interval, 0.1);
        assert_eq!(config.job_queue_processes, 1024);
        assert_eq!(config.nap_time, 0.1);
        assert_eq!(config.startup_delay, 3.0);
        assert_eq!(config.error_delay, 0.5);
        assert_eq!(dbinfo.port, 5432);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_startup_and_error_delay() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_delays.conf");
        let content = "startup_delay=5.5\nerror_delay=2.0\n";
        fs::write(&path, content).expect("write temp config");

        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);

        assert_eq!(config.startup_delay, 5.5);
        assert_eq!(config.error_delay, 2.0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn parse_config_line_whitespace_around_equals() {
        let parsed = parse_config_line("  host  =  myhost  ");
        assert_eq!(parsed, Some(("host".to_string(), "myhost".to_string())));
    }

    #[test]
    fn parse_config_line_tab_separated() {
        let parsed = parse_config_line("\thost\t=\tdb.example.com\t");
        assert_eq!(
            parsed,
            Some(("host".to_string(), "db.example.com".to_string()))
        );
    }

    #[test]
    fn parse_config_line_only_comment_after_equals() {
        let parsed = parse_config_line("key = #value");
        // '#' starts a comment—so the value is empty
        assert_eq!(parsed, Some(("key".to_string(), String::new())));
    }

    #[test]
    fn read_config_logfile_applied_first() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_logfile.conf");
        fs::write(&path, "logfile=/tmp/test_scheduler.log\n").expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        assert_eq!(config.logfile, "/tmp/test_scheduler.log");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_zero_values_rejected() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 10,
            pool_size: 10,
            nap_time: 1.0,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_zero.conf");
        let content = "job_queue_interval=0\nnap_time=0\nstartup_delay=0\nerror_delay=0\njob_queue_processes=0\n";
        fs::write(&path, content).expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        // All should remain at original values
        assert_eq!(config.job_queue_interval, 5.0);
        assert_eq!(config.job_queue_processes, 10);
        assert_eq!(config.nap_time, 1.0);
        assert_eq!(config.startup_delay, 3.0);
        assert_eq!(config.error_delay, 0.5);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_infinity_rejected() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 10,
            pool_size: 10,
            nap_time: 1.0,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_inf.conf");
        let content = "job_queue_interval=inf\nnap_time=inf\nstartup_delay=inf\nerror_delay=inf\n";
        fs::write(&path, content).expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        assert_eq!(config.job_queue_interval, 5.0);
        assert_eq!(config.nap_time, 1.0);
        assert_eq!(config.startup_delay, 3.0);
        assert_eq!(config.error_delay, 0.5);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_unchanged_values_preserved() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 10,
            pool_size: 10,
            nap_time: 1.0,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        // Set pidfile to same value — should remain unchanged
        let path = temp_path("pg_dbms_job_noop.conf");
        fs::write(&path, "pidfile=/tmp/pg_dbms_job.pid\n").expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        assert_eq!(config.pidfile, "/tmp/pg_dbms_job.pid");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_debug_toggle() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_dbg.conf");
        fs::write(&path, "debug=1\n").expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        assert!(config.debug);

        // Turn off
        fs::write(&path, "debug=0\n").expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        assert!(!config.debug);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_pool_size_valid() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_pool.conf");
        fs::write(&path, "pool_size=25\n").expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        assert_eq!(config.pool_size, 25);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_pool_size_invalid_rejected() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_pool_invalid.conf");
        let content = "pool_size=0\npool_size=-10\npool_size=notanumber\n";
        fs::write(&path, content).expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        // Zero, negative, and non-numeric values all rejected — stays at default
        assert_eq!(config.pool_size, 100);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_config_dbinfo_all_fields() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: String::new(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
            pool_size: 100,
            nap_time: 0.1,
            startup_delay: 3.0,
            error_delay: 0.5,
        };
        let mut dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };

        let path = temp_path("pg_dbms_job_dbinfo.conf");
        let content =
            "host=db.example.com\ndatabase=production\nuser=scheduler\npasswd=s3cret\nport=5433\n";
        fs::write(&path, content).expect("write");
        read_config(path.to_str().unwrap(), &mut config, &mut dbinfo, false);
        assert_eq!(dbinfo.host, "db.example.com");
        assert_eq!(dbinfo.database, "production");
        assert_eq!(dbinfo.user, "scheduler");
        assert_eq!(dbinfo.passwd, "s3cret");
        assert_eq!(dbinfo.port, 5433);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn apply_positive_float_accepts_valid() {
        let mut cfg = float_test_config();
        apply_positive_float(&mut cfg, "nap_time", "0.25", |c| &mut c.nap_time);
        assert_eq!(cfg.nap_time, 0.25);
        // Other fields are untouched.
        assert_eq!(cfg.startup_delay, 13.0);
        assert_eq!(cfg.error_delay, 17.0);
        assert_eq!(cfg.job_queue_interval, 7.0);
    }

    #[test]
    fn apply_positive_float_rejects_zero() {
        let mut cfg = float_test_config();
        apply_positive_float(&mut cfg, "nap_time", "0", |c| &mut c.nap_time);
        assert_eq!(cfg.nap_time, 11.0, "zero must not overwrite default");
    }

    #[test]
    fn apply_positive_float_rejects_negative() {
        let mut cfg = float_test_config();
        apply_positive_float(&mut cfg, "startup_delay", "-1.5", |c| &mut c.startup_delay);
        assert_eq!(cfg.startup_delay, 13.0);
    }

    #[test]
    fn apply_positive_float_rejects_nan() {
        let mut cfg = float_test_config();
        apply_positive_float(&mut cfg, "error_delay", "NaN", |c| &mut c.error_delay);
        assert_eq!(cfg.error_delay, 17.0);
    }

    #[test]
    fn apply_positive_float_rejects_infinity() {
        let mut cfg = float_test_config();
        apply_positive_float(&mut cfg, "job_queue_interval", "inf", |c| {
            &mut c.job_queue_interval
        });
        assert_eq!(cfg.job_queue_interval, 7.0);
        // Negative infinity is a separate code path through the parser.
        apply_positive_float(&mut cfg, "job_queue_interval", "-inf", |c| {
            &mut c.job_queue_interval
        });
        assert_eq!(cfg.job_queue_interval, 7.0);
    }

    #[test]
    fn apply_positive_float_rejects_unparseable() {
        let mut cfg = float_test_config();
        apply_positive_float(&mut cfg, "nap_time", "not-a-number", |c| &mut c.nap_time);
        assert_eq!(cfg.nap_time, 11.0);
        // An empty string is also unparseable.
        apply_positive_float(&mut cfg, "nap_time", "", |c| &mut c.nap_time);
        assert_eq!(cfg.nap_time, 11.0);
    }

    #[test]
    fn apply_positive_float_accepts_subnormal_positive() {
        // Tiny positive values are still finite and > 0, so they must pass.
        // (The scheduler will sleep for a vanishingly small interval — the
        // policy decision is "any positive finite number"; we don't second
        // -guess the operator.)
        let mut cfg = float_test_config();
        apply_positive_float(&mut cfg, "nap_time", "1e-300", |c| &mut c.nap_time);
        assert_eq!(cfg.nap_time, 1e-300);
    }

    #[test]
    fn apply_positive_float_independence_across_fields() {
        // Each call only touches the field selected by its closure — this is
        // the property the deduplication relies on.
        let mut cfg = float_test_config();
        apply_positive_float(&mut cfg, "nap_time", "1.0", |c| &mut c.nap_time);
        apply_positive_float(&mut cfg, "startup_delay", "2.0", |c| &mut c.startup_delay);
        apply_positive_float(&mut cfg, "error_delay", "3.0", |c| &mut c.error_delay);
        apply_positive_float(&mut cfg, "job_queue_interval", "4.0", |c| {
            &mut c.job_queue_interval
        });
        assert_eq!(cfg.nap_time, 1.0);
        assert_eq!(cfg.startup_delay, 2.0);
        assert_eq!(cfg.error_delay, 3.0);
        assert_eq!(cfg.job_queue_interval, 4.0);
    }
}
