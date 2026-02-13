//! Configuration file parsing and application.

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
            dprint(
                config,
                "ERROR",
                &format!("can not find the configuration file {config_file}"),
            );
            return;
        }
    }
    let content = content.unwrap();
    let lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();

    // Load logfile first so subsequent logs go to the configured location.
    for line in &lines {
        if let Some((var, val)) = parse_config_line(line)
            && var == "logfile"
            && config.logfile != val
        {
            config.logfile = val;
            dprint(
                config,
                "LOG",
                &format!(
                    "Setting logfile from configuration file to {}",
                    config.logfile
                ),
            );
        }
    }

    // Apply remaining settings and database connection information.
    for line in &lines {
        if let Some((var, val)) = parse_config_line(line) {
            match var.as_str() {
                "pidfile" => {
                    if config.pidfile != val {
                        config.pidfile = val;
                        dprint(
                            config,
                            "LOG",
                            &format!(
                                "Setting pidfile from configuration file to {}",
                                config.pidfile
                            ),
                        );
                    }
                }
                "debug" => {
                    let debug_val = val.parse::<i32>().unwrap_or(0) != 0;
                    if config.debug != debug_val {
                        config.debug = debug_val;
                        dprint(
                            config,
                            "LOG",
                            &format!(
                                "Setting debug from configuration file to {}",
                                config.debug as i32
                            ),
                        );
                    }
                }
                "job_queue_interval" => {
                    if let Ok(v) = val.parse::<f64>() {
                        config.job_queue_interval = v;
                        dprint(
                            config,
                            "LOG",
                            &format!(
                                "Setting job_queue_interval from configuration file to {}",
                                config.job_queue_interval
                            ),
                        );
                    }
                }
                "job_queue_processes" => {
                    if let Ok(v) = val.parse::<usize>()
                        && config.job_queue_processes != v
                    {
                        config.job_queue_processes = v;
                        dprint(
                            config,
                            "LOG",
                            &format!(
                                "Setting job_queue_processes from configuration file to {}",
                                config.job_queue_processes
                            ),
                        );
                    }
                }
                "nap_time" => {
                    if let Ok(v) = val.parse::<f64>() {
                        config.nap_time = v;
                        dprint(
                            config,
                            "LOG",
                            &format!(
                                "Setting nap_time from configuration file to {}",
                                config.nap_time
                            ),
                        );
                    }
                }
                "startup_delay" => {
                    if let Ok(v) = val.parse::<f64>() {
                        config.startup_delay = v;
                        dprint(
                            config,
                            "LOG",
                            &format!(
                                "Setting startup_delay from configuration file to {}",
                                config.startup_delay
                            ),
                        );
                    }
                }
                "error_delay" => {
                    if let Ok(v) = val.parse::<f64>() {
                        config.error_delay = v;
                        dprint(
                            config,
                            "LOG",
                            &format!(
                                "Setting error_delay from configuration file to {}",
                                config.error_delay
                            ),
                        );
                    }
                }
                "host" => dbinfo.host = val,
                "database" => dbinfo.database = val,
                "user" => dbinfo.user = val,
                "passwd" => dbinfo.passwd = val,
                "port" => {
                    if let Ok(v) = val.parse::<u16>() {
                        dbinfo.port = v;
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
    use super::{parse_config_line, read_config};
    use crate::model::{Config, DbInfo};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn read_config_updates_values() {
        let mut config = Config {
            debug: false,
            pidfile: "/tmp/pg_dbms_job.pid".to_string(),
            logfile: "".to_string(),
            log_truncate_on_rotation: false,
            job_queue_interval: 0.1,
            job_queue_processes: 1024,
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

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pg_dbms_job_test_{now}.conf"));
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
}
