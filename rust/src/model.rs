//! Data models shared across the scheduler.

#[derive(Clone)]
/// Runtime configuration values for the scheduler.
pub struct Config {
    /// Whether debug logging is enabled.
    pub debug: bool,
    /// Path to the daemon pid file.
    pub pidfile: String,
    /// Path template for the log file.
    pub logfile: String,
    /// Whether to truncate log file on rotation.
    pub log_truncate_on_rotation: bool,
    /// Interval (seconds) for queue polling.
    pub job_queue_interval: f64,
    /// Max number of concurrent jobs.
    pub job_queue_processes: usize,
    /// Sleep time between loops (seconds).
    pub nap_time: f64,
    /// Initial delay before starting the scheduler or when we need to restart it (seconds).
    pub startup_delay: f64,
    /// Delay after an error before retrying (seconds).
    /// For example when do we reached queue limit.
    pub error_delay: f64,
}

#[derive(Clone)]
/// Database connection settings.
pub struct DbInfo {
    /// Database host.
    pub host: String,
    /// Database name.
    pub database: String,
    /// Database user.
    pub user: String,
    /// Database password.
    pub passwd: String,
    /// Database port.
    pub port: u16,
}

#[derive(Clone)]
/// A job definition fetched from the scheduler tables.
pub struct Job {
    /// Job identifier.
    pub job: i64,
    /// SQL or PL/pgSQL block to execute.
    pub what: String,
    /// Optional log owner for the job.
    pub log_user: Option<String>,
    /// Optional schema owner for the job.
    pub schema_user: Option<String>,
}

#[derive(Copy, Clone)]
/// Kind of job for execution and logging.
pub enum JobKind {
    /// Async jobs are triggered via notification or queue polling.
    Async,
    /// Scheduled jobs run at computed intervals.
    Scheduled,
}

#[cfg(test)]
mod tests {
    use super::{Config, DbInfo, Job, JobKind};

    #[test]
    fn model_structs_hold_values() {
        let config = Config {
            debug: true,
            pidfile: "/tmp/test.pid".to_string(),
            logfile: "/tmp/test.log".to_string(),
            log_truncate_on_rotation: true,
            job_queue_interval: 10.0,
            job_queue_processes: 2,
            nap_time: 0.5,
            startup_delay: 3.0,
            error_delay: 1.0,
        };
        assert!(config.debug);
        assert_eq!(config.pidfile, "/tmp/test.pid");

        let dbinfo = DbInfo {
            host: "localhost".to_string(),
            database: "db".to_string(),
            user: "user".to_string(),
            passwd: "pass".to_string(),
            port: 5432,
        };
        assert_eq!(dbinfo.database, "db");

        let job = Job {
            job: 1,
            what: "SELECT 1".to_string(),
            log_user: Some("user".to_string()),
            schema_user: None,
        };
        assert_eq!(job.job, 1);
        assert!(matches!(JobKind::Async, JobKind::Async));
    }
}
