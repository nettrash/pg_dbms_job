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
    /// Max number of database connections in the pool.
    pub pool_size: usize,
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
            pool_size: 2,
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

    #[test]
    fn config_clone() {
        let config = Config {
            debug: true,
            pidfile: "/tmp/test.pid".to_string(),
            logfile: "/tmp/test.log".to_string(),
            log_truncate_on_rotation: false,
            job_queue_interval: 5.0,
            job_queue_processes: 10,
            pool_size: 10,
            nap_time: 0.1,
            startup_delay: 1.0,
            error_delay: 0.5,
        };
        let cloned = config.clone();
        assert_eq!(cloned.pidfile, config.pidfile);
        assert_eq!(cloned.debug, config.debug);
        assert_eq!(cloned.job_queue_processes, config.job_queue_processes);
    }

    #[test]
    fn dbinfo_clone() {
        let dbinfo = DbInfo {
            host: "host".to_string(),
            database: "db".to_string(),
            user: "u".to_string(),
            passwd: "p".to_string(),
            port: 5433,
        };
        let cloned = dbinfo.clone();
        assert_eq!(cloned.host, "host");
        assert_eq!(cloned.port, 5433);
    }

    #[test]
    fn job_clone() {
        let job = Job {
            job: 42,
            what: "DO SOMETHING".to_string(),
            log_user: Some("admin".to_string()),
            schema_user: Some("public".to_string()),
        };
        let cloned = job.clone();
        assert_eq!(cloned.job, 42);
        assert_eq!(cloned.what, "DO SOMETHING");
        assert_eq!(cloned.log_user, Some("admin".to_string()));
        assert_eq!(cloned.schema_user, Some("public".to_string()));
    }

    #[test]
    fn job_with_none_optionals() {
        let job = Job {
            job: 0,
            what: String::new(),
            log_user: None,
            schema_user: None,
        };
        assert_eq!(job.job, 0);
        assert!(job.what.is_empty());
        assert!(job.log_user.is_none());
        assert!(job.schema_user.is_none());
    }

    #[test]
    fn jobkind_copy_and_match() {
        let kind = JobKind::Async;
        let copied = kind; // Copy
        assert!(matches!(copied, JobKind::Async));
        assert!(matches!(kind, JobKind::Async)); // still usable after copy

        let scheduled = JobKind::Scheduled;
        assert!(matches!(scheduled, JobKind::Scheduled));
        assert!(!matches!(scheduled, JobKind::Async));
    }
}
