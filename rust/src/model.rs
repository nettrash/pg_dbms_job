//! Data models shared across the scheduler.

use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Interval (seconds) between periodic job-statistics LOG lines.
    /// 0 disables periodic stats logging.
    pub stats_interval: u64,
}

/// Cross-thread counters incremented by worker threads.
///
/// `started` is bumped when a worker enters `execute_job`; `finished` is bumped
/// when it leaves (including via early-return error paths and panics, courtesy
/// of [`JobStatsGuard`]). The main loop reads the counters periodically via
/// [`JobStats::drain`] and emits them at LOG level.
#[derive(Default)]
pub struct JobStats {
    pub started: AtomicU64,
    pub finished: AtomicU64,
}

impl JobStats {
    /// Atomically read and reset both counters. Returns `(started, finished)`.
    pub fn drain(&self) -> (u64, u64) {
        (
            self.started.swap(0, Ordering::Relaxed),
            self.finished.swap(0, Ordering::Relaxed),
        )
    }
}

/// RAII guard that bumps `started` on construction and `finished` on drop.
///
/// Using Drop means every exit from `execute_job` — clean return, early error
/// return, or unwind from a panic — is counted as a finished job exactly once.
pub struct JobStatsGuard<'a> {
    stats: &'a JobStats,
}

impl<'a> JobStatsGuard<'a> {
    pub fn new(stats: &'a JobStats) -> Self {
        stats.started.fetch_add(1, Ordering::Relaxed);
        Self { stats }
    }
}

impl Drop for JobStatsGuard<'_> {
    fn drop(&mut self) {
        self.stats.finished.fetch_add(1, Ordering::Relaxed);
    }
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

impl JobKind {
    /// Short label used in log lines and the application_name PG metadata.
    pub fn label(self) -> &'static str {
        match self {
            JobKind::Async => "async",
            JobKind::Scheduled => "scheduled",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, DbInfo, Job, JobKind, JobStats, JobStatsGuard};

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
            stats_interval: 0,
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
            stats_interval: 30,
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

    #[test]
    fn jobkind_label_async() {
        assert_eq!(JobKind::Async.label(), "async");
    }

    #[test]
    fn jobkind_label_scheduled() {
        assert_eq!(JobKind::Scheduled.label(), "scheduled");
    }

    #[test]
    fn job_stats_default_is_zero_and_drain_resets() {
        let stats = JobStats::default();
        assert_eq!(stats.drain(), (0, 0));
        stats
            .started
            .fetch_add(3, std::sync::atomic::Ordering::Relaxed);
        stats
            .finished
            .fetch_add(2, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(stats.drain(), (3, 2));
        // After drain both counters return to zero.
        assert_eq!(stats.drain(), (0, 0));
    }

    #[test]
    fn job_stats_guard_increments_started_and_finished() {
        let stats = JobStats::default();
        {
            let _g = JobStatsGuard::new(&stats);
            // While the guard is alive, started is bumped but finished isn't yet.
            assert_eq!(stats.started.load(std::sync::atomic::Ordering::Relaxed), 1);
            assert_eq!(stats.finished.load(std::sync::atomic::Ordering::Relaxed), 0);
        }
        // After drop, finished caught up.
        assert_eq!(stats.drain(), (1, 1));
    }

    #[test]
    fn job_stats_guard_counts_panicking_workers() {
        // The guard's Drop runs during unwinding, so a panicking worker still
        // contributes to the finished count — important for the LOG output
        // not to silently undercount.
        let stats = JobStats::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = JobStatsGuard::new(&stats);
            panic!("worker exploded");
        }));
        assert!(result.is_err());
        assert_eq!(stats.drain(), (1, 1));
    }

    #[test]
    fn jobkind_label_is_stable_across_copies() {
        // The label is used in `application_name` strings emitted to PG and
        // in log lines: changing it would break existing log-grepping
        // pipelines, so this test pins the spelling.
        let kinds = [JobKind::Async, JobKind::Scheduled];
        let labels: Vec<&str> = kinds.iter().map(|k| k.label()).collect();
        assert_eq!(labels, vec!["async", "scheduled"]);
    }
}
