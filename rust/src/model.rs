#[derive(Clone)]
pub struct Config {
    pub debug: bool,
    pub pidfile: String,
    pub logfile: String,
    pub log_truncate_on_rotation: bool,
    pub job_queue_interval: f64,
    pub job_queue_processes: usize,
    pub nap_time: f64,
}

#[derive(Clone)]
pub struct DbInfo {
    pub host: String,
    pub database: String,
    pub user: String,
    pub passwd: String,
    pub port: u16,
}

#[derive(Clone)]
pub struct Job {
    pub job: i64,
    pub what: String,
    pub log_user: Option<String>,
    pub schema_user: Option<String>,
}

#[derive(Copy, Clone)]
pub enum JobKind {
    Async,
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
