//! Database connection helpers.

use crate::constants::POOL_CONNECTION_TIMEOUT_SECS;
use crate::logging::dprint;
use crate::model::{Config, DbInfo};
use crate::util::die;
use postgres::{Client, NoTls};
use r2d2_postgres::PostgresConnectionManager;
use std::fmt;
use std::time::Duration;

pub type JobPool = r2d2::Pool<PostgresConnectionManager<NoTls>>;
pub type PooledJobClient = r2d2::PooledConnection<PostgresConnectionManager<NoTls>>;

/// Error type for database connection failures.
#[derive(Debug)]
pub enum ConnectError {
    /// The database is a replica in recovery mode.
    InRecovery,
    /// Any other connection error.
    Other(String),
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectError::InRecovery => write!(f, "database is in recovery"),
            ConnectError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

/// Connect to the scheduler database and set up notifications.
///
/// Returns an error if another scheduler instance is already running.
pub fn connect_db(dbinfo: &DbInfo, config: &Config) -> Result<Client, ConnectError> {
    let conn_str = build_conn_str(dbinfo);
    let mut client =
        Client::connect(&conn_str, NoTls).map_err(|e| ConnectError::Other(e.to_string()))?;
    client
        .batch_execute("SET application_name TO 'pg_dbms_job:main'")
        .map_err(|e| ConnectError::Other(e.to_string()))?;

    let row = client
        .query_one(
            "SELECT count(*), pg_is_in_recovery() FROM pg_catalog.pg_stat_activity WHERE datname=$1 AND application_name='pg_dbms_job:main'",
            &[&dbinfo.database],
        )
        .map_err(|e| ConnectError::Other(e.to_string()))?;
    let count: i64 = row.get(0);
    let in_recovery: bool = row.get(1);
    if count > 1 && !config.allow_concurrent_schedulers {
        dprint(
            config,
            "FATAL",
            "another pg_dbms_job process is running on this database! Aborting. \
             (set allow_concurrent_schedulers=1 to run multiple schedulers against one database)",
        );
        die("FATAL: another pg_dbms_job process is running on this database! Aborting.");
    }
    if in_recovery {
        return Err(ConnectError::InRecovery);
    }

    // Warn loudly if the scheduler's own role is subject to Row Level Security
    // on the job tables. The RLS policy is `USING (log_user = current_user)`, so
    // a non-owner, non-superuser role silently sees and claims ZERO jobs
    // submitted by other users — they appear to vanish with no error. Owner,
    // superuser and BYPASSRLS roles are exempt (this is the documented
    // requirement). A NULL result means the tables don't exist yet (extension
    // not installed), so the check is skipped. Best-effort: never fatal.
    match client.query_one(
        "SELECT bool_and(r.rolsuper OR r.rolbypassrls \
                OR (c.relowner = r.oid AND NOT c.relforcerowsecurity)) \
           FROM pg_catalog.pg_class c \
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
           JOIN pg_catalog.pg_roles r ON r.rolname = current_user \
          WHERE n.nspname = 'dbms_job' \
            AND c.relname IN ('all_scheduled_jobs', 'all_async_jobs')",
        &[],
    ) {
        Ok(row) if row.get::<_, Option<bool>>(0) == Some(false) => {
            dprint(
                config,
                "WARNING",
                "the scheduler's database role is subject to Row Level Security on the dbms_job \
                 tables: it will only see jobs it submitted itself and will silently skip jobs \
                 owned by other users. Connect as the tables' owner or a superuser (see README).",
            );
        }
        Ok(_) => {}
        Err(_) => dprint(
            config,
            "DEBUG",
            "could not verify the scheduler role's RLS exemption (non-fatal)",
        ),
    }

    // Only register the NOTIFY listeners when notify wake-up is enabled. With
    // enable_notify=off the daemon relies purely on the job_queue_interval poll,
    // so it must not LISTEN — a listener that falls behind (e.g. while its worker
    // pool is saturated) pins the cluster-wide async-notify queue tail.
    if config.enable_notify {
        client
            .batch_execute("LISTEN dbms_job_scheduled_notify")
            .map_err(|e| ConnectError::Other(e.to_string()))?;
        client
            .batch_execute("LISTEN dbms_job_async_notify")
            .map_err(|e| ConnectError::Other(e.to_string()))?;
        dprint(
            config,
            "LOG",
            "NOTIFY wake-up enabled: LISTEN on dbms_job_async_notify and dbms_job_scheduled_notify",
        );
    } else {
        dprint(
            config,
            "LOG",
            "NOTIFY wake-up disabled (enable_notify=off); dispatching on job_queue_interval poll only",
        );
    }

    Ok(client)
}

/// Connect a standalone client for the background maintenance thread (stale-job
/// reaping and stats). Unlike [`connect_db`] this does NOT run the singleton
/// guard — it uses a distinct `application_name` and is expected to coexist with
/// the `pg_dbms_job:main` connection — and it does not LISTEN for notifications.
pub fn connect_maintenance(dbinfo: &DbInfo) -> Result<Client, String> {
    let conn_str = build_conn_str(dbinfo);
    let mut client =
        Client::connect(&conn_str, NoTls).map_err(|e: postgres::Error| e.to_string())?;
    client
        .batch_execute("SET application_name TO 'pg_dbms_job:maintenance'")
        .map_err(|e| e.to_string())?;
    Ok(client)
}

/// Create a connection pool for job execution.
pub fn create_job_pool(dbinfo: &DbInfo, pool_size: u32) -> Result<JobPool, String> {
    let conn_str = build_conn_str(dbinfo);
    let manager = PostgresConnectionManager::new(
        conn_str
            .parse()
            .map_err(|e: postgres::Error| e.to_string())?,
        NoTls,
    );
    r2d2::Pool::builder()
        .max_size(pool_size)
        .min_idle(Some(0))
        .connection_timeout(Duration::from_secs(POOL_CONNECTION_TIMEOUT_SECS))
        .build(manager)
        .map_err(|e| e.to_string())
}

/// Get a connection from the pool for a specific job execution.
pub fn get_job_connection(
    pool: &JobPool,
    application_name: &str,
) -> Result<PooledJobClient, String> {
    let mut client = pool.get().map_err(|e| e.to_string())?;
    let sanitized_name = application_name.replace('\'', "''");
    client
        .batch_execute(&format!("SET application_name TO '{sanitized_name}'"))
        .map_err(|e| e.to_string())?;
    Ok(client)
}

/// Reset session state on a pooled connection after job execution.
pub fn reset_job_connection(client: &mut PooledJobClient) {
    let _ = client
        .batch_execute("RESET ROLE; RESET search_path; SET application_name TO 'pg_dbms_job:idle'");
}

/// Build a libpq-style connection string from settings.
fn build_conn_str(dbinfo: &DbInfo) -> String {
    format!(
        "host={} port={} user={} password={} dbname={}",
        dbinfo.host, dbinfo.port, dbinfo.user, dbinfo.passwd, dbinfo.database
    )
}

#[cfg(test)]
mod tests {
    use super::{ConnectError, build_conn_str};
    use crate::model::DbInfo;

    #[test]
    fn connect_error_in_recovery_display() {
        let err = ConnectError::InRecovery;
        assert_eq!(err.to_string(), "database is in recovery");
    }

    #[test]
    fn connect_error_other_display() {
        let err = ConnectError::Other("connection refused".to_string());
        assert_eq!(err.to_string(), "connection refused");
    }

    #[test]
    fn connect_error_in_recovery_debug() {
        let err = ConnectError::InRecovery;
        let debug = format!("{:?}", err);
        assert!(debug.contains("InRecovery"));
    }

    #[test]
    fn connect_error_other_debug() {
        let err = ConnectError::Other("timeout".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Other"));
        assert!(debug.contains("timeout"));
    }

    #[test]
    fn connect_error_variants_are_distinct() {
        let recovery = ConnectError::InRecovery;
        let other = ConnectError::Other("database is in recovery".to_string());
        // Both display the same text, but the enum discriminant differs
        assert_eq!(recovery.to_string(), other.to_string());
        assert!(matches!(recovery, ConnectError::InRecovery));
        assert!(matches!(other, ConnectError::Other(_)));
    }

    #[test]
    fn build_conn_str_includes_fields() {
        let dbinfo = DbInfo {
            host: "localhost".to_string(),
            database: "db".to_string(),
            user: "user".to_string(),
            passwd: "pass".to_string(),
            port: 5432,
        };
        let conn = build_conn_str(&dbinfo);
        assert!(conn.contains("host=localhost"));
        assert!(conn.contains("port=5432"));
        assert!(conn.contains("user=user"));
        assert!(conn.contains("password=pass"));
        assert!(conn.contains("dbname=db"));
    }

    #[test]
    fn build_conn_str_different_port() {
        let dbinfo = DbInfo {
            host: "192.168.1.1".to_string(),
            database: "mydb".to_string(),
            user: "admin".to_string(),
            passwd: "secret".to_string(),
            port: 5433,
        };
        let conn = build_conn_str(&dbinfo);
        assert!(conn.contains("host=192.168.1.1"));
        assert!(conn.contains("port=5433"));
        assert!(conn.contains("dbname=mydb"));
    }

    #[test]
    fn build_conn_str_empty_fields() {
        let dbinfo = DbInfo {
            host: String::new(),
            database: String::new(),
            user: String::new(),
            passwd: String::new(),
            port: 5432,
        };
        let conn = build_conn_str(&dbinfo);
        assert!(conn.contains("host="));
        assert!(conn.contains("dbname="));
    }

    #[test]
    fn build_conn_str_special_chars_in_password() {
        let dbinfo = DbInfo {
            host: "localhost".to_string(),
            database: "db".to_string(),
            user: "user".to_string(),
            passwd: "p@ss w0rd=!".to_string(),
            port: 5432,
        };
        let conn = build_conn_str(&dbinfo);
        assert!(conn.contains("password=p@ss w0rd=!"));
    }

    #[test]
    fn build_conn_str_field_order() {
        let dbinfo = DbInfo {
            host: "h".to_string(),
            database: "d".to_string(),
            user: "u".to_string(),
            passwd: "p".to_string(),
            port: 1234,
        };
        let conn = build_conn_str(&dbinfo);
        let host_pos = conn.find("host=").unwrap();
        let port_pos = conn.find("port=").unwrap();
        let user_pos = conn.find("user=").unwrap();
        let pass_pos = conn.find("password=").unwrap();
        let db_pos = conn.find("dbname=").unwrap();
        // host comes first, then port, user, password, dbname
        assert!(host_pos < port_pos);
        assert!(port_pos < user_pos);
        assert!(user_pos < pass_pos);
        assert!(pass_pos < db_pos);
    }

    #[test]
    fn connect_error_display_impl() {
        let err = ConnectError::Other("test error".to_string());
        // Verify Display impl works via to_string
        let s = format!("{err}");
        assert_eq!(s, "test error");
    }
}
