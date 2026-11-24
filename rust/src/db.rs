//! Database connection helpers.

use crate::constants::POOL_CONNECTION_TIMEOUT_SECS;
use crate::logging::dprint;
use crate::model::{Config, DbInfo};
use crate::util::die;
use postgres::{Client, NoTls};
use r2d2_postgres::PostgresConnectionManager;
use std::fmt;
use std::time::Duration;

pub type JobPool = r2d2::Pool<JobConnectionManager>;
pub type PooledJobClient = r2d2::PooledConnection<JobConnectionManager>;

/// Describe a `postgres::Error` with what actually went wrong.
///
/// The error's own `Display` is only its kind (`db error`, `error connecting to
/// server`, ...); the reason is its `source()`. Formatting it with `{}` alone
/// therefore loses exactly the part an operator needs, which is why failed jobs
/// used to be logged and recorded as just `db error`.
///
/// An error the server reported renders like psql shows it, on one line:
/// `ERROR: division by zero`, followed by ` DETAIL: ...` and ` HINT: ...` when
/// the server sent them. Any other error keeps its kind and appends each cause,
/// e.g. `error connecting to server: Connection refused (os error 111)`.
pub fn pg_error_text(err: &postgres::Error) -> String {
    if let Some(db) = err.as_db_error() {
        return format_db_error(db.severity(), db.message(), db.detail(), db.hint());
    }
    let mut text = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// One-line rendering of a server error report, see [`pg_error_text`].
fn format_db_error(
    severity: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
) -> String {
    let mut text = format!("{severity}: {message}");
    if let Some(detail) = detail {
        text.push_str(&format!(" DETAIL: {detail}"));
    }
    if let Some(hint) = hint {
        text.push_str(&format!(" HINT: {hint}"));
    }
    text
}

/// A `postgres::Error` whose `Display` is [`pg_error_text`].
#[derive(Debug)]
pub struct PgError(pub postgres::Error);

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&pg_error_text(&self.0))
    }
}

impl std::error::Error for PgError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Connection manager for the job pool: [`PostgresConnectionManager`] with
/// errors that keep the server's message.
///
/// When the pool cannot hand out a connection, r2d2 reports
/// `timed out waiting for connection: <last connect error>`, built from that
/// error's `to_string()`. With a bare `postgres::Error` the reason was only its
/// kind, so a cause such as `FATAL: sorry, too many clients already` never
/// reached the log; [`PgError`] carries it through.
pub struct JobConnectionManager(PostgresConnectionManager<NoTls>);

impl r2d2::ManageConnection for JobConnectionManager {
    type Connection = Client;
    type Error = PgError;

    fn connect(&self) -> Result<Client, PgError> {
        self.0.connect().map_err(PgError)
    }

    fn is_valid(&self, client: &mut Client) -> Result<(), PgError> {
        self.0.is_valid(client).map_err(PgError)
    }

    fn has_broken(&self, client: &mut Client) -> bool {
        self.0.has_broken(client)
    }
}

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

impl From<postgres::Error> for ConnectError {
    fn from(err: postgres::Error) -> Self {
        ConnectError::Other(pg_error_text(&err))
    }
}

/// Connect to the scheduler database and set up notifications.
///
/// Returns an error if another scheduler instance is already running.
pub fn connect_db(dbinfo: &DbInfo, config: &Config) -> Result<Client, ConnectError> {
    let conn_str = build_conn_str(dbinfo);
    let mut client = Client::connect(&conn_str, NoTls)?;
    client.batch_execute("SET application_name TO 'pg_dbms_job:main'")?;

    let row = client
        .query_one(
            "SELECT count(*), pg_is_in_recovery() FROM pg_catalog.pg_stat_activity WHERE datname=$1 AND application_name='pg_dbms_job:main'",
            &[&dbinfo.database],
        )?;
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
        client.batch_execute("LISTEN dbms_job_scheduled_notify")?;
        client.batch_execute("LISTEN dbms_job_async_notify")?;
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
///
/// It does apply the same recovery guard as [`connect_db`]: a standby is
/// read-only, so every reap UPDATE would fail there. Returning
/// [`ConnectError::InRecovery`] drops the connection, so no maintenance session
/// is left on the standby; the caller backs off and retries, and picks the
/// database up again once it is promoted.
pub fn connect_maintenance(dbinfo: &DbInfo) -> Result<Client, ConnectError> {
    let conn_str = build_conn_str(dbinfo);
    let mut client = Client::connect(&conn_str, NoTls)?;
    client.batch_execute("SET application_name TO 'pg_dbms_job:maintenance'")?;
    let in_recovery: bool = client.query_one("SELECT pg_is_in_recovery()", &[])?.get(0);
    if in_recovery {
        return Err(ConnectError::InRecovery);
    }
    Ok(client)
}

/// Create a connection pool for job execution.
pub fn create_job_pool(dbinfo: &DbInfo, pool_size: u32) -> Result<JobPool, String> {
    let conn_str = build_conn_str(dbinfo);
    let manager = JobConnectionManager(PostgresConnectionManager::new(
        conn_str
            .parse()
            .map_err(|e: postgres::Error| pg_error_text(&e))?,
        NoTls,
    ));
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
        .map_err(|e| pg_error_text(&e))?;
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
    use super::{ConnectError, PgError, build_conn_str, format_db_error, pg_error_text};
    use crate::model::DbInfo;

    /// A real `postgres::Error` without a server: a malformed connection string.
    /// Its own `Display` is only the kind; the reason is its source.
    fn config_parse_error() -> postgres::Error {
        "port=not-a-port"
            .parse::<postgres::Config>()
            .expect_err("port must be numeric")
    }

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

    #[test]
    fn pg_error_text_keeps_the_cause_the_kind_alone_drops() {
        let err = config_parse_error();
        // The regression: `to_string()` is just the kind.
        assert_eq!(err.to_string(), "invalid connection string");
        assert_eq!(
            pg_error_text(&err),
            "invalid connection string: invalid value for option `port`"
        );
    }

    #[test]
    fn format_db_error_reads_like_psql_on_one_line() {
        assert_eq!(
            format_db_error("ERROR", "division by zero", None, None),
            "ERROR: division by zero"
        );
        assert_eq!(
            format_db_error(
                "ERROR",
                "duplicate key value violates unique constraint \"t_pkey\"",
                Some("Key (id)=(1) already exists."),
                Some("Use another id."),
            ),
            "ERROR: duplicate key value violates unique constraint \"t_pkey\" \
             DETAIL: Key (id)=(1) already exists. HINT: Use another id."
        );
        assert_eq!(
            format_db_error("FATAL", "sorry, too many clients already", None, None),
            "FATAL: sorry, too many clients already"
        );
    }

    #[test]
    fn connect_error_from_postgres_error_keeps_the_cause() {
        let err = ConnectError::from(config_parse_error());
        assert_eq!(
            err.to_string(),
            "invalid connection string: invalid value for option `port`"
        );
    }

    #[test]
    fn pg_error_display_matches_pg_error_text_and_keeps_the_source() {
        let wrapped = PgError(config_parse_error());
        assert_eq!(wrapped.to_string(), pg_error_text(&wrapped.0));
        // r2d2 builds its pool-timeout message from this Display, so the
        // reason must be in it, not only in the source chain.
        assert!(
            wrapped
                .to_string()
                .contains("invalid value for option `port`")
        );
        assert!(std::error::Error::source(&wrapped).is_some());
    }
}
