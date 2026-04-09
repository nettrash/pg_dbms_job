//! Database connection helpers.

use crate::logging::dprint;
use crate::model::{Config, DbInfo};
use crate::util::die;
use postgres::{Client, NoTls};
use r2d2_postgres::PostgresConnectionManager;

pub type JobPool = r2d2::Pool<PostgresConnectionManager<NoTls>>;
pub type PooledJobClient = r2d2::PooledConnection<PostgresConnectionManager<NoTls>>;

/// Connect to the scheduler database and set up notifications.
///
/// Returns an error if another scheduler instance is already running.
pub fn connect_db(dbinfo: &DbInfo, config: &Config) -> Result<Client, String> {
    let conn_str = build_conn_str(dbinfo);
    let mut client = Client::connect(&conn_str, NoTls).map_err(|e| e.to_string())?;
    client
        .batch_execute("SET application_name TO 'pg_dbms_job:main'")
        .map_err(|e| e.to_string())?;

    let row = client
        .query_one(
            "SELECT count(*), pg_is_in_recovery() FROM pg_catalog.pg_stat_activity WHERE datname=$1 AND application_name='pg_dbms_job:main'",
            &[&dbinfo.database],
        )
        .map_err(|e| e.to_string())?;
    let count: i64 = row.get(0);
    let in_recovery: bool = row.get(1);
    if count > 1 {
        dprint(
            config,
            "FATAL",
            "another pg_dbms_job process is running on this database! Aborting.",
        );
        die("FATAL: another pg_dbms_job process is running on this database! Aborting.");
    }
    if in_recovery {
        return Err("database is in recovery".to_string());
    }

    client
        .batch_execute("LISTEN dbms_job_scheduled_notify")
        .map_err(|e| e.to_string())?;
    client
        .batch_execute("LISTEN dbms_job_async_notify")
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
        .build(manager)
        .map_err(|e| e.to_string())
}

/// Get a connection from the pool for a specific job execution.
pub fn get_job_connection(
    pool: &JobPool,
    application_name: &str,
) -> Result<PooledJobClient, String> {
    let mut client = pool.get().map_err(|e| e.to_string())?;
    client
        .batch_execute(&format!("SET application_name TO '{application_name}'"))
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
    use super::build_conn_str;
    use crate::model::DbInfo;

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
}
