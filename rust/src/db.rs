use crate::logging::dprint;
use crate::model::{Config, DbInfo};
use crate::util::die;
use postgres::{Client, NoTls};

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

pub fn connect_job_db(dbinfo: &DbInfo, application_name: &str) -> Result<Client, postgres::Error> {
    let conn_str = build_conn_str(dbinfo);
    let mut client = Client::connect(&conn_str, NoTls)?;
    client.batch_execute(&format!("SET application_name TO '{application_name}'"))?;
    Ok(client)
}

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
}
