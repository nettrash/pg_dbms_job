//! Job discovery and execution logic.

use crate::db::{JobPool, get_job_connection, reset_job_connection};
use crate::dlog;
use crate::logging::dprint;
use crate::model::{Config, Job, JobKind};
use chrono::Local;
use postgres::Client;
use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

/// Collect scheduled jobs that are ready to run.
///
/// Clears and refills `jobs` in place to reuse the existing allocation.
pub fn get_scheduled_jobs(
    client: &mut Client,
    config: &Config,
    config_invalidated: &mut bool,
    jobs: &mut HashMap<i64, Job>,
) {
    dprint(config, "DEBUG", "Get scheduled jobs to run");
    jobs.clear();
    let query = "UPDATE dbms_job.all_scheduled_jobs SET this_date = current_timestamp, next_date = dbms_job.get_next_date(interval), instance = instance+1 WHERE interval IS NOT NULL AND NOT broken AND this_date IS NULL AND next_date <= current_timestamp RETURNING job, what, log_user, schema_user";
    match client.query(query, &[]) {
        Ok(rows) => {
            for row in rows {
                let job = Job {
                    job: row.get::<_, i64>("job"),
                    what: row.get::<_, String>("what"),
                    log_user: row.get::<_, Option<String>>("log_user"),
                    schema_user: row.get::<_, Option<String>>("schema_user"),
                };
                jobs.insert(job.job, job);
            }
        }
        Err(err) => {
            dlog!(config, "ERROR", "can't execute statement, {err}");
            *config_invalidated = true;
        }
    }
    dlog!(
        config,
        "DEBUG",
        "Found {} scheduled jobs to run",
        jobs.len()
    );
}

/// Collect asynchronous jobs queued for execution.
///
/// Clears and refills `jobs` in place to reuse the existing allocation.
pub fn get_async_jobs(client: &mut Client, config: &Config, jobs: &mut HashMap<i64, Job>) {
    jobs.clear();
    let query = "UPDATE dbms_job.all_async_jobs SET this_date = current_timestamp WHERE this_date IS NULL RETURNING job, what, log_user, schema_user";
    if let Ok(rows) = client.query(query, &[]) {
        for row in rows {
            let job = Job {
                job: row.get::<_, i64>("job"),
                what: row.get::<_, String>("what"),
                log_user: row.get::<_, Option<String>>("log_user"),
                schema_user: row.get::<_, Option<String>>("schema_user"),
            };
            jobs.insert(job.job, job);
        }
    } else {
        dprint(config, "ERROR", "can't execute statement");
    }

    let query = "UPDATE dbms_job.all_scheduled_jobs SET this_date = current_timestamp WHERE this_date IS NULL AND interval IS NULL AND next_date <= current_timestamp RETURNING job, what, log_user, schema_user";
    if let Ok(rows) = client.query(query, &[]) {
        for row in rows {
            let job = Job {
                job: row.get::<_, i64>("job"),
                what: row.get::<_, String>("what"),
                log_user: row.get::<_, Option<String>>("log_user"),
                schema_user: row.get::<_, Option<String>>("schema_user"),
            };
            jobs.insert(job.job, job);
        }
    } else {
        dprint(config, "ERROR", "can't execute statement");
    }

    dlog!(
        config,
        "DEBUG",
        "Found {} asynchronous jobs to run",
        jobs.len()
    );
}

/// Remove a job from the async queue (or fallback to scheduled).
pub fn delete_job(client: &mut Client, config: &Config, jobid: i64) {
    dlog!(
        config,
        "DEBUG",
        "Deleting asynchronous job {jobid} from queue"
    );
    let row = client
        .query_opt(
            "DELETE FROM dbms_job.all_async_jobs WHERE job = $1 RETURNING job",
            &[&jobid],
        )
        .ok()
        .flatten();
    if row.is_none() {
        let _ = client.execute(
            "DELETE FROM dbms_job.all_scheduled_jobs WHERE job = $1",
            &[&jobid],
        );
    }
}

/// Spawn a worker thread to execute a job.
pub fn spawn_job(
    kind: JobKind,
    job: Job,
    pool: &Arc<JobPool>,
    config: &Arc<Config>,
    running_workers: &mut HashMap<u64, JoinHandle<()>>,
    next_worker_id: &mut u64,
) {
    let worker_id = *next_worker_id;
    *next_worker_id = next_worker_id.wrapping_add(1);

    let pool_clone = Arc::clone(pool);
    let config_clone = Arc::clone(config);

    let handle = std::thread::spawn(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match kind {
            JobKind::Async => subprocess_async(job, &pool_clone, &config_clone),
            JobKind::Scheduled => subprocess_scheduled(job, &pool_clone, &config_clone),
        }));
    });

    running_workers.insert(worker_id, handle);
}

/// Execute an asynchronous job in a child process.
fn subprocess_async(job: Job, pool: &Arc<JobPool>, config: &Config) {
    let start_t = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    dlog!(config, "LOG", "executing async job {}", job.job);

    dlog!(
        config,
        "DEBUG",
        "connecting to database for job {}",
        job.job
    );

    let app_name = format!("pg_dbms_job:async:{}", job.job);
    let mut client = match get_job_connection(pool, &app_name) {
        Ok(c) => c,
        Err(err) => {
            dlog!(config, "ERROR", "{}", err);
            return;
        }
    };

    dlog!(config, "DEBUG", "connected to database for job {}", job.job);

    if let Some(log_user) = &job.log_user {
        let quoted = quote_ident(log_user);
        dlog!(config, "DEBUG", "SET ROLE {quoted}");
        if let Err(err) = client.batch_execute(&format!("SET ROLE {quoted}")) {
            dlog!(config, "ERROR", "can not change role, reason: {err}");
            return;
        }
    } else {
        dprint(config, "DEBUG", "log_user is not set, using default role");
    }

    dprint(config, "DEBUG", "BEGIN");
    if let Err(err) = client.batch_execute("BEGIN") {
        dlog!(
            config,
            "ERROR",
            "can not start a transaction, reason: {err}"
        );
        return;
    }

    if let Some(schema_user) = &job.schema_user {
        let quoted_path = quote_search_path(schema_user);
        dlog!(config, "DEBUG", "SET LOCAL search_path TO {quoted_path}");
        if let Err(err) = client.batch_execute(&format!("SET LOCAL search_path TO {quoted_path}")) {
            dlog!(
                config,
                "ERROR",
                "can not change the search_path, reason: {err}"
            );
            return;
        }
    } else {
        dprint(
            config,
            "DEBUG",
            "schema_user is not set, using default search_path",
        );
    }

    let mut status_text = String::new();
    let mut err_text = String::new();
    let mut sqlstate = String::new();

    let t0 = Instant::now();
    let code = build_do_block(job.job, &job.what);
    dprint(config, "DEBUG", "code to execute:");
    dprint(config, "DEBUG", &code);
    if let Err(err) = client.batch_execute(&code) {
        err_text = err.to_string();
        sqlstate = err.code().map(|c| c.code().to_string()).unwrap_or_default();
        status_text = "ERROR".to_string();
        dlog!(
            config,
            "ERROR",
            "job {} failure, reason: {}",
            job.job,
            err_text
        );
        dprint(config, "DEBUG", "ROLLBACK");
        if let Err(err) = client.batch_execute("ROLLBACK") {
            dlog!(
                config,
                "ERROR",
                "can not rollback a transaction, reason: {err}"
            );
        }
    } else {
        dprint(config, "DEBUG", "COMMIT");

        if let Err(err) = client.batch_execute("COMMIT") {
            dlog!(
                config,
                "ERROR",
                "can not commit a transaction, reason: {err}"
            );
        }
    }

    dprint(config, "DEBUG", "delete job");
    delete_job(&mut client, config, job.job);

    let duration_secs = t0.elapsed().as_secs() as i64;
    let details = JobExecutionDetails {
        owner: job.log_user.as_deref().unwrap_or(""),
        jobid: job.job,
        start_date: &start_t,
        duration_secs,
        status_text: &status_text,
        err_text: &err_text,
        sqlstate: &sqlstate,
    };
    dlog!(
        config,
        "DEBUG",
        "storing job execution details: {:?}",
        details
    );
    store_job_execution_details(&mut client, details);

    reset_job_connection(&mut client);

    dlog!(
        config,
        "LOG",
        "finished executing async job {} in {} seconds",
        job.job,
        duration_secs
    );
}

/// Execute a scheduled job in a child process.
fn subprocess_scheduled(job: Job, pool: &Arc<JobPool>, config: &Config) {
    let start_t = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    dlog!(config, "LOG", "executing scheduled job {}", job.job);

    dlog!(
        config,
        "DEBUG",
        "connecting to database for job {}",
        job.job
    );

    let app_name = format!("pg_dbms_job:scheduled:{}", job.job);
    let mut client = match get_job_connection(pool, &app_name) {
        Ok(c) => c,
        Err(err) => {
            dlog!(config, "ERROR", "{}", err);
            return;
        }
    };

    dlog!(config, "DEBUG", "connected to database for job {}", job.job);

    if let Some(log_user) = &job.log_user {
        let quoted = quote_ident(log_user);
        dlog!(config, "DEBUG", "SET ROLE {quoted}");
        if let Err(err) = client.batch_execute(&format!("SET ROLE {quoted}")) {
            dlog!(config, "ERROR", "can not change role, reason: {err}");
            return;
        }
    } else {
        dprint(config, "DEBUG", "log_user is not set, using default role");
    }

    if let Err(err) = client.batch_execute("BEGIN") {
        dlog!(
            config,
            "ERROR",
            "can not start a transaction, reason: {err}"
        );
        return;
    }

    if let Some(schema_user) = &job.schema_user {
        let quoted_path = quote_search_path(schema_user);
        dlog!(config, "DEBUG", "SET LOCAL search_path TO {quoted_path}");
        if let Err(err) = client.batch_execute(&format!("SET LOCAL search_path TO {quoted_path}")) {
            dlog!(
                config,
                "ERROR",
                "can not change the search_path, reason: {err}"
            );
            return;
        }
    } else {
        dprint(
            config,
            "DEBUG",
            "schema_user is not set, using default search_path",
        );
    }

    let mut status_text = String::new();
    let mut err_text = String::new();
    let mut sqlstate = String::new();

    let t0 = Instant::now();
    let code = build_do_block(job.job, &job.what);
    dprint(config, "DEBUG", "code to execute:");
    dprint(config, "DEBUG", &code);
    if let Err(err) = client.batch_execute(&code) {
        err_text = err.to_string();
        sqlstate = err.code().map(|c| c.code().to_string()).unwrap_or_default();
        status_text = "ERROR".to_string();
        dlog!(
            config,
            "ERROR",
            "job {} failure, reason: {}",
            job.job,
            err_text
        );
        dprint(config, "DEBUG", "ROLLBACK");
        if let Err(err) = client.batch_execute("ROLLBACK") {
            dlog!(
                config,
                "ERROR",
                "can not rollback a transaction, reason: {err}"
            );
        } else {
            let _ = client.execute(
                "UPDATE dbms_job.all_scheduled_jobs SET this_date = NULL, failures = failures+1 WHERE job = $1",
                &[&job.job],
            );
        }
    } else {
        dprint(config, "DEBUG", "COMMIT");
        if let Err(err) = client.batch_execute("COMMIT") {
            dlog!(
                config,
                "ERROR",
                "can not commit a transaction, reason: {err}"
            );
        }

        let duration_secs = t0.elapsed().as_secs() as i64;
        let _ = client.execute(
            "UPDATE dbms_job.all_scheduled_jobs SET this_date = NULL, last_date = current_timestamp, total_time = ($1 || ' seconds')::interval, failures = 0, instance = instance+1 WHERE job = $2",
            &[&duration_secs.to_string(), &job.job],
        );
    }

    let duration_secs = t0.elapsed().as_secs() as i64;

    let details = JobExecutionDetails {
        owner: job.log_user.as_deref().unwrap_or(""),
        jobid: job.job,
        start_date: &start_t,
        duration_secs,
        status_text: &status_text,
        err_text: &err_text,
        sqlstate: &sqlstate,
    };
    dlog!(
        config,
        "DEBUG",
        "storing job execution details: {:?}",
        details
    );
    store_job_execution_details(&mut client, details);

    reset_job_connection(&mut client);

    dlog!(
        config,
        "LOG",
        "finished executing scheduled job {} in {} seconds",
        job.job,
        duration_secs
    );
}

/// Escape a PostgreSQL identifier with double-quote quoting.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quote a comma-separated list of schema names for use with SET search_path.
fn quote_search_path(raw: &str) -> String {
    raw.split(',')
        .map(|s| quote_ident(s.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Data captured for job execution history.
#[derive(Debug)]
struct JobExecutionDetails<'a> {
    owner: &'a str,
    jobid: i64,
    start_date: &'a str,
    duration_secs: i64,
    status_text: &'a str,
    err_text: &'a str,
    sqlstate: &'a str,
}

/// Store job execution details in the database.
fn store_job_execution_details(client: &mut Client, details: JobExecutionDetails<'_>) {
    let query = r#"
    INSERT INTO dbms_job.all_scheduler_job_run_details
        (owner, job_name, status, error, req_start_date, actual_start_date, run_duration, slave_pid, additional_info)
    VALUES
        ($1, $2, $3, $4::bigint, NULL,
         to_timestamp($5, 'YYYY-MM-DD HH24:MI:SS'),
         $6,
         $7, $8)
    "#;

    let error_code: Option<i64> = details.sqlstate.parse::<i64>().ok();
    let additional_info = if details.sqlstate.is_empty() {
        details.err_text.to_string()
    } else if details.err_text.is_empty() {
        format!("sqlstate={}", details.sqlstate)
    } else {
        format!("sqlstate={}, {}", details.sqlstate, details.err_text)
    };

    match client.execute(
        query,
        &[
            &details.owner,
            &details.jobid.to_string(),
            &details.status_text,
            &error_code, // parameter 3 / $4
            &details.start_date,
            &details.duration_secs, // bigint
            &(process::id() as i32),
            &additional_info,
        ],
    ) {
        Ok(_) => (),
        Err(err) => {
            if let Some(db) = err.as_db_error() {
                eprintln!(
                    "failed to store job execution details: code={} message={} detail={:?} hint={:?}",
                    db.code().code(),
                    db.message(),
                    db.detail(),
                    db.hint()
                );
            } else {
                eprintln!("failed to store job execution details: {err}");
            }
        }
    };
}

/// Build a DO block wrapper for the job body.
fn build_do_block(jobid: i64, what: &str) -> String {
    format!(
        "DO $pg_dbms_job$\nDECLARE\n\tjob bigint := {jobid};\n\tnext_date timestamp with time zone := current_timestamp;\n\tbroken boolean := false;\nBEGIN\n\t{what}\nEND;\n$pg_dbms_job$;"
    )
}

#[cfg(test)]
mod tests {
    use super::{build_do_block, quote_ident, quote_search_path};

    #[test]
    fn build_do_block_includes_job_and_code() {
        let code = "RAISE NOTICE 'hello';";
        let block = build_do_block(42, code);
        assert!(block.contains("job bigint := 42"));
        assert!(block.contains(code));
        assert!(block.contains("DO $pg_dbms_job$"));
    }

    #[test]
    fn build_do_block_structure() {
        let block = build_do_block(1, "NULL;");
        assert!(block.starts_with("DO $pg_dbms_job$\n"));
        assert!(block.contains("DECLARE\n"));
        assert!(block.contains("BEGIN\n"));
        assert!(block.contains("\nEND;\n$pg_dbms_job$;"));
        assert!(block.contains("next_date timestamp with time zone"));
        assert!(block.contains("broken boolean := false"));
    }

    #[test]
    fn build_do_block_negative_jobid() {
        let block = build_do_block(-1, "SELECT 1;");
        assert!(block.contains("job bigint := -1"));
    }

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("myuser"), "\"myuser\"");
    }

    #[test]
    fn quote_ident_with_double_quotes() {
        assert_eq!(quote_ident("my\"user"), "\"my\"\"user\"");
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn quote_ident_multiple_quotes() {
        assert_eq!(quote_ident("a\"b\"c"), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn quote_ident_with_spaces() {
        assert_eq!(quote_ident("my user"), "\"my user\"");
    }

    #[test]
    fn quote_ident_sql_injection_attempt() {
        let result = quote_ident("admin\"; DROP TABLE users; --");
        // The embedded double-quote is doubled, preventing breakout
        assert_eq!(result, "\"admin\"\"; DROP TABLE users; --\"");
    }

    #[test]
    fn quote_search_path_single() {
        assert_eq!(quote_search_path("public"), "\"public\"");
    }

    #[test]
    fn quote_search_path_multiple() {
        assert_eq!(quote_search_path("myapp, public"), "\"myapp\", \"public\"");
    }

    #[test]
    fn quote_search_path_trims_whitespace() {
        assert_eq!(quote_search_path("  foo ,  bar  "), "\"foo\", \"bar\"");
    }

    #[test]
    fn quote_search_path_injection_attempt() {
        let result = quote_search_path("public; DROP TABLE users; --");
        // The whole thing is treated as one schema name and safely quoted
        assert_eq!(result, "\"public; DROP TABLE users; --\"");
    }

    #[test]
    fn quote_search_path_with_embedded_quotes() {
        assert_eq!(quote_search_path("my\"schema"), "\"my\"\"schema\"");
    }

    #[test]
    fn build_do_block_empty_what() {
        let block = build_do_block(1, "");
        assert!(block.contains("BEGIN\n\t\nEND;"));
    }

    #[test]
    fn build_do_block_multiline_what() {
        let what = "RAISE NOTICE 'line1';\nRAISE NOTICE 'line2';";
        let block = build_do_block(7, what);
        assert!(block.contains(what));
    }

    #[test]
    fn build_do_block_special_characters() {
        let what = "RAISE NOTICE 'it''s a $dollar$ test';";
        let block = build_do_block(99, what);
        assert!(block.contains(what));
        // The outer delimiters should not be broken
        assert!(block.starts_with("DO $pg_dbms_job$"));
        assert!(block.ends_with("$pg_dbms_job$;"));
    }

    #[test]
    fn build_do_block_large_jobid() {
        let block = build_do_block(i64::MAX, "NULL;");
        assert!(block.contains(&format!("job bigint := {}", i64::MAX)));
    }

    #[test]
    fn quote_ident_unicode() {
        let result = quote_ident("ñoño");
        assert_eq!(result, "\"ñoño\"");
    }

    #[test]
    fn quote_ident_backslash() {
        let result = quote_ident("a\\b");
        assert_eq!(result, "\"a\\b\"");
    }

    #[test]
    fn quote_ident_newline() {
        let result = quote_ident("a\nb");
        assert_eq!(result, "\"a\nb\"");
    }
}
