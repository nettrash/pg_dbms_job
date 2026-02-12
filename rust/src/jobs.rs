//! Job discovery and execution logic.

use crate::db::connect_job_db;
use crate::logging::dprint;
use crate::model::{Config, DbInfo, Job, JobKind};
use chrono::Local;
use nix::unistd::{ForkResult, Pid, fork};
use postgres::Client;
use std::collections::{HashMap, HashSet};
use std::process;
use std::time::Instant;

/// Collect scheduled jobs that are ready to run.
pub fn get_scheduled_jobs(
    client: &mut Client,
    config: &Config,
    config_invalidated: &mut bool,
) -> HashMap<i64, Job> {
    dprint(config, "DEBUG", "Get scheduled jobs to run");
    let mut jobs = HashMap::new();
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
            dprint(config, "ERROR", &format!("can't execute statement, {err}"));
            *config_invalidated = true;
        }
    }
    dprint(
        config,
        "DEBUG",
        &format!("Found {} scheduled jobs to run", jobs.len()),
    );
    jobs
}

/// Collect asynchronous jobs queued for execution.
pub fn get_async_jobs(client: &mut Client, config: &Config) -> HashMap<i64, Job> {
    let mut jobs = HashMap::new();
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

    dprint(
        config,
        "DEBUG",
        &format!("Found {} asynchronous jobs to run", jobs.len()),
    );
    jobs
}

/// Remove a job from the async queue (or fallback to scheduled).
pub fn delete_job(client: &mut Client, config: &Config, jobid: i64) {
    dprint(
        config,
        "DEBUG",
        &format!("Deleting asynchronous job {jobid} from queue"),
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

/// Spawn a child process to execute a job.
pub fn spawn_job(
    kind: JobKind,
    job: Job,
    dbinfo: &DbInfo,
    config: &Config,
    running_pids: &mut HashSet<Pid>,
) {
    match unsafe { fork() } {
        Ok(ForkResult::Parent { child }) => {
            running_pids.insert(child);
        }
        Ok(ForkResult::Child) => {
            // Child executes the job then exits.
            match kind {
                JobKind::Async => subprocess_async(job, dbinfo, config),
                JobKind::Scheduled => subprocess_scheduled(job, dbinfo, config),
            }
            process::exit(0);
        }
        Err(err) => {
            dprint(config, "ERROR", &format!("cannot fork: {err}"));
        }
    }
}

/// Execute an asynchronous job in a child process.
fn subprocess_async(job: Job, dbinfo: &DbInfo, config: &Config) {
    let start_t = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    dprint(config, "LOG", &format!("executing async job {}", job.job));

    dprint(
        config,
        "DEBUG",
        &format!("connecting to database for job {}", job.job),
    );

    let app_name = format!("pg_dbms_job:async:{}", job.job);
    let mut client = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        connect_job_db(dbinfo, &app_name)
    })) {
        Ok(Ok(c)) => c,
        Ok(Err(err)) => {
            dprint(config, "ERROR", &format!("{err}"));
            return;
        }
        Err(_) => {
            dprint(config, "ERROR", "connect_job_db panicked");
            return;
        }
    };

    dprint(
        config,
        "DEBUG",
        &format!("connected to database for job {}", job.job),
    );

    if let Some(log_user) = &job.log_user {
        dprint(config, "DEBUG", &format!("SET ROLE {log_user}"));
        if let Err(err) = client.batch_execute(&format!("SET ROLE {log_user}")) {
            dprint(
                config,
                "ERROR",
                &format!("can not change role, reason: {err}"),
            );
            return;
        }
    } else {
        dprint(config, "DEBUG", "log_user is not set, using default role");
    }

    dprint(config, "DEBUG", "BEGIN");
    if let Err(err) = client.batch_execute("BEGIN") {
        dprint(
            config,
            "ERROR",
            &format!("can not start a transaction, reason: {err}"),
        );
        return;
    }

    if let Some(schema_user) = &job.schema_user {
        dprint(
            config,
            "DEBUG",
            &format!("SET LOCAL search_path TO {schema_user}"),
        );
        if let Err(err) = client.batch_execute(&format!("SET LOCAL search_path TO {schema_user}")) {
            dprint(
                config,
                "ERROR",
                &format!("can not change the search_path, reason: {err}"),
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
        dprint(
            config,
            "ERROR",
            &format!("job {} failure, reason: {}", job.job, err_text),
        );
        dprint(config, "DEBUG", "ROLLBACK");
        if let Err(err) = client.batch_execute("ROLLBACK") {
            dprint(
                config,
                "ERROR",
                &format!("can not rollback a transaction, reason: {err}"),
            );
        }
    } else {
        dprint(config, "DEBUG", "COMMIT");

        if let Err(err) = client.batch_execute("COMMIT") {
            dprint(
                config,
                "ERROR",
                &format!("can not commit a transaction, reason: {err}"),
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
    dprint(
        config,
        "DEBUG",
        &format!("storing job execution details: {:?}", details),
    );
    let _ = store_job_execution_details(&mut client, details);
}

/// Execute a scheduled job in a child process.
fn subprocess_scheduled(job: Job, dbinfo: &DbInfo, config: &Config) {
    let start_t = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    dprint(
        config,
        "LOG",
        &format!("executing scheduled job {}", job.job),
    );

    let mut client = match connect_job_db(dbinfo, &format!("pg_dbms_job:scheduled:{}", job.job)) {
        Ok(c) => c,
        Err(err) => {
            dprint(config, "ERROR", &format!("{err}"));
            return;
        }
    };

    if let Some(log_user) = &job.log_user {
        if let Err(err) = client.batch_execute(&format!("SET ROLE {log_user}")) {
            dprint(
                config,
                "ERROR",
                &format!("can not change role, reason: {err}"),
            );
            return;
        }
    }

    if let Err(err) = client.batch_execute("BEGIN") {
        dprint(
            config,
            "ERROR",
            &format!("can not start a transaction, reason: {err}"),
        );
        return;
    }

    if let Some(schema_user) = &job.schema_user {
        if let Err(err) = client.batch_execute(&format!("SET LOCAL search_path TO {schema_user}")) {
            dprint(
                config,
                "ERROR",
                &format!("can not change the search_path, reason: {err}"),
            );
            return;
        }
    }

    let mut status_text = String::new();
    let mut err_text = String::new();
    let mut sqlstate = String::new();

    let t0 = Instant::now();
    let code = build_do_block(job.job, &job.what);
    if let Err(err) = client.batch_execute(&code) {
        err_text = err.to_string();
        sqlstate = err.code().map(|c| c.code().to_string()).unwrap_or_default();
        status_text = "ERROR".to_string();
        dprint(
            config,
            "ERROR",
            &format!("job {} failure, reason: {}", job.job, err_text),
        );
        if let Err(err) = client.batch_execute("ROLLBACK") {
            dprint(
                config,
                "ERROR",
                &format!("can not rollback a transaction, reason: {err}"),
            );
        } else {
            let _ = client.execute(
                "UPDATE dbms_job.all_scheduled_jobs SET this_date = NULL, failures = failures+1 WHERE job = $1",
                &[&job.job],
            );
        }
    } else if let Err(err) = client.batch_execute("COMMIT") {
        dprint(
            config,
            "ERROR",
            &format!("can not commit a transaction, reason: {err}"),
        );
    }

    let duration_secs = t0.elapsed().as_secs() as i64;
    let _ = client.execute(
        "UPDATE dbms_job.all_scheduled_jobs SET this_date = NULL, last_date = current_timestamp, total_time = ($1 || ' seconds')::interval, failures = 0, instance = instance+1 WHERE job = $2",
        &[&duration_secs.to_string(), &job.job],
    );

    let details = JobExecutionDetails {
        owner: job.log_user.as_deref().unwrap_or(""),
        jobid: job.job,
        start_date: &start_t,
        duration_secs,
        status_text: &status_text,
        err_text: &err_text,
        sqlstate: &sqlstate,
    };
    let _ = store_job_execution_details(&mut client, details);
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
fn store_job_execution_details(
    client: &mut Client,
    details: JobExecutionDetails<'_>,
) -> Result<(), postgres::Error> {
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

    Ok(())
}

/// Build a DO block wrapper for the job body.
fn build_do_block(jobid: i64, what: &str) -> String {
    format!(
        "DO $pg_dbms_job$\nDECLARE\n\tjob bigint := {jobid};\n\tnext_date timestamp with time zone := current_timestamp;\n\tbroken boolean := false;\nBEGIN\n\t{what}\nEND;\n$pg_dbms_job$;"
    )
}

#[cfg(test)]
mod tests {
    use super::build_do_block;

    #[test]
    fn build_do_block_includes_job_and_code() {
        let code = "RAISE NOTICE 'hello';";
        let block = build_do_block(42, code);
        assert!(block.contains("job bigint := 42"));
        assert!(block.contains(code));
        assert!(block.contains("DO $pg_dbms_job$"));
    }
}
