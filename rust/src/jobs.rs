//! Job discovery and execution logic.

use crate::constants::WORKER_STACK_SIZE;
use crate::db::{JobPool, get_job_connection, reset_job_connection};
use crate::dlog;
use crate::logging::dprint;
use crate::model::{Config, Job, JobKind, JobRunDetails, JobStats, JobStatsGuard};
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
    limit: i64,
) {
    dprint(config, "DEBUG", "Get scheduled jobs to run");
    jobs.clear();
    // Claim at most `limit` (one worker-pool's worth) due rows per cycle rather
    // than the whole backlog: this bounds the in-memory claimed set and the
    // number of rows left flagged running if the daemon dies mid-drain. The
    // inner SELECT ... FOR UPDATE SKIP LOCKED also makes the claim safe for
    // multiple concurrent schedulers (a future scale-out).
    let query = "UPDATE dbms_job.all_scheduled_jobs AS j SET this_date = current_timestamp, next_date = dbms_job.get_next_date(j.interval), instance = j.instance+1 WHERE j.job IN (SELECT s.job FROM dbms_job.all_scheduled_jobs s WHERE s.interval IS NOT NULL AND NOT s.broken AND s.this_date IS NULL AND s.next_date <= current_timestamp ORDER BY s.next_date LIMIT $1 FOR UPDATE SKIP LOCKED) RETURNING j.job, j.what, j.log_user, j.schema_user";
    match client.query(query, &[&limit]) {
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
pub fn get_async_jobs(
    client: &mut Client,
    config: &Config,
    jobs: &mut HashMap<i64, Job>,
    limit: i64,
) {
    jobs.clear();
    // Bounded, SKIP LOCKED claim — see get_scheduled_jobs for the rationale.
    let query = "UPDATE dbms_job.all_async_jobs AS j SET this_date = current_timestamp WHERE j.job IN (SELECT a.job FROM dbms_job.all_async_jobs a WHERE a.this_date IS NULL ORDER BY a.job LIMIT $1 FOR UPDATE SKIP LOCKED) RETURNING j.job, j.what, j.log_user, j.schema_user";
    if let Ok(rows) = client.query(query, &[&limit]) {
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

    let query = "UPDATE dbms_job.all_scheduled_jobs AS j SET this_date = current_timestamp WHERE j.job IN (SELECT s.job FROM dbms_job.all_scheduled_jobs s WHERE s.this_date IS NULL AND s.interval IS NULL AND s.next_date <= current_timestamp ORDER BY s.next_date LIMIT $1 FOR UPDATE SKIP LOCKED) RETURNING j.job, j.what, j.log_user, j.schema_user";
    if let Ok(rows) = client.query(query, &[&limit]) {
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

/// Delete a successfully-run async job's row as part of the *current*
/// transaction (so it commits atomically with the job body), returning any
/// database error so the caller can roll the whole job back. Mirrors
/// [`delete_job`]'s two-table lookup: async jobs live in `all_async_jobs`, but a
/// one-shot scheduled row (`interval IS NULL`) is dispatched as async too and
/// lives in `all_scheduled_jobs`.
fn delete_completed_async(client: &mut Client, jobid: i64) -> Result<(), postgres::Error> {
    let n = client.execute(
        "DELETE FROM dbms_job.all_async_jobs WHERE job = $1",
        &[&jobid],
    )?;
    if n == 0 {
        client.execute(
            "DELETE FROM dbms_job.all_scheduled_jobs WHERE job = $1",
            &[&jobid],
        )?;
    }
    Ok(())
}

/// Re-queue jobs left flagged running by workers that never finished.
///
/// A worker that returns before clearing its row — most commonly because it
/// could not obtain a pooled connection (`get_job_connection`), but also on a
/// failed `SET ROLE`/`BEGIN`/`search_path`, a panic, or a daemon crash — leaves
/// `this_date` set. Such rows are invisible to the dispatch scans
/// (`WHERE this_date IS NULL`) forever, so the job silently never runs again:
/// a "zombie".
///
/// This clears the marker for rows older than `stale_job_timeout`, but only
/// when no live worker backend is executing the job (checked via the
/// `pg_dbms_job:<kind>:<job>` `application_name` in `pg_stat_activity`). The
/// liveness check means a legitimately long-running job is never re-queued
/// while it is still running, so there is no risk of double execution; the age
/// threshold keeps the reaper from racing a row that was only just dispatched.
///
/// The leaked rows have not executed their body (they fail during setup, before
/// the DO block), so clearing the marker re-queues them for another attempt.
/// Scheduled rows additionally bump `failures`, mirroring the normal
/// failure-path bookkeeping (NULL-safe, see `execute_job`).
pub fn reap_stale_jobs(client: &mut Client, config: &Config) {
    let timeout = config.stale_job_timeout;
    if timeout <= 0.0 {
        return;
    }

    match client.execute(
        "UPDATE dbms_job.all_async_jobs AS j SET this_date = NULL \
         WHERE j.this_date IS NOT NULL \
           AND j.this_date < current_timestamp - make_interval(secs => $1) \
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a \
                           WHERE a.application_name = 'pg_dbms_job:async:' || j.job)",
        &[&timeout],
    ) {
        Ok(n) if n > 0 => dlog!(config, "LOG", "reaped {} stale asynchronous job(s)", n),
        Ok(_) => {}
        Err(err) => dlog!(
            config,
            "ERROR",
            "failed to reap stale asynchronous jobs: {}",
            err
        ),
    }

    match client.execute(
        "UPDATE dbms_job.all_scheduled_jobs AS j SET this_date = NULL, failures = COALESCE(failures, 0) + 1 \
         WHERE j.this_date IS NOT NULL \
           AND j.this_date < current_timestamp - make_interval(secs => $1) \
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a \
                           WHERE a.application_name = 'pg_dbms_job:scheduled:' || j.job)",
        &[&timeout],
    ) {
        Ok(n) if n > 0 => dlog!(config, "LOG", "reaped {} stale scheduled job(s)", n),
        Ok(_) => {}
        Err(err) => dlog!(
            config,
            "ERROR",
            "failed to reap stale scheduled jobs: {}",
            err
        ),
    }
}

/// Spawn a worker thread to execute a job.
pub fn spawn_job(
    kind: JobKind,
    job: Job,
    pool: &Arc<JobPool>,
    config: &Arc<Config>,
    stats: &Arc<JobStats>,
    running_workers: &mut HashMap<u64, JoinHandle<()>>,
    next_worker_id: &mut u64,
) {
    let worker_id = *next_worker_id;
    *next_worker_id = next_worker_id.wrapping_add(1);

    let pool_clone = Arc::clone(pool);
    let config_clone = Arc::clone(config);
    let stats_clone = Arc::clone(stats);

    // Workers only drive SQL over a pooled connection, so a small stack is
    // plenty; the default 2 MiB per thread is what made a burst of in-flight
    // jobs balloon RSS. See `WORKER_STACK_SIZE`.
    let spawn_result = std::thread::Builder::new()
        .name(format!("job-{}", job.job))
        .stack_size(WORKER_STACK_SIZE)
        .spawn(move || {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_job(kind, job, &pool_clone, &config_clone, &stats_clone);
            }));
        });

    match spawn_result {
        Ok(handle) => {
            running_workers.insert(worker_id, handle);
        }
        Err(err) => {
            // The dispatch UPDATE already set this_date on the row; leaving it
            // set means the stale-job reaper re-queues it later, so a transient
            // thread-spawn failure (e.g. resource exhaustion) doesn't lose work.
            dlog!(config, "ERROR", "failed to spawn worker thread: {err}");
        }
    }
}

/// Execute a job (async or scheduled) on a pooled connection.
///
/// The two flavours share virtually all setup, so the kind only influences
/// three things: the application_name and log labels, the post-commit /
/// post-rollback bookkeeping for scheduled rows, and whether the row is
/// removed from the async queue afterwards.
fn execute_job(kind: JobKind, job: Job, pool: &Arc<JobPool>, config: &Config, stats: &JobStats) {
    // Bump started now, finished on Drop — survives every early return below
    // and any panic, so the periodic stats LOG line stays balanced.
    let _stats_guard = JobStatsGuard::new(stats);
    let kind_label = kind.label();
    let start_t = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    dlog!(config, "DEBUG", "executing {} job {}", kind_label, job.job);

    dlog!(
        config,
        "DEBUG",
        "connecting to database for job {}",
        job.job
    );

    let app_name = format!("pg_dbms_job:{}:{}", kind_label, job.job);
    let mut client = match get_job_connection(pool, &app_name) {
        Ok(c) => c,
        Err(err) => {
            dlog!(config, "ERROR", "{}", err);
            return;
        }
    };

    dlog!(config, "DEBUG", "connected to database for job {}", job.job);

    // Fold the whole per-job setup — optional SET ROLE, BEGIN, the per-job
    // timeout guards, and the optional SET LOCAL search_path — into a single
    // round-trip instead of up to four. At high job rates these fixed
    // per-job round-trips dominate, so collapsing them raises the short-job
    // ceiling. It is also all-or-nothing: if any part fails the job body is not
    // run, matching the previous behaviour.
    let setup = build_setup_stmt(
        config.statement_timeout,
        config.idle_in_transaction_timeout,
        job.log_user.as_deref(),
        job.schema_user.as_deref(),
    );
    dprint(config, "DEBUG", &setup);
    if let Err(err) = client.batch_execute(&setup) {
        dlog!(
            config,
            "ERROR",
            "job {} setup (role/begin/search_path) failed, reason: {}",
            job.job,
            err
        );
        // A partial setup can leave the pooled connection in an aborted
        // transaction and/or with SET ROLE still active; clean it before the
        // connection returns to the pool so it does not poison the next job.
        let _ = client.batch_execute("ROLLBACK; RESET ROLE; RESET search_path");
        return;
    }

    let mut status_text = String::new();
    let mut err_text = String::new();
    let mut sqlstate = String::new();

    let t0 = Instant::now();
    let code = build_do_block(job.job, &job.what);
    dprint(config, "DEBUG", "code to execute:");
    dprint(config, "DEBUG", &code);

    let exec_result = client.batch_execute(&code);
    let duration_secs = t0.elapsed().as_secs() as i64;

    if let Err(err) = exec_result {
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
            match kind {
                // Scheduled: the row's `this_date` is still set from dispatch;
                // clear it and bump `failures` so the row retries. COALESCE
                // because schemas older than 3.2.1 have no default on
                // `failures`, so never-succeeded jobs there still hold NULL,
                // and `NULL + 1` would keep them NULL forever.
                JobKind::Scheduled => {
                    if let Err(err) = client.execute(
                        "UPDATE dbms_job.all_scheduled_jobs SET this_date = NULL, failures = COALESCE(failures, 0) + 1 WHERE job = $1",
                        &[&job.job],
                    ) {
                        dlog!(
                            config,
                            "ERROR",
                            "failed to record failure for scheduled job {}: {}",
                            job.job,
                            err
                        );
                    }
                }
                // Async is one-shot: remove it from the queue regardless of
                // outcome so it is not retried.
                JobKind::Async => delete_job(&mut client, config, job.job),
            }
        }
    } else {
        // Body succeeded. Fold the completion bookkeeping into THIS transaction
        // so the mark-done commits atomically with the job's effects: a crash
        // between the body's commit and the mark can no longer leave a job that
        // committed its work yet gets re-run by the reaper. (Effects OUTSIDE the
        // database transaction remain at-least-once by nature.)
        let completion = match kind {
            JobKind::Scheduled => client
                .execute(
                    "UPDATE dbms_job.all_scheduled_jobs SET this_date = NULL, last_date = current_timestamp, total_time = ($1 || ' seconds')::interval, failures = 0, instance = instance+1 WHERE job = $2",
                    &[&duration_secs.to_string(), &job.job],
                )
                .map(|_| ()),
            JobKind::Async => delete_completed_async(&mut client, job.job),
        };
        match completion {
            Ok(()) => {
                dprint(config, "DEBUG", "COMMIT");
                if let Err(err) = client.batch_execute("COMMIT") {
                    // The whole transaction (body + completion) rolls back, so
                    // the row keeps its marker and the reaper re-runs it later.
                    dlog!(
                        config,
                        "ERROR",
                        "can not commit a transaction, reason: {err}"
                    );
                }
            }
            Err(err) => {
                dlog!(
                    config,
                    "ERROR",
                    "failed to record completion for job {}, rolling back: {}",
                    job.job,
                    err
                );
                let _ = client.batch_execute("ROLLBACK");
                status_text = "ERROR".to_string();
                err_text = format!("completion bookkeeping failed: {err}");
            }
        }
    }
    // `status_text` is "ERROR" only when the job failed; empty on success.
    let failed = !status_text.is_empty();
    let record_details = match config.job_run_details {
        JobRunDetails::All => true,
        JobRunDetails::Errors => failed,
        JobRunDetails::None => false,
    };
    if record_details {
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
        store_job_execution_details(&mut client, config, details);
    } else {
        dlog!(
            config,
            "DEBUG",
            "skipping job execution details for job {} (job_run_details={})",
            job.job,
            config.job_run_details.as_str()
        );
    }

    reset_job_connection(&mut client);

    dlog!(
        config,
        "DEBUG",
        "finished executing {} job {} in {} seconds",
        kind_label,
        job.job,
        duration_secs
    );
}

/// Escape a PostgreSQL identifier with double-quote quoting.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quote a comma-separated list of schema names for use with SET search_path.
/// Segments already wrapped in double quotes are passed through unchanged so
/// that reserved placeholders like "$user" keep their special meaning.
fn quote_search_path(raw: &str) -> String {
    raw.split(',')
        .map(|s| {
            let s = s.trim();
            if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
                s.to_string()
            } else {
                quote_ident(s)
            }
        })
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
fn store_job_execution_details(
    client: &mut Client,
    config: &Config,
    details: JobExecutionDetails<'_>,
) {
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

    if let Err(err) = client.execute(
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
        if let Some(db) = err.as_db_error() {
            dlog!(
                config,
                "ERROR",
                "failed to store job execution details for job {}: code={} message={} detail={:?} hint={:?}",
                details.jobid,
                db.code().code(),
                db.message(),
                db.detail(),
                db.hint()
            );
        } else {
            dlog!(
                config,
                "ERROR",
                "failed to store job execution details for job {}: {}",
                details.jobid,
                err
            );
        }
    }
}

/// Build the transaction-opening statement for a job, folding the per-job
/// `statement_timeout` / `idle_in_transaction_session_timeout` guards into the
/// same round-trip as `BEGIN` via `SET LOCAL` (so they auto-reset at
/// COMMIT/ROLLBACK). A value of `0` leaves the corresponding timeout unset
/// (unlimited), preserving the historical behaviour. Bounding these is what
/// stops a single slow/hung job body from pinning a worker slot and pooled
/// connection indefinitely under load.
fn build_begin_stmt(statement_timeout: f64, idle_in_transaction_timeout: f64) -> String {
    let mut stmt = String::from("BEGIN");
    if statement_timeout > 0.0 {
        let ms = (statement_timeout * 1000.0).round() as i64;
        stmt.push_str(&format!("; SET LOCAL statement_timeout = {ms}"));
    }
    if idle_in_transaction_timeout > 0.0 {
        let ms = (idle_in_transaction_timeout * 1000.0).round() as i64;
        stmt.push_str(&format!(
            "; SET LOCAL idle_in_transaction_session_timeout = {ms}"
        ));
    }
    stmt
}

/// Build the combined per-job setup statement executed in one round-trip:
/// optional session `SET ROLE`, then `BEGIN` with the per-job timeout guards
/// (see [`build_begin_stmt`]), then the optional transaction-local
/// `SET LOCAL search_path`. `SET ROLE` stays before `BEGIN` so it is
/// session-level (undone by `reset_job_connection`), preserving the original
/// ordering; `search_path` is `SET LOCAL` so it is scoped to the transaction.
fn build_setup_stmt(
    statement_timeout: f64,
    idle_in_transaction_timeout: f64,
    log_user: Option<&str>,
    schema_user: Option<&str>,
) -> String {
    let mut stmt = String::new();
    if let Some(user) = log_user {
        stmt.push_str(&format!("SET ROLE {}; ", quote_ident(user)));
    }
    stmt.push_str(&build_begin_stmt(
        statement_timeout,
        idle_in_transaction_timeout,
    ));
    if let Some(schema) = schema_user {
        stmt.push_str(&format!(
            "; SET LOCAL search_path TO {}",
            quote_search_path(schema)
        ));
    }
    stmt
}

/// Build a DO block wrapper for the job body.
fn build_do_block(jobid: i64, what: &str) -> String {
    format!(
        "DO $pg_dbms_job$\nDECLARE\n\tjob bigint := {jobid};\n\tnext_date timestamp with time zone := current_timestamp;\n\tbroken boolean := false;\nBEGIN\n\t{what}\nEND;\n$pg_dbms_job$;"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        build_begin_stmt, build_do_block, build_setup_stmt, quote_ident, quote_search_path,
    };

    #[test]
    fn build_setup_stmt_no_role_no_schema_is_just_begin() {
        // With neither role nor schema and no timeouts it collapses to BEGIN.
        assert_eq!(build_setup_stmt(0.0, 0.0, None, None), "BEGIN");
    }

    #[test]
    fn build_setup_stmt_role_and_schema_and_timeout() {
        // SET ROLE stays before BEGIN (session-level); search_path is SET LOCAL
        // after BEGIN (transaction-scoped); the timeout guard sits inside.
        assert_eq!(
            build_setup_stmt(30.0, 0.0, Some("bob"), Some("app, public")),
            "SET ROLE \"bob\"; BEGIN; SET LOCAL statement_timeout = 30000; SET LOCAL search_path TO \"app\", \"public\""
        );
    }

    #[test]
    fn build_setup_stmt_quotes_role_defensively() {
        // A hostile role name cannot break out of the identifier quoting.
        let s = build_setup_stmt(0.0, 0.0, Some("a\"; DROP ROLE x; --"), None);
        assert_eq!(s, "SET ROLE \"a\"\"; DROP ROLE x; --\"; BEGIN");
    }

    #[test]
    fn build_setup_stmt_schema_only_uses_set_local() {
        assert_eq!(
            build_setup_stmt(0.0, 0.0, None, Some("public")),
            "BEGIN; SET LOCAL search_path TO \"public\""
        );
    }

    #[test]
    fn build_begin_stmt_no_timeouts_is_plain_begin() {
        // 0 means "unset" — preserve the historical unlimited behaviour and the
        // single-statement BEGIN (no extra round-trip cost).
        assert_eq!(build_begin_stmt(0.0, 0.0), "BEGIN");
    }

    #[test]
    fn build_begin_stmt_statement_timeout_only() {
        // Seconds are converted to whole milliseconds for the GUC.
        assert_eq!(
            build_begin_stmt(30.0, 0.0),
            "BEGIN; SET LOCAL statement_timeout = 30000"
        );
    }

    #[test]
    fn build_begin_stmt_idle_timeout_only() {
        assert_eq!(
            build_begin_stmt(0.0, 1.5),
            "BEGIN; SET LOCAL idle_in_transaction_session_timeout = 1500"
        );
    }

    #[test]
    fn build_begin_stmt_both_timeouts() {
        assert_eq!(
            build_begin_stmt(30.0, 60.0),
            "BEGIN; SET LOCAL statement_timeout = 30000; SET LOCAL idle_in_transaction_session_timeout = 60000"
        );
    }

    #[test]
    fn build_begin_stmt_rounds_fractional_millis() {
        // 0.0015 s = 1.5 ms rounds to 2 ms; sub-millisecond values still produce
        // a valid integer GUC value rather than a float.
        assert_eq!(
            build_begin_stmt(0.0015, 0.0),
            "BEGIN; SET LOCAL statement_timeout = 2"
        );
    }

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
    fn quote_search_path_preserves_user_placeholder() {
        // Postgres default: "$user", public — the "$user" token must not be
        // re-quoted or it stops being substituted with the session user.
        assert_eq!(
            quote_search_path("\"$user\", public"),
            "\"$user\", \"public\""
        );
    }

    #[test]
    fn quote_search_path_preserves_already_quoted_segments() {
        assert_eq!(
            quote_search_path("\"MyApp\", public"),
            "\"MyApp\", \"public\""
        );
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
