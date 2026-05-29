# pg_dbms_job

PostgreSQL extension to schedules and manages jobs in a job queue similar to Oracle DBMS_JOB package.

> **About this fork.** This variant replaces the original Perl scheduler with a
> standalone **Rust daemon** (in [`rust/`](rust/)) and adds several performance
> and operational improvements that ship with the extension: partial dispatch
> indexes and per-table autovacuum tuning on the queue tables, an r2d2
> connection pool, a reaper that recovers abandoned ("zombie") jobs, a
> configurable job-history recording mode, and monthly partitioning + retention
> for the job-history table. The SQL interface remains compatible with the
> original. See [`rust/README.md`](rust/README.md) for the full scheduler
> reference.

* [Description](#description)
* [Installation](#installation)
* [Manage the extension](#manage-the-extension)
* [Running the scheduler](#running-the-scheduler)
* [Configuration](#configuration)
* [Jobs definition](#jobs-definition)
  - [Scheduled jobs](#scheduled-jobs)
  - [Asynchronous jobs](#asynchronous-jobs)
* [View ALL_JOBS](#view-all_jobs)
* [Security](#secutity)
* [Jobs execution history](#jobs-execution-history)
* [Procedures](#procedures)
  - [BROKEN](#broken)
  - [CHANGE](#change)
  - [INTERVAL](#interval)
  - [NEXT_DATE](#next_date)
  - [REMOVE](#remove)
  - [RUN](#run)
  - [SUBMIT](#submit)
  - [WHAT](#what)
* [Limitations](#limitations)
* [Authors](#authors)
* [License](#license)

## [Description](#description)

This PostgreSQL extension provided full compatibility with the DBMS_JOB Oracle module.

It allows to manage scheduled jobs from a job queue or to execute immediately jobs asynchronously. A job definition consist on a code to execute, the next date of execution and how often the job is to be run. A job runs a SQL command, plpgsql code or an existing stored procedure.

If the submit stored procedure is called without the next_date (when) and interval (how often) attributes, the job is executed immediately in an asynchronous process. If interval is NULL and that next_date is lower or equal to current timestamp the job is also executed immediately as an asynchronous process. In all other cases the job is to be started when appropriate but if interval is NULL the job is executed only once and the job is deleted.

If a scheduled job completes successfully, then its new execution date is placed in next_date. The new date is calculated by evaluating the SQL expression defined as interval. The interval parameter must evaluate to a time in the future.

This extension consist in a SQL script to create all the objects related to its operation and a daemon that must be run attached to the database where jobs are defined. The daemon is responsible to execute the queued asynchronous jobs and the scheduled ones. It can be run on the same host of the database, where the jobs are defined, or on any other host. The schedule time is taken from the database host not where the daemon is running.

The number of jobs that can be executed at the same time is limited to `job_queue_processes` (1024 by default), and the daemon never opens more than `pool_size` database connections (100 by default). If the concurrency limit is reached the daemon waits for a running job to finish before starting a new one.

The scheduler is implemented as a standalone Rust daemon rather than a PostgreSQL background worker. This is a deliberate choice: the work runs in a separate process (it can even run on a different host than the database), executes jobs concurrently on worker threads drawn from a bounded connection pool, and is not constrained by background-worker slots.

The job execution is caused by a NOTIFY event received by the scheduler when a new job is submitted or modified. The notifications are polled every `nap_time` seconds (0.1 second by default). When there is no notification the scheduler polls every `job_queue_interval` seconds (0.1 second by default) the tables where job definitions are stored. This means that at worst a job will be executed `job_queue_interval` seconds after the next execution date defined.


## [Installation](#installation)

The SQL extension only requires a PostgreSQL version that supports extensions
(>= 9.1). The scheduler is a standalone daemon written in Rust (see
[`rust/`](rust/) and [`rust/README.md`](rust/README.md)); build it with `cargo`
or use the provided Docker image — it has no Perl or other runtime dependency.

To install the SQL extension execute

    make
    sudo make install

To build the scheduler daemon:

    cd rust && cargo build --release

The Rust scheduler's unit tests run with:

    cd rust && cargo test

## [Manage the extension](#manage-the-extension)

Each database that needs to use `pg_dbms_job` must creates the extension:

    psql -d mydb -c "CREATE EXTENSION pg_dbms_job"

To upgrade to a new version execute:

    psql -d mydb -c 'ALTER EXTENSION pg_dbms_job UPDATE TO "3.0.0"'

If you doesn't have the privileges to create an extension you can just import the extension file into the database, for example:

    psql -d mydb -f sql/pg_dbms_job--3.0.0.sql

This is especially useful for database in DBaas cloud services. To upgrade just import the extension upgrade files using psql.

A dedicated scheduler per database using the extension must be started.

## [Running the scheduler](#running-the-scheduler)

The scheduler is a standalone daemon (the Rust binary, see [`rust/README.md`](rust/README.md)) that runs in background; it can be executed by any system user as follow:

    pg_dbms_job -c /etc/pg_dbms_job/mydb-dbms_job.conf

There must be one scheduler daemon running per database using the extension with a dedicated configuration file.

The configuration file must define the database connection settings where the pg_dbms_job extension is used. This connection must be the extension tables owner or have the superuser privileges to be able to bypass the Row Level Security rules defined on the pg_dbms_job tables.

```
usage: pg_dbms_job [options]

options:

  -c, --config  file  configuration file. Default: /etc/pg_dbms_job/pg_dbms_job.conf
  -d, --debug         run in debug mode.
  -k, --kill          stop current running daemon gracefully waiting
                      for all job completion.
  -m, --immediate     stop running daemon and jobs immediatly.
  -r, --reload        reload configuration file and jobs definition.
  -s, --single        do not detach and run in single loop mode and exit.
```

To stop gracefully the scheduler daemon after all running jobs are terminated, you can run the same command but with the `-k` option:
```
pg_dbms_job -c /etc/pg_dbms_job/mydb-dbms_job.conf -k
```
you can also send the TERM signal to the main process:
```
$ ps auwx | grep "pg_dbms_job:main" | grep -v grep
postgres   14754  0.0  0.0  39636 17492 ?        Ss   10:15   0:00 pg_dbms_job:main

$ kill -15 14754
```

To force the scheduler to stop immedialely interrupting the running jobs use the `-m` option:
```
pg_dbms_job -c /etc/pg_dbms_job/mydb-dbms_job.conf -m
```
or send the INT signal:
```
$ ps auwx | grep "pg_dbms_job:main" | grep -v grep
postgres   14754  0.0  0.0  39636 17492 ?        Ss   10:15   0:00 pg_dbms_job:main

$ kill -2 14754
```

### Reload semantics (SIGHUP / `-r`)

`SIGHUP` re-reads the configuration file and re-opens the log file (so it
plays nicely with `logrotate`). The reload only affects **new** activity:

- New polling cycles, newly spawned worker threads, and new database
  connections pick up the updated config.
- Workers already running when the signal arrives finish under the
  configuration they started with — settings are not retro-applied.
- Database/pool changes (`host`, `port`, `user`, `database`, `pool_size`)
  cause the main connection and the pool to be recreated on the next loop
  iteration; in-flight workers keep their existing pooled connection.
- If `pidfile` is changed, the old file is renamed to the new path.

If a tighter ordering is required (e.g. drain all workers, then reload),
stop the daemon with `-k` and start it again with the new config.

#### Log rotation with `logrotate`

The daemon keeps the log file open between writes. The recommended setup
is the standard `create` mode plus a `postrotate` hook that signals the
daemon to re-open its log file:

```
/var/log/pg_dbms_job/pg_dbms_job.log {
    daily
    rotate 14
    missingok
    notifempty
    compress
    delaycompress
    create 0640 postgres postgres
    postrotate
        kill -HUP "$(cat /run/pg_dbms_job/pg_dbms_job.pid)" 2>/dev/null || true
    endscript
}
```

As a safety net the daemon also notices on its own when the file it has
open has been renamed aside or removed (the configured path now resolves
to a different inode) and re-opens it on the next write — so rotation
still works if the `postrotate` hook is missing, though with a small
delay. `copytruncate` is also handled (the handle is kept; an `O_APPEND`
write lands at the new start of the file), but `create` mode is preferred.

Time-based rotation needs no signal at all: put an `strftime()` escape in
`logfile` (e.g. `logfile=/var/log/pg_dbms_job/pg_dbms_job-%Y%m%d.log`) and
the daemon switches files automatically when the formatted name changes.

### Dispatch performance (built in)

The scheduler polls the job tables on every notification and on every
`job_queue_interval`, so the dispatch `UPDATE`s are a hot path. To keep them
fast this extension **ships** the supporting partial indexes and aggressive
per-table autovacuum settings — no manual step is needed on a fresh install:

```sql
-- partial indexes covering only the not-yet-running jobs
CREATE INDEX all_async_jobs_pending_idx
    ON dbms_job.all_async_jobs (job)        WHERE this_date IS NULL;
CREATE INDEX all_scheduled_jobs_pending_idx
    ON dbms_job.all_scheduled_jobs (next_date) WHERE this_date IS NULL;

-- vacuum/analyze the high-churn queue tables aggressively so they stay compact
ALTER TABLE dbms_job.all_async_jobs     SET (autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 100, autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 100, autovacuum_vacuum_cost_delay = 0);
ALTER TABLE dbms_job.all_scheduled_jobs SET ( ... same ... );
```

If you are upgrading a database created before these were added, run the same
statements once (they are idempotent). The indexes are partial so they stay
small even with a large queue.

## [Configuration](#configuration)

The configuration file uses simple `key = value` lines (the same style as `postgresql.conf`). The settings below are the ones most commonly tuned; [`rust/README.md`](rust/README.md) is the authoritative reference for every option, and [`etc/pg_dbms_job.conf`](etc/pg_dbms_job.conf) is a ready-to-edit template.

### General

- `debug`: debug mode (`0`/`1`). Default `0`. The `-d` CLI flag overrides it.
- `pidfile`: path to the pid file. Default `/tmp/pg_dbms_job.pid`.
- `logfile`: log file name pattern; may contain `strftime()` escapes (e.g. `%a` for a
   per-weekday file, `%Y%m%d` for a daily file). Default empty, which logs to stderr.
- `log_truncate_on_rotation`: if `1`, an existing log file with the same name as the new
   one is truncated rather than appended to (only on time-driven rotation, not on restart).
   Default `0`.
- `job_queue_interval`: fallback poll interval of the job tables, in seconds (float). Default `0.1`.
- `job_queue_processes`: maximum number of jobs running concurrently. Default `1024`.
- `pool_size`: maximum number of PostgreSQL connections in the worker pool; clamped at
   runtime to `min(pool_size, job_queue_processes)`. Default `100`.
- `nap_time`: `LISTEN`/notification timeout per main-loop cycle, in seconds (float). Default `0.1`.
- `startup_delay`: delay before retrying after a failed connection or a database in
   recovery, in seconds. Default `3.0`.
- `error_delay`: delay applied when the worker queue is saturated, in seconds. Default `0.5`.
- `stats_interval`: period for the periodic `jobs started/finished` LOG line, in seconds;
   `0` disables it. Default `15`.
- `job_run_details`: how much job history is written to `all_scheduler_job_run_details`
   (`all` | `errors` | `none`); `errors` records only failed runs, `none` disables recording.
   Default `all`. See [Jobs execution history](#jobs-execution-history).
- `stale_job_timeout`: age (seconds) after which a job flagged running with no live worker
   backend is treated as abandoned and re-queued by the reaper; `0` disables it. Default `3600`.

### Database

- `host`: ip address or hostname where the PostgreSQL cluster is running.
- `port`: port where the PostgreSQL cluster is listening.
- `database`: name of the database to connect to.
- `user`: role used to connect; it must own the `dbms_job` tables or be a superuser, so it can bypass Row Level Security and run each job under its owner's role via `SET ROLE`.
- `passwd`: password for this role.

### Example
```
#-----------
#  General
#-----------
# Toggle debug mode
debug=0
# Path to the pid file
pidfile=/tmp/pg_dbms_job.pid
# log file name pattern, can include strftime() escapes, for example
# to have a log file per week day use %a in the log file name.
logfile=/tmp/pg_dbms_job.log
# If activated an existing log file with the same name as the new log
# file will be truncated rather than appended to. But such truncation
# only occurs on time-driven rotation, not on restarts.
log_truncate_on_rotation=0
# Fallback poll interval of the job tables (seconds)
job_queue_interval=0.1
# Maximum number of jobs running concurrently
job_queue_processes=1024
# Maximum PostgreSQL connections in the worker pool (size to the server)
pool_size=100
# Main-loop LISTEN timeout (seconds) - controls notification latency
nap_time=0.1
# Delay before retrying after connect failures (seconds)
startup_delay=3.0
# Delay when the worker queue is saturated (seconds)
error_delay=0.5
# Period (seconds) for the periodic job-stats LOG line; 0 disables it
stats_interval=15
# Job-run history: all = every run, errors = failures only, none = disabled
job_run_details=all
# Re-queue jobs flagged running with no live worker after N seconds; 0 disables
stale_job_timeout=3600

#-----------
#  Database
#-----------
host=localhost
port=5432
database=dbms_job
user=postgres
passwd=postgres
```

To force the scheduler to reread the configuration file after changes you can use the `-r` option:
```
pg_dbms_job -c /etc/pg_dbms_job/mydb-dbms_job.conf -r
```
or send the HUP signal:
```
$ ps auwx | grep "pg_dbms_job:main" | grep -g grep
postgres   14758  0.0  0.0  39636 17492 ?        Ss   10:17   0:00 pg_dbms_job:main

$ kill -1 14758
```

## [Jobs definition](#jobs-definition)

### [Scheduled jobs](#scheduled-jobs)

Jobs to run are stored in table `dbms_job.all_scheduled_jobs` which is the same structure as the one in Oracle. Some columns are just here for compatibility but are not used. They are executed when current timestamp of the database polled by the scheduler is upper or equal to the date defined in the `next_date` attribute.

Unlike with cron-like scheduler, when the pg_dbms_job scheduler starts it executes all active jobs with a next date in the past. That also mean that the interval of execution will be the same but the first execution date will change.

```
CREATE TABLE dbms_job.all_scheduled_jobs
(
	job bigint DEFAULT nextval('dbms_job.jobseq') PRIMARY KEY, -- identifier of job
	log_user name DEFAULT current_user, -- user that submit the job
	priv_user name DEFAULT current_user, -- user whose default privileges apply to this job (not used)
	schema_user text DEFAULT current_setting('search_path'), -- default schema used to parse the job
	last_date timestamp with time zone, -- date on which this job last successfully executed
	last_sec text, -- same as last_date (not used)
	this_date timestamp with time zone, -- date that this job started executing, null when the job is not running
	this_sec text, -- same as this_date (not used)
	next_date timestamp(0) with time zone NOT NULL, -- date that this job will next be executed
	next_sec timestamp with time zone, -- same as next_date (not used)
	total_time interval, -- total wall clock time spent by the system on this job, in seconds
	broken boolean DEFAULT false, -- true: no attempt is made to run this job, false: an attempt is made to run this job
	interval text, -- a date function, evaluated at the start of execution, becomes next next_date
	failures bigint, -- number of times the job has started and failed since its last success
	what text  NOT NULL, -- body of the anonymous pl/sql block that the job executes
	nls_env text, -- session parameters describing the nls environment of the job (not used)
	misc_env bytea, -- Other session parameters that apply to this job (not used)
	instance integer DEFAULT 0 -- ID of the instance that can execute or is executing the job (not used)
);
```

### [Asynchronous jobs](#asynchronous-jobs)

Job submitted without execution date are jobs that need to be executed asynchronously as soon as possible after being created. They are stored in the queue (FIFO) table `dbms_job.all_async_jobs`.

Same as for scheduled jobs, if jobs exist in the queue at start of the scheduler, they are executed immediately.

```
CREATE TABLE dbms_job.all_async_jobs
(
        job bigint DEFAULT nextval('dbms_job.jobseq') PRIMARY KEY, -- identifier of job
        log_user name DEFAULT current_user, -- user that submit the job
        schema_user text DEFAULT current_setting('search_path'), -- default search_path used to execute the job
        create_date timestamp with time zone DEFAULT current_timestamp, -- date on which this job has been created.
        what text NOT NULL -- body of the anonymous pl/sql block that the job executes
);
```
## [View ALL_JOBS](#view-all_jobs)

All jobs that have to be executed can be listed from the view `dbms_job.all_jobs`, this is the equivalent of the Oracle table DBMS_JOB.ALL_JOBS. This view reports all jobs to be run by execution a union between the two tables described in previous chapters.

## [Security](#secutity)

Jobs are only visible by their own creator. A user can not access to a job defined by an other user unless it has the superuser privileges or it is the owner of the pg_dbms_job tables.

By default a user can not use pg_dbms_job, he must be granted privileges to the pg_dbms_job objects as follow.

```
GRANT USAGE ON SCHEMA dbms_job TO <role>;
GRANT ALL ON ALL TABLES IN SCHEMA dbms_job TO <role>;
GRANT ALL ON ALL SEQUENCES IN SCHEMA dbms_job TO <role>;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_job TO <role>;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA dbms_job TO <role>;
```

A job will be taken in account by the scheduler only when the transaction where it has been created is committed. It is transactional so no risk that it will be executed if the transaction is aborted.

When starting or when it is reloaded the pg_dbms_job daemon first checks that another daemon is not already attached to the same database. If this is the case it will refuse to continue. This is a double verification, the first one is on an existing pid file and the second is done by looking at pg_stat_activity to see if a `pg_dbms_job:main` process already exists.

By default the scheduler allow 1000 job to be executed at the same time, you may want to control this value to a lower or a upper value. This limit can be changed in the configuration file with directive `job_queue_processes`. Note that if your system doesn't enough resources to run all the job at the same time it could be problematic. You must also take attention to who is authorised to submit jobs because this could affect the performances of the server.

Jobs are executed with as the user that defined the job and with the search path used at the time of the job submission. This information is available in attributes `log_user` and `schema_user` of table `dbms_job.all_scheduled_jobs` and `dbms_job.all_async_jobs`. That mean that the database connection user of the scheduler must have the privilege to change the user using `SET ROLE <jobuser>.`. This allow the user that have submitted the job to view its entries in the history table.


## [Jobs execution history](#jobs-execution-history)

Oracle DBMS_JOB doesn't provide a log history. This feature is available in DBMS_SCHEDULER and the past activity of the scheduler can be seen in table ALL_SCHEDULER_JOB_RUN_DETAILS. This extension stores all PG_DBMS_JOB activity in a similar table named `dbms_job.all_scheduler_job_run_details`.

```
CREATE TABLE dbms_job.all_scheduler_job_run_details
(
        log_id bigserial, -- unique identifier of the log entry
        log_date timestamp with time zone NOT NULL DEFAULT current_timestamp, -- date of the log entry (partition key)
        owner name, -- owner of the scheduler job
        job_name varchar(261), -- name of the scheduler job
        job_subname varchar(261), -- Subname of the Scheduler job (for a chain step job)
        status text, -- status of the job run
        error char(5), -- error code in the case of an error
        req_start_date timestamp with time zone, -- requested start date of the job run
        actual_start_date timestamp with time zone, -- actual date on which the job was run
        run_duration bigint, -- duration of the job run in seconds
        instance_id integer, -- identifier of the instance on which the job was run
        session_id integer, -- session identifier of the job run
        slave_pid integer, -- process identifier of the slave on which the job was run
        cpu_used integer, -- amount of cpu used for the job run
        additional_info text, -- additional information on the job run, error message, etc.
        PRIMARY KEY (log_id, log_date)
) PARTITION BY RANGE (log_date);
```

### Partitioning and retention

This is a write-only table: the scheduler appends one row per job execution and never reads it back. On a busy system it therefore grows without bound (it is easy to reach tens of GB and hundreds of millions of rows), which also slows down the queue scans by evicting their pages from cache. To keep it bounded it is **range-partitioned by `log_date`** (one partition per month), so old history is removed by dropping whole partitions instead of `DELETE` + `VACUUM`. This requires **PostgreSQL 11+**.

`log_date` is part of the primary key because a partitioned table's primary key must include its partition key. The scheduler never sets `log_id` or `log_date` explicitly (both use their column defaults), so this is transparent to the daemon.

A maintenance function creates upcoming partitions and prunes old ones:

```sql
-- ensure the current + next month exist, drop partitions older than 3 months
SELECT dbms_job.maintain_run_details_partitions(months_ahead => 1, retention_months => 3);
```

A **DEFAULT partition** (`all_scheduler_job_run_details_default`) is created as a safety net, so inserts never fail even if the maintenance function is not called — a lapse only leaves rows in the DEFAULT partition and pauses pruning, it never breaks logging.

Call the maintenance function regularly (it issues `CREATE`/`DROP TABLE`, so run it as a privileged role) — either from `cron`, or by submitting it as a recurring job:

```sql
SELECT dbms_job.submit(
    'PERFORM dbms_job.maintain_run_details_partitions();',
    current_timestamp,
    'current_timestamp + interval ''1 day''');
```

To turn off pruning while still creating partitions, pass `retention_months => 0`.

#### Converting an existing (non-partitioned) install

Fresh installs (`CREATE EXTENSION pg_dbms_job`) already get the partitioned table. An install created with an earlier version has a plain table that must be converted once, using the migration script shipped in [`updates/migrate_all_scheduler_job_run_details_to_partitioned.sql`](updates/migrate_all_scheduler_job_run_details_to_partitioned.sql):

```bash
psql -d <database> -f updates/migrate_all_scheduler_job_run_details_to_partitioned.sql
```

What it does, and how to use it:

- **Run it as the table owner / a superuser** (the same role the extension was installed with) on **PostgreSQL 11+**.
- It runs in a single transaction and **copies no data**, so it only holds a brief `ACCESS EXCLUSIVE` lock (sub-second). The scheduler's inserts pause for that moment and then continue against the new table — **no daemon restart is required**.
- Existing history is **preserved**, not deleted: the old table is renamed aside to `dbms_job.all_scheduler_job_run_details_old`. The new partitioned table starts empty (with the current/next month and DEFAULT partitions).
- The script refuses to run if the table is already partitioned, so it is safe to invoke by mistake.

After verifying the daemon still logs into the new table, finish up (these steps are listed, commented, at the bottom of the migration file):

```sql
-- optional: register the new objects with the extension (keeps pg_dump / DROP EXTENSION correct)
ALTER EXTENSION pg_dbms_job ADD FUNCTION dbms_job.maintain_run_details_partitions(integer, integer);
ALTER EXTENSION pg_dbms_job ADD TABLE dbms_job.all_scheduler_job_run_details;

-- reclaim the disk used by the old history (irreversible — this is where the space is freed)
ALTER EXTENSION pg_dbms_job DROP TABLE dbms_job.all_scheduler_job_run_details_old;
DROP TABLE dbms_job.all_scheduler_job_run_details_old;
```

To keep some recent history instead of discarding all of it, copy it across **before** dropping the old table (the target month partitions must already exist):

```sql
INSERT INTO dbms_job.all_scheduler_job_run_details
SELECT * FROM dbms_job.all_scheduler_job_run_details_old
WHERE log_date >= current_date - interval '7 days';
```

## [Procedures](#procedures)

### [BROKEN](#broken)

Disables or suspend job execution. This procedure sets the broken flag. Broken jobs are never run.

Syntax:

	pg_dbms_job.broken ( 
		job       IN  bigint,
		broken    IN  boolean
		next_date IN  timestamp DEFAULT current_timestamp);

Parameters:

- job : ID of the job being run.
- broken : Sets the job as broken or not broken. `true` sets it as broken; `false` sets it as not broken.
- next_date : Next date when the job will be run, default is `current_timestamp`.

If you set job as broken while it is running, unlike Oracle, the scheduler will not reset the job's status to normal after the job completes. Therefore, you can execute this procedure for jobs when they are running they will be disabled.

Example:

	BEGIN;
	CALL pg_dbms_job.broken(12345, true);
	COMMIT;

### [CHANGE](#change)

Alters any of the user-definable parameters associated with a job. Any value you do not want to change can be specified as NULL.

Syntax:

	dbms_job.change ( 
		job       IN  bigint,
		what      IN  text,
		next_date IN  timestamp with time zone,
		interval  IN  text
		[, instance  IN  integer DEFAULT 0,
		   force     IN  boolean DEFAULT false ]);

Parameters:

- job : ID of the job being run.
- what : PL/SQL procedure to run.
- next_date : Next date when the job will be run.
- interval : Date function; evaluated immediately before the job starts running.
- instance : unused
- force : unused

Your job change will not be available for processing by the job queue in the background until it is committed.
If the parameters what, next_date, or interval are NULL, then leave that value as it is.

Example:

Change the interval of execution of job 12345 to run every 3 days

	BEGIN;
	CALL pg_dbms_job.change(12345, null, null, 'current_timestamp + ''3 days''::interval');
	COMMIT;

### [INTERVAL](#interval)

Alters the interval between executions for a specified job

Syntax:

	dbms_job.interval ( 
		job       IN  bigint,
		interval  IN  text);

Parameters:

- job : ID of the job being run.
- interval : Code of the date function, evaluated immediately before the job starts running.

If the job completes successfully, then this new date is placed in next_date. `interval` is evaluated by plugging it into the statement select interval into next_date;

The interval parameter must evaluate to a time in the future.

If interval evaluates to NULL and if a job completes successfully, then the job is automatically deleted from the queue.

With Oracle this is the kind of interval values that we can find:

- Execute daily: `SYSDATE + 1`
- Execute once per week: `SYSDATE + 7`
- Execute hourly: `SYSDATE + 1/24`
- Execute every 2 hour: `SYSDATE + 2/24`
- Execute every 12 hour: `SYSDATE + 12/24`
- Execute every 10 min.: `SYSDATE + 10/1440`
- Execute every 30 sec.: `SYSDATE + 30/86400`

The equivalent to use with pg_dbms_job are the following:

- Execute daily: `date_trunc('second',LOCALTIMESTAMP) + '1 day'::interval`
- Execute once per week: `date_trunc('second',LOCALTIMESTAMP) + '7 days'::interval` or `date_trunc('second',current_timestamp) + '1 week'::interval`
- Execute hourly: `date_trunc('second',LOCALTIMESTAMP) + '1 hour'::interval`
- Execute every 2 hour: `date_trunc('second',LOCALTIMESTAMP) + '2 hours'::interval`
- Execute every 12 hour: `date_trunc('second',LOCALTIMESTAMP) + '12 hours'::interval`
- Execute every 10 min.: `date_trunc('second',LOCALTIMESTAMP) + '10 minutes'::interval`
- Execute every 30 sec.: `date_trunc('second',LOCALTIMESTAMP) + '30 secondes'::interval`

Example:

	BEGIN;
	CALL pg_dbms_job.interval(12345, 'current_timestamp + '10 seconds'::interval);
	COMMIT;

### [NEXT_DATE](#next_date)

Alters the next execution time for a specified job

Syntax:

	dbms_job.next_date ( 
		job       IN  bigint,
		next_date IN  timestamp with time zone);

Parameters:

- job : ID of the job being run.
- next_date : Date of the next refresh: it is when the job will be automatically run, assuming there are background processes attempting to run it.

Example:

	BEGIN;
	CALL pg_dbms_job.next_date(12345, current_timestamp + '1 day'::interval);
	COMMIT;

### [REMOVE](#remove)

Removes specified job from the job queue. You can only remove jobs that you own. If this is run while the job is executing, it will not be interrupted but will not be run again.

Syntax:

	dbms_job.remove ( 
		job       IN  bigint);

Parameters:

- job : ID of the job being run.

Example:

	BEGIN;
	CALL pg_dbms_job.remove(12345);
	COMMIT;

### [RUN](#run)

Forces a specified job to run. This procedure runs the job now. It runs even if it is broken. If it was broken and it runs successfully, the job is updated to indicates that it is no longer broken and goes back to running on its schedule.

Running the job recomputes next_date based on the time you run the procedure.

When runs in foreground there is no logging to the jobs history table but information on the dbms_job.all_scheduled_jobs table are updated in case of error or success. In case of error the exception is raise to the client.

Syntax:

	dbms_job.run ( 
		job       IN  bigint);

Parameters:

- job : ID of the job being run.

Example:

	BEGIN;
	CALL pg_dbms_job.run(12345, false);
	COMMIT;

### [SUBMIT](#submit)

Submits a new job to the job queue. It chooses the job from the sequence dbms_job.jobseq.

Actually this is a function as PostgreSQL < 14 do not support out parameters.

Syntax

	dbms_job.submit ( 
		job       OUT bigint,
		what      IN  text,
		[ next_date IN  timestamp(0) with time zone DEFAULT current_timestamp
		[ , interval  IN  text DEFAULT NULL
		[ , no_parse  IN  boolean DEFAULT false ] ] ] );

Parameters:

- job : ID of the job being run.
- what : text of the code to the job to be run. This must be a valid SQL statement or block of plpgsql code. The SQL code that you submit in the `what` parameter is wrapped in the following plpgsql block:
```
DO $$
DECLARE
    job bigint := $jobid;
    next_date timestamp with time zone := current_timestamp;
    broken boolean := false;
BEGIN
    WHAT
END;
$$;
```

Ensure that you include the ; semi-colon with the statement.

- next_date : Next date when the job will be run.
- interval : Date function that calculates the next time to run the job. The default is NULL. This must evaluate to a either a future point in time or NULL.
- no_parse : Unused.

Example:

This submits a new job to the job queue. The job calls ANALYZE to generate optimizer statistics for the table public.accounts. The job is run every 24 hours:

	BEGIN;
	DO $$
	DECLARE
	    jobno bigint;
	BEGIN
	   SELECT dbms_job.submit(
	      'ANALYZE public.accounts.',
	      LOCALTIMESTAMP, 'LOCALTIMESTAMP + ''1 day''::interval') INTO jobno;
	END;
	COMMIT;

### [WHAT](#what)

Alters the job description for a specified job. This procedure changes what an existing job does, and replaces its environment.

Syntax:

	dbms_job.what ( 
		job       IN  bigint,
		what      IN  text);

Parameters:

- job : ID of the job being run.
- what : PL/SQL procedure to run.

Example:

	BEGIN;
	CALL dbms_job.what('ANALYZE public.accounts.');
	COMMIT;

## [Limitations](#limitations)

Job activity is highly write-intensive (one `UPDATE` per dispatch and per completion), so the queue tables can bloat and slow down the scheduler's collection scans. This extension already ships aggressive per-table autovacuum settings (see [Dispatch performance](#dispatch-performance-built-in)) to keep that in check, but a database that accumulated bloat before those settings were applied — or one that has been under sustained heavy load — can still benefit from a one-off compaction when there is no activity:

```
VACUUM FULL dbms_job.all_scheduled_jobs, dbms_job.all_async_jobs;
```

If you have a very high job execution use that generates thousands of NOTIFY per seconds you should better disable this feature to avoid filling the notify queue. The queue is quite large (8GB in a standard installation) but when it is full the transaction that emit the NOTIFY will fail.  Once the queue is half full you will see warnings in the log file. If you experience this limitation you can disable this feature by dropping the triggers responsible of the notification.
```
DROP TRIGGER dbms_job_scheduled_notify_trg ON dbms_job.all_scheduled_jobs;
DROP TRIGGER dbms_job_async_notify_trg ON dbms_job.all_async_jobs;
```
Once the triggers are dropped the polling of jobs will only be done every `job_queue_interval` seconds (0.1 second by default).

## [Authors](#authors)

- Gilles Darold — original `pg_dbms_job` extension and Perl scheduler.
- nettrash — Rust scheduler daemon and the performance/operational improvements in this fork (connection pool, dispatch indexes and autovacuum tuning, zombie-job reaper, configurable job-history recording, history-table partitioning).

## [License](#license)

This extension is free software distributed under the MIT License (see the
[`LICENSE`](LICENSE) file). The original extension and Perl scheduler were
released by MigOps Inc. under the PostgreSQL License.

    Copyright (c) 2021-2023 MigOps Inc. (original extension and Perl scheduler)
    Copyright (c) 2025-2026 nettrash (Rust scheduler and fork enhancements)
