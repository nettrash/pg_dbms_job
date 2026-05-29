# Rust Port of pg_dbms_job

This folder contains a Rust implementation of the `pg_dbms_job` scheduler daemon. It mirrors the Perl daemon logic (config parsing, LISTEN/NOTIFY, job polling, fork-per-job execution, and run history writes), but is built as a standalone Rust crate.

## Build

```bash
cargo build --manifest-path rust/Cargo.toml
```

## Run

Foreground single-pass (no daemonize):

```bash
cargo run --manifest-path rust/Cargo.toml -- --single
```

Daemon mode (forks and detaches):

```bash
cargo run --manifest-path rust/Cargo.toml
```

## Common Flags

- `-c, --config <file>`: path to config file (default: `/etc/pg_dbms_job/pg_dbms_job.conf`)
- `-d, --debug`: enable debug logging
- `-k, --kill`: stop current daemon gracefully
- `-m, --immediate`: stop daemon immediately
- `-r, --reload`: reload config and job definitions
- `-s, --single`: run one loop in foreground and exit
- `-v, --version`: show version

## Configuration

The configuration file uses the same `key=value` syntax as `postgresql.conf`:
one entry per line, `#` introduces a comment. Unknown keys are silently
ignored. The Rust scheduler accepts the following keys.

### General

- `debug` (`0`/`1`, default `0`) — toggle DEBUG-level logging. The `-d`
  CLI flag overrides whatever the file sets.
- `pidfile` (path, default `/tmp/pg_dbms_job.pid`) — pid file location.
  Held with an advisory exclusive lock for the lifetime of the daemon,
  so a stale file from a crashed previous run is reused automatically.
- `logfile` (path template, default empty = stderr) — may contain
  `strftime(3)` escapes such as `%Y-%m-%d` to roll the file on a date
  boundary.
- `log_truncate_on_rotation` (`0`/`1`, default `0`) — truncate the new
  log file on time-driven rotation rather than appending. Restart never
  truncates.
- `job_queue_interval` (seconds, float > 0, default `0.1`) — poll
  interval for the async and scheduled job tables. Caps the maximum
  time a queued job waits when no `NOTIFY` arrives.
- `job_queue_processes` (positive integer, default `1024`) — maximum
  number of jobs running concurrently. When the limit is hit, the main
  loop sleeps for `error_delay` seconds and reaps finished workers
  before trying again.
- `pool_size` (positive integer, default `100`) — maximum number of
  PostgreSQL connections in the worker connection pool. Clamped at
  runtime to `min(pool_size, job_queue_processes)`.
- `nap_time` (seconds, float > 0, default `0.1`) — timeout for each
  `LISTEN`/notification cycle in the main loop. Lower values give
  faster response to notifications at the cost of CPU.
- `startup_delay` (seconds, float > 0, default `3.0`) — delay before
  retrying after a failed database connection or when the database is
  in recovery.
- `error_delay` (seconds, float > 0, default `0.5`) — delay applied
  when the worker queue is saturated, to give in-flight jobs time to
  finish before re-checking.
- `stats_interval` (non-negative integer seconds, default `15`) —
  period for the periodic job-statistics LOG line
  `LOG: stats: jobs started=<N>, finished=<M> in last <S> seconds`.
  Counters are atomically read-and-reset on each report; panicking
  workers are still counted. Set to `0` to disable.
- `job_run_details` (`all`/`errors`/`none`, default `all`) — how much
  history is written to `dbms_job.all_scheduler_job_run_details`, one row
  per job execution. `all` keeps the original behaviour; `errors` records
  only failed runs (status = ERROR), keeping diagnostics while avoiding
  most of the growth; `none` disables recording entirely. The table is
  never read by the scheduler, so on busy systems it bloats without
  bound — use `errors` or `none` to keep it in check.
- `stale_job_timeout` (seconds, float ≥ 0, default `3600`) — age after
  which a job still flagged running (`this_date` set) but with no live
  worker backend is treated as abandoned and re-queued. This recovers
  "zombie" rows left behind when a worker could not obtain a connection,
  failed during `SET ROLE`/`BEGIN`/`search_path`, panicked, or the daemon
  crashed — without it such rows vanish from the queue permanently. The
  reaper checks `pg_stat_activity` for the job's `pg_dbms_job:<kind>:<job>`
  backend, so a legitimately long-running job is never re-queued while
  still executing (no double execution); pick a value comfortably above
  your longest expected job runtime. Set to `0` to disable reaping.

### Database

- `host` (default empty) — hostname or IP of the PostgreSQL server.
- `port` (`u16`, default `5432`).
- `database` (default empty) — database to connect to.
- `user` (default empty) — role used by the scheduler; must be a
  superuser because the scheduler runs each job under the job owner's
  role via `SET ROLE`.
- `passwd` (default empty) — password for that role.

### Reload

All of the above are re-read on `SIGHUP` (or `pg_dbms_job -r`). Each
reload reopens the log file (so `logrotate`-style rotation works),
unconditionally drops the database connection and worker pool so the
next iteration reconnects with the current settings, and renames the
pid file in place if `pidfile` itself changed.

### Example

```ini
#-----------
#  General
#-----------
# Toggle debug mode
debug=0
# Path to the pid file
pidfile=/tmp/pg_dbms_job.pid
# Log file pattern — %Y-%m-%d rolls daily
logfile=/var/log/pg_dbms_job_%Y-%m-%d.log
# Truncate on time-driven rotation rather than append
log_truncate_on_rotation=0
# Poll interval of the job queue (seconds)
job_queue_interval=5
# Maximum concurrent jobs
job_queue_processes=1024
# Maximum PG connections in the worker pool
pool_size=100
# Main-loop LISTEN timeout (seconds) — controls notification latency
nap_time=0.1
# Delay before retrying after connect failures (seconds)
startup_delay=3.0
# Delay when the worker queue is saturated (seconds)
error_delay=1
# Period (seconds) for the periodic job-stats LOG line; 0 disables it
stats_interval=15
# Job-run history recorded in all_scheduler_job_run_details:
# all = every run, errors = failures only, none = disabled
job_run_details=all
# Re-queue jobs flagged running with no live worker after this many
# seconds (recovers abandoned "zombie" rows); 0 disables
stale_job_timeout=3600

#-----------
#  Database
#-----------
host=localhost
port=5432
database=dbms_job
user=postgres
passwd=secret
```

## Notes

- The Rust implementation uses `postgres` crate and `nix` for forking and signals.
- The scheduler uses `LISTEN` on `dbms_job_async_notify` and `dbms_job_scheduled_notify`.
- Logs follow the same timestamped format as the Perl daemon.
