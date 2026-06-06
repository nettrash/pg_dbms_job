#!/usr/bin/env bash
#
# Async-job load test for the pg_dbms_job scheduler.
#
# Submits a burst of asynchronous jobs, runs the real scheduler daemon against a
# live PostgreSQL, and asserts two things:
#   1. the async queue drains to zero within a time budget, and
#   2. the daemon's peak resident memory (VmHWM) stays under a ceiling.
#
# The memory check guards the bounded-concurrency / bounded-logging work: with
# `job_queue_processes` set far above `pool_size`, a regression that removes the
# worker-count cap would spawn a thread (and a glibc arena) per queued job and
# blow past the ceiling, while the current code caps live workers at the pool
# size. The drain check guards correctness — every submitted job must execute.
#
# All knobs are overridable via environment variables (see below) so the same
# script can be run locally against any PostgreSQL.
set -euo pipefail

# ---- tunables --------------------------------------------------------------
JOBS="${LOAD_TEST_JOBS:-25000}"                 # async jobs submitted in one burst
JOB_BODY="${LOAD_TEST_JOB_BODY:-PERFORM pg_sleep(0.05);}"  # keeps a worker busy ~50ms
DRAIN_TIMEOUT="${LOAD_TEST_DRAIN_TIMEOUT:-180}"          # seconds allowed to fully drain
RSS_CEILING_KB="${LOAD_TEST_RSS_CEILING_KB:-524288}"     # 512 MiB peak-RSS ceiling
POOL_SIZE="${LOAD_TEST_POOL_SIZE:-32}"         # pooled connections == max live workers
QUEUE_PROCESSES="${LOAD_TEST_QUEUE_PROCESSES:-32}"     # deliberately >> pool_size
POLL_INTERVAL="${LOAD_TEST_POLL_INTERVAL:-2}"          # seconds between queue-depth samples

BIN="${LOAD_TEST_BIN:-rust/target/release/pg_dbms_job}"
CONF="${LOAD_TEST_CONF:-/tmp/pg_dbms_job_loadtest.conf}"
LOG="${LOAD_TEST_LOG:-/tmp/pg_dbms_job_loadtest.log}"
PIDFILE="${LOAD_TEST_PIDFILE:-/tmp/pg_dbms_job_loadtest.pid}"

export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-dbms}"
export PGPASSWORD="${PGPASSWORD:-dbms}"
export PGDATABASE="${PGDATABASE:-dbms_job}"

# Scalar query helper: -q quiet, -t tuples-only, -A unaligned, -X no psqlrc.
psql_scalar() { psql -v ON_ERROR_STOP=1 -qtAX -c "$1"; }

summary() { [ -n "${GITHUB_STEP_SUMMARY:-}" ] && echo "$1" >>"$GITHUB_STEP_SUMMARY" || true; }

cleanup() {
  # Stop the daemon if it is still running, regardless of how we exit.
  if [ -f "$PIDFILE" ]; then
    "$BIN" -c "$CONF" -k >/dev/null 2>&1 || kill "$(cat "$PIDFILE" 2>/dev/null)" 2>/dev/null || true
  fi
}
trap cleanup EXIT

command -v psql >/dev/null || { echo "psql not found on PATH"; exit 1; }
[ -x "$BIN" ] || { echo "scheduler binary not found at $BIN"; exit 1; }

EXTVERSION="$(sed -nE "s/.*default_version[[:space:]]*=[[:space:]]*'([^']+)'.*/\1/p" pg_dbms_job.control)"

echo "::group::Install schema (v${EXTVERSION})"
# The base SQL assumes the dbms_job schema exists (CREATE EXTENSION would create
# it); for the raw-load path used in CI we create it ourselves first.
psql -v ON_ERROR_STOP=1 -qX -c "DROP SCHEMA IF EXISTS dbms_job CASCADE; CREATE SCHEMA dbms_job;"
psql -v ON_ERROR_STOP=1 -qX -f "sql/pg_dbms_job--${EXTVERSION}.sql"
echo "::endgroup::"

echo "::group::Scheduler config"
cat >"$CONF" <<EOF
debug=0
pidfile=$PIDFILE
logfile=$LOG
log_truncate_on_rotation=0
job_queue_interval=0.5
job_queue_processes=$QUEUE_PROCESSES
pool_size=$POOL_SIZE
nap_time=0.05
startup_delay=2.0
error_delay=0.5
stats_interval=5
job_run_details=errors
stale_job_timeout=600
host=$PGHOST
port=$PGPORT
database=$PGDATABASE
user=$PGUSER
passwd=$PGPASSWORD
EOF
cat "$CONF"
echo "::endgroup::"

echo "::group::Submit $JOBS async jobs"
# Submit the whole burst BEFORE starting the daemon: on startup the scheduler
# does a full table scan, so it picks up the entire backlog at once — the
# heaviest memory path (it materialises every queued row in one fetch).
# Feed the query on stdin, NOT via -c: psql only performs :'var' variable
# interpolation when reading from stdin or -f, never on a -c command string, so
# a -c form would send the literal :'job_body' to the server and fail with a
# syntax error at ":". The quoted heredoc delimiter keeps the shell out of it;
# the job count is passed as a psql var too.
psql -v ON_ERROR_STOP=1 -qX -v job_body="$JOB_BODY" -v njobs="$JOBS" <<'SQL'
SELECT count(*) AS submitted
FROM (SELECT dbms_job.submit(:'job_body') FROM generate_series(1, :njobs)) s;
SQL
test_submit_ts="$(psql_scalar "SELECT clock_timestamp();")"
error_logs_before="$(psql_scalar "SELECT count(*) FROM dbms_job.all_scheduler_job_run_details WHERE status <> 'SUCCEEDED';")"
queued="$(psql_scalar "SELECT count(*) FROM dbms_job.all_async_jobs;")"
echo "queued before start: $queued"
[ "$queued" -eq "$JOBS" ] || { echo "FAIL: expected $JOBS queued, found $queued"; exit 1; }
echo "::endgroup::"

echo "::group::Start scheduler"
"$BIN" -c "$CONF"   # daemonizes; the parent returns immediately
PID=""
for _ in $(seq 1 50); do
  [ -f "$PIDFILE" ] && PID="$(cat "$PIDFILE")" && [ -n "$PID" ] && break
  sleep 0.2
done
[ -n "$PID" ] && [ -d "/proc/$PID" ] || { echo "FAIL: scheduler did not start"; cat "$LOG" 2>/dev/null || true; exit 1; }
echo "scheduler pid: $PID"
echo "::endgroup::"

echo "::group::Drain"
start="$(date +%s)"
remaining="$queued"
completed=0
samples=0
peak_remaining="$queued"
t50="NA"
t90="NA"
while :; do
  remaining="$(psql_scalar "SELECT count(*) FROM dbms_job.all_async_jobs;")"
  elapsed=$(( $(date +%s) - start ))
  completed=$(( JOBS - remaining ))
  samples=$(( samples + 1 ))

  if [ "$remaining" -gt "$peak_remaining" ]; then
    peak_remaining="$remaining"
  fi

  if [ "$t50" = "NA" ] && [ "$completed" -ge $(( JOBS / 2 )) ]; then
    t50="${elapsed}"
  fi

  if [ "$t90" = "NA" ] && [ "$completed" -ge $(( JOBS * 9 / 10 )) ]; then
    t90="${elapsed}"
  fi

  if [ "$remaining" -eq 0 ]; then echo "drained in ${elapsed}s"; break; fi
  if [ "$elapsed" -ge "$DRAIN_TIMEOUT" ]; then echo "TIMEOUT after ${elapsed}s, $remaining jobs left"; break; fi
  echo "t=${elapsed}s remaining=$remaining completed=$completed"
  sleep "$POLL_INTERVAL"
done
drain_secs="$elapsed"
echo "::endgroup::"

# VmHWM is the kernel-tracked peak RSS over the process lifetime, so reading it
# now (before we stop the daemon) captures the high-water mark during the burst.
peak_kb="$(awk '/VmHWM/{print $2}' "/proc/$PID/status" 2>/dev/null || echo 0)"

error_logs_after="$(psql_scalar "SELECT count(*) FROM dbms_job.all_scheduler_job_run_details WHERE status <> 'SUCCEEDED';")"
error_logs_delta=$(( error_logs_after - error_logs_before ))

if [ "$drain_secs" -gt 0 ]; then
  throughput_jps="$(awk -v jobs="$completed" -v secs="$drain_secs" 'BEGIN { printf "%.2f", jobs / secs }')"
else
  throughput_jps="NA"
fi

required_throughput_jps="$(awk -v jobs="$JOBS" -v secs="$DRAIN_TIMEOUT" 'BEGIN { if (secs > 0) printf "%.2f", jobs / secs; else printf "NA" }')"

if [ "$JOBS" -gt 0 ]; then
  drained_pct="$(awk -v done="$completed" -v total="$JOBS" 'BEGIN { printf "%.2f", (done * 100.0) / total }')"
else
  drained_pct="NA"
fi

echo "----------------------------------------------------------------"
echo "jobs submitted : $JOBS"
echo "drain time     : ${drain_secs}s (timeout ${DRAIN_TIMEOUT}s)"
echo "drained        : ${completed}/${JOBS} (${drained_pct}%)"
echo "throughput     : ${throughput_jps} jobs/s (required ${required_throughput_jps} jobs/s to hit timeout)"
echo "progress marks : t50=${t50}s t90=${t90}s"
echo "samples        : ${samples} (interval ${POLL_INTERVAL}s)"
echo "remaining jobs : $remaining"
echo "error logs     : +${error_logs_delta} rows in all_scheduler_job_run_details"
echo "peak RSS       : ${peak_kb} kB (ceiling ${RSS_CEILING_KB} kB)"
echo "----------------------------------------------------------------"

summary "### Async load test"
summary ""
summary "| metric | value |"
summary "| --- | --- |"
summary "| jobs submitted | ${JOBS} |"
summary "| drain time | ${drain_secs}s / ${DRAIN_TIMEOUT}s |"
summary "| drained jobs | ${completed}/${JOBS} (${drained_pct}%) |"
summary "| throughput | ${throughput_jps} jobs/s (required ${required_throughput_jps}) |"
summary "| progress markers | t50=${t50}s, t90=${t90}s |"
summary "| queue samples | ${samples} at ${POLL_INTERVAL}s interval |"
summary "| remaining jobs | ${remaining} |"
summary "| error log rows delta | ${error_logs_delta} |"
summary "| peak RSS (VmHWM) | ${peak_kb} kB / ${RSS_CEILING_KB} kB ceiling |"

fail=0
if [ "$remaining" -ne 0 ]; then
  echo "FAIL: queue did not drain within ${DRAIN_TIMEOUT}s ($remaining jobs left)"; fail=1
fi
if [ "$peak_kb" -eq 0 ]; then
  echo "FAIL: could not read peak RSS for pid $PID"; fail=1
elif [ "$peak_kb" -gt "$RSS_CEILING_KB" ]; then
  echo "FAIL: peak RSS ${peak_kb} kB exceeds ceiling ${RSS_CEILING_KB} kB"; fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo "==== scheduler log ===="; cat "$LOG" 2>/dev/null || true
  exit 1
fi

echo "load test passed"
