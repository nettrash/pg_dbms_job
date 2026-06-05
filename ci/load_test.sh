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
JOBS="${LOAD_TEST_JOBS:-5000}"                 # async jobs submitted in one burst
JOB_BODY="${LOAD_TEST_JOB_BODY:-PERFORM pg_sleep(0.1);}"  # keeps a worker busy ~100ms
DRAIN_TIMEOUT="${LOAD_TEST_DRAIN_TIMEOUT:-180}"          # seconds allowed to fully drain
RSS_CEILING_KB="${LOAD_TEST_RSS_CEILING_KB:-524288}"     # 512 MiB peak-RSS ceiling
POOL_SIZE="${LOAD_TEST_POOL_SIZE:-50}"         # pooled connections == max live workers
QUEUE_PROCESSES="${LOAD_TEST_QUEUE_PROCESSES:-2048}"     # deliberately >> pool_size

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
psql -v ON_ERROR_STOP=1 -qX -c \
  "SELECT count(*) AS submitted FROM (SELECT dbms_job.submit('${JOB_BODY}') FROM generate_series(1, ${JOBS})) s;"
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
while :; do
  remaining="$(psql_scalar "SELECT count(*) FROM dbms_job.all_async_jobs;")"
  elapsed=$(( $(date +%s) - start ))
  if [ "$remaining" -eq 0 ]; then echo "drained in ${elapsed}s"; break; fi
  if [ "$elapsed" -ge "$DRAIN_TIMEOUT" ]; then echo "TIMEOUT after ${elapsed}s, $remaining jobs left"; break; fi
  echo "t=${elapsed}s remaining=$remaining"
  sleep 2
done
drain_secs="$elapsed"
echo "::endgroup::"

# VmHWM is the kernel-tracked peak RSS over the process lifetime, so reading it
# now (before we stop the daemon) captures the high-water mark during the burst.
peak_kb="$(awk '/VmHWM/{print $2}' "/proc/$PID/status" 2>/dev/null || echo 0)"

echo "----------------------------------------------------------------"
echo "jobs submitted : $JOBS"
echo "drain time     : ${drain_secs}s (timeout ${DRAIN_TIMEOUT}s)"
echo "remaining jobs : $remaining"
echo "peak RSS       : ${peak_kb} kB (ceiling ${RSS_CEILING_KB} kB)"
echo "----------------------------------------------------------------"

summary "### Async load test"
summary ""
summary "| metric | value |"
summary "| --- | --- |"
summary "| jobs submitted | ${JOBS} |"
summary "| drain time | ${drain_secs}s / ${DRAIN_TIMEOUT}s |"
summary "| remaining jobs | ${remaining} |"
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
