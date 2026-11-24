#!/usr/bin/env bash
#
# PostgreSQL compatibility test for pg_dbms_job.
#
# Runs against one PostgreSQL server that already has this checkout's extension
# files installed and checks, on that server version, that:
#   1. the installed extension files are the ones in this checkout,
#   2. ALTER EXTENSION ... UPDATE reaches the current version from every base
#      version shipped in sql/,
#   3. CREATE EXTENSION installs the current version, and the SQL API (submit,
#      what, interval, next_date, broken, change, run, remove) works for an
#      ordinary role holding only the grants the README documents,
#   4. the scheduler daemon runs that role's async, recurring, one-shot and
#      failing jobs as that role, records their history, runs the partition
#      maintenance job CREATE EXTENSION schedules, and stops cleanly.
#
# CI runs it once per supported PostgreSQL major version (the pg-compat job in
# .github/workflows/rust.yml); it runs the same way locally against any server.
#
# Connection settings are the libpq PG* variables. PGUSER must be a superuser
# (it creates databases, a role and the extension) that can log in over TCP
# with PGPASSWORD, because the daemon connects with the same credentials.
# PGDATABASE is only the database CREATE/DROP DATABASE are issued from.
#
# Install the extension with `make install`, plus the older base scripts the
# upgrade checks start from (make install ships only the current one):
#   make install
#   install -m 644 sql/pg_dbms_job--*.sql "$(pg_config --sharedir)/extension/"
set -euo pipefail

BIN="${COMPAT_TEST_BIN:-rust/target/release/pg_dbms_job}"
TEST_DB="${COMPAT_TEST_DB:-pg_dbms_job_compat}"
APP_ROLE="${COMPAT_TEST_ROLE:-dbms_job_compat_user}"
RUN_TIMEOUT="${COMPAT_TEST_RUN_TIMEOUT:-60}"   # seconds the daemon gets to run the jobs
PG_MAJOR="${COMPAT_TEST_PG_MAJOR:-}"           # when set, the server must be this major

export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export PGDATABASE="${PGDATABASE:-postgres}"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/pg_dbms_job_compat.XXXXXX")"
CONF="$WORK/pg_dbms_job.conf"
LOG="$WORK/pg_dbms_job.log"
PIDFILE="$WORK/pg_dbms_job.pid"
PID=""
passed=0
leftover=""   # what a failed run leaves in the server, for the failure message

# -q quiet, -t tuples only, -A unaligned, -X no psqlrc; stop on the first error.
q() { psql -v ON_ERROR_STOP=1 -qtAX "$@"; }

fail() { echo "FAIL: $*" >&2; exit 1; }

# check <description> <expected> <actual>
check() {
  if [ "$2" = "$3" ]; then
    echo "ok - $1"
  else
    fail "$1: expected '$2', got '$3'"
  fi
}

# check_ge <description> <minimum> <actual>
check_ge() {
  if [ "$3" -ge "$2" ] 2>/dev/null; then
    echo "ok - $1 ($3 times)"
  else
    fail "$1: expected at least $2, got '$3'"
  fi
}

cleanup() {
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    "$BIN" -c "$CONF" -k >/dev/null 2>&1 || kill "$PID" 2>/dev/null || true
  fi
  if [ "$passed" -eq 1 ]; then
    q -c "SET client_min_messages = warning" \
      -c "DROP DATABASE IF EXISTS $TEST_DB" \
      -c "DROP ROLE IF EXISTS $APP_ROLE" || true
    rm -rf "$WORK"
  else
    if [ -f "$LOG" ]; then
      echo "==== scheduler log ($LOG) ===="
      cat "$LOG"
    fi
    if [ -n "$leftover" ]; then echo "$leftover left in place for inspection"; fi
  fi
}
trap cleanup EXIT

command -v psql >/dev/null || fail "psql not found on PATH"
[ -x "$BIN" ] || fail "scheduler binary not found at $BIN"

EXTVERSION="$(sed -nE "s/.*default_version[[:space:]]*=[[:space:]]*'([^']+)'.*/\1/p" pg_dbms_job.control)"
[ -n "$EXTVERSION" ] || fail "could not read default_version from pg_dbms_job.control"

# ---- server -----------------------------------------------------------------
server_version="$(q -c "SHOW server_version")"
server_num="$(q -c "SHOW server_version_num")"
scheduler_version="$("$BIN" --version)"
echo "PostgreSQL $server_version, pg_dbms_job $EXTVERSION, scheduler ${scheduler_version#Version: }"
if [ -n "$PG_MAJOR" ]; then
  check "server is PostgreSQL $PG_MAJOR" "$PG_MAJOR" "$(( server_num / 10000 ))"
fi
check "connected as a superuser" "on" "$(q -c "SHOW is_superuser")"
check "installed control file is this checkout's ($EXTVERSION)" "$EXTVERSION" \
  "$(q -c "SELECT default_version FROM pg_available_extensions WHERE name = 'pg_dbms_job'")"

# ---- upgrade paths ----------------------------------------------------------
# Every base install script shipped in sql/ must reach the current version
# through the chain of updates/ scripts, and leave a working API behind.
base_versions="$(ls sql | sed -nE 's/^pg_dbms_job--([0-9][0-9.]*)\.sql$/\1/p' \
  | sort -t. -k1,1n -k2,2n -k3,3n)"
for v in $base_versions; do
  [ "$v" = "$EXTVERSION" ] && continue
  if [ "$(q -c "SELECT count(*) FROM pg_available_extension_versions
                WHERE name = 'pg_dbms_job' AND version = '$v'")" != "1" ]; then
    fail "base script for $v is not installed on the server; make install ships only" \
         "the current one, install the rest with:" \
         "install -m 644 sql/pg_dbms_job--*.sql \"\$(pg_config --sharedir)/extension/\""
  fi
  db="${TEST_DB}_from_${v//./_}"
  leftover="database $db"
  q -c "SET client_min_messages = warning" \
    -c "DROP DATABASE IF EXISTS $db" \
    -c "CREATE DATABASE $db"
  got="$(q -d "$db" \
    -c "SET client_min_messages = warning" \
    -c "CREATE EXTENSION pg_dbms_job VERSION '$v'" \
    -c "ALTER EXTENSION pg_dbms_job UPDATE" \
    -c "DO \$\$ BEGIN PERFORM dbms_job.submit('NULL;'); PERFORM count(*) FROM dbms_job.all_jobs; END \$\$" \
    -c "SELECT extversion FROM pg_extension WHERE extname = 'pg_dbms_job'")" \
    || fail "upgrade $v -> $EXTVERSION (psql error above)"
  check "upgrade $v -> $EXTVERSION" "$EXTVERSION" "$got"
  q -c "DROP DATABASE $db"
  leftover=""
done

# ---- fresh install ----------------------------------------------------------
q -c "SET client_min_messages = warning" \
  -c "DROP DATABASE IF EXISTS $TEST_DB" \
  -c "DROP ROLE IF EXISTS $APP_ROLE" \
  -c "CREATE ROLE $APP_ROLE LOGIN" \
  -c "CREATE DATABASE $TEST_DB"
leftover="database $TEST_DB and role $APP_ROLE"

q -d "$TEST_DB" -v role="$APP_ROLE" <<'SQL'
CREATE EXTENSION pg_dbms_job;
-- The grants the README's Security section documents for a job user.
GRANT USAGE ON SCHEMA dbms_job TO :"role";
GRANT ALL ON ALL TABLES IN SCHEMA dbms_job TO :"role";
GRANT ALL ON ALL SEQUENCES IN SCHEMA dbms_job TO :"role";
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_job TO :"role";
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA dbms_job TO :"role";
-- Jobs write here; run_as records the role each one executed as.
CREATE TABLE public.compat_results (
    tag    text NOT NULL,
    run_as name NOT NULL DEFAULT current_user,
    at     timestamptz NOT NULL DEFAULT clock_timestamp()
);
GRANT SELECT, INSERT ON public.compat_results TO :"role";
SQL
check "CREATE EXTENSION installs $EXTVERSION" "$EXTVERSION" \
  "$(q -d "$TEST_DB" -c "SELECT extversion FROM pg_extension WHERE extname = 'pg_dbms_job'")"

# ---- SQL API ----------------------------------------------------------------
q -d "$TEST_DB" -v role="$APP_ROLE" <<'SQL'
SET ROLE :"role";
DO $$
DECLARE
    j bigint;
    r dbms_job.all_scheduled_jobs%ROWTYPE;
BEGIN
    -- Row level security: the partition-maintenance job CREATE EXTENSION
    -- submitted as the installing superuser is invisible to this role.
    IF EXISTS (SELECT 1 FROM dbms_job.all_scheduled_jobs) THEN
        RAISE EXCEPTION 'row level security: % sees jobs it did not submit', current_user;
    END IF;

    j := dbms_job.submit('INSERT INTO public.compat_results (tag) VALUES (''api'');',
                         current_timestamp + interval '1 day',
                         'current_timestamp + interval ''1 day''');
    CALL dbms_job.what(j, 'INSERT INTO public.compat_results (tag) VALUES (''api-run'');');
    CALL dbms_job.interval(j, 'current_timestamp + interval ''2 days''');
    CALL dbms_job.next_date(j, current_timestamp + interval '3 days');
    CALL dbms_job.broken(j, true, current_timestamp + interval '3 days');
    CALL dbms_job.change(j, NULL, NULL, 'current_timestamp + interval ''4 days''');

    SELECT * INTO r FROM dbms_job.all_scheduled_jobs s WHERE s.job = j;
    IF r.log_user <> current_user
       OR r.what <> 'INSERT INTO public.compat_results (tag) VALUES (''api-run'');'
       OR r.interval <> 'current_timestamp + interval ''4 days'''
       OR r.next_date <> current_timestamp + interval '3 days'
       OR NOT r.broken THEN
        RAISE EXCEPTION 'unexpected job row after what/interval/next_date/broken/change: %', r;
    END IF;

    CALL dbms_job.broken(j, false, current_timestamp + interval '3 days');
    CALL dbms_job.run(j);  -- runs the job in the foreground, in this session
    SELECT * INTO r FROM dbms_job.all_scheduled_jobs s WHERE s.job = j;
    IF r.last_date IS NULL OR r.this_date IS NOT NULL OR r.failures <> 0 OR r.broken THEN
        RAISE EXCEPTION 'unexpected job row after run: %', r;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.compat_results
                   WHERE tag = 'api-run' AND run_as = current_user) THEN
        RAISE EXCEPTION 'dbms_job.run did not execute the job';
    END IF;

    CALL dbms_job.remove(j);
    IF EXISTS (SELECT 1 FROM dbms_job.all_scheduled_jobs s WHERE s.job = j) THEN
        RAISE EXCEPTION 'dbms_job.remove left job % in the queue', j;
    END IF;
END
$$;
SQL
echo "ok - SQL API (submit, what, interval, next_date, broken, change, run, remove) as $APP_ROLE"

# ---- scheduler daemon -------------------------------------------------------
# One job of each kind, submitted as the role; submit() prints each job id.
ids="$(q -d "$TEST_DB" -v role="$APP_ROLE" <<'SQL' | tr '\n' ' '
SET ROLE :"role";
SELECT dbms_job.submit('INSERT INTO public.compat_results (tag) VALUES (''async'');');
SELECT dbms_job.submit('RAISE EXCEPTION ''compat async failure'';');
SELECT dbms_job.submit('INSERT INTO public.compat_results (tag) VALUES (''recurring'');',
                       current_timestamp, 'current_timestamp + interval ''1 second''');
SELECT dbms_job.submit('INSERT INTO public.compat_results (tag) VALUES (''oneshot'');',
                       current_timestamp + interval '2 seconds');
SELECT dbms_job.submit('RAISE EXCEPTION ''compat scheduled failure'';',
                       current_timestamp, 'current_timestamp + interval ''1 second''');
SQL
)"
read -r async_ok async_fail recurring oneshot sched_fail <<<"$ids"
[ -n "$sched_fail" ] || fail "could not submit the test jobs (got ids '$ids')"

cat >"$CONF" <<EOF
debug=0
pidfile=$PIDFILE
logfile=$LOG
job_queue_interval=0.2
job_queue_processes=8
pool_size=4
nap_time=0.05
startup_delay=1
error_delay=0.2
stats_interval=0
job_run_details=all
stale_job_timeout=600
host=$PGHOST
port=$PGPORT
database=$TEST_DB
user=$PGUSER
passwd=$PGPASSWORD
EOF

"$BIN" -c "$CONF"   # daemonizes; the parent returns immediately
for _ in $(seq 1 50); do
  if [ -s "$PIDFILE" ]; then PID="$(cat "$PIDFILE")"; break; fi
  sleep 0.2
done
if [ -z "$PID" ] || ! kill -0 "$PID" 2>/dev/null; then
  fail "scheduler did not start"
fi
echo "scheduler started, pid $PID"

# Prints "<done> <progress>", done being true once every job has run as expected.
poll() {
  q -d "$TEST_DB" -v oneshot="$oneshot" -v sched_fail="$sched_fail" <<'SQL'
WITH s AS (
    SELECT
        (SELECT count(*) FROM dbms_job.all_async_jobs) AS async_queued,
        (SELECT count(*) FROM public.compat_results WHERE tag = 'async') AS async_runs,
        (SELECT count(*) FROM public.compat_results WHERE tag = 'recurring') AS recurring_runs,
        (SELECT count(*) FROM public.compat_results WHERE tag = 'oneshot') AS oneshot_runs,
        (SELECT count(*) FROM dbms_job.all_scheduled_jobs
          WHERE job = :oneshot) AS oneshot_queued,
        (SELECT coalesce(max(failures), 0) FROM dbms_job.all_scheduled_jobs
          WHERE job = :sched_fail) AS scheduled_failures,
        (SELECT count(*) FROM dbms_job.all_scheduled_jobs
          WHERE what LIKE '%maintain_run_details_partitions%'
            AND last_date IS NOT NULL) AS maintenance_runs
)
SELECT (async_queued = 0 AND async_runs = 1 AND recurring_runs >= 2
        AND oneshot_runs = 1 AND oneshot_queued = 0
        AND scheduled_failures >= 2 AND maintenance_runs = 1)::text
       || format(' async_queued=%s async_runs=%s recurring_runs=%s oneshot_runs=%s'
                 ' oneshot_queued=%s scheduled_failures=%s maintenance_runs=%s',
                 async_queued, async_runs, recurring_runs, oneshot_runs,
                 oneshot_queued, scheduled_failures, maintenance_runs)
FROM s;
SQL
}

start="$(date +%s)"
while :; do
  line="$(poll)" || fail "progress query failed"
  read -r done progress <<<"$line"
  elapsed=$(( $(date +%s) - start ))
  echo "t=${elapsed}s $progress"
  [ "$done" = "true" ] && break
  [ "$elapsed" -ge "$RUN_TIMEOUT" ] && fail "jobs did not all run within ${RUN_TIMEOUT}s"
  kill -0 "$PID" 2>/dev/null || fail "scheduler exited while jobs were pending"
  sleep 1
done
echo "ok - scheduler ran async, recurring, one-shot, failing and maintenance jobs"

"$BIN" -c "$CONF" -k
for _ in $(seq 1 150); do
  kill -0 "$PID" 2>/dev/null || break
  sleep 0.2
done
if kill -0 "$PID" 2>/dev/null; then
  fail "scheduler still running 30s after -k"
fi
PID=""
echo "ok - scheduler stopped on -k"

# ---- results ----------------------------------------------------------------
# hist <job id> <condition>: count of that job's history rows matching condition.
# A successful run is recorded with status '' and a failed one with 'ERROR'.
hist() {
  q -d "$TEST_DB" -c "SELECT count(*) FROM dbms_job.all_scheduler_job_run_details
                      WHERE job_name = '$1' AND owner = '$APP_ROLE' AND ($2)"
}
ok_run="status IS DISTINCT FROM 'ERROR'"
failed_run="status = 'ERROR' AND additional_info LIKE 'sqlstate=P0001%'"

check "every job ran as $APP_ROLE" "0" \
  "$(q -d "$TEST_DB" -c "SELECT count(*) FROM public.compat_results WHERE run_as <> '$APP_ROLE'")"
check "history: async job succeeded once" "1" "$(hist "$async_ok" "$ok_run")"
check "history: failing async job failed once (SQLSTATE P0001)" "1" "$(hist "$async_fail" "$failed_run")"
check "history: one-shot job succeeded once" "1" "$(hist "$oneshot" "$ok_run")"
check_ge "history: recurring job succeeded" 2 "$(hist "$recurring" "$ok_run")"
check_ge "history: failing recurring job failed (SQLSTATE P0001)" 2 "$(hist "$sched_fail" "$failed_run")"
check "$APP_ROLE sees only its own history (row level security)" "t" \
  "$(q -d "$TEST_DB" -c "SET ROLE $APP_ROLE" \
                     -c "SELECT count(*) > 0 AND bool_and(owner = current_user)
                           FROM dbms_job.all_scheduler_job_run_details")"
if grep -qi 'panic' "$LOG"; then
  fail "scheduler log mentions a panic"
fi

passed=1
echo "PostgreSQL $server_version: all pg_dbms_job compatibility checks passed"
