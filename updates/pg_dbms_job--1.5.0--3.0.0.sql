----
-- Upgrade pg_dbms_job from 1.5.0 to 3.0.0.
--
-- Run with: ALTER EXTENSION pg_dbms_job UPDATE TO '3.0.0';
--
-- This applies the safe, idempotent schema changes that a fresh 3.0.0 install
-- gets: the partial dispatch indexes, aggressive autovacuum on the queue
-- tables, and the partition-maintenance function.
--
-- It does NOT convert the existing all_scheduler_job_run_details table to a
-- partitioned table, because that rewrites the table (rename + recreate) and
-- is better run deliberately rather than as part of an automated extension
-- update. To complete the migration to the partitioned history table, run the
-- standalone script once, by hand:
--
--   psql -d <db> -f updates/migrate_all_scheduler_job_run_details_to_partitioned.sql
--
-- Until then the history table keeps working as a plain (non-partitioned)
-- table; only the dropping-old-partitions retention feature is unavailable.
----

-- Partial indexes supporting the dispatch scans (see jobs.rs).
CREATE INDEX IF NOT EXISTS all_async_jobs_pending_idx
    ON dbms_job.all_async_jobs (job)
    WHERE this_date IS NULL;
CREATE INDEX IF NOT EXISTS all_scheduled_jobs_pending_idx
    ON dbms_job.all_scheduled_jobs (next_date)
    WHERE this_date IS NULL;

-- Aggressive autovacuum for the high-churn queue tables.
ALTER TABLE dbms_job.all_async_jobs SET (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 100,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 100,
    autovacuum_vacuum_cost_delay = 0
);
ALTER TABLE dbms_job.all_scheduled_jobs SET (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 100,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 100,
    autovacuum_vacuum_cost_delay = 0
);

-- Partition-maintenance helper for all_scheduler_job_run_details. Harmless to
-- define even before the table is converted to partitioned; it only takes
-- effect once the standalone migration above has been run.
CREATE OR REPLACE FUNCTION dbms_job.maintain_run_details_partitions(
    months_ahead integer DEFAULT 1,
    retention_months integer DEFAULT 3
) RETURNS void
    LANGUAGE PLPGSQL
    AS $$
DECLARE
    start_d date;
    end_d   date;
    part    text;
    cutoff  date;
    r       record;
BEGIN
    FOR i IN 0..GREATEST(months_ahead, 0) LOOP
        start_d := (date_trunc('month', current_date) + make_interval(months => i))::date;
        end_d   := (start_d + interval '1 month')::date;
        part    := 'all_scheduler_job_run_details_' || to_char(start_d, 'YYYYMM');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'dbms_job' AND c.relname = part
        ) THEN
            EXECUTE format(
                'CREATE TABLE dbms_job.%I PARTITION OF dbms_job.all_scheduler_job_run_details '
                'FOR VALUES FROM (%L) TO (%L)', part, start_d, end_d);
        END IF;
    END LOOP;

    IF retention_months IS NOT NULL AND retention_months > 0 THEN
        cutoff := (date_trunc('month', current_date)
                   - make_interval(months => retention_months))::date;
        FOR r IN
            SELECT c.relname
            FROM pg_inherits inh
            JOIN pg_class c ON c.oid = inh.inhrelid
            JOIN pg_class p ON p.oid = inh.inhparent
            JOIN pg_namespace n ON n.oid = p.relnamespace
            WHERE n.nspname = 'dbms_job'
              AND p.relname = 'all_scheduler_job_run_details'
              AND c.relname ~ '^all_scheduler_job_run_details_[0-9]{6}$'
        LOOP
            IF to_date(right(r.relname, 6), 'YYYYMM') < cutoff THEN
                EXECUTE format('DROP TABLE IF EXISTS dbms_job.%I', r.relname);
            END IF;
        END LOOP;
    END IF;
END;
$$;
REVOKE ALL ON FUNCTION dbms_job.maintain_run_details_partitions(integer, integer) FROM PUBLIC;
