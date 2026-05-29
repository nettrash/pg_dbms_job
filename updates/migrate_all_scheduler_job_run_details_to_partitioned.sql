----
-- One-off migration: convert dbms_job.all_scheduler_job_run_details into a
-- monthly range-partitioned table.
--
-- This is a MANUAL migration for existing installs (the base 1.5.0 script
-- already creates the table partitioned for fresh installs). It is NOT an
-- `ALTER EXTENSION ... UPDATE` step, so run it by hand with psql:
--
--     psql -d <db> -f migrate_all_scheduler_job_run_details_to_partitioned.sql
--
-- Prerequisites / notes:
--   * PostgreSQL 11+ (declarative partitioning + DEFAULT partition).
--   * Run as the table owner / a superuser (same role the extension was
--     installed with), so RLS does not hide rows and DDL is permitted.
--   * The whole thing runs in ONE transaction and copies NO data, so it only
--     holds a brief ACCESS EXCLUSIVE lock (sub-second) — inserts from the
--     scheduler block for that moment, then resume against the new table.
--   * Existing history is PRESERVED in all_scheduler_job_run_details_old and
--     left untouched. Drop it yourself once satisfied (see the end of file) to
--     reclaim its space — this is where the 26 GB actually frees up.
--   * The log_id sequence is reused, so new log_id values keep climbing past
--     the old maximum (no collisions).
----

BEGIN;

-- Refuse to run twice / on an already-partitioned table.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'dbms_job'
          AND c.relname = 'all_scheduler_job_run_details'
    ) THEN
        RAISE EXCEPTION
            'dbms_job.all_scheduler_job_run_details is already partitioned; nothing to do';
    END IF;
END$$;

-- 1. Partition-maintenance helper (same definition as the base 1.5.0 script).
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

-- 2. Move the existing table (with all its data) out of the way.
ALTER TABLE dbms_job.all_scheduler_job_run_details
    RENAME TO all_scheduler_job_run_details_old;

-- 2b. RENAME TABLE does not rename indexes, so the old primary-key index still
--     occupies the schema-unique name `all_scheduler_job_run_details_pkey`,
--     which the new table's PK would also want. Rename it aside first.
DO $$
DECLARE
    v_idx_name text;
BEGIN
    SELECT c.relname INTO v_idx_name
    FROM pg_index x
    JOIN pg_class c ON c.oid = x.indexrelid
    WHERE x.indrelid = 'dbms_job.all_scheduler_job_run_details_old'::regclass
      AND x.indisprimary;
    IF v_idx_name IS NOT NULL THEN
        EXECUTE format('ALTER INDEX dbms_job.%I RENAME TO %I',
                       v_idx_name, v_idx_name || '_old');
    END IF;
END$$;

-- 3. Create the new partitioned parent with the identical column layout.
CREATE TABLE dbms_job.all_scheduler_job_run_details (
    log_id bigint NOT NULL,
    log_date timestamp with time zone NOT NULL DEFAULT current_timestamp,
    owner name,
    job_name varchar(261),
    job_subname varchar(261),
    status text,
    error char(5),
    req_start_date timestamp with time zone,
    actual_start_date timestamp with time zone,
    run_duration bigint,
    instance_id integer,
    session_id integer,
    slave_pid integer,
    cpu_used integer,
    additional_info text,
    PRIMARY KEY (log_id, log_date)
) PARTITION BY RANGE (log_date);

-- 4. Reuse the original sequence so log_id keeps climbing past the old max.
ALTER TABLE dbms_job.all_scheduler_job_run_details
    ALTER COLUMN log_id
    SET DEFAULT nextval('dbms_job.all_scheduler_job_run_details_log_id_seq');
ALTER SEQUENCE dbms_job.all_scheduler_job_run_details_log_id_seq
    OWNED BY dbms_job.all_scheduler_job_run_details.log_id;

-- 5. Restore the original permissions, RLS and comment.
REVOKE ALL ON dbms_job.all_scheduler_job_run_details FROM PUBLIC;
ALTER TABLE dbms_job.all_scheduler_job_run_details ENABLE ROW LEVEL SECURITY;
CREATE POLICY dbms_job_policy ON dbms_job.all_scheduler_job_run_details
    USING (owner = current_user);
COMMENT ON TABLE dbms_job.all_scheduler_job_run_details
    IS 'Table used to store the information about the jobs executed.';

-- 6. Catch-all + current/next month partitions, retaining 3 months.
CREATE TABLE dbms_job.all_scheduler_job_run_details_default
    PARTITION OF dbms_job.all_scheduler_job_run_details DEFAULT;
SELECT dbms_job.maintain_run_details_partitions(1, 3);

COMMIT;

----
-- Optional follow-up (run separately, when ready):
--
-- Register the new objects with the extension so pg_dump / DROP EXTENSION stay
-- correct (the manual conversion above creates them outside extension control):
--   ALTER EXTENSION pg_dbms_job
--       ADD FUNCTION dbms_job.maintain_run_details_partitions(integer, integer);
--   ALTER EXTENSION pg_dbms_job ADD TABLE dbms_job.all_scheduler_job_run_details;
--
-- Reclaim the old history (this is what frees the disk; irreversible):
--   ALTER EXTENSION pg_dbms_job DROP TABLE dbms_job.all_scheduler_job_run_details_old;
--   DROP TABLE dbms_job.all_scheduler_job_run_details_old;
--
-- To keep some recent history instead of discarding all of it, copy it across
-- BEFORE dropping (partitions for those months must exist first), e.g.:
--   INSERT INTO dbms_job.all_scheduler_job_run_details
--   SELECT * FROM dbms_job.all_scheduler_job_run_details_old
--   WHERE log_date >= current_date - interval '7 days';
----
