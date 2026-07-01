-- 3.0.2 -> 3.1.0
--
-- Auto-schedule the run-details partition maintenance.
--
-- maintain_run_details_partitions() (added in 3.0.0) creates next month's
-- partition ahead of time and prunes partitions older than the retention
-- window. Until 3.1.0 the operator had to remember to schedule it; if they did
-- not, every all_scheduler_job_run_details row fell into the DEFAULT partition,
-- which then blocks attaching the monthly partitions that overlap those rows
-- and bloats without bound -- degrading the per-job history INSERT that every
-- executed job performs.
--
-- This submits it as a daily recurring dbms_job so the behaviour is correct by
-- default. Idempotent: only submitted when an equivalent recurring job does not
-- already exist, so re-running the update (or a dump/restore) never duplicates
-- it. Runs as the extension owner performing the UPDATE, which the scheduler
-- then re-enacts via SET ROLE, so the CREATE/DROP TABLE it issues are permitted.
--
-- No schema change; there is also an accompanying scheduler-daemon release
-- (per-job statement_timeout / idle_in_transaction_timeout, bounded
-- SKIP LOCKED job claiming, a shorter default stale_job_timeout, and a startup
-- Row-Level-Security exemption check). Restart the scheduler binary to pick it up.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM dbms_job.all_scheduled_jobs
        WHERE what LIKE '%maintain_run_details_partitions%'
    ) THEN
        PERFORM dbms_job.submit(
            'PERFORM dbms_job.maintain_run_details_partitions();',
            current_timestamp,
            'current_timestamp + interval ''1 day'''
        );
    END IF;
END
$$;
