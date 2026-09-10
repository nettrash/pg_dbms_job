-- 3.2.0 -> 3.2.1
--
-- Fix all_scheduled_jobs.failures staying NULL forever for jobs that never
-- succeed (GitHub issue #25).
--
-- The column had no DEFAULT and submit() does not set it, so every new
-- scheduled job started with failures = NULL. Every failure path bumps it with
-- `failures = failures + 1`, which is NULL + 1 = NULL in PostgreSQL, so the
-- counter could only leave NULL via the `failures = 0` written on a successful
-- run. A job that failed on every attempt since it was created therefore showed
-- failures = NULL no matter how many times it had actually failed.
--
-- This migration:
--   1. Gives the column a DEFAULT of 0, so newly submitted jobs start counting
--      from zero.
--   2. Backfills the existing NULLs to 0. Those rows are exactly the jobs that
--      have never completed successfully (last_date IS NULL); their pre-upgrade
--      failure count was never recorded, so they start counting from 0 now.
--   3. Makes the column NOT NULL so the counter can never silently fall back
--      to NULL again.
--
-- SET DEFAULT runs first so its ACCESS EXCLUSIVE lock is taken up front: no
-- concurrent submit can insert a NULL between the backfill and SET NOT NULL,
-- and the migration never has to upgrade a weaker lock while a scheduler worker
-- holds one on the table. The lock lasts only for the backfill of the NULL rows
-- and the single scan SET NOT NULL makes to validate the column.

ALTER TABLE dbms_job.all_scheduled_jobs ALTER COLUMN failures SET DEFAULT 0;
UPDATE dbms_job.all_scheduled_jobs SET failures = 0 WHERE failures IS NULL;
ALTER TABLE dbms_job.all_scheduled_jobs ALTER COLUMN failures SET NOT NULL;
