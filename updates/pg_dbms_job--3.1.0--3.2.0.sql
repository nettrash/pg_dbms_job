-- 3.1.0 -> 3.2.0
--
-- Optional pure-polling mode: let the scheduler run without LISTEN/NOTIFY.
--
-- The Rust daemon gains an `enable_notify` setting (default on). When off it
-- does not LISTEN and dispatches purely on the job_queue_interval poll, so
-- submitters no longer pay pg_notify's cluster-wide commit-time serialization
-- (a heavyweight lock on database 0, held until commit) and a saturated
-- scheduler that stops draining notifications can no longer pin and fill the
-- shared async-notify queue (which, when full, fails NOTIFY-issuing commits).
--
-- This migration:
--   1. Tidies the two statement-level notify trigger functions. They referenced
--      NEW/OLD, which are always NULL in a statement-level trigger, so that
--      logic was dead: the empty-interval normalisation never ran and the
--      "IF NEW.instance = OLD.instance" guard made the scheduled-UPDATE branch
--      never notify. Behaviour is preserved (INSERT/DELETE/TRUNCATE notify;
--      UPDATE does not) with the dead NEW/OLD references removed.
--   2. Recreates the scheduled notify trigger to fire only on the events that
--      actually notify (INSERT/DELETE/TRUNCATE), dropping the no-op UPDATE case
--      so the daemon's hot dispatch/completion UPDATEs no longer fire it.
--   3. Adds dbms_job.set_notify(boolean) to enable/disable both notify triggers
--      as a supported, reversible operation (replacing the manual DROP TRIGGER
--      previously documented for high-NOTIFY-rate workloads).

CREATE OR REPLACE FUNCTION dbms_job.job_scheduled_notify()
    RETURNS trigger
    LANGUAGE PLPGSQL
    AS $$
BEGIN
    -- Statement-level trigger: NEW and OLD are NULL here and the return value is
    -- ignored, so this function cannot inspect or modify the changed rows. Its
    -- sole purpose is to wake the scheduler to re-scan all_scheduled_jobs. The
    -- daemon reads only the channel name, so the payload is purely informational.
    --
    -- It fires for INSERT (submit of a scheduled job), DELETE (remove / one-shot
    -- completion) and TRUNCATE only -- see the trigger definition. It does NOT
    -- fire on UPDATE: the scheduler's own dispatch/completion UPDATEs would only
    -- wake it to re-scan work it is already doing, and user-driven UPDATEs from
    -- change(), broken(), interval(), next_date() and what() are picked up by the
    -- job_queue_interval poll (at worst one interval later). A statement-level
    -- trigger cannot tell the daemon's writes apart from a user's, so notifying
    -- on UPDATE could not be done without also self-waking on every execution.
    PERFORM pg_notify('dbms_job_scheduled_notify', TG_OP);
    RETURN NULL;
END;
$$;
COMMENT ON FUNCTION dbms_job.job_scheduled_notify()
    IS 'Wake the scheduler to re-scan the scheduled-jobs table';

CREATE OR REPLACE FUNCTION dbms_job.job_async_notify()
    RETURNS trigger
    LANGUAGE PLPGSQL
    AS $$
BEGIN
    -- Statement-level trigger (NEW/OLD are NULL, return value ignored): wake the
    -- scheduler on submit of one or more asynchronous jobs. The daemon reads only
    -- the channel name.
    PERFORM pg_notify('dbms_job_async_notify', 'New asynchronous job received');
    RETURN NULL;
END;
$$;
COMMENT ON FUNCTION dbms_job.job_async_notify()
    IS 'Notify the scheduler that a new asynchronous job was submitted';

-- Recreate the scheduled notify trigger without the UPDATE event (its branch
-- never notified). A trigger's event list cannot be altered in place, so drop
-- and recreate. The async trigger is unchanged (still AFTER INSERT).
DROP TRIGGER IF EXISTS dbms_job_scheduled_notify_trg ON dbms_job.all_scheduled_jobs;
CREATE TRIGGER dbms_job_scheduled_notify_trg
    AFTER INSERT OR DELETE OR TRUNCATE
    ON dbms_job.all_scheduled_jobs
    FOR STATEMENT EXECUTE FUNCTION dbms_job.job_scheduled_notify();

-- Enable or disable the LISTEN/NOTIFY wake-up triggers as a supported,
-- reversible operation. Pair dbms_job.set_notify(false) with the daemon's
-- enable_notify=off for a fully NOTIFY-free, poll-only deployment. Idempotent.
CREATE OR REPLACE PROCEDURE dbms_job.set_notify(enabled boolean)
    LANGUAGE PLPGSQL
    AS $$
BEGIN
    IF enabled THEN
        DROP TRIGGER IF EXISTS dbms_job_scheduled_notify_trg ON dbms_job.all_scheduled_jobs;
        CREATE TRIGGER dbms_job_scheduled_notify_trg
            AFTER INSERT OR DELETE OR TRUNCATE
            ON dbms_job.all_scheduled_jobs
            FOR STATEMENT EXECUTE FUNCTION dbms_job.job_scheduled_notify();
        DROP TRIGGER IF EXISTS dbms_job_async_notify_trg ON dbms_job.all_async_jobs;
        CREATE TRIGGER dbms_job_async_notify_trg
            AFTER INSERT
            ON dbms_job.all_async_jobs
            FOR STATEMENT EXECUTE FUNCTION dbms_job.job_async_notify();
        RAISE NOTICE 'pg_dbms_job: NOTIFY triggers enabled';
    ELSE
        DROP TRIGGER IF EXISTS dbms_job_scheduled_notify_trg ON dbms_job.all_scheduled_jobs;
        DROP TRIGGER IF EXISTS dbms_job_async_notify_trg ON dbms_job.all_async_jobs;
        RAISE NOTICE 'pg_dbms_job: NOTIFY triggers disabled; set the daemon enable_notify=off for a poll-only deployment';
    END IF;
END;
$$;
COMMENT ON PROCEDURE dbms_job.set_notify(boolean)
    IS 'Enable or disable the LISTEN/NOTIFY wake-up triggers (poll-only when disabled)';
REVOKE ALL ON PROCEDURE dbms_job.set_notify(boolean) FROM PUBLIC;
