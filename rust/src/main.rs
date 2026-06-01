//! pg_dbms_job scheduler entry point.

mod args;
mod config;
mod constants;
mod db;
mod jobs;
mod logging;
mod model;
mod process;
mod util;

use crate::args::{parse_args, usage};
use crate::config::read_config;
use crate::constants::{REAP_INTERVAL_SECS, VERSION};
use crate::db::JobPool;
use crate::db::{ConnectError, connect_db, create_job_pool};
use crate::jobs::{get_async_jobs, get_scheduled_jobs, reap_stale_jobs, spawn_job};
use crate::logging::{dprint, reopen_logger, shutdown_logger};
use crate::model::{Config, DbInfo, Job, JobKind, JobRunDetails, JobStats};
use crate::process::{
    daemonize, reap_children, release_pidfile, signal_handling, wait_all_children, write_pidfile,
};
use crate::util::die;
use fallible_iterator::FallibleIterator;
use nix::sys::signal::Signal;
use postgres::{Client, Notification};
use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGTERM};
use signal_hook::flag;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

fn main() {
    let mut args = parse_args();
    if args.config_file.is_empty() {
        args.config_file = "/etc/pg_dbms_job/pg_dbms_job.conf".to_string();
    }

    if args.help {
        usage(&args.config_file);
        return;
    }

    if args.version {
        println!("Version: {VERSION}");
        return;
    }

    let mut config = default_config();
    let mut dbinfo = default_dbinfo();

    read_config(&args.config_file, &mut config, &mut dbinfo, false);

    if let Some(debug) = args.debug_override {
        config.debug = debug;
    }

    if args.kill {
        signal_handling(&config.pidfile, Signal::SIGTERM);
        return;
    } else if args.abort {
        signal_handling(&config.pidfile, Signal::SIGINT);
        return;
    } else if args.reload {
        signal_handling(&config.pidfile, Signal::SIGHUP);
        return;
    }

    if !args.single {
        daemonize(&config);
    }

    if let Err(err) = write_pidfile(&config.pidfile) {
        die(&format!("FATAL: {err}"));
    }

    let terminate_flag = Arc::new(AtomicBool::new(false));
    let reload_flag = Arc::new(AtomicBool::new(false));

    flag::register(SIGINT, Arc::clone(&terminate_flag)).expect("register SIGINT");
    flag::register(SIGTERM, Arc::clone(&terminate_flag)).expect("register SIGTERM");
    flag::register(SIGHUP, Arc::clone(&reload_flag)).expect("register SIGHUP");

    dprint(&config, "LOG", "Entering main loop.");

    let mut config = Arc::new(config);
    let mut dbh: Option<Client> = None;
    let mut job_pool: Option<Arc<JobPool>> = None;
    let mut running_workers: HashMap<u64, JoinHandle<()>> = HashMap::new();
    let mut next_worker_id: u64 = 1;
    let mut scheduled_jobs: HashMap<i64, Job> = HashMap::new();
    let mut async_jobs: HashMap<i64, Job> = HashMap::new();
    let mut previous_async_exec = Instant::now();
    let mut previous_scheduled_exec = Instant::now();
    let mut previous_reap = Instant::now();
    let job_stats = Arc::new(JobStats::default());
    let mut last_stats_at = Instant::now();
    let mut startup = true;
    let mut config_invalidated = false;
    let mut in_recovery_logged = false;

    while !terminate_flag.load(Ordering::Relaxed) {
        reap_children(&mut running_workers);

        if config.stats_interval > 0 && last_stats_at.elapsed().as_secs() >= config.stats_interval {
            let elapsed = last_stats_at.elapsed().as_secs();
            let (started, finished) = job_stats.drain();
            dlog!(
                &config,
                "LOG",
                "stats: jobs started={}, finished={} in last {} seconds",
                started,
                finished,
                elapsed
            );
            last_stats_at = Instant::now();
        }

        if reload_flag.swap(false, Ordering::Relaxed) {
            // Drop the persistent log file handle *before* writing anything.
            // After logrotate-style rotation (rename pg_dbms_job.log →
            // pg_dbms_job.log.1, create a fresh pg_dbms_job.log) our open fd
            // still points at the renamed-aside inode, so any line emitted now
            // — including the "Received reload" line below and whatever
            // read_config() logs — would land in the old file. Reopening first
            // makes the next write re-open the configured path, i.e. the new
            // file, which is also what `lsof` will then show.
            reopen_logger();
            dprint(&config, "LOG", "Received reload signal HUP.");
            let mut cfg = Config::clone(&config);
            let old_pidfile = cfg.pidfile.clone();
            read_config(&args.config_file, &mut cfg, &mut dbinfo, true);
            if old_pidfile != cfg.pidfile {
                if let Err(err) = std::fs::rename(&old_pidfile, &cfg.pidfile) {
                    cfg.pidfile = old_pidfile.clone();
                    config = Arc::new(cfg);
                    dlog!(
                        &config,
                        "ERROR",
                        "can't change path to pid keeping old one {}, {}",
                        old_pidfile,
                        err
                    );
                } else {
                    config = Arc::new(cfg);
                    dlog!(
                        &config,
                        "LOG",
                        "path to pid file has changed, rename {} into {}",
                        old_pidfile,
                        config.pidfile
                    );
                }
            } else {
                config = Arc::new(cfg);
            }
            config_invalidated = true;
        }

        if config_invalidated {
            let _ = dbh.take();
            job_pool = None;
        }

        if dbh.is_none() {
            match connect_db(&dbinfo, &config) {
                Ok(client) => {
                    if in_recovery_logged {
                        dprint(&config, "LOG", "database has exited recovery mode");
                        in_recovery_logged = false;
                    }
                    dbh = Some(client);
                }
                Err(ConnectError::InRecovery) => {
                    if !in_recovery_logged {
                        dprint(
                            &config,
                            "WARNING",
                            "database is in recovery, retrying later",
                        );
                        in_recovery_logged = true;
                    }
                    thread::sleep(Duration::from_secs_f64(config.startup_delay));
                    startup = true;
                    config_invalidated = true;
                    continue;
                }
                Err(err) => {
                    dlog!(&config, "ERROR", "{}", err);
                    thread::sleep(Duration::from_secs_f64(config.startup_delay));
                    startup = true;
                    config_invalidated = true;
                    continue;
                }
            }
        }

        if job_pool.is_none() {
            let effective_pool_size = config.pool_size.min(config.job_queue_processes) as u32;
            match create_job_pool(&dbinfo, effective_pool_size) {
                Ok(pool) => {
                    dlog!(
                        &config,
                        "LOG",
                        "Connection pool created with max size {}",
                        effective_pool_size
                    );
                    job_pool = Some(Arc::new(pool));
                }
                Err(err) => {
                    dlog!(&config, "ERROR", "Failed to create connection pool: {err}");
                    thread::sleep(Duration::from_secs_f64(config.startup_delay));
                    startup = true;
                    config_invalidated = true;
                    continue;
                }
            }
        }

        let mut async_count = 0usize;
        let mut scheduled_count = 0usize;

        if let Some(client) = dbh.as_mut() {
            config_invalidated = false;
            let mut notifications = client.notifications();
            collect_notifications(
                &mut notifications,
                &config,
                Duration::from_secs_f64(config.nap_time),
                &mut async_count,
                &mut scheduled_count,
            );
        } else {
            thread::sleep(Duration::from_secs_f64(config.startup_delay));
            startup = true;
            config_invalidated = true;
            continue;
        }

        if async_count == 0
            && !startup
            && previous_async_exec.elapsed().as_secs_f64() >= config.job_queue_interval
        {
            dprint(
                &config,
                "DEBUG",
                "job_queue_interval reached, forcing collect of asynchronous jobs",
            );
            async_count = 1;
        }
        if scheduled_count == 0
            && !startup
            && previous_scheduled_exec.elapsed().as_secs_f64() >= config.job_queue_interval
        {
            dprint(
                &config,
                "DEBUG",
                "job_queue_interval reached, forcing collect of scheduled jobs",
            );
            scheduled_count = 1;
        }

        if async_count > 0 || startup {
            if let Some(client) = dbh.as_mut() {
                get_async_jobs(client, &config, &mut async_jobs);
            }
            previous_async_exec = Instant::now();
        }

        if scheduled_count > 0 || startup {
            if let Some(client) = dbh.as_mut() {
                get_scheduled_jobs(
                    client,
                    &config,
                    &mut config_invalidated,
                    &mut scheduled_jobs,
                );
            }
            previous_scheduled_exec = Instant::now();
            if config_invalidated {
                thread::sleep(Duration::from_secs_f64(config.startup_delay));
                startup = true;
                continue;
            }
        }

        config_invalidated = false;
        startup = false;

        // Periodically re-queue jobs abandoned by workers that never cleared
        // their dispatch marker (e.g. a worker that could not obtain a pooled
        // connection, or a crashed worker/daemon). Without this such rows stay
        // flagged running forever and silently disappear from the queue. The
        // check cadence is capped so it is never coarser than the eligibility
        // age itself.
        if config.stale_job_timeout > 0.0
            && previous_reap.elapsed().as_secs_f64()
                >= REAP_INTERVAL_SECS.min(config.stale_job_timeout)
        {
            if let Some(client) = dbh.as_mut() {
                reap_stale_jobs(client, &config);
            }
            previous_reap = Instant::now();
        }

        for (_, job) in scheduled_jobs.drain() {
            while running_workers.len() >= config.job_queue_processes {
                dlog!(
                    &config,
                    "WARNING",
                    "max job queue size is reached ({}) waiting the end of an other job",
                    config.job_queue_processes
                );
                thread::sleep(Duration::from_secs_f64(config.error_delay));
                reap_children(&mut running_workers);
            }
            spawn_job(
                JobKind::Scheduled,
                job,
                job_pool.as_ref().unwrap(),
                &config,
                &job_stats,
                &mut running_workers,
                &mut next_worker_id,
            );
        }

        for (_, job) in async_jobs.drain() {
            while running_workers.len() >= config.job_queue_processes {
                dlog!(
                    &config,
                    "WARNING",
                    "max job queue size is reached ({}) waiting the end of an other job",
                    config.job_queue_processes
                );
                thread::sleep(Duration::from_secs_f64(config.error_delay));
                reap_children(&mut running_workers);
            }
            spawn_job(
                JobKind::Async,
                job,
                job_pool.as_ref().unwrap(),
                &config,
                &job_stats,
                &mut running_workers,
                &mut next_worker_id,
            );
        }

        if args.single {
            break;
        }
    }

    wait_all_children(&mut running_workers);
    release_pidfile();
    if Path::new(&config.pidfile).exists()
        && let Err(err) = std::fs::remove_file(&config.pidfile)
    {
        dlog!(
            &config,
            "ERROR",
            "Unable to remove pid file {}, {}",
            config.pidfile,
            err
        );
    }

    dprint(&config, "LOG", "pg_dbms_job scheduler stopped.");
    shutdown_logger();
}

/// The minimal view of a backend notification the main loop needs: enough to
/// route it to the right job-table scan and to log it. Abstracted into a trait
/// so the dispatch-collection logic can be unit tested without a live database.
trait NotificationLike {
    fn channel(&self) -> &str;
    fn process_id(&self) -> i32;
    fn payload(&self) -> &str;
}

impl NotificationLike for Notification {
    fn channel(&self) -> &str {
        Notification::channel(self)
    }
    fn process_id(&self) -> i32 {
        Notification::process_id(self)
    }
    fn payload(&self) -> &str {
        Notification::payload(self)
    }
}

/// A source of backend notifications, split into the one operation that may
/// block (`wait_first`) and the strictly non-blocking drain (`next_buffered`).
/// Keeping them separate is what lets the main loop block at most once per
/// dispatch cycle — see [`collect_notifications`].
trait NotificationSource {
    type Item: NotificationLike;
    /// Block up to `timeout` for the next notification, or return `Ok(None)`
    /// when the timeout elapses with none pending.
    fn wait_first(&mut self, timeout: Duration) -> Result<Option<Self::Item>, postgres::Error>;
    /// Return the next already-buffered notification without blocking, or
    /// `Ok(None)` when the buffer is drained.
    fn next_buffered(&mut self) -> Result<Option<Self::Item>, postgres::Error>;
}

impl NotificationSource for postgres::Notifications<'_> {
    type Item = Notification;
    fn wait_first(&mut self, timeout: Duration) -> Result<Option<Notification>, postgres::Error> {
        self.timeout_iter(timeout).next()
    }
    fn next_buffered(&mut self) -> Result<Option<Notification>, postgres::Error> {
        // A fresh non-blocking iter() per call polls the connection once and
        // pops one buffered notification; it never waits on the network.
        self.iter().next()
    }
}

/// Count a received notification against the async or scheduled tally so the
/// main loop knows which job tables to scan this cycle.
fn tally_notification<N: NotificationLike>(
    config: &Config,
    notification: &N,
    async_count: &mut usize,
    scheduled_count: &mut usize,
) {
    dlog!(
        config,
        "DEBUG",
        "Received notification: ({}, {}, {})",
        notification.channel(),
        notification.process_id(),
        notification.payload()
    );
    if notification.channel() == "dbms_job_async_notify" {
        *async_count += 1;
    } else if notification.channel() == "dbms_job_scheduled_notify" {
        *scheduled_count += 1;
    }
}

/// Collect the notifications driving this dispatch cycle, tallying them per
/// channel into `async_count` / `scheduled_count`.
///
/// Blocks up to `nap_time` for the *first* notification so the idle loop stays
/// cheap, then drains any others that are already buffered WITHOUT blocking a
/// second `nap_time`. This is the crux of the dispatch latency: the postgres
/// `TimeoutIter` resets its delay on every notification and only returns `None`
/// after a full `nap_time` of silence, so reusing it to drain would tax every
/// dispatch with one extra `nap_time` of latency. `wait_first` is therefore
/// invoked at most once per cycle.
fn collect_notifications<S: NotificationSource>(
    source: &mut S,
    config: &Config,
    nap_time: Duration,
    async_count: &mut usize,
    scheduled_count: &mut usize,
) {
    match source.wait_first(nap_time) {
        Ok(Some(notification)) => {
            tally_notification(config, &notification, async_count, scheduled_count)
        }
        // Nothing arrived this cycle: do not poll again, leave the tallies at 0.
        Ok(None) => return,
        Err(err) => {
            dlog!(config, "ERROR", "notification error: {err}");
            return;
        }
    }

    loop {
        match source.next_buffered() {
            Ok(Some(notification)) => {
                tally_notification(config, &notification, async_count, scheduled_count)
            }
            Ok(None) => break,
            Err(err) => {
                dlog!(config, "ERROR", "notification error: {err}");
                break;
            }
        }
    }
}

/// Default scheduler configuration values.
fn default_config() -> Config {
    Config {
        debug: false,
        pidfile: "/tmp/pg_dbms_job.pid".to_string(),
        logfile: String::new(),
        log_truncate_on_rotation: false,
        job_queue_interval: 0.1,
        job_queue_processes: 1024,
        pool_size: 100,
        nap_time: 0.1,
        startup_delay: 3.0,
        error_delay: 0.5,
        stats_interval: 15,
        job_run_details: JobRunDetails::All,
        stale_job_timeout: 3600.0,
    }
}

/// Default database connection settings.
fn default_dbinfo() -> DbInfo {
    DbInfo {
        host: String::new(),
        database: String::new(),
        user: String::new(),
        passwd: String::new(),
        port: 5432,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        NotificationLike, NotificationSource, collect_notifications, default_config, default_dbinfo,
    };
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    /// A notification stub carrying only the channel name the tally logic reads.
    struct FakeNotification {
        channel: String,
    }

    impl FakeNotification {
        fn new(channel: &str) -> Self {
            FakeNotification {
                channel: channel.to_string(),
            }
        }
    }

    impl NotificationLike for FakeNotification {
        fn channel(&self) -> &str {
            &self.channel
        }
        fn process_id(&self) -> i32 {
            0
        }
        fn payload(&self) -> &str {
            ""
        }
    }

    /// A scripted notification source that records how often the (potentially
    /// blocking) `wait_first` and the non-blocking `next_buffered` are called,
    /// and can simulate `wait_first` blocking for a fixed duration.
    struct FakeSource {
        first: Option<FakeNotification>,
        buffered: VecDeque<FakeNotification>,
        wait_first_calls: usize,
        next_buffered_calls: usize,
        wait_first_sleep: Option<Duration>,
    }

    impl FakeSource {
        fn new(first: Option<FakeNotification>, buffered: Vec<FakeNotification>) -> Self {
            FakeSource {
                first,
                buffered: buffered.into(),
                wait_first_calls: 0,
                next_buffered_calls: 0,
                wait_first_sleep: None,
            }
        }
    }

    impl NotificationSource for FakeSource {
        type Item = FakeNotification;
        fn wait_first(
            &mut self,
            _timeout: Duration,
        ) -> Result<Option<FakeNotification>, postgres::Error> {
            self.wait_first_calls += 1;
            if let Some(d) = self.wait_first_sleep.take() {
                std::thread::sleep(d);
            }
            Ok(self.first.take())
        }
        fn next_buffered(&mut self) -> Result<Option<FakeNotification>, postgres::Error> {
            self.next_buffered_calls += 1;
            Ok(self.buffered.pop_front())
        }
    }

    // The whole point of the refactor: across a dispatch cycle the loop blocks
    // at most ONCE (the first notification). Everything else is drained without
    // blocking, so a cycle can never cost two nap_times.
    #[test]
    fn collect_notifications_blocks_at_most_once() {
        let config = default_config();
        let mut source = FakeSource::new(
            Some(FakeNotification::new("dbms_job_async_notify")),
            vec![
                FakeNotification::new("dbms_job_scheduled_notify"),
                FakeNotification::new("dbms_job_async_notify"),
            ],
        );
        let (mut async_count, mut scheduled_count) = (0usize, 0usize);

        collect_notifications(
            &mut source,
            &config,
            Duration::from_millis(100),
            &mut async_count,
            &mut scheduled_count,
        );

        assert_eq!(
            source.wait_first_calls, 1,
            "must block at most once per cycle"
        );
        assert_eq!(async_count, 2);
        assert_eq!(scheduled_count, 1);
    }

    // When nothing arrives within nap_time we must not poll again — the cycle
    // ends with empty tallies and a single blocking wait.
    #[test]
    fn collect_notifications_idle_does_not_drain() {
        let config = default_config();
        let mut source = FakeSource::new(None, vec![]);
        let (mut async_count, mut scheduled_count) = (0usize, 0usize);

        collect_notifications(
            &mut source,
            &config,
            Duration::from_millis(100),
            &mut async_count,
            &mut scheduled_count,
        );

        assert_eq!(source.wait_first_calls, 1);
        assert_eq!(source.next_buffered_calls, 0, "idle cycle must not drain");
        assert_eq!(async_count, 0);
        assert_eq!(scheduled_count, 0);
    }

    // Latency guard: simulate the worst case where the first notification only
    // arrives at the nap_time deadline, then drain the rest. The whole cycle
    // must take ~one nap_time, never two — the regression this protects against
    // (reusing TimeoutIter to drain) would have cost a second full nap_time.
    #[test]
    fn collect_notifications_does_not_wait_two_nap_times() {
        let nap_time = Duration::from_millis(150);
        let config = default_config();
        let mut source = FakeSource::new(
            Some(FakeNotification::new("dbms_job_async_notify")),
            vec![FakeNotification::new("dbms_job_async_notify")],
        );
        source.wait_first_sleep = Some(nap_time);
        let (mut async_count, mut scheduled_count) = (0usize, 0usize);

        let start = Instant::now();
        collect_notifications(
            &mut source,
            &config,
            nap_time,
            &mut async_count,
            &mut scheduled_count,
        );
        let elapsed = start.elapsed();

        assert!(
            elapsed < nap_time * 2,
            "dispatch cycle took {elapsed:?}, expected well under two nap_times ({:?})",
            nap_time * 2
        );
        assert_eq!(async_count, 2);
    }

    #[test]
    fn default_config_values() {
        let config = default_config();
        assert_eq!(config.pidfile, "/tmp/pg_dbms_job.pid");
        assert_eq!(config.job_queue_processes, 1024);
        assert_eq!(config.job_queue_interval, 0.1);
    }

    #[test]
    fn default_dbinfo_values() {
        let dbinfo = default_dbinfo();
        assert_eq!(dbinfo.port, 5432);
        assert!(dbinfo.host.is_empty());
    }

    #[test]
    fn default_config_debug_off() {
        let config = default_config();
        assert!(!config.debug);
    }

    #[test]
    fn default_config_logfile_empty() {
        let config = default_config();
        assert!(config.logfile.is_empty());
    }

    #[test]
    fn default_config_nap_time() {
        let config = default_config();
        assert!(config.nap_time > 0.0);
    }

    #[test]
    fn default_dbinfo_all_strings_empty() {
        let dbinfo = default_dbinfo();
        assert!(dbinfo.host.is_empty());
        assert!(dbinfo.database.is_empty());
        assert!(dbinfo.user.is_empty());
        assert!(dbinfo.passwd.is_empty());
    }

    #[test]
    fn default_config_pool_size() {
        let config = default_config();
        assert_eq!(config.pool_size, 100);
    }

    #[test]
    fn default_config_pool_size_capped_by_processes() {
        let config = default_config();
        // The main loop clamps the pool size to the configured number of
        // worker threads. Defaults guarantee pool_size <= job_queue_processes.
        let effective = config.pool_size.min(config.job_queue_processes);
        assert_eq!(effective, config.pool_size);
    }

    #[test]
    fn default_config_delays_positive_and_finite() {
        let config = default_config();
        assert!(config.startup_delay > 0.0 && config.startup_delay.is_finite());
        assert!(config.error_delay > 0.0 && config.error_delay.is_finite());
        assert!(config.job_queue_interval > 0.0 && config.job_queue_interval.is_finite());
    }
}
