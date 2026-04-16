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
use crate::constants::VERSION;
use crate::db::JobPool;
use crate::db::{ConnectError, connect_db, create_job_pool};
use crate::jobs::{get_async_jobs, get_scheduled_jobs, spawn_job};
use crate::logging::{dprint, shutdown_logger};
use crate::model::{Config, DbInfo, Job, JobKind};
use crate::process::{daemonize, reap_children, signal_handling, wait_all_children, write_pidfile};
use crate::util::die;
use fallible_iterator::FallibleIterator;
use nix::sys::signal::Signal;
use postgres::Client;
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

    if Path::new(&config.pidfile).exists() {
        die(&format!(
            "FATAL: pid file already exists at {}, does another pg_dbms_job process is running?",
            config.pidfile
        ));
    }

    if !args.single {
        daemonize(&config);
    }

    write_pidfile(&config.pidfile);

    let terminate_flag = Arc::new(AtomicBool::new(false));
    let reload_flag = Arc::new(AtomicBool::new(false));

    flag::register(SIGINT, Arc::clone(&terminate_flag)).expect("register SIGINT");
    flag::register(SIGTERM, Arc::clone(&terminate_flag)).expect("register SIGTERM");
    flag::register(SIGHUP, Arc::clone(&reload_flag)).expect("register SIGHUP");

    dprint(&config, "LOG", "Entering main loop.");

    let mut dbh: Option<Client> = None;
    let mut job_pool: Option<Arc<JobPool>> = None;
    let mut running_workers: HashMap<u64, JoinHandle<()>> = HashMap::new();
    let mut next_worker_id: u64 = 1;
    let mut scheduled_jobs: HashMap<i64, Job> = HashMap::new();
    let mut async_jobs: HashMap<i64, Job> = HashMap::new();
    let mut previous_async_exec = Instant::now();
    let mut previous_scheduled_exec = Instant::now();
    let mut startup = true;
    let mut config_invalidated = false;
    let mut in_recovery_logged = false;

    while !terminate_flag.load(Ordering::Relaxed) {
        reap_children(&mut running_workers);

        if reload_flag.swap(false, Ordering::Relaxed) {
            dprint(&config, "LOG", "Received reload signal HUP.");
            let old_pidfile = config.pidfile.clone();
            read_config(&args.config_file, &mut config, &mut dbinfo, true);
            if old_pidfile != config.pidfile {
                if let Err(err) = std::fs::rename(&old_pidfile, &config.pidfile) {
                    dlog!(
                        &config,
                        "ERROR",
                        "can't change path to pid keeping old one {}, {}",
                        old_pidfile,
                        err
                    );
                    config.pidfile = old_pidfile;
                } else {
                    dlog!(
                        &config,
                        "LOG",
                        "path to pid file has changed, rename {} into {}",
                        old_pidfile,
                        config.pidfile
                    );
                }
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
            match create_job_pool(&dbinfo, config.job_queue_processes as u32) {
                Ok(pool) => {
                    dlog!(
                        &config,
                        "LOG",
                        "Connection pool created with max size {}",
                        config.job_queue_processes
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
            let mut iter = notifications.iter();
            loop {
                match iter.next() {
                    Ok(Some(notification)) => {
                        dlog!(
                            &config,
                            "DEBUG",
                            "Received notification: ({}, {}, {})",
                            notification.channel(),
                            notification.process_id(),
                            notification.payload()
                        );
                        if notification.channel() == "dbms_job_async_notify" {
                            async_count += 1;
                        } else if notification.channel() == "dbms_job_scheduled_notify" {
                            scheduled_count += 1;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        dlog!(&config, "ERROR", "notification error: {err}");
                        break;
                    }
                }
            }
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
                async_jobs = get_async_jobs(client, &config);
            }
            previous_async_exec = Instant::now();
        }

        if scheduled_count > 0 || startup {
            if let Some(client) = dbh.as_mut() {
                scheduled_jobs = get_scheduled_jobs(client, &config, &mut config_invalidated);
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
                &mut running_workers,
                &mut next_worker_id,
            );
        }

        if args.single {
            break;
        }

        thread::sleep(Duration::from_secs_f64(config.nap_time));
    }

    wait_all_children(&mut running_workers);
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

/// Default scheduler configuration values.
fn default_config() -> Config {
    Config {
        debug: false,
        pidfile: "/tmp/pg_dbms_job.pid".to_string(),
        logfile: String::new(),
        log_truncate_on_rotation: false,
        job_queue_interval: 0.1,
        job_queue_processes: 1024,
        nap_time: 0.1,
        startup_delay: 3.0,
        error_delay: 0.5,
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
    use super::{default_config, default_dbinfo};

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
}
