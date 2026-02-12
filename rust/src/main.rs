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
use crate::db::connect_db;
use crate::jobs::{get_async_jobs, get_scheduled_jobs, spawn_job};
use crate::logging::dprint;
use crate::model::{Config, DbInfo, Job, JobKind};
use crate::process::{daemonize, reap_children, signal_handling, wait_all_children, write_pidfile};
use crate::util::die;
use fallible_iterator::FallibleIterator;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use postgres::Client;
use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGTERM};
use signal_hook::flag;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
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
    let mut running_pids: HashSet<Pid> = HashSet::new();
    let mut scheduled_jobs: HashMap<i64, Job> = HashMap::new();
    let mut async_jobs: HashMap<i64, Job> = HashMap::new();
    let mut previous_async_exec = Instant::now();
    let mut previous_scheduled_exec = Instant::now();
    let mut startup = true;
    let mut config_invalidated = false;

    while !terminate_flag.load(Ordering::Relaxed) {
        reap_children(&mut running_pids);

        if reload_flag.swap(false, Ordering::Relaxed) {
            dprint(&config, "LOG", "Received reload signal HUP.");
            let old_pidfile = config.pidfile.clone();
            read_config(&args.config_file, &mut config, &mut dbinfo, true);
            if old_pidfile != config.pidfile {
                if let Err(err) = std::fs::rename(&old_pidfile, &config.pidfile) {
                    dprint(
                        &config,
                        "ERROR",
                        &format!(
                            "can't change path to pid keeping old one {}, {}",
                            old_pidfile, err
                        ),
                    );
                    config.pidfile = old_pidfile;
                } else {
                    dprint(
                        &config,
                        "LOG",
                        &format!(
                            "path to pid file has changed, rename {} into {}",
                            old_pidfile, config.pidfile
                        ),
                    );
                }
            }
            config_invalidated = true;
        }

        if config_invalidated {
            let _ = dbh.take();
        }

        if dbh.is_none() {
            match connect_db(&dbinfo, &config) {
                Ok(client) => dbh = Some(client),
                Err(err) => {
                    dprint(&config, "ERROR", &err.to_string());
                    thread::sleep(Duration::from_secs(3));
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
                        dprint(
                            &config,
                            "DEBUG",
                            &format!(
                                "Received notification: ({}, {}, {})",
                                notification.channel(),
                                notification.process_id(),
                                notification.payload()
                            ),
                        );
                        if notification.channel() == "dbms_job_async_notify" {
                            async_count += 1;
                        } else if notification.channel() == "dbms_job_scheduled_notify" {
                            scheduled_count += 1;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        dprint(&config, "ERROR", &format!("notification error: {err}"));
                        break;
                    }
                }
            }
        } else {
            thread::sleep(Duration::from_secs(3));
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
                thread::sleep(Duration::from_secs(3));
                startup = true;
                continue;
            }
        }

        config_invalidated = false;
        startup = false;

        let scheduled_keys: Vec<i64> = scheduled_jobs.keys().copied().collect();
        for jobid in scheduled_keys {
            while running_pids.len() >= config.job_queue_processes {
                dprint(
                    &config,
                    "WARNING",
                    &format!(
                        "max job queue size is reached ({}) waiting the end of an other job",
                        config.job_queue_processes
                    ),
                );
                thread::sleep(Duration::from_secs(1));
                reap_children(&mut running_pids);
            }
            if let Some(job) = scheduled_jobs.get(&jobid).cloned() {
                spawn_job(JobKind::Scheduled, job, &dbinfo, &config, &mut running_pids);
            }
        }
        scheduled_jobs.clear();

        let async_keys: Vec<i64> = async_jobs.keys().copied().collect();
        for jobid in async_keys {
            while running_pids.len() >= config.job_queue_processes {
                dprint(
                    &config,
                    "WARNING",
                    &format!(
                        "max job queue size is reached ({}) waiting the end of an other job",
                        config.job_queue_processes
                    ),
                );
                thread::sleep(Duration::from_secs(1));
                reap_children(&mut running_pids);
            }
            if let Some(job) = async_jobs.get(&jobid).cloned() {
                spawn_job(JobKind::Async, job, &dbinfo, &config, &mut running_pids);
            }
        }
        async_jobs.clear();

        if args.single {
            break;
        }

        thread::sleep(Duration::from_secs_f64(config.nap_time));
    }

    wait_all_children(&mut running_pids);
    if Path::new(&config.pidfile).exists()
        && let Err(err) = std::fs::remove_file(&config.pidfile)
    {
        dprint(
            &config,
            "ERROR",
            &format!("Unable to remove pid file {}, {}", config.pidfile, err),
        );
    }

    dprint(&config, "LOG", "pg_dbms_job scheduler stopped.");
}

/// Default scheduler configuration values.
fn default_config() -> Config {
    Config {
        debug: false,
        pidfile: "/tmp/pg_dbms_job.pid".to_string(),
        logfile: "".to_string(),
        log_truncate_on_rotation: false,
        job_queue_interval: 0.5,
        job_queue_processes: 100000,
        nap_time: 0.1,
    }
}

/// Default database connection settings.
fn default_dbinfo() -> DbInfo {
    DbInfo {
        host: "".to_string(),
        database: "".to_string(),
        user: "".to_string(),
        passwd: "".to_string(),
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
        assert_eq!(config.job_queue_processes, 100000);
        assert_eq!(config.job_queue_interval, 0.5);
    }

    #[test]
    fn default_dbinfo_values() {
        let dbinfo = default_dbinfo();
        assert_eq!(dbinfo.port, 5432);
        assert!(dbinfo.host.is_empty());
    }
}
