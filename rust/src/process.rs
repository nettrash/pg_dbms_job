use crate::constants::PROGRAM;
use crate::logging::dprint;
use crate::model::Config;
use crate::util::die;
use nix::sys::signal::{kill, Signal};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{fork, setsid, ForkResult, Pid};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::process::{self, Command};
use std::thread;
use std::time::Duration;

pub fn daemonize(config: &Config) {
    match unsafe { fork() } {
        Ok(ForkResult::Parent { .. }) => process::exit(0),
        Ok(ForkResult::Child) => {}
        Err(err) => die(&format!("FATAL: Couldn't fork: {err}")),
    }

    if let Err(err) = setsid() {
        die(&format!("Can't detach: {err}"));
    }
    dprint(
        config,
        "DEBUG",
        &format!("Detach from terminal with pid: {}", process::id()),
    );

    let _ = OpenOptions::new()
        .read(true)
        .open("/dev/null")
        .and_then(|f| nix::unistd::dup2(f.as_raw_fd(), 0).map_err(io::Error::other));
    let _ = OpenOptions::new()
        .write(true)
        .open("/dev/null")
        .and_then(|f| nix::unistd::dup2(f.as_raw_fd(), 1).map_err(io::Error::other));
}

pub fn write_pidfile(path: &str) {
    let mut file = File::create(path).unwrap_or_else(|err| {
        die(&format!("FATAL: can't create pid file {}, {}", path, err));
    });
    let _ = writeln!(file, "{}", process::id());
}

pub fn signal_handling(pidfile: &str, sig: Signal) {
    let pid = read_pid_from_file(pidfile).or_else(read_pid_from_ps);
    let pid = match pid {
        Some(pid) => pid,
        None => die(&format!(
            "ERROR: can't find a pid to send SIG{:?}, is {PROGRAM} running?",
            sig
        )),
    };
    if let Err(err) = kill(Pid::from_raw(pid), sig) {
        eprintln!("FATAL: failed to execute: {err}");
    } else {
        println!("OK: {PROGRAM} exited");
    }
    process::exit(0);
}

fn read_pid_from_file(path: &str) -> Option<i32> {
    let mut buf = String::new();
    File::open(path).ok()?.read_to_string(&mut buf).ok()?;
    buf.trim().parse::<i32>().ok()
}

fn read_pid_from_ps() -> Option<i32> {
    let output = Command::new("ps")
        .args(["h", "-opid", "-Cpg_dbms_job"])
        .output()
        .ok()?;
    let pid_str = String::from_utf8_lossy(&output.stdout);
    pid_str.split_whitespace().next()?.parse::<i32>().ok()
}

pub fn reap_children(running: &mut std::collections::HashSet<Pid>) {
    loop {
        match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(pid, _)) => {
                running.remove(&pid);
            }
            Ok(WaitStatus::Signaled(pid, _, _)) => {
                running.remove(&pid);
            }
            Ok(WaitStatus::StillAlive) => break,
            Err(_) => break,
            _ => {}
        }
    }
}

pub fn wait_all_children(running: &mut std::collections::HashSet<Pid>) {
    while !running.is_empty() {
        reap_children(running);
        thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::write_pidfile;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn write_pidfile_creates_file() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pg_dbms_job_pid_{now}.pid"));
        write_pidfile(path.to_str().unwrap());
        let content = fs::read_to_string(&path).expect("read pidfile");
        assert_eq!(content.trim(), std::process::id().to_string());
        let _ = fs::remove_file(path);
    }
}
