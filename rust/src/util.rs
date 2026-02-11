//! Small utilities used across the scheduler.

/// Print a fatal message and terminate the process.
pub fn die(msg: &str) -> ! {
    eprintln!("{msg}");
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::die;
    use std::process::Command;

    #[test]
    fn die_exits_child() {
        if std::env::var("PG_DBMS_JOB_DIE_TEST").is_ok() {
            die("die test");
        }
    }

    #[test]
    fn die_exits_parent() {
        let exe = std::env::current_exe().expect("current_exe");
        let status = Command::new(exe)
            .env("PG_DBMS_JOB_DIE_TEST", "1")
            .arg("--exact")
            .arg("util::tests::die_exits_child")
            .arg("--nocapture")
            .status()
            .expect("spawn test binary");
        assert_eq!(status.code(), Some(1));
    }
}
