# Rust Port of pg_dbms_job

This folder contains a Rust implementation of the `pg_dbms_job` scheduler daemon. It mirrors the Perl daemon logic (config parsing, LISTEN/NOTIFY, job polling, fork-per-job execution, and run history writes), but is built as a standalone Rust crate.

## Build

```bash
cargo build --manifest-path rust/Cargo.toml
```

## Run

Foreground single-pass (no daemonize):

```bash
cargo run --manifest-path rust/Cargo.toml -- --single
```

Daemon mode (forks and detaches):

```bash
cargo run --manifest-path rust/Cargo.toml
```

## Common Flags

- `-c, --config <file>`: path to config file (default: `/etc/pg_dbms_job/pg_dbms_job.conf`)
- `-d, --debug`: enable debug logging
- `-k, --kill`: stop current daemon gracefully
- `-m, --immediate`: stop daemon immediately
- `-r, --reload`: reload config and job definitions
- `-s, --single`: run one loop in foreground and exit
- `-v, --version`: show version

## Configuration

The Rust port reads the same configuration file format as the Perl daemon, including:

- `host`, `port`, `database`, `user`, `passwd`
- `pidfile`, `logfile`, `debug`
- `job_queue_interval`, `job_queue_processes`, `nap_time`
- `log_truncate_on_rotation`

## Notes

- The Rust implementation uses `postgres` crate and `nix` for forking and signals.
- The scheduler uses `LISTEN` on `dbms_job_async_notify` and `dbms_job_scheduled_notify`.
- Logs follow the same timestamped format as the Perl daemon.
