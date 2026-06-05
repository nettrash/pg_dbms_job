//! Build-time constants for pg_dbms_job.

/// Current scheduler version string, sourced from `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// How often (seconds) the main loop scans for and clears stale dispatch
/// markers left by workers that never finished. The eligibility *age* is the
/// configurable `stale_job_timeout`; this is only the check cadence, capped so
/// it is never coarser than the timeout itself.
pub const REAP_INTERVAL_SECS: f64 = 60.0;
/// Program name used in usage text and messaging, sourced from `Cargo.toml`.
pub const PROGRAM: &str = env!("CARGO_PKG_NAME");

/// Stack size (bytes) for each per-job worker thread. Workers only issue SQL
/// over a pooled connection and format short strings — the heavy PL/pgSQL work
/// happens inside the PostgreSQL backend, not here — so the 2 MiB default stack
/// is wasteful. Under a burst of async jobs the scheduler can hold one live
/// thread per in-flight job; a smaller stack keeps that fan-out from ballooning
/// RSS. 512 KiB leaves ample headroom for the call chain.
pub const WORKER_STACK_SIZE: usize = 512 * 1024;

/// Bound on the number of pending log lines buffered between the producer
/// threads and the single writer thread. The channel is intentionally bounded
/// so a logging burst (e.g. debug logging under heavy async load) applies
/// backpressure to producers instead of growing memory without limit.
pub const LOG_CHANNEL_CAPACITY: usize = 16384;

/// Maximum time (seconds) a worker waits to check out a pooled connection
/// before giving up. With the worker count capped at the pool size a checkout
/// should almost never block, so this only bounds the worst case (a stalled
/// backend) instead of letting a worker hold its stack for the r2d2 default of
/// 30 seconds.
pub const POOL_CONNECTION_TIMEOUT_SECS: u64 = 10;

#[cfg(test)]
mod tests {
    use super::{
        LOG_CHANNEL_CAPACITY, POOL_CONNECTION_TIMEOUT_SECS, PROGRAM, VERSION, WORKER_STACK_SIZE,
    };

    #[test]
    fn worker_stack_size_is_sane() {
        // Small enough to keep a burst of in-flight workers from ballooning
        // RSS (well under the 2 MiB default), but large enough to hold the
        // worker call chain comfortably.
        const {
            assert!(
                WORKER_STACK_SIZE >= 128 * 1024,
                "stack too small to be safe"
            );
            assert!(
                WORKER_STACK_SIZE < 2 * 1024 * 1024,
                "stack should be smaller than the 2 MiB default it replaces"
            );
        }
    }

    #[test]
    fn log_channel_capacity_is_bounded_and_nonzero() {
        // Bounded (so logging applies backpressure instead of growing without
        // limit) yet generous enough to absorb normal bursts without blocking.
        const {
            assert!(LOG_CHANNEL_CAPACITY > 0);
            assert!(LOG_CHANNEL_CAPACITY <= 1 << 20);
        }
    }

    #[test]
    fn pool_connection_timeout_is_shorter_than_r2d2_default() {
        // We deliberately undercut r2d2's 30s default so a worker can't hold a
        // stack waiting that long when the pool is momentarily saturated.
        const {
            assert!(POOL_CONNECTION_TIMEOUT_SECS > 0);
            assert!(POOL_CONNECTION_TIMEOUT_SECS < 30);
        }
    }

    #[test]
    fn constants_are_expected() {
        assert_eq!(PROGRAM, "pg_dbms_job");
        // Sanity check: version must be non-empty and match the Cargo.toml value.
        assert!(!VERSION.is_empty());
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn version_has_no_prerelease_suffix() {
        // Since 3.0.0 the scheduler and the SQL extension share one plain
        // semver (the historical "-rust" suffix was dropped). Guard against a
        // suffix or build-metadata creeping back in.
        assert!(
            !VERSION.contains('-') && !VERSION.contains('+'),
            "VERSION ({VERSION}) must be a plain MAJOR.MINOR.PATCH; check Cargo.toml"
        );
    }

    #[test]
    fn version_is_semver_numeric() {
        // VERSION must be a dotted numeric semver (e.g. "3.0.0"). Catches typos
        // like "3.0..0" or "v3.0.0".
        let parts: Vec<&str> = VERSION.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "version must be MAJOR.MINOR.PATCH, got {VERSION}"
        );
        for p in &parts {
            assert!(
                !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()),
                "version component {p:?} is not numeric"
            );
        }
    }

    #[test]
    fn program_matches_crate_name() {
        // PROGRAM is wired to CARGO_PKG_NAME — guard against accidental drift
        // (e.g. if someone renames the crate but expects the binary name to
        // stay the same in user-facing strings).
        assert_eq!(PROGRAM, env!("CARGO_PKG_NAME"));
    }
}
