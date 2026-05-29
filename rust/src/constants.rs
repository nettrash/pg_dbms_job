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

#[cfg(test)]
mod tests {
    use super::{PROGRAM, VERSION};

    #[test]
    fn constants_are_expected() {
        assert_eq!(PROGRAM, "pg_dbms_job");
        // Sanity check: version must be non-empty and match the Cargo.toml value.
        assert!(!VERSION.is_empty());
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn version_has_rust_suffix() {
        // Project convention: every Rust scheduler release tags the version
        // with "-rust" so it never collides with the SQL extension's version.
        assert!(
            VERSION.ends_with("-rust"),
            "VERSION ({VERSION}) must end with -rust; check Cargo.toml"
        );
    }

    #[test]
    fn version_has_semver_numeric_prefix() {
        // The portion before "-rust" must be a dotted numeric semver
        // (e.g. "1.5.11"). Catches typos like "1.5..11-rust" or "v1.5.11-rust".
        let prefix = VERSION.strip_suffix("-rust").expect("checked above");
        let parts: Vec<&str> = prefix.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "version prefix must be MAJOR.MINOR.PATCH, got {prefix}"
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
