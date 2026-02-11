pub const VERSION: &str = "1.5";
pub const PROGRAM: &str = "pg_dbms_job";

#[cfg(test)]
mod tests {
	use super::{PROGRAM, VERSION};

	#[test]
	fn constants_are_expected() {
		assert_eq!(PROGRAM, "pg_dbms_job");
		assert_eq!(VERSION, "1.5");
	}
}
