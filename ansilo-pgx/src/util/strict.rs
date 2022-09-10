//! We provide a function "strict" which simply errors
//! out if the provided flag is false.
//! The purpose of this is ensuring that RLS policies error
//! out if the check fails.

use pgx::*;

extension_sql!(
    r#"
CREATE FUNCTION "strict" (
	"flag" bool,
	"message" text
) RETURNS bool
LANGUAGE c /* Rust */
COST 1
AS 'MODULE_PATHNAME', 'strict_wrapper';

CREATE FUNCTION "strict" (
	"flag" bool
) RETURNS bool
LANGUAGE c /* Rust */
COST 1
AS 'MODULE_PATHNAME', 'strict_wrapper';
"#,
    name = "string_function"
);

#[pg_extern(sql = "")]
fn strict(flag: bool, message: Option<String>) -> bool {
    if flag {
        return true;
    }

    if let Some(message) = message {
        error!("Strict check failed: {message}");
    } else {
        error!("Strict check failed");
    }
}
