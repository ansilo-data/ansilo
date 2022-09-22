use pgx::{
    pg_sys::{self},
    *,
};

mod auth;
mod fdw;
mod rq;
mod sqlil;
mod util;

pg_module_magic!();

// Register our FDW
extension_sql!(
    r#"
        CREATE FUNCTION "ansilo_fdw_handler_typed"() RETURNS fdw_handler STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'ansilo_fdw_handler_wrapper';
        CREATE FOREIGN DATA WRAPPER ansilo_fdw HANDLER ansilo_fdw_handler_typed;
"#,
    name = "ansilo_fdw"
);

// Create a schema for private objects we dont want our
// users to rely on.
extension_sql!(
    r#"
        CREATE SCHEMA IF NOT EXISTS __ansilo_private;
    "#,
    name = "ansilo_private_schema"
);

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    ansilo_logging::init();
}

/// This can be used to sense check the extension is running
/// ```sql
/// SELECT hello_ansilo();
/// ```
#[pg_extern]
fn hello_ansilo() -> &'static str {
    "Hello from Ansilo"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_hello_ansilopg() {
        assert_eq!("Hello from Ansilo", crate::hello_ansilo());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
