mod fdw;
pub mod sqlil;
pub mod util;

use pgx::{
    pg_sys::{self},
    *,
};

pg_module_magic!();

extension_sql!(
    r#"
        -- Register our FDW
        CREATE FUNCTION "ansilo_fdw_handler_typed"() RETURNS fdw_handler STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'ansilo_fdw_handler_wrapper';
        CREATE FOREIGN DATA WRAPPER ansilo_fdw HANDLER ansilo_fdw_handler_typed;
"#,
    name = "ansilo_fdw"
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
