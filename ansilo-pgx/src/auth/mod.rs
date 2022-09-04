pub(crate) mod ctx;
pub(crate) mod get_auth_context;
pub(crate) mod reset_auth_context;
pub(crate) mod set_auth_context;

use pgx::*;

extension_sql!(
    r#"
        CREATE SCHEMA IF NOT EXISTS __ansilo_auth;
    "#,
    name = "ansilo_auth_schema"
);