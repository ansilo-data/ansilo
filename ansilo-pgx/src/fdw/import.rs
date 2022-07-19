use pgx::*;
use pgx::pg_sys::{ImportForeignSchemaStmt, Oid, List};

#[pg_guard]
pub unsafe extern "C" fn import_foreign_schema(
    stmt: *mut ImportForeignSchemaStmt,
    server_oid: Oid,
) -> *mut List {
    unimplemented!()
}
