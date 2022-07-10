use pgx::pg_sys::*;

pub unsafe extern "C" fn import_foreign_schema(
    stmt: *mut ImportForeignSchemaStmt,
    server_oid: Oid,
) -> *mut List {
    unimplemented!()
}
