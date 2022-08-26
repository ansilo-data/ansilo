use std::ptr;

use ansilo_core::sqlil::EntityId;
use ansilo_pg::fdw::proto::EntityDiscoverOptions;
use itertools::Itertools;
use pgx::pg_sys::{GetForeignServer, ImportForeignSchemaStmt, List, Oid};
use pgx::*;

use crate::fdw::common::connect_server;
use crate::sqlil::to_pg_type_name;
use crate::util::def_elem::parse_def_elems_to_hash_map;
use crate::util::string::{parse_to_owned_utf8_string, to_pg_cstr, to_pg_str_literal};

#[pg_guard]
pub unsafe extern "C" fn import_foreign_schema(
    stmt: *mut ImportForeignSchemaStmt,
    server_oid: Oid,
) -> *mut List {
    let server = GetForeignServer(server_oid);
    let mut ctx = connect_server(server_oid);

    // Parse the statement into EntityDiscoverOptions
    let remote_schema = parse_to_owned_utf8_string((*stmt).remote_schema).unwrap();
    let other = parse_def_elems_to_hash_map(PgList::from_pg((*stmt).options)).unwrap();
    let opts = EntityDiscoverOptions::new(remote_schema, other);

    // Retrieve the entity configurations from the remote data source
    let entities = ctx.discover_entities(opts).unwrap();

    // Construct the CREATE FOREIGN TABLE statements
    let stmts = entities
        .into_iter()
        .map(|e| {
            let table_name =
                parse_to_owned_utf8_string(pg_sys::quote_identifier(to_pg_cstr(&e.id).unwrap()))
                    .unwrap();
            let server_name =
                parse_to_owned_utf8_string(pg_sys::quote_identifier((*server).servername)).unwrap();
            let config = to_pg_str_literal(&serde_yaml::to_string(&e.source.options).unwrap());

            let cols = e
                .attributes
                .iter()
                .map(|a| {
                    let mut col = parse_to_owned_utf8_string(pg_sys::quote_identifier(
                        to_pg_cstr(&a.id).unwrap(),
                    ))
                    .unwrap();
                    col.push(' ');
                    col.push_str(&to_pg_type_name(&a.r#type).unwrap());

                    if !a.nullable {
                        col.push_str(" NOT NULL");
                    }

                    col
                })
                .join(",\n    ");

            format!(
                r#"CREATE FOREIGN TABLE {table_name} (
    {cols}
)
SERVER {server_name}
OPTIONS (
    __config {config}
)"#
            )
        })
        .collect::<Vec<_>>();

    // Construct a pg list and return
    let mut pg_stmts = PgList::<i8>::from_pg(ptr::null_mut());

    for stmt in stmts.into_iter() {
        let pg_str = to_pg_cstr(&stmt).unwrap();
        pg_stmts.push(pg_str);
    }

    pg_stmts.as_ptr()
}
