use std::ptr;

use ansilo_core::sqlil::EntityId;
use ansilo_pg::fdw::proto::EntityDiscoverOptions;
use ansilo_util_pg::query::{pg_quote_identifier, pg_str_literal};
use itertools::Itertools;
use pgx::pg_sys::{GetForeignServer, ImportForeignSchemaStmt, List, Oid};
use pgx::*;

use crate::fdw::common::connect_server;
use crate::sqlil::to_pg_type_name;
use crate::util::def_elem::parse_def_elems_to_hash_map;
use crate::util::string::{parse_to_owned_utf8_string, to_pg_cstr};

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
    // We also support a "table_prefix" option which will prefix all table names
    // with the supplied string
    let prefix = other.get("table_prefix").cloned();
    let opts = EntityDiscoverOptions::new(remote_schema, other);

    // Retrieve the entity configurations from the remote data source
    let entities = ctx.discover_entities(opts).unwrap();

    // Construct the CREATE FOREIGN TABLE statements
    let stmts = entities
        .into_iter()
        .filter(|e| {
            if e.attributes.is_empty() {
                warning!("Could not import table '{}': no columns are defined", e.id);
                return false;
            }

            return true;
        })
        .map(|e| {
            let table_name = pg_quote_identifier(&if let Some(pfx) = prefix.as_ref() {
                format!("{pfx}{}", e.id)
            } else {
                e.id.clone()
            });
            let entity_id = pg_str_literal(&e.id);
            let server_name =
                pg_quote_identifier(&parse_to_owned_utf8_string((*server).servername).unwrap());
            let config = pg_str_literal(&serde_yaml::to_string(&e).unwrap());

            let cols = e
                .attributes
                .iter()
                .map(|a| {
                    let mut col = pg_quote_identifier(&a.id);
                    col.push(' ');
                    col.push_str(&to_pg_type_name(&a.r#type).unwrap());

                    if a.primary_key {
                        col.push_str(" OPTIONS (primary_key 'true')");
                    }

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
    entity_id {entity_id},
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
