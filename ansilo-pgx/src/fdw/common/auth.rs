use ansilo_pg::fdw::proto::AuthDataSource;

use pgx::{
    pg_sys::{DefElem, GetForeignServer, GetForeignTable, Oid},
    *,
};

use crate::{
    sqlil::parse_entity_version_id_from_foreign_table, fdw::ctx::FdwContext
};

use super::ServerOptions;

pub(crate) fn current_auth_token() -> String {
    // TODO: implement
    "TOKEN".to_string()
}
