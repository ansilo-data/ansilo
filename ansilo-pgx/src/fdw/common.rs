use ansilo_core::err::{bail, Context, Result};
use ansilo_pg::fdw::proto::AuthDataSource;
use cstr::cstr;
use std::path::PathBuf;

use pgx::{
    pg_sys::{defGetString, strcmp, DefElem, GetForeignServer, GetForeignTable, Oid},
    *,
};

use crate::{
    sqlil::parse_entity_version_id_from_foreign_table, util::string::parse_to_owned_utf8_string,
};

use super::ctx::FdwContext;

#[derive(Debug)]
struct ServerOptions {
    /// The data source ID of this server
    data_source: String,
    /// The path of the socket
    socket: PathBuf,
}

impl ServerOptions {
    unsafe fn parse(opts: PgList<DefElem>) -> Result<Self> {
        let mut data_source = None;
        let mut socket = None;

        for opt in opts.iter_ptr() {
            if strcmp((*opt).defname, cstr!("data_source").as_ptr()) == 0 {
                let _ = data_source.insert(def_get_owned_utf8_string(opt)?);
            }

            if strcmp((*opt).defname, cstr!("socket").as_ptr()) == 0 {
                let _ = socket.insert(def_get_owned_utf8_string(opt)?);
            }
        }

        let data_source =
            data_source.context("Server option 'data_source' must be defined on foreign table")?;
        let socket =
            socket.context("Server option 'socket_path' must be defined on foreign table")?;
        let socket = PathBuf::from(socket);

        Ok(Self {
            data_source,
            socket,
        })
    }
}

/// Connects to ansilo using the appropriate data source from the supplied RelOptInfo
pub unsafe fn connect(foreign_table_oid: Oid) -> PgBox<FdwContext> {
    let table = GetForeignTable(foreign_table_oid);
    let entity = parse_entity_version_id_from_foreign_table(foreign_table_oid).unwrap();
    let server = GetForeignServer((*table).serverid);

    let opts = ServerOptions::parse(PgList::<DefElem>::from_pg((*server).options))
        .expect("Failed to parse server options");

    let auth = AuthDataSource::new(current_auth_token(), opts.data_source);

    let mut ctx = FdwContext::new(&auth.data_source_id, entity);
    ctx.connect(&opts.socket, auth).expect("Failed to connect");

    PgBox::new(ctx).into_pg_boxed()
}

unsafe fn def_get_owned_utf8_string(opt: *mut DefElem) -> Result<String> {
    if opt.is_null() {
        bail!("Failed to parse option as string");
    }

    let ptr = defGetString(opt);

    parse_to_owned_utf8_string(ptr)
}

fn current_auth_token() -> String {
    // TODO: implement
    "TOKEN".to_string()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::pg_sys::{makeDefElem, makeString};

    use super::*;

    #[pg_test]
    fn test_fdw_common_server_options_parse() {
        unsafe {
            let mut opts = PgList::<DefElem>::new();
            opts.push(makeDefElem(
                cstr!("data_source").as_ptr() as _,
                makeString(cstr!("data_source_id").as_ptr() as _) as _,
                0,
            ));
            opts.push(makeDefElem(
                cstr!("socket").as_ptr() as _,
                makeString(cstr!("/some/path.sock").as_ptr() as _) as _,
                0,
            ));

            let parsed = ServerOptions::parse(opts).unwrap();

            assert_eq!(parsed.data_source, "data_source_id");
            assert_eq!(parsed.socket, PathBuf::from("/some/path.sock"));
        }
    }

    #[pg_test]
    fn test_fdw_common_server_options_parse_missing_data_source() {
        unsafe {
            let mut opts = PgList::<DefElem>::new();
            opts.push(makeDefElem(
                cstr!("socket").as_ptr() as _,
                makeString(cstr!("/some/path.sock").as_ptr() as _) as _,
                0,
            ));

            ServerOptions::parse(opts).unwrap_err();
        }
    }

    #[pg_test]
    fn test_fdw_common_server_options_parse_missing_socket() {
        unsafe {
            let mut opts = PgList::<DefElem>::new();
            opts.push(makeDefElem(
                cstr!("data_source").as_ptr() as _,
                makeString(cstr!("data_source_id").as_ptr() as _) as _,
                0,
            ));

            ServerOptions::parse(opts).unwrap_err();
        }
    }

    #[pg_test]
    fn test_fdw_common_server_options_parse_no_options() {
        unsafe {
            let opts = PgList::<DefElem>::new();

            ServerOptions::parse(opts).unwrap_err();
        }
    }
}
