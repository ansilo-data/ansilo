use ansilo_core::err::{Context, Result};
use cstr::cstr;
use std::{env, path::PathBuf};

use pgx::{
    pg_sys::{strcmp, DefElem, GetForeignTable},
    *,
};

use crate::util::def_elem::{def_get_owned_utf8_string, parse_def_elems_to_hash_map};

#[derive(Debug)]
pub struct ServerOptions {
    /// The data source ID of this server
    pub data_source: String,
    /// The path of the socket
    pub socket: PathBuf,
}

impl ServerOptions {
    pub unsafe fn parse(opts: PgList<DefElem>) -> Result<Self> {
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
            data_source.context("Server option 'data_source' must be defined on CREATE SERVER")?;
        let socket = socket
            .or(env::var("ANSILO_PG_FDW_SOCKET_PATH").ok())
            .context(
            "Server option 'socket_path' must be defined on CREATE SERVER or provided through env",
        )?;
        let socket = PathBuf::from(socket);

        Ok(Self {
            data_source,
            socket,
        })
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct TableOptions {
    /// The user-defined function to call before SELECT queries
    pub before_select: Option<String>,
    /// The user-defined function to call before INSERT queries
    pub before_insert: Option<String>,
    /// The user-defined function to call before INSERT queries
    pub before_update: Option<String>,
    /// The user-defined function to call before DELETE queries
    pub before_delete: Option<String>,
    /// The user-defined function to call before INSERT/UPDATE/DELETE queries
    pub before_modify: Option<String>,
    /// Max batch size for inserts
    pub max_batch_size: Option<u32>,
}

impl TableOptions {
    pub unsafe fn parse(opts: PgList<DefElem>) -> Result<Self> {
        let opts = parse_def_elems_to_hash_map(opts)?;

        Ok(Self {
            before_select: opts.get("before_select").cloned(),
            before_insert: opts.get("before_insert").cloned(),
            before_update: opts.get("before_update").cloned(),
            before_delete: opts.get("before_delete").cloned(),
            before_modify: opts.get("before_modify").cloned(),
            max_batch_size: opts
                .get("max_batch_size")
                .and_then(|v| v.parse::<u32>().ok()),
        })
    }

    pub unsafe fn from_id(oid: pg_sys::Oid) -> Result<Self> {
        let table = GetForeignTable(oid);
        Self::parse(PgList::from_pg((*table).options))
    }
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

    #[pg_test]
    fn test_fdw_common_table_options_parse_no_options() {
        unsafe {
            let opts = PgList::<DefElem>::new();

            assert_eq!(
                TableOptions::parse(opts).unwrap(),
                TableOptions {
                    before_select: None,
                    before_insert: None,
                    before_update: None,
                    before_delete: None,
                    before_modify: None,
                    max_batch_size: None,
                }
            );
        }
    }

    #[pg_test]
    fn test_fdw_common_table_options_parse_all() {
        unsafe {
            let mut opts = PgList::<DefElem>::new();
            opts.push(makeDefElem(
                cstr!("before_select").as_ptr() as _,
                makeString(cstr!("select_func").as_ptr() as _) as _,
                0,
            ));
            opts.push(makeDefElem(
                cstr!("before_insert").as_ptr() as _,
                makeString(cstr!("insert_func").as_ptr() as _) as _,
                0,
            ));
            opts.push(makeDefElem(
                cstr!("before_update").as_ptr() as _,
                makeString(cstr!("update_func").as_ptr() as _) as _,
                0,
            ));
            opts.push(makeDefElem(
                cstr!("before_delete").as_ptr() as _,
                makeString(cstr!("delete_func").as_ptr() as _) as _,
                0,
            ));
            opts.push(makeDefElem(
                cstr!("before_modify").as_ptr() as _,
                makeString(cstr!("modify_func").as_ptr() as _) as _,
                0,
            ));
            opts.push(makeDefElem(
                cstr!("max_batch_size").as_ptr() as _,
                makeString(cstr!("123").as_ptr() as _) as _,
                0,
            ));

            assert_eq!(
                TableOptions::parse(opts).unwrap(),
                TableOptions {
                    before_select: Some("select_func".into()),
                    before_insert: Some("insert_func".into()),
                    before_update: Some("update_func".into()),
                    before_delete: Some("delete_func".into()),
                    before_modify: Some("modify_func".into()),
                    max_batch_size: Some(123)
                }
            );
        }
    }
}
