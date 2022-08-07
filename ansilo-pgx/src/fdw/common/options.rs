use ansilo_core::err::{Context, Result};
use cstr::cstr;
use std::path::PathBuf;

use pgx::{
    pg_sys::{strcmp, DefElem},
    *,
};

use crate::util::def_elem::def_get_owned_utf8_string;

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
