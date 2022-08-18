use std::{io::Write, os::unix::net::UnixStream, thread};

use ansilo_core::err::Result;
use ansilo_pg::PostgresConnectionPools;
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};

use crate::conf::pg_conf;

/// Handler for postgres-wire-protocol connections
pub struct PostgresConnectionHandler {
    pool: PostgresConnectionPools,
}

impl PostgresConnectionHandler {
    pub fn new(pool: PostgresConnectionPools) -> Self {
        Self { pool }
    }
}

impl ConnectionHandler for PostgresConnectionHandler {
    fn handle(&self, mut client: Box<dyn IOStream>) -> Result<()> {
        let sock_path = pg_conf().pg_socket_path();
        let mut con = UnixStream::connect(sock_path)?;
        con.set_nonblocking(true)?;

        let mut buf = [0u8; 1024];
        loop {
            let read = client.read(&mut buf)?;

            if read == 0 {
                break;
            }

            con.write_all(&buf[..read])?;
        }

        Ok(())
    }
}
