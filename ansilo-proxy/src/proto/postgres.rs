use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
};

use ansilo_core::err::{bail, Result};

use crate::{conf::ProxyConf, peekable::Peekable, stream::Stream};

use super::Protocol;

pub struct PostgresProtocol {
    conf: &'static ProxyConf,
}

impl PostgresProtocol {
    pub fn new(conf: &'static ProxyConf) -> Self {
        Self { conf }
    }
}

const PG_SSL_REQUEST: [u8; 8] = [0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f];
const PG_PROTOCOL_VERSION: [u8; 4] = [0x00, 0x03, 0x00, 0x00];

/// Postgres protocol proxy.
///
/// @see https://www.postgresql.org/docs/current/protocol-message-formats.html
///
/// For postgres, TLS is handled at the application layer
impl Protocol<TcpStream> for PostgresProtocol {
    fn matches(&self, con: &mut Peekable<TcpStream>) -> Result<bool> {
        // First, check if this is a SSLRequest
        let mut buf = [0u8; 8];
        con.peek(&mut buf[..])?;

        if buf == PG_SSL_REQUEST {
            return Ok(true);
        }

        // Second, check if this is a StartupRequest
        if &buf[4..] == &PG_PROTOCOL_VERSION {
            return Ok(true);
        }

        Ok(false)
    }

    fn handle(&mut self, mut con: Peekable<TcpStream>) -> Result<()> {
        // Since postgres handles TLS at the application logic we have
        // to handle this at that this point.
        // @see https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.12

        if self.conf.tls.is_some() {
            // If TLS is enabled, lets validate we received an SSLRequest packet
            // We intentionally consume it from the stream, so when we initiate TLS
            // the stream will start with the clients next message (ClientHello)
            let mut buf = [0u8; 8];
            con.read_exact(&mut buf[..])?;

            if buf != PG_SSL_REQUEST {
                // We did not receive the expected SSLRequest, reply with an ErrorResponse and close the connection
                con.write_all(&[
                    b'E', // Byte1('E') (type)
                    0x00, 0x00, 0x00, 0x22, // Int32 (length)
                    b'S', // Byte1 (severity)
                    b'S', b'S', b'L', b' ', b'i', b's', b' ', b'r', b'e', b'q', b'u', b'i',
                    b'r', // String (message)
                    b'e', b'd', // Byte1 (terminator)
                    0,
                ])?;
                bail!("Postgres client tried to connect without TLS on TLS-enabled server");
            }

            // Confirm server is willing to accept TLS
            con.write_all(b"S")?;

            // Process TLS
            let config = Arc::clone(&self.conf.tls.as_ref().unwrap().server_config);
            let tls_state = rustls::ServerConnection::new(config)?;
            let con = rustls::StreamOwned::new(tls_state, con);

            // At this point the client should send StartupMessage
            self.conf.handlers.postgres.handle(Box::new(Stream(con)))
        } else {
            // If TLS is disabled, reply N to SSLRequest, if it was received
            // We peek first as we do not want to accidentally consume StartupMessage
            // from the underlying stream
            let mut buf = [0u8; 8];
            con.peek(&mut buf[..])?;

            if buf == PG_SSL_REQUEST {
                // Confirm server is unwilling to accept TLS and consume SSLRequest
                con.read_exact(&mut buf)?;
                con.write_all(b"N")?;
            }

            // At this point the client should send StartupMessage
            self.conf.handlers.postgres.handle(Box::new(Stream(con)))
        }
    }
}
