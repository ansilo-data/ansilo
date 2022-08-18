use std::{
    io::{Read, Write},
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
const PG_SSL_REQUIRED_ERROR: [u8; 19] = [
    b'E', // Byte1('E') (type)
    0x00, 0x00, 0x00, 0x19, // Int32 (length)
    b'S', // Byte1 (severity)
    b'S', b'S', b'L', b' ', b'r', b'e', b'q', b'u', b'i', b'r', b'e',
    b'd', // String (message)
    0,    // Byte1 (terminator)
];

/// Postgres protocol proxy.
///
/// @see https://www.postgresql.org/docs/current/protocol-message-formats.html
///
/// For postgres, TLS is handled at the application layer
impl<S: Read + Write + Send + 'static> Protocol<S> for PostgresProtocol {
    fn matches(&self, con: &mut Peekable<S>) -> Result<bool> {
        // First, check if this is a SSLRequest
        let mut buf = [0u8; 8];
        if let Err(_) = con.peek(&mut buf[..]) {
            return Ok(false);
        }

        if buf == PG_SSL_REQUEST {
            return Ok(true);
        }

        // Second, check if this is a StartupRequest
        if &buf[4..] == &PG_PROTOCOL_VERSION {
            return Ok(true);
        }

        Ok(false)
    }

    fn handle(&mut self, mut con: Peekable<S>) -> Result<()> {
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
                con.write_all(&PG_SSL_REQUIRED_ERROR)?;
                bail!("Postgres client tried to connect without TLS on TLS-enabled server");
            }

            // Confirm server is willing to accept TLS
            con.write_all(b"S")?;
            con.flush()?;

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
                con.flush()?;
            }

            // At this point the client should send StartupMessage
            self.conf.handlers.postgres.handle(Box::new(Stream(con)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{os::unix::net::UnixStream, thread};

    use rustls::ServerConnection;

    use crate::test::{
        create_socket_pair, mock_config_no_tls, mock_config_tls, mock_tls_client_config,
        MockConnectionHandler,
    };

    use super::*;

    #[test]
    fn test_proto_postgres_matches() {
        let proto = PostgresProtocol::new(mock_config_no_tls());

        assert_eq!(proto.matches(&mut vec![0u8].into()).unwrap(), false);
        assert_eq!(proto.matches(&mut b"abc".to_vec().into()).unwrap(), false);
        assert_eq!(
            proto
                .matches(&mut b"GET / HTTP/1.1".to_vec().into())
                .unwrap(),
            false
        );
        assert_eq!(
            proto
                .matches(&mut b"POST /abc HTTP/1.1".to_vec().into())
                .unwrap(),
            false
        );
        assert_eq!(
            proto
                .matches(&mut b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec().into())
                .unwrap(),
            false
        );
        assert_eq!(
            proto.matches(&mut PG_SSL_REQUEST.to_vec().into()).unwrap(),
            true
        );
        assert_eq!(
            proto
                .matches(
                    // StartupRequest
                    &mut [0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00]
                        .to_vec()
                        .into()
                )
                .unwrap(),
            true
        );
    }

    #[test]
    fn test_proto_postgres_handle_no_tls_direct_startup() {
        let mut proto = PostgresProtocol::new(mock_config_no_tls());

        let (mut client_con, server_con) = create_socket_pair();

        client_con
            .write(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00])
            .unwrap();

        proto.handle(Peekable::new(server_con)).unwrap();

        // Should pass through to handler
        let handler = MockConnectionHandler::from_boxed(&proto.conf.handlers.postgres);

        assert_eq!(handler.num_received(), 1);
    }

    #[test]
    fn test_proto_postgres_handle_no_tls_ssl_request() {
        let mut proto = PostgresProtocol::new(mock_config_no_tls());

        let (mut client_con, server_con) = create_socket_pair();

        client_con.write(&PG_SSL_REQUEST).unwrap();

        proto.handle(Peekable::new(server_con)).unwrap();

        // Should receive 'N' response from server
        let mut buf = [0u8; 1];
        assert_eq!(client_con.read(&mut buf).unwrap(), 1);
        assert_eq!(&buf, b"N");

        // Should pass through to handler
        let handler = MockConnectionHandler::from_boxed(&proto.conf.handlers.postgres);

        assert_eq!(handler.num_received(), 1);
    }

    #[test]
    fn test_proto_postgres_handle_with_tls_direct_startup() {
        let mut proto = PostgresProtocol::new(mock_config_tls());

        let (mut client_con, server_con) = create_socket_pair();

        client_con
            .write(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00])
            .unwrap();

        // Should fail due to lack of SSLRequest
        proto.handle(Peekable::new(server_con)).unwrap_err();

        // Should receive error response
        let mut buf = [0u8; 19];
        assert_eq!(client_con.read(&mut buf).unwrap(), buf.len());
        assert_eq!(buf, PG_SSL_REQUIRED_ERROR);

        // Should NOT pass through to handler
        let handler = MockConnectionHandler::from_boxed(&proto.conf.handlers.postgres);

        assert_eq!(handler.num_received(), 0);
    }

    #[test]
    fn test_proto_postgres_handle_with_tls_ssl_request() {
        let mut proto = PostgresProtocol::new(mock_config_tls());

        let (mut client_con, server_con) = create_socket_pair();

        client_con.write(&PG_SSL_REQUEST).unwrap();

        // Should succeed and initiate TLS
        proto.handle(Peekable::new(server_con)).unwrap();

        // Should receive 'S' response from server
        let mut buf = [0u8; 1];
        assert_eq!(client_con.read(&mut buf).unwrap(), 1);
        assert_eq!(&buf, b"S");

        let client_config = mock_tls_client_config();
        let tls_state =
            rustls::ClientConnection::new(Arc::new(client_config), "mock.test".try_into().unwrap())
                .unwrap();
        let mut client_con = rustls::StreamOwned::new(tls_state, client_con);
        assert_eq!(client_con.conn.is_handshaking(), true);

        // TLS handshake should pass and become writable
        // We do this in a seperate thread so we can process the server-side
        // of the TLS handshake on the main thread
        let handle = thread::spawn(move || {
            client_con.write_all(b"test").unwrap();
            client_con.flush().unwrap();
            assert_eq!(client_con.conn.is_handshaking(), false);
            std::mem::forget(client_con);
        });

        // Should pass through to handler
        let handler = MockConnectionHandler::from_boxed(&proto.conf.handlers.postgres);

        assert_eq!(handler.num_received(), 1);

        // Process the server-side of the TLS connection
        let mut server_con = handler.received.lock().unwrap();
        let server_con: &mut Stream<rustls::StreamOwned<ServerConnection, Peekable<UnixStream>>> =
            server_con[0].as_any().downcast_mut().unwrap();

        // Should receive the data sent from the client thread
        let mut buf = [0u8; 4];
        server_con.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"test");

        handle.join().unwrap();
    }
}
