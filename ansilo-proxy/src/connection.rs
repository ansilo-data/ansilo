use std::{
    io::{Read, Write},
    sync::Arc,
};

use ansilo_core::err::{bail, Result};

use crate::{
    conf::ProxyConf,
    peekable::Peekable,
    proto::{http1::Http1Protocol, http2::Http2Protocol, postgres::PostgresProtocol, Protocol},
};

/// A connection made to the proxy server
pub struct Connection<S: Read + Write + Send + 'static> {
    conf: &'static ProxyConf,
    inner: Peekable<S>,
}

impl<S: Read + Write + Send + 'static> Connection<S> {
    pub fn new(conf: &'static ProxyConf, inner: S) -> Self {
        Self {
            conf,
            inner: Peekable::new(inner),
        }
    }

    /// Handles the incoming connection
    pub fn handle(self) -> Result<()> {
        if self.conf.tls.is_some() {
            self.handle_tls()
        } else {
            self.handle_tcp()
        }
    }

    /// Handle connection for TLS-enabled server
    fn handle_tls(mut self) -> Result<()> {
        let mut pg = PostgresProtocol::new(self.conf);

        // First check if this is a postgres connection
        if let Ok(true) = pg.matches(&mut self.inner) {
            // For postgres, TLS is handled at the application layer
            return pg.handle(self.inner);
        }

        // Otherwise, for http, we require TLS transport layer
        let config = Arc::clone(&self.conf.tls.as_ref().unwrap().server_config);
        let con = rustls::ServerConnection::new(config)?;
        let mut con = Peekable::new(rustls::StreamOwned::new(con, self.inner));

        // Now check for http/2, http/1
        // Importantly we check for http/1 first as it has the smaller peek-ahead length
        let mut http1 = Http1Protocol::new(self.conf);
        if let Ok(true) = http1.matches(&mut con) {
            return http1.handle(con);
        }

        let mut http2 = Http2Protocol::new(self.conf);
        if let Ok(true) = http2.matches(&mut con) {
            return http2.handle(con);
        }

        bail!("Unknown protocol");
    }

    /// Handle connection for TLS-disabled server
    fn handle_tcp(mut self) -> Result<()> {
        let mut pg = PostgresProtocol::new(self.conf);

        // First check if this is a postgres connection
        if let Ok(true) = pg.matches(&mut self.inner) {
            return pg.handle(self.inner);
        }

        // Now check for http/2, http/1
        // Importantly we check for http/1 first as it has the smaller peek-ahead length
        let mut http1 = Http1Protocol::new(self.conf);
        if let Ok(true) = http1.matches(&mut self.inner) {
            return http1.handle(self.inner);
        }

        let mut http2 = Http2Protocol::new(self.conf);
        if let Ok(true) = http2.matches(&mut self.inner) {
            return http2.handle(self.inner);
        }

        bail!("Unknown protocol");
    }
}

#[cfg(test)]
mod tests {
    use std::{os::unix::net::UnixStream, thread};

    use crate::test::{
        create_socket_pair, mock_config_no_tls, mock_config_tls, mock_tls_client_config,
        MockConnectionHandler,
    };

    use super::*;

    fn mock_connection(conf: &'static ProxyConf) -> (UnixStream, Connection<UnixStream>) {
        let (client, server) = create_socket_pair();

        (client, Connection::new(conf, server))
    }

    #[derive(Debug, PartialEq)]
    struct ReceivedConnections {
        postgres: usize,
        http2: usize,
        http1: usize,
    }

    impl From<&'static ProxyConf> for ReceivedConnections {
        fn from(c: &'static ProxyConf) -> Self {
            Self {
                postgres: MockConnectionHandler::from_boxed(&c.handlers.postgres).num_received(),
                http2: MockConnectionHandler::from_boxed(&c.handlers.http2).num_received(),
                http1: MockConnectionHandler::from_boxed(&c.handlers.http1).num_received(),
            }
        }
    }

    #[test]
    fn test_connection_no_tls_postgres_protocol() {
        let conf = mock_config_no_tls();
        let (mut client, connection) = mock_connection(conf);

        // Send postgres StartupMessage
        client
            .write_all(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00])
            .unwrap();
        client.flush().unwrap();

        connection.handle().unwrap();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 1,
                http2: 0,
                http1: 0
            }
        )
    }

    #[test]
    fn test_connection_no_tls_http2_protocol() {
        let conf = mock_config_no_tls();
        let (mut client, connection) = mock_connection(conf);

        // Send HTTP/2 PRI
        client
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .unwrap();
        client.flush().unwrap();

        connection.handle().unwrap();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 1,
                http1: 0
            }
        )
    }

    #[test]
    fn test_connection_no_tls_http1_protocol() {
        let conf = mock_config_no_tls();
        let (mut client, connection) = mock_connection(conf);

        // Send HTTP/1.1 GET
        client.write_all(b"GET / HTTP/1.1").unwrap();
        client.flush().unwrap();

        connection.handle().unwrap();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 1
            }
        )
    }

    #[test]
    fn test_connection_no_tls_unknown_protocol() {
        let conf = mock_config_no_tls();
        let (mut client, connection) = mock_connection(conf);

        client.write_all(b"who knows???????????????").unwrap();

        connection.handle().unwrap_err();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 0
            }
        )
    }

    #[test]
    fn test_connection_with_tls_postgres_protocol() {
        let conf = mock_config_tls();
        let (mut client, connection) = mock_connection(conf);

        // Send postgres SSLRequest
        client
            .write_all(&[0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f])
            .unwrap();
        client.flush().unwrap();

        connection.handle().unwrap();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 1,
                http2: 0,
                http1: 0
            }
        )
    }

    #[test]
    fn test_connection_with_tls_http2_protocol() {
        let conf = mock_config_tls();
        let (client, connection) = mock_connection(conf);

        // Process server-side TLS handshake in server-thread
        let server_thread = thread::spawn(move || {
            connection.handle().unwrap();
        });

        // Perform TLS-hanshake
        let client_config = mock_tls_client_config();
        let tls_state =
            rustls::ClientConnection::new(Arc::new(client_config), "mock.test".try_into().unwrap())
                .unwrap();
        let mut client_con = rustls::StreamOwned::new(tls_state, client);

        // Send HTTP/2 PRI
        client_con
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .unwrap();
        client_con.flush().unwrap();

        // Wait for server-side to finish processing
        server_thread.join().unwrap();
        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 1,
                http1: 0
            }
        )
    }

    #[test]
    fn test_connection_with_tls_http1_protocol() {
        let conf = mock_config_tls();
        let (client, connection) = mock_connection(conf);

        // Process server-side TLS handshake in server-thread
        let server_thread = thread::spawn(move || {
            connection.handle().unwrap();
        });

        // Perform TLS-hanshake
        let client_config = mock_tls_client_config();
        let tls_state =
            rustls::ClientConnection::new(Arc::new(client_config), "mock.test".try_into().unwrap())
                .unwrap();
        let mut client_con = rustls::StreamOwned::new(tls_state, client);

        // Send HTTP/1 POST request
        client_con.write_all(b"POST /abc HTTP/1.1").unwrap();
        client_con.flush().unwrap();

        // Wait for server-side to finish processing
        server_thread.join().unwrap();
        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 1
            }
        )
    }

    #[test]
    fn test_connection_with_tls_invalid_handshake() {
        let conf = mock_config_tls();
        let (mut client, connection) = mock_connection(conf);

        client.write_all(b"who knows???????????????").unwrap();

        connection.handle().unwrap_err();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 0
            }
        )
    }

    #[test]
    fn test_connection_with_tls_valid_handshake_but_invalid_protocol() {
        let conf = mock_config_tls();
        let (client, connection) = mock_connection(conf);

        // Process server-side TLS handshake in server-thread
        let server_thread = thread::spawn(move || {
            connection.handle().unwrap_err();
        });

        // Perform TLS-hanshake
        let client_config = mock_tls_client_config();
        let tls_state =
            rustls::ClientConnection::new(Arc::new(client_config), "mock.test".try_into().unwrap())
                .unwrap();
        let mut client_con = rustls::StreamOwned::new(tls_state, client);

        client_con.write_all(b"WHO KNOWS??????????????").unwrap();
        client_con.flush().unwrap();

        // Wait for server-side to finish processing
        server_thread.join().unwrap();
        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 0
            }
        )
    }
}
