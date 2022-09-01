use ansilo_core::err::{bail, Result};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    conf::ProxyConf,
    peekable::Peekable,
    proto::{http1::Http1Protocol, http2::Http2Protocol, postgres::PostgresProtocol, Protocol},
};

/// A connection made to the proxy server
pub struct Connection<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    conf: &'static ProxyConf,
    inner: Peekable<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Connection<S> {
    pub fn new(conf: &'static ProxyConf, inner: S) -> Self {
        Self {
            conf,
            inner: Peekable::new(inner),
        }
    }

    /// Handles the incoming connection
    pub async fn handle(self) -> Result<()> {
        if self.conf.tls.is_some() {
            self.handle_tls().await
        } else {
            self.handle_tcp().await
        }
    }

    /// Handle connection for TLS-enabled server
    async fn handle_tls(mut self) -> Result<()> {
        let mut pg = PostgresProtocol::new(self.conf);

        // First check if this is a postgres connection
        if let Ok(true) = pg.matches(&mut self.inner).await {
            // For postgres, TLS is handled at the application layer
            return pg.handle(self.inner).await;
        }

        // Otherwise, for http, we require TLS transport layer
        let tls = self.conf.tls.as_ref().unwrap().acceptor()?;
        let mut con = Peekable::new(tls.accept(self.inner).await?);

        // Now check for http/2, http/1
        // Importantly we check for http/1 first as it has the smaller peek-ahead length
        let mut http1 = Http1Protocol::new(self.conf);
        if let Ok(true) = http1.matches(&mut con).await {
            return http1.handle(con).await;
        }

        let mut http2 = Http2Protocol::new(self.conf);
        if let Ok(true) = http2.matches(&mut con).await {
            return http2.handle(con).await;
        }

        bail!("Unknown protocol");
    }

    /// Handle connection for TLS-disabled server
    async fn handle_tcp(mut self) -> Result<()> {
        let mut pg = PostgresProtocol::new(self.conf);

        // First check if this is a postgres connection
        if let Ok(true) = pg.matches(&mut self.inner).await {
            return pg.handle(self.inner).await;
        }

        // Now check for http/2, http/1
        // Importantly we check for http/1 first as it has the smaller peek-ahead length
        let mut http1 = Http1Protocol::new(self.conf);
        if let Ok(true) = http1.matches(&mut self.inner).await {
            return http1.handle(self.inner).await;
        }

        let mut http2 = Http2Protocol::new(self.conf);
        if let Ok(true) = http2.matches(&mut self.inner).await {
            return http2.handle(self.inner).await;
        }

        bail!("Unknown protocol");
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixStream,
    };

    use crate::test::{
        create_socket_pair, mock_config_no_tls, mock_config_tls, mock_tls_connector,
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

    #[tokio::test]
    async fn test_connection_no_tls_postgres_protocol() {
        let conf = mock_config_no_tls();
        let (mut client, connection) = mock_connection(conf);

        // Send postgres StartupMessage
        client
            .write_all(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00])
            .await
            .unwrap();
        client.flush().await.unwrap();

        connection.handle().await.unwrap();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 1,
                http2: 0,
                http1: 0
            }
        )
    }

    #[tokio::test]
    async fn test_connection_no_tls_http2_protocol() {
        let conf = mock_config_no_tls();
        let (mut client, connection) = mock_connection(conf);

        // Send HTTP/2 PRI
        client
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        connection.handle().await.unwrap();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 1,
                http1: 0
            }
        )
    }

    #[tokio::test]
    async fn test_connection_no_tls_http1_protocol() {
        let conf = mock_config_no_tls();
        let (mut client, connection) = mock_connection(conf);

        // Send HTTP/1.1 GET
        client.write_all(b"GET / HTTP/1.1").await.unwrap();
        client.flush().await.unwrap();

        connection.handle().await.unwrap();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 1
            }
        )
    }

    #[tokio::test]
    async fn test_connection_no_tls_unknown_protocol() {
        let conf = mock_config_no_tls();
        let (mut client, connection) = mock_connection(conf);

        client.write_all(b"who knows???????????????").await.unwrap();

        connection.handle().await.unwrap_err();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 0
            }
        )
    }

    #[tokio::test]
    async fn test_connection_with_tls_postgres_protocol() {
        let conf = mock_config_tls();
        let (mut client, connection) = mock_connection(conf);

        // Process server-side TLS handshake in server task
        let server = tokio::spawn(async move {
            connection.handle().await.unwrap();
        });

        // Send postgres SSLRequest
        client
            .write_all(&[0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f])
            .await
            .unwrap();
        client.flush().await.unwrap();

        // Read response to SSLRequest
        assert_eq!(client.read_u8().await.unwrap(), b'S');

        // Perform TLS-hanshake
        let _client_con = mock_tls_connector()
            .connect("mock.test".try_into().unwrap(), client)
            .await
            .unwrap();

        server.await.unwrap();
        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 1,
                http2: 0,
                http1: 0
            }
        )
    }

    #[tokio::test]
    async fn test_connection_with_tls_http2_protocol() {
        let conf = mock_config_tls();
        let (client, connection) = mock_connection(conf);

        // Process server-side TLS handshake in server task
        let server = tokio::spawn(async move {
            connection.handle().await.unwrap();
        });

        // Perform TLS-hanshake
        let mut client_con = mock_tls_connector()
            .connect("mock.test".try_into().unwrap(), client)
            .await
            .unwrap();

        // Send HTTP/2 PRI
        client_con
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        client_con.flush().await.unwrap();

        // Wait for server-side to finish processing
        server.await.unwrap();
        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 1,
                http1: 0
            }
        )
    }

    #[tokio::test]
    async fn test_connection_with_tls_http1_protocol() {
        let conf = mock_config_tls();
        let (client, connection) = mock_connection(conf);

        // Process server-side TLS handshake in server task
        let server = tokio::spawn(async move {
            connection.handle().await.unwrap();
        });

        // Perform TLS-hanshake
        let mut client_con = mock_tls_connector()
            .connect("mock.test".try_into().unwrap(), client)
            .await
            .unwrap();

        // Send HTTP/1 POST request
        client_con.write_all(b"POST /abc HTTP/1.1").await.unwrap();
        client_con.flush().await.unwrap();

        // Wait for server-side to finish processing
        server.await.unwrap();
        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 1
            }
        )
    }

    #[tokio::test]
    async fn test_connection_with_tls_invalid_handshake() {
        let conf = mock_config_tls();
        let (mut client, connection) = mock_connection(conf);

        client.write_all(b"who knows???????????????").await.unwrap();

        connection.handle().await.unwrap_err();

        assert_eq!(
            ReceivedConnections::from(conf),
            ReceivedConnections {
                postgres: 0,
                http2: 0,
                http1: 0
            }
        )
    }

    #[tokio::test]
    async fn test_connection_with_tls_valid_handshake_but_invalid_protocol() {
        let conf = mock_config_tls();
        let (client, connection) = mock_connection(conf);

        // Process server-side TLS handshake in server task
        let server = tokio::spawn(async move {
            connection.handle().await.unwrap_err();
        });

        // Perform TLS-hanshake
        let mut client_con = mock_tls_connector()
            .connect("mock.test".try_into().unwrap(), client)
            .await
            .unwrap();

        client_con
            .write_all(b"WHO KNOWS??????????????????")
            .await
            .unwrap();
        client_con.flush().await.unwrap();

        // Wait for server-side to finish processing
        server.await.unwrap();
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
