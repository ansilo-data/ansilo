use std::sync::Arc;

use ansilo_core::err::{bail, Result};
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

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
#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Protocol<S> for PostgresProtocol {
    async fn matches(&self, con: &mut Peekable<S>) -> Result<bool> {
        // First, check if this is a SSLRequest
        let mut buf = [0u8; 8];
        if let Err(_) = con.peek(&mut buf[..]).await {
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

    async fn handle(&mut self, mut con: Peekable<S>) -> Result<()> {
        // Since postgres handles TLS at the application logic we have
        // to handle this at that this point.
        // @see https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.12

        if self.conf.tls.is_some() {
            // If TLS is enabled, lets validate we received an SSLRequest packet
            // We intentionally consume it from the stream, so when we initiate TLS
            // the stream will start with the clients next message (ClientHello)
            let mut buf = [0u8; 8];
            con.read_exact(&mut buf[..]).await?;

            if buf != PG_SSL_REQUEST {
                // We did not receive the expected SSLRequest, reply with an ErrorResponse and close the connection
                con.write_all(&PG_SSL_REQUIRED_ERROR).await?;
                bail!("Postgres client tried to connect without TLS on TLS-enabled server");
            }

            // Confirm server is willing to accept TLS
            con.write_all(b"S").await?;
            con.flush().await?;

            // Process TLS
            let config = Arc::clone(&self.conf.tls.as_ref().unwrap().server_config);
            let con = TlsAcceptor::from(config).accept(con).await?;

            // At this point the client should send StartupMessage
            self.conf
                .handlers
                .postgres
                .handle(Box::new(Stream(con)))
                .await
        } else {
            // If TLS is disabled, reply N to SSLRequest, if it was received
            // We peek first as we do not want to accidentally consume StartupMessage
            // from the underlying stream
            let mut buf = [0u8; 8];
            con.peek(&mut buf[..]).await?;

            if buf == PG_SSL_REQUEST {
                // Confirm server is unwilling to accept TLS and consume SSLRequest
                con.read_exact(&mut buf).await?;
                con.write_all(b"N").await?;
                con.flush().await?;
            }

            // At this point the client should send StartupMessage
            self.conf
                .handlers
                .postgres
                .handle(Box::new(Stream(con)))
                .await
        }
    }
}

#[cfg(test)]
mod tests {

    use tokio::net::UnixStream;

    use crate::test::{
        create_socket_pair, mock_config_no_tls, mock_config_tls, mock_tls_client_config,
        MockConnectionHandler,
    };

    use super::*;

    #[tokio::test]
    async fn test_proto_postgres_matches() {
        let proto = PostgresProtocol::new(mock_config_no_tls());

        assert_eq!(proto.matches(&mut vec![0u8].into()).await.unwrap(), false);
        assert_eq!(
            proto.matches(&mut b"abc".to_vec().into()).await.unwrap(),
            false
        );
        assert_eq!(
            proto
                .matches(&mut b"GET / HTTP/1.1".to_vec().into())
                .await
                .unwrap(),
            false
        );
        assert_eq!(
            proto
                .matches(&mut b"POST /abc HTTP/1.1".to_vec().into())
                .await
                .unwrap(),
            false
        );
        assert_eq!(
            proto
                .matches(&mut b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec().into())
                .await
                .unwrap(),
            false
        );
        assert_eq!(
            proto
                .matches(&mut PG_SSL_REQUEST.to_vec().into())
                .await
                .unwrap(),
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
                .await
                .unwrap(),
            true
        );
    }

    #[tokio::test]
    async fn test_proto_postgres_handle_no_tls_direct_startup() {
        let mut proto = PostgresProtocol::new(mock_config_no_tls());

        let (mut client_con, server_con) = create_socket_pair();

        client_con
            .write(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00])
            .await
            .unwrap();

        proto.handle(Peekable::new(server_con)).await.unwrap();

        // Should pass through to handler
        let handler = MockConnectionHandler::from_boxed(&proto.conf.handlers.postgres);

        assert_eq!(handler.num_received(), 1);
    }

    #[tokio::test]
    async fn test_proto_postgres_handle_no_tls_ssl_request() {
        let mut proto = PostgresProtocol::new(mock_config_no_tls());

        let (mut client_con, server_con) = create_socket_pair();

        client_con.write(&PG_SSL_REQUEST).await.unwrap();

        proto.handle(Peekable::new(server_con)).await.unwrap();

        // Should receive 'N' response from server
        let mut buf = [0u8; 1];
        assert_eq!(client_con.read(&mut buf).await.unwrap(), 1);
        assert_eq!(&buf, b"N");

        // Should pass through to handler
        let handler = MockConnectionHandler::from_boxed(&proto.conf.handlers.postgres);

        assert_eq!(handler.num_received(), 1);
    }

    #[tokio::test]
    async fn test_proto_postgres_handle_with_tls_direct_startup() {
        let mut proto = PostgresProtocol::new(mock_config_tls());

        let (mut client_con, server_con) = create_socket_pair();

        client_con
            .write(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00])
            .await
            .unwrap();

        // Should fail due to lack of SSLRequest
        proto.handle(Peekable::new(server_con)).await.unwrap_err();

        // Should receive error response
        let mut buf = [0u8; 19];
        assert_eq!(client_con.read(&mut buf).await.unwrap(), buf.len());
        assert_eq!(buf, PG_SSL_REQUIRED_ERROR);

        // Should NOT pass through to handler
        let handler = MockConnectionHandler::from_boxed(&proto.conf.handlers.postgres);

        assert_eq!(handler.num_received(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_proto_postgres_handle_with_tls_ssl_request() {
        let mut proto = PostgresProtocol::new(mock_config_tls());

        let (mut client_con, server_con) = create_socket_pair();

        client_con.write_all(&PG_SSL_REQUEST).await.unwrap();
        client_con.flush().await.unwrap();

        // Should succeed and initiate TLS
        // We do this in a seperate thread so we can process the server-side
        // of the TLS handshake on the main thread
        let server = tokio::spawn(async move {
            proto.handle(Peekable::new(server_con)).await.unwrap();
            proto
        });

        // TLS handshake should pass and become writable
        let client = tokio::spawn(async move {
            // Should receive 'S' response from server
            let mut buf = [0u8; 1];
            assert_eq!(client_con.read(&mut buf).await.unwrap(), 1);
            assert_eq!(&buf, b"S");

            let client_config = mock_tls_client_config();
            let mut client_con = tokio_rustls::TlsConnector::from(Arc::new(client_config))
                .connect("mock.test".try_into().unwrap(), client_con)
                .await
                .unwrap();

            client_con.write_all(b"test").await.unwrap();
            client_con.flush().await.unwrap();
            std::mem::forget(client_con);
        });

        let (_, proto) = tokio::try_join!(client, server).unwrap();

        // Should pass through to handler
        let handler = MockConnectionHandler::from_boxed(&proto.conf.handlers.postgres);

        assert_eq!(handler.num_received(), 1);

        // Process the server-side of the TLS connection
        let mut server_con = handler.received.lock().unwrap();
        let server_con: &mut Stream<tokio_rustls::server::TlsStream<Peekable<UnixStream>>> =
            server_con[0].as_any().downcast_mut().unwrap();

        // Should receive the data sent from the client thread
        let mut buf = [0u8; 4];
        server_con.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"test");
    }
}
