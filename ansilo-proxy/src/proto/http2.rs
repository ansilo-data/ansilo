use ansilo_core::err::Result;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{conf::ProxyConf, peekable::Peekable, stream::Stream};

use super::Protocol;

pub struct Http2Protocol {
    conf: &'static ProxyConf,
}

impl Http2Protocol {
    pub fn new(conf: &'static ProxyConf) -> Self {
        Self { conf }
    }
}

const HTTP_PRI: [u8; 24] = *b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// HTTP/2 protocol proxy.
///
/// @see https://www.rfc-editor.org/rfc/rfc7540.html
///
/// We are generic over the inner stream to support TLS and non-TLS transports
#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> Protocol<S> for Http2Protocol {
    async fn matches(&self, con: &mut Peekable<S>) -> Result<bool> {
        let mut buf = [0u8; 24];
        if let Err(_) = con.peek(&mut buf[..]).await {
            return Ok(false);
        }

        if buf == HTTP_PRI {
            return Ok(true);
        }

        Ok(false)
    }

    async fn handle(&mut self, con: Peekable<S>) -> Result<()> {
        self.conf.handlers.http2.handle(Box::new(Stream(con))).await
    }
}

#[cfg(test)]
mod tests {
    use crate::test::mock_config_no_tls;

    use super::*;

    #[tokio::test]
    async fn test_proto_http2_matches() {
        let proto = Http2Protocol::new(mock_config_no_tls());

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
            proto.matches(&mut HTTP_PRI.to_vec().into()).await.unwrap(),
            true
        );
    }
}
