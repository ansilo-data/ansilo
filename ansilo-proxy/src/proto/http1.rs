use ansilo_core::err::Result;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{conf::ProxyConf, peekable::Peekable, stream::Stream};

use super::Protocol;

pub struct Http1Protocol {
    conf: &'static ProxyConf,
}

impl Http1Protocol {
    pub fn new(conf: &'static ProxyConf) -> Self {
        Self { conf }
    }
}

const HTTP_METHODS: [&str; 8] = [
    "OPTIONS ",
    "GET ",
    "HEAD ",
    "POST ",
    "PUT ",
    "DELETE ",
    "TRACE ",
    "CONNECTION ",
];
const PEEK_LENGTH: usize = "CONNECTION ".len();

/// HTTP/1.1 protocol proxy.
///
/// @see https://www.rfc-editor.org/rfc/rfc2616.html
///
/// We are generic over the inner stream to support TLS and non-TLS transports
#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> Protocol<S> for Http1Protocol {
    async fn matches(&self, con: &mut Peekable<S>) -> Result<bool> {
        let mut buf = [0u8; PEEK_LENGTH];
        if let Err(_) = con.peek(&mut buf[..]).await {
            return Ok(false);
        }

        for method in HTTP_METHODS {
            if &buf[..method.len()] == method.as_bytes() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn handle(&mut self, con: Peekable<S>) -> Result<()> {
        self.conf.handlers.http1.handle(Box::new(Stream(con))).await
    }
}

#[cfg(test)]
mod tests {
    use crate::test::mock_config_no_tls;

    use super::*;

    #[tokio::test]
    async fn test_proto_http1_matches() {
        let proto = Http1Protocol::new(mock_config_no_tls());

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
            true
        );
        assert_eq!(
            proto
                .matches(&mut b"POST /abc HTTP/1.1".to_vec().into())
                .await
                .unwrap(),
            true
        );
    }
}
