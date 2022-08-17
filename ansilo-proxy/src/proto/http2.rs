use std::io::{Read, Write};

use ansilo_core::err::Result;

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
impl<S: Read + Write + Send + 'static> Protocol<S> for Http2Protocol {
    fn matches(&self, con: &mut Peekable<S>) -> Result<bool> {
        let mut buf = [0u8; 24];
        con.peek(&mut buf[..])?;

        if buf == HTTP_PRI {
            return Ok(true);
        }

        Ok(false)
    }

    fn handle(&mut self, con: Peekable<S>) -> Result<()> {
        self.conf.handlers.http2.handle(Box::new(Stream(con)))
    }
}
