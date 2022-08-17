use std::{net::TcpStream, sync::Arc};

use ansilo_core::err::{bail, Result};

use crate::{
    conf::ProxyConf,
    peekable::Peekable,
    proto::{http2::Http2Protocol, postgres::PostgresProtocol, Protocol, http1::Http1Protocol},
};

/// A connection made to the proxy server
pub struct Connection {
    conf: &'static ProxyConf,
    inner: Peekable<TcpStream>,
}

impl Connection {
    pub fn new(conf: &'static ProxyConf, inner: TcpStream) -> Self {
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
            self.handle_clear()
        }
    }

    /// Handle connection for TLS-enabled server
    fn handle_tls(mut self) -> Result<()> {
        let mut pg = PostgresProtocol::new(self.conf);

        // First check if this is a postgres connection
        if let Ok(true) = pg.matches(&mut self.inner) {
            return pg.handle(self.inner);
        }

        // Otherwise, for http, we require TLS transport layer
        let config = Arc::clone(&self.conf.tls.as_ref().unwrap().server_config);
        let con = rustls::ServerConnection::new(config)?;
        let mut con = Peekable::new(rustls::StreamOwned::new(con, self.inner.inner()));

        // Now check for http/2, http/1
        let mut http2 = Http2Protocol::new(self.conf);
        if let Ok(true) = http2.matches(&mut con) {
            return http2.handle(con);
        }

        let mut http1 = Http1Protocol::new(self.conf);
        if let Ok(true) = http2.matches(&mut con) {
            return http1.handle(con);
        }

        bail!("Unknown protocol");
    }

    /// Handle connection for TLS-disabled server
    fn handle_clear(mut self) -> Result<()> {
        let mut pg = PostgresProtocol::new(self.conf);

        // First check if this is a postgres connection
        if let Ok(true) = pg.matches(&mut self.inner) {
            return pg.handle(self.inner);
        }

        // Now check for http/2, http/1
        let mut http2 = Http2Protocol::new(self.conf);
        if let Ok(true) = http2.matches(&mut self.inner) {
            return http2.handle(self.inner);
        }

        let mut http1 = Http1Protocol::new(self.conf);
        if let Ok(true) = http2.matches(&mut self.inner) {
            return http1.handle(self.inner);
        }

        bail!("Unknown protocol");
    }
}
