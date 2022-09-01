use std::{fs, net::SocketAddr, path::Path};

use ansilo_core::err::{Context, Result};
use tokio_native_tls::{
    native_tls::{self, Protocol},
    TlsAcceptor,
};

use crate::handler::ConnectionHandler;

/// The config for the proxy
pub struct ProxyConf {
    /// The socket addresses to bind on
    pub addrs: Vec<SocketAddr>,
    /// TLS settings
    pub tls: Option<TlsConf>,
    /// Protocol handlers
    pub handlers: HandlerConf,
}

/// TLS configuration
#[derive(Clone)]
pub struct TlsConf {
    /// Server cert and key
    pub identity: native_tls::Identity,
}

impl TlsConf {
    pub fn new(private_key_path: &Path, certificate_path: &Path) -> Result<Self> {
        Ok(Self {
            identity: Self::server_identity(private_key_path, certificate_path)?,
        })
    }

    fn server_identity(
        private_key_path: &Path,
        certificate_path: &Path,
    ) -> Result<native_tls::Identity> {
        let cert = fs::read(certificate_path).context("Failed to read TLS certificate")?;
        let key = fs::read(private_key_path).context("Failed to read TLS private key")?;

        let identity = native_tls::Identity::from_pkcs8(cert.as_slice(), key.as_slice())
            .context("Failed to parse TLS cert and key")?;

        Ok(identity)
    }

    pub fn acceptor(&self) -> Result<TlsAcceptor> {
        native_tls::TlsAcceptor::builder(self.identity.clone())
            .min_protocol_version(Some(Protocol::Tlsv11))
            .build()
            .map(|a| a.into())
            .context("Failed to build TLS acceptor")
    }
}

/// Connection handlers
pub struct HandlerConf {
    pub(crate) postgres: Box<dyn ConnectionHandler>,
    pub(crate) http2: Box<dyn ConnectionHandler>,
    pub(crate) http1: Box<dyn ConnectionHandler>,
}

impl HandlerConf {
    pub fn new(
        postgres: impl ConnectionHandler + 'static,
        http2: impl ConnectionHandler + 'static,
        http1: impl ConnectionHandler + 'static,
    ) -> Self {
        Self {
            postgres: Box::new(postgres),
            http2: Box::new(http2),
            http1: Box::new(http1),
        }
    }
}
