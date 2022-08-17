use std::{
    fs,
    io::{BufReader},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use ansilo_core::err::{bail, Context, Result};

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
    /// Server config
    pub server_config: Arc<rustls::ServerConfig>,
}

impl TlsConf {
    pub fn new(private_key_path: PathBuf, certificate_path: PathBuf) -> Result<Self> {
        Ok(Self {
            server_config: Arc::new(Self::server_config(private_key_path, certificate_path)?),
        })
    }

    fn server_config(
        private_key_path: PathBuf,
        certificate_path: PathBuf,
    ) -> Result<rustls::ServerConfig> {
        let mut cert_rdr = BufReader::new(fs::File::open(certificate_path)?);
        let cert = rustls_pemfile::certs(&mut cert_rdr)
            .context("Failed to read TLS certificate")?
            .into_iter()
            .map(rustls::Certificate)
            .collect();

        let key = {
            // convert it to Vec<u8> to allow reading it again if key is RSA
            let key_vec = fs::read(private_key_path)?;

            if key_vec.is_empty() {
                bail!("Private key is empty");
            }

            let mut pkcs8 = rustls_pemfile::pkcs8_private_keys(&mut key_vec.as_slice())?;

            if !pkcs8.is_empty() {
                rustls::PrivateKey(pkcs8.remove(0))
            } else {
                let mut rsa = rustls_pemfile::rsa_private_keys(&mut key_vec.as_slice())?;

                if !rsa.is_empty() {
                    rustls::PrivateKey(rsa.remove(0))
                } else {
                    bail!("Private key is empty");
                }
            }
        };

        let mut config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert, key)?;
        config.alpn_protocols = vec!["h2".into(), "http/1.1".into()];
        Ok(config)
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
