use std::{net::SocketAddr, path::PathBuf};

/// The config for the proxy
#[derive(Debug, Clone, PartialEq)]
pub struct ProxyConf {
    /// The socket addresses to bind on
    pub addrs: Vec<SocketAddr>,
    /// TLS settings
    pub tls: Option<TlsConf>,
    /// Auth provider settings
    pub auth: Vec<AuthProviderConfig>,
}

/// TLS configuration
#[derive(Debug, Clone, PartialEq)]
pub struct TlsConf {
    /// The path of the TLS private key
    private_key_path: PathBuf,
    /// The path of the TLS certificate
    certificate_path: PathBuf,
}

/// Authentication configuration
#[derive(Debug, Clone, PartialEq)]
pub enum AuthProviderConfig {
    Jwt(JwtAuthProviderConfig),
    // TODO:
    Saml(()),
    // TODO:
    Custom(()),
}

/// JWT configuration
#[derive(Debug, Clone, PartialEq)]
pub struct JwtAuthProviderConfig {
    // TODO
}