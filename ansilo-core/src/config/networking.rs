use std::{path::PathBuf, net::IpAddr};

use serde::{Deserialize, Serialize};

/// Networking options for the node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct NetworkingConfig {
    /// The listening port of the node
    pub port: u16,
    /// The IP address to bind to
    pub bind: Option<IpAddr>,
    // TLS config
    pub tls: Option<TlsConfig>,
}

/// TLS options for the node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct TlsConfig {
    /// The path of the pem-encoded certificate file
    pub certificate: PathBuf,
    /// The path of the pem-encoded private key file
    pub private_key: PathBuf,
}
