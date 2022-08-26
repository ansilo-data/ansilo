use std::{net::IpAddr, path::PathBuf};

use serde::{de, Deserialize, Deserializer, Serialize};
use serde_yaml::Value;

/// Networking options for the node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct NetworkingConfig {
    /// The listening port of the node
    #[serde(deserialize_with = "port_from_num_or_string")]
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

fn port_from_num_or_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u16, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::String(s) => s.parse().map_err(de::Error::custom)?,
        Value::Number(num) => num
            .as_u64()
            .and_then(|num| u16::try_from(num).ok())
            .ok_or(de::Error::custom("failed to parse number as u16"))? as u16,
        _ => return Err(de::Error::custom("must be integer or string")),
    })
}
