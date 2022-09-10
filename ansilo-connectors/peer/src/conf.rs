use ansilo_core::{
    config,
    err::{Context, Result},
};
use reqwest::Url;
use serde::{Deserialize, Serialize};

/// The connection config
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerConfig {
    /// Examples:
    /// https://ansilo.instance.com:4321
    pub url: Url,
    /// Option to explicitly define the username
    /// Otherwise, passthrough authentication will be used
    pub username: Option<String>,
    /// Option to explicitly define the password
    /// Otherwise, passthrough authentication will be used
    pub password: Option<String>,
}

impl PeerConfig {
    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse connection configuration options")
    }
}
