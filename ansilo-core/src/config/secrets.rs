use serde::{Deserialize, Serialize};

/// Configuration for connecting to HashiCorp Vault
/// @see `VaultClientSettings` in `vaultrs` crate
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VaultConfig {
    /// The address of vault
    pub address: String,
    /// The vault API version.
    /// Defaults to 1.
    pub version: Option<u8>,
    /// The vault namespace
    pub namespace: Option<String>,
    /// Whether to perform TLS verification
    /// Defaults to true
    pub verify: Option<bool>,
    /// Connection timeout
    pub timeout_secs: Option<u64>,
    /// Authentication config
    pub auth: VaultAuthMethod,
}

/// Supported authentication methods for HashiCorp Vault
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum VaultAuthMethod {
    #[serde(rename = "token")]
    Token(VaultTokenAuth),
    #[serde(rename = "approle")]
    AppRole(VaultAppRoleAuth),
    #[serde(rename = "kubernetes")]
    Kubernetes(VaultKubernetesAuth),
    #[serde(rename = "userpass")]
    UsernamePassword(VaultUserPasswordAuth),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VaultTokenAuth {
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VaultAppRoleAuth {
    pub mount: String,
    pub role_id: String,
    pub secret_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VaultKubernetesAuth {
    pub mount: String,
    pub role: String,
    pub jwt: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VaultUserPasswordAuth {
    pub mount: String,
    pub username: String,
    pub password: String,
}
