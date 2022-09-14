use serde::{Deserialize, Serialize};

use crate::config::{JwtLoginConfig, SamlLoginConfig};

/// Model for exposing the authentication methods for this node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthMethods {
    pub methods: Vec<AuthMethod>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthMethod {
    pub id: String,
    pub name: Option<String>,
    pub usernames: Option<Vec<String>>,
    #[serde(flatten)]
    pub r#type: AuthMethodType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "options")]
pub enum AuthMethodType {
    #[serde(rename = "username_password")]
    UsernamePassword,
    #[serde(rename = "jwt")]
    Jwt(Option<JwtLoginConfig>),
    #[serde(rename = "saml")]
    Saml(Option<SamlLoginConfig>),
}
