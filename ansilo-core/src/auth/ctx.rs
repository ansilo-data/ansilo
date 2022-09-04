use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Data associated to an authenticated user session
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct AuthContext {
    /// The authenticate user
    pub username: String,
    /// The authentication provider
    pub provider: String,
    /// Unix timestamp of when the authentication took place
    pub authenticated_at: u64,
    /// Provider specific context
    #[serde(flatten)]
    pub more: ProviderAuthContext,
}

impl AuthContext {
    pub fn new(
        username: impl Into<String>,
        provider: impl Into<String>,
        more: ProviderAuthContext,
    ) -> Self {
        Self {
            username: username.into(),
            provider: provider.into(),
            authenticated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            more,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Encode, Decode)]
#[serde(tag = "type")]
pub enum ProviderAuthContext {
    #[serde(rename = "password")]
    Password(PasswordAuthContext),
    #[serde(rename = "jwt")]
    Jwt(JwtAuthContext),
    #[serde(rename = "saml")]
    Saml(SamlAuthContext),
    #[serde(rename = "custom")]
    Custom(CustomAuthContext),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default, Encode, Decode)]
pub struct PasswordAuthContext {
    // Currently no context for password auth
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct JwtAuthContext {
    /// The JWT token itself
    pub raw_token: String,
    /// The decoded token header
    #[bincode(with_serde)]
    pub header: serde_json::Value,
    /// The decoded token claims
    #[bincode(with_serde)]
    pub claims: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct SamlAuthContext {
    /// The SAML XML itself
    pub raw_saml: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct CustomAuthContext {
    /// Context returned from the custom provider
    #[serde(flatten)]
    #[bincode(with_serde)]
    pub data: serde_json::Value,
}
