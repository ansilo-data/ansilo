use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

/// Data associated to an authenticated user session
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug))]
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

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug))]
#[serde(tag = "type")]
pub enum ProviderAuthContext {
    Password(PasswordAuthContext),
    Jwt(JwtAuthContext),
    Saml(SamlAuthContext),
    Custom(CustomAuthContext),
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Default)]
#[cfg_attr(test, derive(Debug))]
pub struct PasswordAuthContext {
    // Currently no context for password auth
}

#[cfg_attr(test, derive(Debug))]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct JwtAuthContext {
    /// The JWT token itself
    pub raw_token: String,
    /// The decoded token header
    pub header: jsonwebtoken::Header,
    /// The decoded token claims
    pub claims: HashMap<String, serde_json::Value>,
}

#[cfg_attr(test, derive(Debug))]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct SamlAuthContext {
    /// The SAML XML itself
    pub raw_saml: String,
}

#[cfg_attr(test, derive(Debug))]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomAuthContext {
    /// Context returned from the custom provider
    #[serde(flatten)]
    pub data: serde_json::Value,
}
