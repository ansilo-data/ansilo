use ansilo_core::{config::AuthProviderTypeConfig, err::Result};

use self::{
    custom::CustomAuthProvider, jwt::JwtAuthProvider, password::PasswordAuthProvider,
    saml::SamlAuthProvider,
};

pub mod check;
pub mod custom;
pub mod jwt;
pub mod password;
pub mod saml;

#[cfg(any(test, feature = "test"))]
pub mod jwt_test;
#[cfg(any(test, feature = "test"))]
pub mod password_test;

/// Container type for authentication provider
pub enum AuthProvider {
    Password(PasswordAuthProvider),
    Jwt(JwtAuthProvider),
    Saml(SamlAuthProvider),
    Custom(CustomAuthProvider),
}

impl AuthProvider {
    pub fn init(conf: &'static AuthProviderTypeConfig) -> Result<Self> {
        Ok(match conf {
            AuthProviderTypeConfig::Jwt(conf) => Self::Jwt(JwtAuthProvider::new(&conf)?),
            AuthProviderTypeConfig::Saml(conf) => Self::Saml(SamlAuthProvider::new(conf)?),
            AuthProviderTypeConfig::Custom(conf) => Self::Custom(CustomAuthProvider::new(conf)?),
        })
    }
}
