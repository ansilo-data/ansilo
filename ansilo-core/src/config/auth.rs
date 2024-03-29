use std::{collections::HashMap, fmt::Debug};

use enum_as_inner::EnumAsInner;
use serde::{Deserialize, Serialize};

/// Authentication options for the node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// List of auth providers, used to validate incoming auth tokens
    #[serde(default)]
    pub providers: Vec<AuthProviderConfig>,
    /// List of users
    pub users: Vec<UserConfig>,
    /// List of service users
    #[serde(default)]
    pub service_users: Vec<ServiceUserConfig>,
}

/// Defines an auth provider, used to authenticate tokens
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AuthProviderConfig {
    /// The id of the auth provider
    pub id: String,
    /// The type-specific options
    #[serde(flatten)]
    pub r#type: AuthProviderTypeConfig,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AuthProviderTypeConfig {
    #[serde(rename = "jwt")]
    Jwt(JwtAuthProviderConfig),
    #[serde(rename = "saml")]
    Saml(SamlAuthProviderConfig),
    #[serde(rename = "custom")]
    Custom(CustomAuthProviderConfig),
}

/// Defines options used for JWT token authentication
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct JwtAuthProviderConfig {
    /// URL of a JWK file used to retrieve token public keys
    pub jwk: Option<String>,
    /// URL of RSA public key
    pub rsa_public_key: Option<String>,
    /// URL of EC public key
    pub ec_public_key: Option<String>,
    /// URL of ED public key
    pub ed_public_key: Option<String>,
    /// Authentication method options
    pub login: Option<JwtLoginConfig>,
}

/// Defines options used for JWT token authentication on the frontend
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JwtLoginConfig {
    #[serde(rename = "oauth2")]
    Oauth2(JwtOauth2Config),
}

/// Options for OAuth2 based authentication
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct JwtOauth2Config {
    /// The redirection uri used for authentication (including query params)
    pub authorize_endpoint: String,
    #[serde(default)]
    pub params: HashMap<String, String>,
}

/// Defines options used for SAML2 authentication
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SamlAuthProviderConfig {
    /// URL of a IDP XML metadata file used to retrieve SAML signing certs
    pub metadata: Option<String>,
    /// Inline signing certificate
    pub x509_certificate: Option<String>,
    /// Authentication method options
    pub login: Option<SamlLoginConfig>,
}

/// Options for Saml based authentication
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SamlLoginConfig {
    /// The redirection uri used for authentication (including query params)
    pub authorize_endpoint: String,
    /// The application entity id
    pub entity_id: String,
}

/// Defines options used for custom authentication
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CustomAuthProviderConfig {
    /// Shell script to invoke to validate authentication
    pub shell: String,
}

/// Defines a user
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct UserConfig {
    /// The username used to login
    pub username: String,
    /// A description of the user
    pub description: Option<String>,
    /// The provider used to authenticate this user
    pub provider: Option<String>,
    /// Authenticate type specific options
    #[serde(flatten)]
    pub r#type: UserTypeOptions,
}

/// Type-specific authentication options for this user
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, EnumAsInner)]
#[serde(untagged)]
pub enum UserTypeOptions {
    #[serde(rename = "password")]
    Password(PasswordUserConfig),
    #[serde(rename = "jwt")]
    Jwt(JwtUserConfig),
    #[serde(rename = "saml")]
    Saml(SamlUserConfig),
    #[serde(rename = "custom")]
    Custom(CustomUserConfig),
}

/// Defines options for user password authentication
#[derive(PartialEq, Clone, Serialize, Deserialize)]
pub struct PasswordUserConfig {
    /// The password
    pub password: String,
}

impl Debug for PasswordUserConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordUserConfig")
            .field("password", &"***REDACTED***")
            .finish()
    }
}

/// Defines options used for JWT user authentication
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct JwtUserConfig {
    /// Defines which claims are required to pass authentication
    /// All claims defined in this node must be present in the token
    /// to succeed.
    #[serde(default)]
    pub claims: HashMap<String, TokenClaimCheck>,
}

/// Defines options used for SAML user authentication
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SamlUserConfig {
    /// Defines which assertions are required to pass authentication
    /// All assertions defined in this node must be present in the SAML payload
    /// to succeed.
    #[serde(default)]
    pub assertions: HashMap<String, TokenClaimCheck>,
}

/// Defines options used for custom user authentication
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct CustomUserConfig {
    /// Any custom value
    pub custom: Option<serde_yaml::Value>,
}

/// Defines a claim validation for a token
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum TokenClaimCheck {
    #[serde(rename = "eq")]
    Eq(String),
    #[serde(rename = "any")]
    Any(Vec<String>),
    #[serde(rename = "all")]
    All(Vec<String>),
}

/// Defines a service user, used to authenticate during build, cron jobs etc
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ServiceUserConfig {
    /// The id of the service user
    /// If not provided, the id is assumed to be the same as "username"
    id: Option<String>,
    /// The username to authenticate as
    pub username: String,
    /// A description of the user
    pub description: Option<String>,
    /// The method used to get the password
    #[serde(flatten)]
    pub password: ServiceUserPasswordMethod,
}

/// Method to get the service user password
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServiceUserPasswordMethod {
    Constant(ConstantServiceUserPassword),
    Shell(ShellServiceUserPassword),
}

/// Constant service user password
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ConstantServiceUserPassword {
    /// The password
    pub password: String,
}

/// Retrieve the password from a shell script
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ShellServiceUserPassword {
    /// The shell script to invoke used to retrieve the token
    /// used to authenticate as this user
    pub shell: String,
}

impl ServiceUserConfig {
    pub fn new(
        id: String,
        username: String,
        description: Option<String>,
        password: ServiceUserPasswordMethod,
    ) -> Self {
        Self {
            id: Some(id),
            username,
            description,
            password,
        }
    }

    /// Gets the id of the service user
    pub fn id(&self) -> &str {
        self.id.as_ref().unwrap_or(&self.username)
    }
}
