use ansilo_core::{
    config::{AuthConfig, UserConfig},
    err::{bail, Context, Result},
};
use ansilo_logging::info;
use ansilo_proxy::Protocol;
use provider::{password::PasswordAuthProvider, AuthProvider};

pub mod ctx;
pub mod handler;
pub mod provider;

/// The entrypoint to the authentication functionality.
///
/// This provides the authentication logic across the supported protocols.
pub struct Authenticator {
    /// The authentication config
    conf: &'static AuthConfig,
    /// The authentication providers
    providers: Vec<(String, AuthProvider)>,
}

impl Authenticator {
    /// Initialises the authentication system.
    pub fn init(conf: &'static AuthConfig) -> Result<Self> {
        // Initialise user-configured auth providers
        let mut providers = conf
            .providers
            .iter()
            .map(|c| {
                info!("Initialising authentication provider '{}'", c.id);
                Ok((c.id.clone(), AuthProvider::init(&c.r#type)?))
            })
            .collect::<Result<Vec<_>>>()?;

        // We have a built-in password auth provider that does not require configuration
        providers.push((
            "password".into(),
            AuthProvider::Password(PasswordAuthProvider::default()),
        ));

        if let Some(invalid) = conf
            .users
            .iter()
            .find(|u| providers.iter().any(|(p, _)| p == &u.provider))
        {
            bail!(
                "Auth provider '{}' defined on user '{}' does not exist",
                invalid.provider,
                invalid.username
            );
        }

        Ok(Self { conf, providers })
    }

    /// Gets the requested user from the auth configuration
    pub fn get_user(&self, username: &str) -> Result<&UserConfig> {
        self.conf
            .users
            .iter()
            .find(|i| i.username == username)
            .with_context(|| format!("User '{}' does not exist", username))
    }

    /// Gets the provider by its id
    pub fn get_provider(&self, provider_id: &str) -> Result<&AuthProvider> {
        self.providers
            .iter()
            .find(|(id, _)| id == provider_id)
            .map(|(_, provider)| provider)
            .with_context(|| format!("Auth provider '{}' does not exist", provider_id))
    }

    // Performs authentication on a postgres connection
    // pub fn authenticate_postgres(&self, )
}
