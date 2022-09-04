use std::sync::Arc;

use ansilo_core::{
    config::{AuthConfig, UserConfig},
    err::{bail, Context, Result},
};
use ansilo_logging::info;
use provider::{password::PasswordAuthProvider, AuthProvider};

pub mod handler;
pub mod provider;

/// The entrypoint to the authentication functionality.
///
/// This provides the authentication logic across the supported protocols.
#[derive(Clone)]
pub struct Authenticator {
    /// The authentication config
    conf: &'static AuthConfig,
    /// The authentication providers
    providers: Arc<Vec<(String, AuthProvider)>>,
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

        if let Some(invalid) = conf.users.iter().find(|u| {
            u.provider.is_some()
                && !providers
                    .iter()
                    .any(|(p, _)| p == u.provider.as_ref().unwrap())
        }) {
            bail!(
                "Auth provider '{}' defined on user '{}' does not exist",
                invalid.provider.as_ref().unwrap(),
                invalid.username
            );
        }

        Ok(Self {
            conf,
            providers: Arc::new(providers),
        })
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

    /// Terminates the authenticator
    pub fn terminate(self) -> Result<()> {
        // no op as of now
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::config::{PasswordUserConfig, UserTypeOptions};

    use super::*;

    #[test]
    fn init_default() {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![],
            service_users: vec![],
        }));
        let authenticator = Authenticator::init(conf).unwrap();

        assert!(matches!(
            authenticator.get_provider("password").unwrap(),
            AuthProvider::Password(_)
        ));
        assert_eq!(authenticator.providers.len(), 1);
    }

    #[test]
    fn test_get_user() {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![UserConfig {
                username: "mary".into(),
                description: None,
                provider: None,
                r#type: UserTypeOptions::Password(PasswordUserConfig {
                    password: "foo".into(),
                }),
            }],
            service_users: vec![],
        }));
        let authenticator = Authenticator::init(conf).unwrap();

        assert_eq!(
            authenticator.get_user("mary").unwrap(),
            &conf.users[0]
        );
    }
}
