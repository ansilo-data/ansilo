use ansilo_core::{
    config::{JwtUserConfig, Mapping, PasswordUserConfig, SamlUserConfig, UserTypeOptions},
    err::{bail, Context, Result},
};
use ansilo_logging::{info, warn};
use ansilo_pg::proto::{
    be::PostgresBackendMessage,
    fe::{PostgresFrontendMessage, PostgresFrontendStartupMessage},
};
use ansilo_proxy::stream::IOStream;
use rand::{rngs::ThreadRng, Rng};

use crate::{
    ctx::{
        AuthContext, CustomAuthContext, JwtAuthContext, PasswordAuthContext, ProviderAuthContext,
        SamlAuthContext,
    },
    provider::{
        custom::CustomAuthProvider, jwt::JwtAuthProvider, password::PasswordAuthProvider,
        saml::SamlAuthProvider, AuthProvider,
    },
    Authenticator,
};

impl Authenticator {
    /// Perform authentication on the supplied client postgres connection
    ///
    /// This assumes a new connection that expects to receive a StartupMessage.
    /// @see https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.3
    pub async fn authenticate_postgres(
        &self,
        client: &mut Box<dyn IOStream>,
    ) -> Result<(AuthContext, PostgresFrontendStartupMessage)> {
        match self.do_postgres_authenticate(client).await {
            Ok(ctx) => Ok(ctx),
            Err(err) => {
                warn!("Error while authenticating postgres connection: {:?}", err);

                let _ = PostgresBackendMessage::ErrorResponse(format!("Error: {}", err))
                    .write(client)
                    .await;

                Err(err)
            }
        }
    }

    async fn do_postgres_authenticate(
        &self,
        client: &mut Box<dyn IOStream>,
    ) -> Result<(AuthContext, PostgresFrontendStartupMessage)> {
        // Recieve startup message
        let startup = PostgresFrontendMessage::read_startup(client)
            .await
            .context("Failed to read startup message")?;

        let username = startup
            .params
            .get("user")
            .context("Username not specified")?;

        let user = self.get_user(username)?;
        let provider = self.get_provider(&user.provider)?;

        let ctx = match (provider, &user.r#type) {
            (AuthProvider::Password(provider), UserTypeOptions::Password(conf)) => {
                ProviderAuthContext::Password(
                    self.do_postgres_password_auth(client, provider, conf)
                        .await?,
                )
            }
            (AuthProvider::Jwt(provider), UserTypeOptions::Jwt(conf)) => {
                ProviderAuthContext::Jwt(self.do_postgres_jwt_auth(client, provider, conf).await?)
            }
            (AuthProvider::Saml(provider), UserTypeOptions::Saml(conf)) => {
                ProviderAuthContext::Saml(self.do_postgres_saml_auth(client, provider, conf).await?)
            }
            (AuthProvider::Custom(provider), UserTypeOptions::Custom(conf)) => {
                ProviderAuthContext::Custom(
                    self.do_postgres_custom_auth(client, provider, conf).await?,
                )
            }
            // Shouldnt happen
            _ => bail!("Auth provider config type mismatch"),
        };

        info!(
            "Postgres connection authenticated as '{}' using '{}' provider",
            user.username, user.provider
        );

        Ok((
            AuthContext::new(&user.username, &user.provider, ctx),
            startup,
        ))
    }

    async fn do_postgres_password_auth(
        &self,
        client: &mut Box<dyn IOStream>,
        provider: &PasswordAuthProvider,
        conf: &PasswordUserConfig,
    ) -> Result<PasswordAuthContext> {
        // TODO: use sasl-scram
        let salt = ThreadRng::default().gen::<[u8; 4]>();
        PostgresBackendMessage::AuthenticationMd5Password(salt)
            .write(client)
            .await
            .context("Failed to send hash request")?;

        let res = PostgresFrontendMessage::read(client)
            .await
            .context("Failed to read response from hash request")?;

        let hash = match res {
            PostgresFrontendMessage::PasswordMessage(hash) => hash,
            _ => bail!("Unexpected response message to hash request: {:?}", res),
        };

        provider.authenticate(conf, &salt, &hash)
    }

    async fn do_postgres_jwt_auth(
        &self,
        client: &mut Box<dyn IOStream>,
        provider: &JwtAuthProvider,
        conf: &JwtUserConfig,
    ) -> Result<JwtAuthContext> {
        PostgresBackendMessage::AuthenticationCleartextPassword
            .write(client)
            .await
            .context("Failed to send jwt request")?;

        let res = PostgresFrontendMessage::read(client)
            .await
            .context("Failed to read response from jwt request")?;

        let jwt = match res {
            PostgresFrontendMessage::PasswordMessage(jwt) => jwt,
            _ => bail!("Unexpected response message to jwt request: {:?}", res),
        };

        let jwt = String::from_utf8(jwt).context("Supplied jwt is invalid")?;

        provider.authenticate(conf, &jwt)
    }

    async fn do_postgres_saml_auth(
        &self,
        _client: &mut Box<dyn IOStream>,
        _provider: &SamlAuthProvider,
        _conf: &SamlUserConfig,
    ) -> Result<SamlAuthContext> {
        todo!()
    }

    async fn do_postgres_custom_auth(
        &self,
        _client: &mut Box<dyn IOStream>,
        _provider: &CustomAuthProvider,
        _conf: &Mapping,
    ) -> Result<CustomAuthContext> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::config::{
        AuthConfig, AuthProviderConfig, JwtAuthProviderConfig, TokenClaimCheck, UserConfig,
    };
    use ansilo_proxy::stream::Stream;
    use jsonwebtoken::EncodingKey;
    use tokio::net::UnixStream;

    use super::*;
    use crate::provider::jwt::tests::*;

    fn mock_password_authentictor() -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![UserConfig {
                username: "user".into(),
                description: None,
                provider: "password".into(),
                r#type: UserTypeOptions::Password(PasswordUserConfig {
                    password: "password1".into(),
                }),
            }],
            service_users: vec![],
        }));

        Authenticator::init(conf).unwrap()
    }

    fn mock_jwt_authentictor() -> (Authenticator, EncodingKey) {
        let (encoding_key, decoding_key) = create_rsa_key_pair();

        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![AuthProviderConfig {
                id: "jwt".into(),
                r#type: ansilo_core::config::AuthProviderTypeConfig::Jwt(JwtAuthProviderConfig {
                    jwk: None,
                    rsa_public_key: Some(format!(
                        "file://{}",
                        decoding_key.path().to_str().unwrap()
                    )),
                    ec_public_key: None,
                    ed_public_key: None,
                }),
            }],
            users: vec![UserConfig {
                username: "user".into(),
                description: None,
                provider: "jwt".into(),
                r#type: UserTypeOptions::Jwt(JwtUserConfig {
                    claims: vec![("scope".into(), TokenClaimCheck::All(vec!["data".into()]))]
                        .into_iter()
                        .collect(),
                }),
            }],
            service_users: vec![],
        }));

        (Authenticator::init(conf).unwrap(), encoding_key)
    }

    fn mock_client_stream() -> (Box<dyn IOStream>, Box<dyn IOStream>) {
        let (a, b) = UnixStream::pair().unwrap();

        (Box::new(Stream(a)), Box::new(Stream(b)))
    }

    #[tokio::test]
    async fn test_postgres_auth_invalid_user() {
        let (mut client, mut output) = mock_client_stream();
        let auth = mock_password_authentictor();

        let (auth_res, _) = tokio::join!(auth.authenticate_postgres(&mut output), async move {
            // send startup
            PostgresFrontendMessage::StartupMessage(PostgresFrontendStartupMessage::new(
                [("user".into(), "invalid".into())].into_iter().collect(),
            ))
            .write(&mut client)
            .await
            .unwrap();

            // should error
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            assert_eq!(
                res.serialise(),
                PostgresBackendMessage::ErrorResponse("test".into()).serialise()
            )
        });

        auth_res.unwrap_err();
    }
}
