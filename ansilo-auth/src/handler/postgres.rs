use ansilo_core::{
    config::{JwtUserConfig, Mapping, PasswordUserConfig, SamlUserConfig, UserTypeOptions},
    err::{bail, Context, Result},
};
use ansilo_logging::{info, warn};
use ansilo_pg::proto::{
    be::PostgresBackendMessage,
    fe::{PostgresFrontendMessage, PostgresFrontendMessageTag, PostgresFrontendStartupMessage},
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

        // Send authentication success to client
        PostgresBackendMessage::AuthenticationOk
            .write(client)
            .await
            .context("Failed to send authentication success message")?;

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
            PostgresFrontendMessage::Other(msg)
                if msg.tag() == Some(PostgresFrontendMessageTag::AuthenticationData as _) =>
            {
                msg.body().to_vec()
            }
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
            PostgresFrontendMessage::Other(msg)
                if msg.tag() == Some(PostgresFrontendMessageTag::AuthenticationData as _) =>
            {
                msg.body().to_vec()
            }
            _ => bail!("Unexpected response message to hash request: {:?}", res),
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
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use md5::{Digest, Md5};
    use tokio::net::UnixStream;

    use super::*;
    use crate::provider::jwt::tests::*;

    fn mock_password_authentictor() -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![UserConfig {
                username: "john".into(),
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
                username: "mary".into(),
                description: None,
                provider: "jwt".into(),
                r#type: UserTypeOptions::Jwt(JwtUserConfig {
                    claims: vec![(
                        "scope".into(),
                        TokenClaimCheck::All(vec!["access_data".into()]),
                    )]
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
                res,
                PostgresBackendMessage::ErrorResponse(
                    "Error: User 'invalid' does not exist".into()
                )
            )
        });

        auth_res.unwrap_err();
    }

    #[tokio::test]
    async fn test_postgres_auth_invalid_password() {
        let (mut client, mut output) = mock_client_stream();
        let auth = mock_password_authentictor();

        let (auth_res, _) = tokio::join!(auth.authenticate_postgres(&mut output), async move {
            // send startup
            PostgresFrontendMessage::StartupMessage(PostgresFrontendStartupMessage::new(
                [("user".into(), "john".into())].into_iter().collect(),
            ))
            .write(&mut client)
            .await
            .unwrap();

            // should receive password hash request
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            let salt = match res {
                PostgresBackendMessage::AuthenticationMd5Password(salt) => salt,
                _ => panic!("Unexpected response {:?}", res),
            };

            let mut hasher = Md5::new();
            hasher.update("invalid".as_bytes());
            hasher.update(salt);
            let hash = hasher.finalize().to_vec();

            // send hash
            PostgresFrontendMessage::PasswordMessage(hash)
                .write(&mut client)
                .await
                .unwrap();

            // should error
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            assert_eq!(
                res,
                PostgresBackendMessage::ErrorResponse("Error: Incorrect password".into())
            )
        });

        auth_res.unwrap_err();
    }

    #[tokio::test]
    async fn test_postgres_auth_valid_password() {
        let (mut client, mut output) = mock_client_stream();
        let auth = mock_password_authentictor();

        let (auth_res, _) = tokio::join!(auth.authenticate_postgres(&mut output), async move {
            // send startup
            PostgresFrontendMessage::StartupMessage(PostgresFrontendStartupMessage::new(
                [("user".into(), "john".into())].into_iter().collect(),
            ))
            .clone()
            .write(&mut client)
            .await
            .unwrap();

            // should receive password hash request
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            let salt = match res {
                PostgresBackendMessage::AuthenticationMd5Password(salt) => salt,
                _ => panic!("Unexpected response {:?}", res),
            };

            let mut hasher = Md5::new();
            hasher.update("password1".as_bytes());
            hasher.update(salt);
            let hash = hasher.finalize().to_vec();

            // send hash
            PostgresFrontendMessage::PasswordMessage(hash)
                .write(&mut client)
                .await
                .unwrap();

            // should authenticate
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            assert_eq!(res, PostgresBackendMessage::AuthenticationOk)
        });

        let (ctx, startup) = auth_res.unwrap();

        assert_eq!(ctx.username, "john".to_string());
        assert_eq!(ctx.provider, "password".to_string());
        assert_eq!(
            ctx.more,
            ProviderAuthContext::Password(PasswordAuthContext {})
        );
        assert_eq!(
            startup,
            PostgresFrontendStartupMessage::new(
                [("user".into(), "john".into())].into_iter().collect(),
            )
        );
    }

    #[tokio::test]
    async fn test_postgres_auth_invalid_jwt() {
        let (mut client, mut output) = mock_client_stream();
        let (auth, _encoding_key) = mock_jwt_authentictor();

        let (auth_res, _) = tokio::join!(auth.authenticate_postgres(&mut output), async move {
            // send startup
            PostgresFrontendMessage::StartupMessage(PostgresFrontendStartupMessage::new(
                [("user".into(), "mary".into())].into_iter().collect(),
            ))
            .write(&mut client)
            .await
            .unwrap();

            // should receive password token request
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            assert_eq!(res, PostgresBackendMessage::AuthenticationCleartextPassword);

            // send token
            PostgresFrontendMessage::PasswordMessage("invalid.token".into())
                .write(&mut client)
                .await
                .unwrap();

            // should error
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            assert_eq!(
                res,
                PostgresBackendMessage::ErrorResponse("Error: Failed to decode JWT header".into())
            )
        });

        auth_res.unwrap_err();
    }

    #[tokio::test]
    async fn test_postgres_auth_valid_jwt_missing_claim() {
        let (mut client, mut output) = mock_client_stream();
        let (auth, encoding_key) = mock_jwt_authentictor();

        let (auth_res, _) = tokio::join!(auth.authenticate_postgres(&mut output), async move {
            // send startup
            PostgresFrontendMessage::StartupMessage(PostgresFrontendStartupMessage::new(
                [("user".into(), "mary".into())].into_iter().collect(),
            ))
            .clone()
            .write(&mut client)
            .await
            .unwrap();

            // should receive password token request
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            assert_eq!(res, PostgresBackendMessage::AuthenticationCleartextPassword);

            // generate valid token
            let header = Header::new(Algorithm::RS512);
            let exp = get_valid_exp_claim();
            let token = create_token(
                &header,
                &format!(r#"{{"sub":"foo", "exp": {exp}}}"#),
                &encoding_key,
            );

            // send token
            PostgresFrontendMessage::PasswordMessage(token.into())
                .write(&mut client)
                .await
                .unwrap();

            // should error
            let res = PostgresBackendMessage::read(&mut client).await.unwrap();
            assert_eq!(
                res,
                PostgresBackendMessage::ErrorResponse("Error: Must provide claim 'scope'".into())
            )
        });

        auth_res.unwrap_err();
    }

    #[tokio::test]
    async fn test_postgres_auth_valid_jwt_with_correct_claims() {
        let (mut client, mut output) = mock_client_stream();
        let (auth, encoding_key) = mock_jwt_authentictor();

        let (auth_res, (raw_token, exp)) =
            tokio::join!(auth.authenticate_postgres(&mut output), async move {
                // send startup
                PostgresFrontendMessage::StartupMessage(PostgresFrontendStartupMessage::new(
                    [("user".into(), "mary".into())].into_iter().collect(),
                ))
                .clone()
                .write(&mut client)
                .await
                .unwrap();

                // should receive password token request
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                assert_eq!(res, PostgresBackendMessage::AuthenticationCleartextPassword);

                // generate valid token
                let header = Header::new(Algorithm::RS512);
                let exp = get_valid_exp_claim();
                let token = create_token(
                    &header,
                    &format!(r#"{{"scope":["access_data"], "exp": {exp}}}"#),
                    &encoding_key,
                );

                // send token
                PostgresFrontendMessage::PasswordMessage(token.clone().into())
                    .write(&mut client)
                    .await
                    .unwrap();

                // should authenticate
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                assert_eq!(res, PostgresBackendMessage::AuthenticationOk);

                (token, exp)
            });

        let (ctx, startup) = auth_res.unwrap();

        assert_eq!(ctx.username, "mary".to_string());
        assert_eq!(ctx.provider, "jwt".to_string());
        assert_eq!(
            ctx.more,
            ProviderAuthContext::Jwt(JwtAuthContext {
                raw_token,
                header: Header::new(Algorithm::RS512),
                claims: [
                    (
                        "scope".into(),
                        serde_json::Value::Array(vec![serde_json::Value::String(
                            "access_data".into()
                        )])
                    ),
                    ("exp".into(), serde_json::Value::Number(exp.into()))
                ]
                .into_iter()
                .collect()
            })
        );
        assert_eq!(
            startup,
            PostgresFrontendStartupMessage::new(
                [("user".into(), "mary".into())].into_iter().collect(),
            )
        );
    }
}
