use crate::proto::{
    be::PostgresBackendMessage,
    fe::{PostgresFrontendMessage, PostgresFrontendMessageTag, PostgresFrontendStartupMessage},
};
use ansilo_core::{
    auth::{
        AuthContext, CustomAuthContext, JwtAuthContext, PasswordAuthContext, ProviderAuthContext,
        SamlAuthContext,
    },
    config::{
        CustomUserConfig, JwtUserConfig, PasswordUserConfig, SamlUserConfig, UserTypeOptions,
    },
    err::{bail, ensure, Context, Result},
};
use ansilo_logging::{info, warn};
use ansilo_proxy::stream::IOStream;
use rand::Rng;

use ansilo_auth::{
    provider::{
        custom::CustomAuthProvider, jwt::JwtAuthProvider, password::PasswordAuthProvider,
        saml::SamlAuthProvider, AuthProvider,
    },
    Authenticator,
};

use super::ProxySession;

impl<'a> ProxySession<'a> {
    /// Perform authentication on the supplied client postgres connection
    ///
    /// This assumes a new connection that expects to receive a StartupMessage.
    /// @see https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.3
    pub(crate) async fn authenticate_postgres(
        auth: &Authenticator,
        client: &mut Box<dyn IOStream>,
        startup: &PostgresFrontendStartupMessage,
        service_user_id: Option<String>,
    ) -> Result<AuthContext> {
        match Self::do_postgres_authenticate(auth, client, startup, service_user_id).await {
            Ok(ctx) => Ok(ctx),
            Err(err) => {
                warn!("Error while authenticating postgres connection: {:?}", err);

                let _ = PostgresBackendMessage::error_msg(format!("{}", err))
                    .write(client)
                    .await;

                Err(err)
            }
        }
    }

    async fn do_postgres_authenticate(
        auth: &Authenticator,
        client: &mut Box<dyn IOStream>,
        startup: &PostgresFrontendStartupMessage,
        service_user_id: Option<String>,
    ) -> Result<AuthContext> {
        let username = startup
            .params
            .get("user")
            .context("Username not specified")?;

        let user = auth.get_user(username)?;
        let provider_id = user.provider.clone().unwrap_or("password".into());
        let provider = auth.get_provider(&provider_id)?;

        let ctx = match (provider, &user.r#type) {
            (AuthProvider::Password(provider), UserTypeOptions::Password(conf)) => {
                ProviderAuthContext::Password(
                    Self::do_postgres_password_auth(auth, client, username, provider, conf).await?,
                )
            }
            (AuthProvider::Jwt(provider), UserTypeOptions::Jwt(conf)) => ProviderAuthContext::Jwt(
                Self::do_postgres_jwt_auth(auth, client, provider, conf).await?,
            ),
            (AuthProvider::Saml(provider), UserTypeOptions::Saml(conf)) => {
                ProviderAuthContext::Saml(
                    Self::do_postgres_saml_auth(auth, client, provider, conf).await?,
                )
            }
            (AuthProvider::Custom(provider), conf) => {
                let conf = match conf {
                    UserTypeOptions::Custom(c) => c.clone(),
                    _ => CustomUserConfig::default(),
                };
                ProviderAuthContext::Custom(
                    Self::do_postgres_custom_auth(auth, client, username, provider, &conf).await?,
                )
            }
            // Shouldnt happen
            _ => bail!("Auth provider config type mismatch"),
        };

        info!(
            "Postgres connection authenticated as '{}' using '{}' provider",
            user.username, provider_id
        );

        // Send authentication success to client
        PostgresBackendMessage::AuthenticationOk
            .write(client)
            .await
            .context("Failed to send authentication success message")?;

        Ok(AuthContext::new(
            &user.username,
            &provider_id,
            service_user_id,
            ctx,
        ))
    }

    async fn do_postgres_password_auth(
        _auth: &Authenticator,
        client: &mut Box<dyn IOStream>,
        username: &str,
        provider: &PasswordAuthProvider,
        conf: &PasswordUserConfig,
    ) -> Result<PasswordAuthContext> {
        // TODO: use sasl-scram
        let salt = rand::thread_rng().gen::<[u8; 4]>();
        PostgresBackendMessage::AuthenticationMd5Password(salt)
            .write(client)
            .await
            .context("Failed to send hash request")?;

        let res = PostgresFrontendMessage::read(client)
            .await
            .context("Failed to read response from hash request")?;

        // @see https://doxygen.postgresql.org/md5__common_8c_source.html#l00144
        // Output format is "md5" followed by a 32-hex-digit MD5 checksum.
        // Hence, the output buffer "buf" must be at least 36 bytes long.
        let data = match res {
            PostgresFrontendMessage::Other(msg)
                if msg.tag() == Some(PostgresFrontendMessageTag::AuthenticationData as _) =>
            {
                msg.body().to_vec()
            }
            _ => bail!("Unexpected response message to hash request: {:?}", res),
        };

        ensure!(data.len() == 36, "Invalid password hash");
        let hex = &data[3..35];
        let hash = hex::decode(hex).context("Invalid password hash")?;

        provider.authenticate(conf, username, &salt, hash.as_slice())
    }

    async fn do_postgres_jwt_auth(
        _auth: &Authenticator,
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

        let mut jwt = match res {
            PostgresFrontendMessage::Other(msg)
                if msg.tag() == Some(PostgresFrontendMessageTag::AuthenticationData as _) =>
            {
                msg.body().to_vec()
            }
            _ => bail!("Unexpected response message to jwt request: {:?}", res),
        };

        // Trim trailing null byte if present
        if jwt.last().cloned() == Some(0) {
            jwt.remove(jwt.len() - 1);
        }

        let jwt = String::from_utf8(jwt).context("Supplied jwt is invalid")?;

        provider.authenticate(conf, &jwt)
    }

    async fn do_postgres_saml_auth(
        _auth: &Authenticator,
        _client: &mut Box<dyn IOStream>,
        _provider: &SamlAuthProvider,
        _conf: &SamlUserConfig,
    ) -> Result<SamlAuthContext> {
        todo!()
    }

    async fn do_postgres_custom_auth(
        _auth: &Authenticator,
        client: &mut Box<dyn IOStream>,
        username: &str,
        provider: &CustomAuthProvider,
        conf: &CustomUserConfig,
    ) -> Result<CustomAuthContext> {
        PostgresBackendMessage::AuthenticationCleartextPassword
            .write(client)
            .await
            .context("Failed to send password request")?;

        let res = PostgresFrontendMessage::read(client)
            .await
            .context("Failed to read response from password request")?;

        let mut password = match res {
            PostgresFrontendMessage::Other(msg)
                if msg.tag() == Some(PostgresFrontendMessageTag::AuthenticationData as _) =>
            {
                msg.body().to_vec()
            }
            _ => bail!("Unexpected response message to password request: {:?}", res),
        };

        // Trim trailing null byte if present
        if password.last().cloned() == Some(0) {
            password.remove(password.len() - 1);
        }

        let password = String::from_utf8(password).context("Supplied jwt is invalid")?;

        provider.authenticate(conf, username, &password)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use ansilo_auth::Authenticator;
    use ansilo_core::config::*;
    use ansilo_proxy::stream::Stream;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use pretty_assertions::assert_eq;
    use tokio::net::UnixStream;

    use super::*;
    use ansilo_auth::provider::jwt_test::*;
    use ansilo_auth::provider::password_test::md5::{Digest, Md5};

    fn mock_password_authentictor() -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![UserConfig {
                username: "john".into(),
                description: None,
                provider: Some("password".into()),
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
                r#type: AuthProviderTypeConfig::Jwt(JwtAuthProviderConfig {
                    jwk: None,
                    rsa_public_key: Some(format!(
                        "file://{}",
                        decoding_key.path().to_str().unwrap()
                    )),
                    ec_public_key: None,
                    ed_public_key: None,
                    login: None,
                }),
            }],
            users: vec![UserConfig {
                username: "mary".into(),
                description: None,
                provider: Some("jwt".into()),
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

    fn mock_custom_authentictor(script: &str) -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![AuthProviderConfig {
                id: "custom".into(),
                r#type: AuthProviderTypeConfig::Custom(CustomAuthProviderConfig {
                    shell: script.into(),
                }),
            }],
            users: vec![UserConfig {
                username: "john".into(),
                description: None,
                provider: Some("custom".into()),
                r#type: UserTypeOptions::Custom(CustomUserConfig { custom: None }),
            }],
            service_users: vec![],
        }));

        Authenticator::init(conf).unwrap()
    }

    fn mock_client_stream() -> (Box<dyn IOStream>, Box<dyn IOStream>) {
        let (a, b) = UnixStream::pair().unwrap();

        (Box::new(Stream(a)), Box::new(Stream(b)))
    }

    fn create_token(header: &Header, claims: &str, key: &EncodingKey) -> String {
        let parsed: serde_json::Value = serde_json::from_str(claims).unwrap();
        encode(&header, &parsed, key).unwrap()
    }

    fn get_valid_exp_claim() -> u64 {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        timestamp + 3600
    }

    #[tokio::test]
    async fn test_postgres_auth_invalid_user() {
        let (mut client, mut output) = mock_client_stream();
        let auth = mock_password_authentictor();

        let startup = PostgresFrontendStartupMessage::new(
            [("user".into(), "invalid".into())].into_iter().collect(),
        );

        let (auth_res, _) = tokio::join!(
            ProxySession::authenticate_postgres(&auth, &mut output, &startup, None),
            async move {
                // should error
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                assert_eq!(
                    res,
                    PostgresBackendMessage::error_msg("User 'invalid' does not exist")
                )
            }
        );

        auth_res.unwrap_err();
    }

    #[tokio::test]
    async fn test_postgres_auth_invalid_password() {
        let (mut client, mut output) = mock_client_stream();
        let auth = mock_password_authentictor();

        let startup = PostgresFrontendStartupMessage::new(
            [("user".into(), "john".into())].into_iter().collect(),
        );

        let (auth_res, _) = tokio::join!(
            ProxySession::authenticate_postgres(&auth, &mut output, &startup, None),
            async move {
                // should receive password hash request
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                let salt = match res {
                    PostgresBackendMessage::AuthenticationMd5Password(salt) => salt,
                    _ => panic!("Unexpected response {:?}", res),
                };

                // stage 1
                let mut hasher = Md5::new();
                hasher.update("invalid".as_bytes());
                hasher.update("john".as_bytes());
                let stage1 = hex::encode(hasher.finalize().to_vec());

                // stage 2
                let mut hasher = Md5::new();
                hasher.update(stage1.as_bytes());
                hasher.update(salt);
                let hash = hex::encode(hasher.finalize().to_vec());

                let r#final = format!("md5{hash}\0").as_bytes().to_vec();

                // send hash
                PostgresFrontendMessage::PasswordMessage(r#final)
                    .write(&mut client)
                    .await
                    .unwrap();

                // should error
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                assert_eq!(res, PostgresBackendMessage::error_msg("Incorrect password"))
            }
        );

        auth_res.unwrap_err();
    }

    #[tokio::test]
    async fn test_postgres_auth_valid_password() {
        let (mut client, mut output) = mock_client_stream();
        let auth = mock_password_authentictor();

        let startup = PostgresFrontendStartupMessage::new(
            [("user".into(), "john".into())].into_iter().collect(),
        );

        let (auth_res, _) = tokio::join!(
            ProxySession::authenticate_postgres(&auth, &mut output, &startup, None),
            async move {
                // should receive password hash request
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                let salt = match res {
                    PostgresBackendMessage::AuthenticationMd5Password(salt) => salt,
                    _ => panic!("Unexpected response {:?}", res),
                };

                // stage 1
                let mut hasher = Md5::new();
                hasher.update("password1".as_bytes());
                hasher.update("john".as_bytes());
                let stage1 = hex::encode(hasher.finalize().to_vec());

                // stage 2
                let mut hasher = Md5::new();
                hasher.update(stage1.as_bytes());
                hasher.update(salt);
                let hash = hex::encode(hasher.finalize().to_vec());

                let r#final = format!("md5{hash}\0").as_bytes().to_vec();

                // send hash
                PostgresFrontendMessage::PasswordMessage(r#final)
                    .write(&mut client)
                    .await
                    .unwrap();

                // should authenticate
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                assert_eq!(res, PostgresBackendMessage::AuthenticationOk)
            }
        );

        let ctx = auth_res.unwrap();

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

        let startup = PostgresFrontendStartupMessage::new(
            [("user".into(), "mary".into())].into_iter().collect(),
        );

        let (auth_res, _) = tokio::join!(
            ProxySession::authenticate_postgres(&auth, &mut output, &startup, None),
            async move {
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
                    PostgresBackendMessage::error_msg("Failed to decode JWT header")
                )
            }
        );

        auth_res.unwrap_err();
    }

    #[tokio::test]
    async fn test_postgres_auth_valid_jwt_missing_claim() {
        let (mut client, mut output) = mock_client_stream();
        let (auth, encoding_key) = mock_jwt_authentictor();

        let startup = PostgresFrontendStartupMessage::new(
            [("user".into(), "mary".into())].into_iter().collect(),
        );

        let (auth_res, _) = tokio::join!(
            ProxySession::authenticate_postgres(&auth, &mut output, &startup, None),
            async move {
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
                    PostgresBackendMessage::error_msg("Must provide claim 'scope'")
                )
            }
        );

        auth_res.unwrap_err();
    }

    #[tokio::test]
    async fn test_postgres_auth_valid_jwt_with_correct_claims() {
        let (mut client, mut output) = mock_client_stream();
        let (auth, encoding_key) = mock_jwt_authentictor();

        let startup = PostgresFrontendStartupMessage::new(
            [("user".into(), "mary".into())].into_iter().collect(),
        );

        let (auth_res, (raw_token, exp)) = tokio::join!(
            ProxySession::authenticate_postgres(&auth, &mut output, &startup, None),
            async move {
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
            }
        );

        let ctx = auth_res.unwrap();

        assert_eq!(ctx.username, "mary".to_string());
        assert_eq!(ctx.provider, "jwt".to_string());
        assert_eq!(
            ctx.more,
            ProviderAuthContext::Jwt(JwtAuthContext {
                raw_token,
                header: serde_json::Value::Object(
                    [
                        ("alg".to_string(), serde_json::Value::String("RS512".into())),
                        ("typ".into(), serde_json::Value::String("JWT".into()))
                    ]
                    .into_iter()
                    .collect()
                ),
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

    #[tokio::test]
    async fn test_postgres_auth_custom_auth_success() {
        let (mut client, mut output) = mock_client_stream();
        let auth = mock_custom_authentictor(r#"echo '{"result": "success", "context": "ctx"}'"#);

        let startup = PostgresFrontendStartupMessage::new(
            [("user".into(), "john".into())].into_iter().collect(),
        );

        let (auth_res, _) = tokio::join!(
            ProxySession::authenticate_postgres(&auth, &mut output, &startup, None),
            async move {
                // should receive password request
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                match res {
                    PostgresBackendMessage::AuthenticationCleartextPassword => {}
                    _ => panic!("Unexpected response {:?}", res),
                };

                // send pw
                PostgresFrontendMessage::PasswordMessage(vec![1, 2, 3])
                    .write(&mut client)
                    .await
                    .unwrap();

                // should authenticate
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                assert_eq!(res, PostgresBackendMessage::AuthenticationOk)
            }
        );

        let ctx = auth_res.unwrap();

        assert_eq!(ctx.username, "john".to_string());
        assert_eq!(ctx.provider, "custom".to_string());
        assert_eq!(
            ctx.more,
            ProviderAuthContext::Custom(CustomAuthContext {
                data: serde_json::Value::String("ctx".into())
            })
        );
        assert_eq!(
            startup,
            PostgresFrontendStartupMessage::new(
                [("user".into(), "john".into())].into_iter().collect(),
            )
        );
    }

    #[tokio::test]
    async fn test_postgres_auth_custom_auth_failure() {
        let (mut client, mut output) = mock_client_stream();
        let auth = mock_custom_authentictor(r#"echo '{"result": "failure", "message": "msg"}'"#);

        let startup = PostgresFrontendStartupMessage::new(
            [("user".into(), "john".into())].into_iter().collect(),
        );

        let (auth_res, _) = tokio::join!(
            ProxySession::authenticate_postgres(&auth, &mut output, &startup, None),
            async move {
                // should receive password request
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                match res {
                    PostgresBackendMessage::AuthenticationCleartextPassword => {}
                    _ => panic!("Unexpected response {:?}", res),
                };

                // send pw
                PostgresFrontendMessage::PasswordMessage(vec![1, 2, 3])
                    .write(&mut client)
                    .await
                    .unwrap();

                // should error
                let res = PostgresBackendMessage::read(&mut client).await.unwrap();
                assert_eq!(res, PostgresBackendMessage::error_msg("msg"))
            }
        );

        auth_res.unwrap_err();
    }
}
