use crate::proto::fe::PostgresFrontendMessage;
use ansilo_core::err::{Context, Result};
use ansilo_logging::{debug, info, warn};
use ansilo_proxy::stream::Stream;

use tokio::net::UnixStream;
use tokio_postgres::NoTls;

use super::PostgresConnectionHandler;

impl PostgresConnectionHandler {
    /// Authenticate to postgres as a service user.
    /// We return a tokio_postgres client that can be used to perform
    /// actions on behalf of the service user
    pub async fn authenticate_as_service_user(
        &self,
        service_user_id: String,
    ) -> Result<tokio_postgres::Client> {
        debug!("Authenticating as service user '{service_user_id}'");

        // Get the credentials for the service user
        // (this could block)
        let creds = {
            let authenticator = self.authenticator.clone();
            let service_user_id = service_user_id.clone();
            tokio::task::spawn_blocking(move || {
                authenticator.get_service_user_creds(&service_user_id)
            })
            .await??
        };

        // Create a unix stream pair
        let (sock_client, mut sock_handler) =
            UnixStream::pair().context("Failed to create unix socket pair")?;

        // Spawn a task to handle the output end of the socket
        {
            let handler = self.clone();
            let service_user_id = service_user_id.clone();
            tokio::spawn(async move {
                // Reat the initial request
                let startup = match PostgresFrontendMessage::read_initial(&mut sock_handler).await {
                    Ok(PostgresFrontendMessage::StartupMessage(startup)) => startup,
                    Ok(_) => {
                        warn!("Unexpected initial message");
                        return;
                    }
                    Err(err) => {
                        warn!("Failed to read initial message: {:?}", err);
                        return;
                    }
                };

                if let Err(err) = handler
                    .handle_connection(
                        Box::new(Stream(sock_handler)),
                        startup,
                        Some(service_user_id),
                    )
                    .await
                {
                    warn!("Error during service user session: {:?}", err);
                }
            });
        }

        // Now connect using tokio_postgres
        let mut config = tokio_postgres::Config::new();
        config.user(&creds.username);
        config.password(&creds.password);
        config.application_name("ansilo-svc-user");

        // No TLS is required over a local connection
        let (client, con) = config
            .connect_raw(sock_client, NoTls)
            .await
            .context("Failed to authenticate as service user")?;

        // Let the connection run in the background
        tokio::spawn(con);

        info!("Authenticated as service user '{service_user_id}'");

        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use ansilo_auth::Authenticator;
    use ansilo_core::config::*;

    use super::super::test::*;

    pub(crate) fn mock_svc_user_auth(svc_user: ServiceUserConfig) -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![UserConfig {
                username: "test_user".into(),
                description: None,
                provider: None,
                r#type: UserTypeOptions::Password(PasswordUserConfig {
                    password: "pass123".into(),
                }),
            }],
            service_users: vec![svc_user],
        }));

        Authenticator::init(conf).unwrap()
    }

    #[tokio::test]
    async fn test_authenticate_as_service_user_success() {
        ansilo_logging::init_for_tests();
        let auth = mock_svc_user_auth(ServiceUserConfig::new(
            "svc".into(),
            "test_user".into(),
            None,
            ServiceUserPasswordMethod::Constant(ConstantServiceUserPassword {
                password: "pass123".into(),
            }),
        ));
        let (_pg, handler) = init_pg_handler("svc-user-success", auth).await;

        let client = handler
            .authenticate_as_service_user("svc".into())
            .await
            .unwrap();
        let res: String = client
            .query_one("SELECT 'Hello pg'", &[])
            .await
            .unwrap()
            .get(0);

        assert_eq!(res, "Hello pg");
    }

    #[tokio::test]
    async fn test_authenticate_as_service_user_invalid_user() {
        ansilo_logging::init_for_tests();
        let auth = mock_svc_user_auth(ServiceUserConfig::new(
            "svc".into(),
            "test_user".into(),
            None,
            ServiceUserPasswordMethod::Constant(ConstantServiceUserPassword {
                password: "pass123".into(),
            }),
        ));
        let (_pg, handler) = init_pg_handler("svc-user-invalid-user", auth).await;

        handler
            .authenticate_as_service_user("invalid".into())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_authenticate_as_service_user_invalid_password() {
        ansilo_logging::init_for_tests();
        let auth = mock_svc_user_auth(ServiceUserConfig::new(
            "svc".into(),
            "test_user".into(),
            None,
            ServiceUserPasswordMethod::Constant(ConstantServiceUserPassword {
                password: "invalid".into(),
            }),
        ));
        let (_pg, handler) = init_pg_handler("svc-user-invalid-pass", auth).await;

        handler
            .authenticate_as_service_user("svc".into())
            .await
            .unwrap_err();
    }
}
