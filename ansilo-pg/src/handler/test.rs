use std::path::PathBuf;

use crate::{conf::PostgresConf, PostgresInstance};
use ansilo_auth::Authenticator;
use ansilo_core::config::{AuthConfig, PasswordUserConfig, UserConfig, UserTypeOptions};
use ansilo_proxy::stream::{IOStream, Stream};
use tokio::net::UnixStream;

use super::PostgresConnectionHandler;

pub fn mock_password_auth_default() -> Authenticator {
    let conf = Box::leak(Box::new(AuthConfig {
        providers: vec![],
        users: vec![
            UserConfig {
                username: "test_user".into(),
                description: None,
                provider: None,
                r#type: UserTypeOptions::Password(PasswordUserConfig {
                    password: "pass123".into(),
                }),
            },
            UserConfig {
                username: "another_user".into(),
                description: None,
                provider: None,
                r#type: UserTypeOptions::Password(PasswordUserConfig {
                    password: "luna456".into(),
                }),
            },
        ],
        service_users: vec![],
    }));

    Authenticator::init(conf).unwrap()
}

pub async fn init_pg(test_name: &'static str, auth: &Authenticator) -> PostgresInstance {
    // This runs blocking code and contains a runtime
    let conf = Box::leak(Box::new(PostgresConf {
        install_dir: PathBuf::from(
            std::env::var("ANSILO_TEST_PG_DIR").unwrap_or("/usr/lib/postgresql/14".into()),
        ),
        postgres_conf_path: None,
        data_dir: PathBuf::from(format!("/tmp/ansilo-tests/main-pg-handler/{}", test_name)),
        socket_dir_path: PathBuf::from(format!("/tmp/ansilo-tests/main-pg-handler/{}", test_name)),
        fdw_socket_path: PathBuf::from("not-used"),
        app_users: auth
            .conf()
            .users
            .iter()
            .map(|i| i.username.clone())
            .collect(),
        init_db_sql: vec![],
    }));

    PostgresInstance::configure(conf).await.unwrap()
}

pub fn init_client_stream() -> (UnixStream, Box<dyn IOStream>) {
    let (a, b) = UnixStream::pair().unwrap();

    (a, Box::new(Stream(b)))
}

pub async fn init_pg_handler(
    test_name: &'static str,
    auth: Authenticator,
) -> (PostgresInstance, PostgresConnectionHandler) {
    let mut pg = init_pg(test_name, &auth).await;

    let handler = PostgresConnectionHandler::new(auth, pg.connections().clone());

    (pg, handler)
}
