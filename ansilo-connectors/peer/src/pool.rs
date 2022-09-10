use ansilo_connectors_base::interface::ConnectionPool;
use ansilo_connectors_native_postgres::{postgres_connector_runtime, UnpooledClient};
use ansilo_core::{
    auth::{AuthContext, ProviderAuthContext},
    build::ansilo_version,
    config::NodeConfig,
    err::{Context, Result},
};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;

use crate::{conf::PeerConfig, PostgresConnection};

/// We do not require connection pooling for peer instances.
/// Connections to ansilo-proxy are very cheap.
#[derive(Clone)]
pub struct PeerConnectionUnpool {
    pub(crate) nc: NodeConfig,
    pub(crate) conf: PeerConfig,
}

impl PeerConnectionUnpool {
    pub fn new(nc: &NodeConfig, conf: PeerConfig) -> Self {
        Self {
            nc: nc.clone(),
            conf,
        }
    }
}

impl ConnectionPool for PeerConnectionUnpool {
    type TConnection = PostgresConnection<UnpooledClient>;

    fn acquire(&mut self, auth: Option<&AuthContext>) -> Result<Self::TConnection> {
        let mut config = tokio_postgres::Config::new();
        config.host(
            &self
                .conf
                .url
                .host()
                .context("Host name must be provided in peer url")?
                .to_string(),
        );
        config.port(
            self.conf
                .url
                .port_or_known_default()
                .context("Port must be specified in peer url")?,
        );
        config.user(
            &self
                .conf
                .username
                .clone()
                .or(auth.map(|a| a.username.clone()))
                .context(
                "Could not connect to peer without configured username or authenticated context",
            )?,
        );
        config.password(&self.conf.password.clone().or(auth.and_then(|a| self.passthough_password(a))).context(
            "Could not connect to peer without configured username or authenticated context",
        )?);
        config.application_name(&format!("ansilo-{}", ansilo_version()));

        let (client, con) = postgres_connector_runtime()
            .block_on(config.connect(MakeTlsConnector::new(
                TlsConnector::new().context("Failed to initialise tls connector")?,
            )))
            .context("Failed to connect to peer")?;

        postgres_connector_runtime().spawn(con);

        Ok(PostgresConnection::new(UnpooledClient(client)))
    }
}

impl PeerConnectionUnpool {
    fn passthough_password(&mut self, a: &AuthContext) -> Option<String> {
        match &a.more {
            ProviderAuthContext::Password(_) => Some(
                self.nc
                    .auth
                    .users
                    .iter()
                    .find(|u| u.username == a.username)
                    .unwrap()
                    .r#type
                    .as_password()
                    .unwrap()
                    .password
                    .clone(),
            ),
            ProviderAuthContext::Jwt(c) => Some(c.raw_token.clone()),
            ProviderAuthContext::Saml(c) => Some(c.raw_saml.clone()),
            ProviderAuthContext::Custom(_) => None,
        }
    }
}
