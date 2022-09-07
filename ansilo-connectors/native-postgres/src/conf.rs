use std::{collections::HashMap, convert::TryInto, str::FromStr, time::Duration};

use ansilo_connectors_base::common::entity::ConnectorEntityConfig;
use ansilo_core::{
    config,
    err::{Context, Error, Result},
};
use serde::{Deserialize, Serialize};
use tokio_postgres::Config;

/// The connection config
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PostgresConnectionConfig {
    /// Hostname/socket
    pub host: Option<String>,
    /// Port
    pub port: Option<u16>,
    /// Connection username
    pub user: Option<String>,
    /// Connection password
    pub password: Option<String>,
    /// Connection db
    pub dbname: Option<String>,
    /// Examples:
    /// host=localhost user=postgres connect_timeout=10 keepalives=0
    /// host=/var/run/postgresql,localhost port=1234 user=postgres password='password with spaces'
    /// @see https://docs.rs/postgres/latest/postgres/config/struct.Config.html#method.options
    pub opts: Option<String>,
    /// Connection pool config
    pub pool: Option<PostgresConnectionPoolConfig>,
}

/// The connection pool config
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PostgresConnectionPoolConfig {
    /// Max pool size
    pub max_size: Option<u16>,
    /// How lont to wait when acquiring a connection
    pub connection_timeout: Option<Duration>,
}

impl TryInto<Config> for PostgresConnectionConfig {
    type Error = Error;

    fn try_into(self) -> Result<Config, Self::Error> {
        let mut conf = match self.opts {
            Some(opts) => Config::from_str(&opts).with_context(|| {
                format!("Failed to parse postgres connections 'opts' field: \"{opts}\"")
            })?,
            None => Config::new(),
        };

        if let Some(host) = self.host {
            conf.host(&host);
        }

        if let Some(port) = self.port {
            conf.port(port);
        }

        if let Some(user) = self.user {
            conf.user(&user);
        }

        if let Some(password) = self.password {
            conf.password(&password);
        }

        if let Some(dbname) = self.dbname {
            conf.dbname(&dbname);
        }

        Ok(conf)
    }
}

pub type PostgresConnectorEntityConfig = ConnectorEntityConfig<PostgresEntitySourceConfig>;

/// Entity source config for Postgres driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PostgresEntitySourceConfig {
    Table(PostgresTableOptions),
}

impl PostgresEntitySourceConfig {
    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse entity source configuration options")
    }
}

/// Entity source configuration for mapping an entity to a table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PostgresTableOptions {
    /// The database name
    pub schema_name: Option<String>,
    /// The table name
    pub table_name: String,
    /// Mapping of attributes to their respective column names
    pub attribute_column_map: HashMap<String, String>,
}

impl PostgresTableOptions {
    pub fn new(
        schema_name: Option<String>,
        table_name: String,
        attribute_column_map: HashMap<String, String>,
    ) -> Self {
        Self {
            schema_name,
            table_name,
            attribute_column_map,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::config::Host;

    use super::*;

    #[test]
    fn test_config_with_opts_url() {
        let conf: Config = {
            let mut conf = PostgresConnectionConfig::default();
            conf.opts = Some("host=localhost user=postgres password=123".into());
            conf.try_into().unwrap()
        };

        assert_eq!(conf.get_hosts(), [Host::Tcp("localhost".into())]);
        assert_eq!(conf.get_user(), Some("postgres"));
        assert_eq!(conf.get_password(), Some("123".as_bytes()));
    }

    #[test]
    fn test_config_with_explicit_opts() {
        let conf: Config = {
            let mut conf = PostgresConnectionConfig::default();
            conf.host = Some("google".into());
            conf.user = Some("superuser".into());
            conf.password = Some("pass123".into());
            conf.try_into().unwrap()
        };

        assert_eq!(conf.get_hosts(), [Host::Tcp("google".into())]);
        assert_eq!(conf.get_user(), Some("superuser"));
        assert_eq!(conf.get_password(), Some("pass123".as_bytes()));
    }
}
