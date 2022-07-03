use std::{
    collections::HashMap,
    os::unix::net::{UnixListener, UnixStream},
};

use ansilo_connectors::{
    interface::*,
    jdbc::{JdbcConnectionPool, JdbcConnection},
    jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcConnector},
};
use ansilo_core::{
    config::NodeConfig,
    sqlil::{self, EntityVersionIdentifier}, err::Result,
};

/// TODO: organise
pub struct AppState {
    /// The ansilo app config
    conf: &'static NodeConfig,
    /// The instance connection pools
    pools: HashMap<String, ConnectionPoolContainer>,
}

impl AppState {
    fn connection(&mut self, data_source_id: &str) -> Result<ConnectionPoolContainer> {
        if !self.pools.contains_key(data_source_id) {
            let source_conf = self.conf.sources.iter().find(|i| i.id == data_source_id).unwrap();
            let pool = match source_conf.r#type.as_str() {
                "jdbc.oracle" => {
                    let connection_conf = OracleJdbcConnector::parse_options(source_conf.options.clone()).unwrap();
                    ConnectionPoolContainer::OracleJdbc(OracleJdbcConnector::create_connection_pool(options, self.conf).unwrap())
                }
            };
            
            self.pools.insert(data_source_id.to_string(), pool);
        }

        let pool = self.pools.get_mut(data_source_id).unwrap();
        
    }
}

pub enum ConnectionPoolContainer {
    OracleJdbc(JdbcConnectionPool<OracleJdbcConnectionConfig>),
}

pub enum ConnectionContainer<'a> {
    Jdbc(JdbcConnection<'a>),
}

/// Handles connections from postgres, serving data from our connectors
pub struct PostgresFdwServer {
    /// The ansilo app config
    conf: &'static NodeConfig,
    /// The unix socket the server listens on
    listener: UnixListener,
}

/// A single connection from the FDW
struct FdwConnection<'a, TConnector: Connector<'a>> {
    /// The unix socket the server listens on
    socket: UnixStream,
    /// Current connection to data source
    connection: Option<TConnector::TConnection>,
    /// Current select query
    select: Option<sqlil::Select>,
}

enum ConnectorType {
    OracleJdbc,
}

impl<'a, TConnector: Connector<'a>> FdwConnection<'a, TConnector> {
    fn create_select(&mut self, entity: EntityVersionIdentifier) {
        let mut select = sqlil::Select::new(entity);
        TConnector::TQueryPlanner::create_base_select(
            self.connection(),
            self.entity_config(),
            self.entity_config().find(entity),
            &mut select,
        );
        self.select.insert(select);
    }
}
