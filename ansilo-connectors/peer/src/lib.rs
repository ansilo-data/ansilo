use ansilo_connectors_base::{
    common::entity::ConnectorEntityConfig,
    interface::{ConnectionPool, Connector, EntityDiscoverOptions},
};
use ansilo_connectors_native_postgres::*;
use ansilo_core::{
    config::{self, DataSourceConfig, NodeConfig},
    err::Result,
};
use conf::PeerConfig;
use entity_searcher::PeerEntitySearcher;
use pool::PeerConnectionUnpool;

pub mod conf;
pub mod entity_searcher;
pub mod pool;

/// The connector for peering with other ansilo nodes
#[derive(Default)]
pub struct PeerConnector;

impl Connector for PeerConnector {
    type TConnectionPool = PeerConnectionUnpool;
    type TConnection = PostgresConnection<UnpooledClient>;
    type TConnectionConfig = PeerConfig;
    type TEntitySearcher = PostgresEntitySearcher<UnpooledClient>;
    type TEntityValidator = PostgresEntityValidator<UnpooledClient>;
    type TEntitySourceConfig = PostgresEntitySourceConfig;
    type TQueryPlanner = PostgresQueryPlanner<UnpooledClient>;
    type TQueryCompiler = PostgresQueryCompiler<UnpooledClient>;
    type TQueryHandle = PostgresPreparedQuery<UnpooledClient>;
    type TQuery = PostgresQuery;
    type TResultSet = PostgresResultSet;
    type TTransactionManager = PostgresConnection<UnpooledClient>;

    const TYPE: &'static str = "peer";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        PeerConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        PostgresEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        options: PeerConfig,
        nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        Ok(PeerConnectionUnpool::new(nc, options))
    }
}

impl PeerConnector {
    /// Connects to a peer
    pub fn connect(config: PeerConfig) -> Result<<Self as Connector>::TConnection> {
        Self::create_connection_pool(
            config.clone(),
            &NodeConfig::default(),
            &ConnectorEntityConfig::new(),
        )?
        .acquire(None)
    }

    pub fn discover_unauthenticated(
        conf: &DataSourceConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<config::EntityConfig>> {
        PeerEntitySearcher::discover_unauthenticated(conf, opts)
    }
}
