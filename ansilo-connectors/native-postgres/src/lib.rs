mod conf;
use ansilo_connectors_base::{
    common::entity::ConnectorEntityConfig,
    interface::{ConnectionPool, Connector},
};
use ansilo_core::{
    config::{self, NodeConfig},
    err::Result,
};
pub use conf::*;
mod connection;
pub use connection::*;
mod data;
pub use data::*;
mod entity_searcher;
pub use entity_searcher::*;
mod entity_validator;
pub use entity_validator::*;
mod pool;
pub use pool::*;
mod query;
pub use query::*;
mod query_compiler;
pub use query_compiler::*;
mod query_planner;
pub use query_planner::*;
mod result_set;
pub use result_set::*;
mod runtime;
pub use runtime::*;

/// The connector for Postgres built on tokio-postgres
#[derive(Default)]
pub struct PostgresConnector;

impl Connector for PostgresConnector {
    type TConnectionPool = PostgresConnectionPool;
    type TConnection = PostgresConnection<PooledClient>;
    type TConnectionConfig = PostgresConnectionConfig;
    type TEntitySearcher = PostgresEntitySearcher<PooledClient>;
    type TEntityValidator = PostgresEntityValidator<PooledClient>;
    type TEntitySourceConfig = PostgresEntitySourceConfig;
    type TQueryPlanner = PostgresQueryPlanner<PooledClient>;
    type TQueryCompiler = PostgresQueryCompiler<PooledClient>;
    type TQueryHandle = PostgresPreparedQuery<PooledClient>;
    type TQuery = PostgresQuery;
    type TResultSet = PostgresResultSet;
    type TTransactionManager = PostgresConnection<PooledClient>;

    const TYPE: &'static str = "native.postgres";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        PostgresConnectionConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        PostgresEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        options: PostgresConnectionConfig,
        _nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        PostgresConnectionPool::new(options)
    }
}

impl PostgresConnector {
    /// Connects an postgres database
    pub fn connect(config: PostgresConnectionConfig) -> Result<<Self as Connector>::TConnection> {
        PostgresConnector::create_connection_pool(
            config.clone(),
            &NodeConfig::default(),
            &ConnectorEntityConfig::new(),
        )?
        .acquire(None)
    }
}
