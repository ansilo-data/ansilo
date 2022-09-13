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

/// The connector for Sqlite built on rusqlite
#[derive(Default)]
pub struct SqliteConnector;

impl Connector for SqliteConnector {
    type TConnectionPool = SqliteConnectionUnpool;
    type TConnection = SqliteConnection;
    type TConnectionConfig = SqliteConnectionConfig;
    type TEntitySearcher = SqliteEntitySearcher;
    type TEntityValidator = SqliteEntityValidator;
    type TEntitySourceConfig = SqliteEntitySourceConfig;
    type TQueryPlanner = SqliteQueryPlanner;
    type TQueryCompiler = SqliteQueryCompiler;
    type TQueryHandle = SqlitePreparedQuery;
    type TQuery = SqliteQuery;
    type TResultSet = SqliteResultSet;
    type TTransactionManager = SqliteConnection;

    const TYPE: &'static str = "native.sqlite";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        SqliteConnectionConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        SqliteEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        options: SqliteConnectionConfig,
        _nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        Ok(SqliteConnectionUnpool::new(options))
    }
}

impl SqliteConnector {
    /// Connects an sqlite database
    pub fn connect(config: SqliteConnectionConfig) -> Result<<Self as Connector>::TConnection> {
        SqliteConnector::create_connection_pool(
            config.clone(),
            &NodeConfig::default(),
            &ConnectorEntityConfig::new(),
        )?
        .acquire(None)
    }
}
