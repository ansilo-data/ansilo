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

/// The connector for Mongodb built on rumongodb
#[derive(Default)]
pub struct MongodbConnector;

impl Connector for MongodbConnector {
    type TConnectionPool = MongodbConnectionUnpool;
    type TConnection = MongodbConnection;
    type TConnectionConfig = MongodbConnectionConfig;
    type TEntitySearcher = MongodbEntitySearcher;
    type TEntityValidator = MongodbEntityValidator;
    type TEntitySourceConfig = MongodbEntitySourceConfig;
    type TQueryPlanner = MongodbQueryPlanner;
    type TQueryCompiler = MongodbQueryCompiler;
    type TQueryHandle = MongodbPreparedQuery;
    type TQuery = MongodbQuery;
    type TResultSet = MongodbResultSet;
    type TTransactionManager = MongodbConnection;

    const TYPE: &'static str = "native.mongodb";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        MongodbConnectionConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        MongodbEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        options: MongodbConnectionConfig,
        _nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        Ok(MongodbConnectionUnpool::new(options))
    }
}

impl MongodbConnector {
    /// Connects an mongodb database
    pub fn connect(config: MongodbConnectionConfig) -> Result<<Self as Connector>::TConnection> {
        MongodbConnector::create_connection_pool(
            config.clone(),
            &NodeConfig::default(),
            &ConnectorEntityConfig::new(),
        )?
        .acquire(None)
    }
}
