mod conf;
pub mod executor;
use ansilo_connectors_base::{
    common::entity::ConnectorEntityConfig,
    interface::{Connector, OperationCost},
};
use ansilo_core::{
    config::{self, NodeConfig},
    err::Result,
};
pub use conf::*;
mod connection;
pub use connection::*;
mod query;
pub use query::*;
mod result_set;
pub use result_set::*;
mod entity_searcher;
pub use entity_searcher::*;
mod entity_validator;
pub use entity_validator::*;
mod query_planner;
pub use query_planner::*;
mod query_compiler;
pub use query_compiler::*;

/// The connector for an in-memory dataset
/// Most useful for testing
#[derive(Default)]
pub struct MemoryConnector;

impl Connector for MemoryConnector {
    type TConnectionPool = MemoryConnectionPool;
    type TConnection = MemoryConnection;
    type TConnectionConfig = MemoryDatabase;
    type TEntitySearcher = MemoryEntitySearcher;
    type TEntityValidator = MemoryEntityValidator;
    type TEntitySourceConfig = MemoryConnectorEntitySourceConfig;
    type TQueryPlanner = MemoryQueryPlanner;
    type TQueryCompiler = MemoryQueryCompiler;
    type TQueryHandle = MemoryQueryHandle;
    type TQuery = MemoryQuery;
    type TResultSet = MemoryResultSet;
    type TTransactionManager = MemoryConnection;

    const TYPE: &'static str = "test.memory";

    fn parse_options(_options: config::Value) -> Result<Self::TConnectionConfig> {
        Ok(MemoryDatabase::new())
    }

    fn parse_entity_source_options(_options: config::Value) -> Result<Self::TEntitySourceConfig> {
        Ok(MemoryConnectorEntitySourceConfig::default())
    }

    fn create_connection_pool(
        conf: MemoryDatabase,
        _nc: &NodeConfig,
        entities: &ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        MemoryConnectionPool::new(conf, entities.clone())
    }
}

#[derive(Clone, Default, PartialEq, Debug)]
pub struct MemoryConnectorEntitySourceConfig {
    pub mock_entity_size: Option<OperationCost>,
}

impl MemoryConnectorEntitySourceConfig {
    pub fn new(mock_entity_size: Option<OperationCost>) -> Self {
        Self { mock_entity_size }
    }
}

#[cfg(test)]
mod tests {}
