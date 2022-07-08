use crate::{common::entity::ConnectorEntityConfig, interface::Connector};

mod conf;
pub mod executor;
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
    type TConnectionConfig = MemoryConnectionConfig;
    type TEntitySearcher = MemoryEntitySearcher;
    type TEntityValidator = MemoryEntityValidator;
    type TEntitySourceConfig = ();
    type TQueryPlanner = MemoryQueryPlanner;
    type TQueryCompiler = MemoryQueryCompiler;
    type TQueryHandle = MemoryQueryHandle;
    type TQuery = MemoryQuery;
    type TResultSet = MemoryResultSet;

    const TYPE: &'static str = "test.memory";

    fn parse_options(_options: config::Value) -> Result<Self::TConnectionConfig> {
        Ok(MemoryConnectionConfig::new())
    }

    fn parse_entity_source_options(_options: config::Value) -> Result<Self::TEntitySourceConfig> {
        Ok(())
    }

    fn create_connection_pool(
        conf: MemoryConnectionConfig,
        _nc: &NodeConfig,
        entities: &ConnectorEntityConfig<()>,
    ) -> Result<Self::TConnectionPool> {
        MemoryConnectionPool::new(conf, entities.clone())
    }
}

#[cfg(test)]
mod tests {}
