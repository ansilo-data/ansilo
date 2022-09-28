use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::Connector};
use ansilo_core::{
    config::{self, NodeConfig},
    err::Result,
};

mod conf;
pub(crate) mod data;
pub(crate) mod schema;
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

/// The connector for arvo files
#[derive(Default)]
pub struct ArvoConnector;

impl Connector for ArvoConnector {
    type TConnectionPool = ArvoConnectionUnpool;
    type TConnection = ArvoConnection;
    type TConnectionConfig = ArvoConfig;
    type TEntitySearcher = ArvoEntitySearcher;
    type TEntityValidator = ArvoEntityValidator;
    type TEntitySourceConfig = ArvoFile;
    type TQueryPlanner = ArvoQueryPlanner;
    type TQueryCompiler = ArvoQueryCompiler;
    type TQueryHandle = ArvoQueryHandle;
    type TQuery = ArvoQuery;
    type TResultSet = ArvoResultSet;
    type TTransactionManager = ();

    const TYPE: &'static str = "file.arvo";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        ArvoConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        ArvoFile::parse(options)
    }

    fn create_connection_pool(
        conf: ArvoConfig,
        _nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<ArvoFile>,
    ) -> Result<Self::TConnectionPool> {
        Ok(ArvoConnectionUnpool::new(conf))
    }
}
