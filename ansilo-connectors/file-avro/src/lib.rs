use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::Connector};
use ansilo_connectors_file_base::{
    FileConnection, FileConnectionUnpool, FileEntitySearcher, FileEntityValidator, FileQuery,
    FileQueryCompiler, FileQueryHandle, FileQueryPlanner, FileResultSet, FileSourceConfig,
};
use ansilo_core::{
    config::{self, NodeConfig},
    err::Result,
};

mod conf;
pub(crate) mod data;
pub(crate) mod estimate;
pub(crate) mod schema;
pub use conf::*;
mod io;
pub use io::*;

/// The connector for avro files
#[derive(Default)]
pub struct AvroConnector;

impl Connector for AvroConnector {
    type TConnectionPool = FileConnectionUnpool<AvroIO>;
    type TConnection = FileConnection<AvroIO>;
    type TConnectionConfig = AvroConfig;
    type TEntitySearcher = FileEntitySearcher<AvroIO>;
    type TEntityValidator = FileEntityValidator<AvroIO>;
    type TEntitySourceConfig = FileSourceConfig;
    type TQueryPlanner = FileQueryPlanner<AvroIO>;
    type TQueryCompiler = FileQueryCompiler<AvroIO>;
    type TQueryHandle = FileQueryHandle<AvroIO>;
    type TQuery = FileQuery;
    type TResultSet = FileResultSet<AvroReader>;
    type TTransactionManager = ();

    const TYPE: &'static str = "file.avro";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        AvroConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        FileSourceConfig::parse(options)
    }

    fn create_connection_pool(
        conf: AvroConfig,
        _nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        Ok(FileConnectionUnpool::new(conf))
    }
}
