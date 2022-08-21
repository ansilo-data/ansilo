mod conf;
pub mod executor;
use ansilo_connectors_base::{
    common::entity::ConnectorEntityConfig,
    interface::{Connector, OperationCost},
};
use ansilo_core::{
    config::{self, NodeConfig},
    data::DataValue,
    err::{Context, Result},
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
use serde::{Deserialize, Serialize};

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

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        let db = MemoryDatabase::new();

        if let Some(data) = options.as_mapping() {
            for (id, data) in data.iter() {
                if let (Some(id), Some(rows)) = (id.as_str(), data.as_sequence()) {
                    let rows = rows
                        .iter()
                        .filter_map(|r| r.as_sequence())
                        .map(|r| {
                            r.iter()
                                .map(|d| match d {
                                    config::Value::Null => DataValue::Null,
                                    config::Value::Bool(b) => DataValue::Boolean(*b),
                                    config::Value::Number(n) if n.is_i64() => {
                                        DataValue::Int64(n.as_i64().unwrap())
                                    }
                                    config::Value::Number(n) if n.is_f64() => {
                                        DataValue::Float64(n.as_f64().unwrap())
                                    }
                                    config::Value::String(s) => DataValue::Utf8String(s.clone()),
                                    v => DataValue::Utf8String(serde_json::to_string(v).unwrap()),
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();

                    db.set_data(id, rows);
                }
            }
        }

        Ok(db)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        MemoryConnectorEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        conf: MemoryDatabase,
        _nc: &NodeConfig,
        entities: &ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        MemoryConnectionPool::new(conf, entities.clone())
    }
}

#[derive(Clone, Default, PartialEq, Debug, Deserialize, Serialize)]
pub struct MemoryConnectorEntitySourceConfig {
    pub mock_entity_size: Option<OperationCost>,
}

impl MemoryConnectorEntitySourceConfig {
    pub fn new(mock_entity_size: Option<OperationCost>) -> Self {
        Self { mock_entity_size }
    }

    fn parse(options: config::Value) -> Result<MemoryConnectorEntitySourceConfig> {
        serde_yaml::from_value(options).context("Failed to parse")
    }
}

#[cfg(test)]
mod tests {}
