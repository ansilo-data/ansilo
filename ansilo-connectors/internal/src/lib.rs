use ansilo_connectors_base::{
    common::entity::ConnectorEntityConfig,
    interface::{Connection, ConnectionPool, Connector},
};
use ansilo_core::{
    auth::AuthContext,
    config::{self, NodeConfig},
    err::Result,
};

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

/// The connector querying entities internal to ansilo.
/// Used for jobs, service users etc.
#[derive(Default)]
pub struct InternalConnector;

impl Connector for InternalConnector {
    type TConnectionPool = InternalConnection;
    type TConnection = InternalConnection;
    type TConnectionConfig = ();
    type TEntitySearcher = InternalEntitySearcher;
    type TEntityValidator = InternalEntityValidator;
    type TEntitySourceConfig = ();
    type TQueryPlanner = InternalQueryPlanner;
    type TQueryCompiler = InternalQueryCompiler;
    type TQueryHandle = InternalQuery;
    type TQuery = InternalQuery;
    type TResultSet = InternalResultSet;
    type TTransactionManager = ();

    const TYPE: &'static str = "internal";

    fn parse_options(_options: config::Value) -> Result<Self::TConnectionConfig> {
        Ok(())
    }

    fn parse_entity_source_options(_options: config::Value) -> Result<Self::TEntitySourceConfig> {
        Ok(())
    }

    fn create_connection_pool(
        _conf: (),
        nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<()>,
    ) -> Result<Self::TConnectionPool> {
        Ok(InternalConnection(Box::leak(Box::new(nc.clone()))))
    }
}

#[derive(Clone)]
pub struct InternalConnection(pub &'static NodeConfig);

impl ConnectionPool for InternalConnection {
    type TConnection = InternalConnection;

    fn acquire(&mut self, _auth: Option<&AuthContext>) -> Result<Self::TConnection> {
        Ok(self.clone())
    }
}

impl Connection for InternalConnection {
    type TQuery = InternalQuery;
    type TQueryHandle = InternalQuery;
    type TTransactionManager = ();

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        Ok(query)
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        None
    }
}
