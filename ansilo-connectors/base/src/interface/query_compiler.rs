use ansilo_core::{err::Result, sqlil as sql};

use crate::common::entity::ConnectorEntityConfig;

use super::Connection;

/// The query compiler compiles SQLIL queries into a format that can be executed by the connector
pub trait QueryCompiler {
    type TConnection: Connection;
    type TQuery;
    type TEntitySourceConfig: Clone;

    /// Compiles the query into a connector-specific query object
    fn compile_query(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        select: sql::Query,
    ) -> Result<Self::TQuery>;
}
