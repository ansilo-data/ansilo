use ansilo_core::{
    err::Result,
    sqlil::{expr::*, select::*},
};

use crate::{
    interface::{EntitySizeEstimate, EntityVersionMetadata, QueryOperationResult, QueryPlanner},
    jdbc::{JdbcConnection, JdbcQuery},
};

use super::{OracleJdbcEntitySourceConfig, OracleJdbcQueryCompiler};

/// Query planner for Oracle JDBC driver
pub struct OracleJdbcQueryPlanner {}

impl<'a> QueryPlanner<JdbcConnection<'a>, JdbcQuery, OracleJdbcEntitySourceConfig>
    for OracleJdbcQueryPlanner
{
    fn estimate_size(
        &self,
        connection: &JdbcConnection<'a>,
        entity_version: EntityVersionMetadata<OracleJdbcEntitySourceConfig>,
    ) -> Result<EntitySizeEstimate> {
        todo!()
    }

    fn create_base_select(
        &self,
        connection: &JdbcConnection<'a>,
        entity: EntityVersionMetadata<OracleJdbcEntitySourceConfig>,
        select: &mut Select,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_col_expr(
        &self,
        connection: &JdbcConnection<'a>,
        entity: EntityVersionMetadata<OracleJdbcEntitySourceConfig>,
        select: &mut Select,
        expr: Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_where_clause(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut Select,
        expr: Expr,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_join(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut Select,
        join: Join,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_group_by(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut Select,
        expr: Expr,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_order_by(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut Select,
        ordering: Ordering,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn set_row_limit(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut Select,
        row_limit: u64,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn set_rows_to_skip(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut Select,
        row_skip: u64,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn convert(&self, connection: &JdbcConnection<'a>, select: &Select) -> Result<JdbcQuery> {
        OracleJdbcQueryCompiler::compile_select(connection, select)
    }
}
