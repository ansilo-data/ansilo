use ansilo_core::{err::Result, sqlil as sql};

use crate::{
    interface::{EntitySizeEstimate, EntityVersionMetadata, QueryOperationResult, QueryPlanner},
    jdbc::{JdbcConnection, JdbcQuery},
};

use super::OracleJdbcEntitySourceConfig;

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
        select: &mut sql::Select,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_col_expr(
        &self,
        connection: &JdbcConnection<'a>,
        entity: EntityVersionMetadata<OracleJdbcEntitySourceConfig>,
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_where_clause(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut sql::Select,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_join(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut sql::Select,
        join: sql::Join,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_group_by(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut sql::Select,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn add_order_by(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut sql::Select,
        ordering: sql::Ordering,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn set_row_limit(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut sql::Select,
        row_limit: u64,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn set_rows_to_skip(
        &self,
        connection: &JdbcConnection<'a>,
        select: &mut sql::Select,
        row_skip: u64,
    ) -> Result<QueryOperationResult> {
        todo!()
    }
}
