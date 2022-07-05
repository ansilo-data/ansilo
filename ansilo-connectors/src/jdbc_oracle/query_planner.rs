use ansilo_core::{
    common::data::DataValue,
    err::{bail, Context, Result},
    sqlil as sql,
};

use crate::{
    common::{data::ResultSetReader, entity::EntitySource},
    interface::{
        Connection, EntitySizeEstimate, OperationCost, QueryHandle, QueryOperationResult,
        QueryPlanner,
    },
    jdbc::{JdbcConnection, JdbcQuery},
};

use super::{
    OracleJdbcConnectorEntityConfig, OracleJdbcEntitySourceConfig, OracleJdbcQueryCompiler,
};

/// Query planner for Oracle JDBC driver
pub struct OracleJdbcQueryPlanner {}

impl QueryPlanner<JdbcConnection, JdbcQuery, OracleJdbcEntitySourceConfig>
    for OracleJdbcQueryPlanner
{
    fn estimate_size(
        connection: &JdbcConnection,
        entity: &EntitySource<OracleJdbcEntitySourceConfig>,
    ) -> Result<EntitySizeEstimate> {
        // TODO: custom query support
        // TODO: multiple sample options

        let table = OracleJdbcQueryCompiler::compile_source_identifier(&entity.source_conf)?;

        let mut query = connection.prepare(JdbcQuery::new(
            format!("SELECT COUNT(*) * 1000 FROM {} SAMPLE(0.1)", table),
            vec![],
        ))?;

        let mut result_set = ResultSetReader::new(query.execute()?)?;
        let value = result_set
            .read_data_value()?
            .context("Unexpected empty result set")?;

        let num_rows = match value {
            DataValue::Int64(i) => i,
            _ => bail!("Unexpected data value returned: {:?}", value),
        };

        // TODO: row width
        Ok(EntitySizeEstimate::new(Some(num_rows as _), None))
    }

    fn create_base_select(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        _entity: &EntitySource<OracleJdbcEntitySourceConfig>,
        _select: &mut sql::Select,
    ) -> Result<QueryOperationResult> {
        // TODO: costs
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_col_expr(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        select.cols.insert(alias, expr);
        // TODO: costs
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_where_clause(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        select: &mut sql::Select,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        select.r#where.push(expr);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_join(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        select: &mut sql::Select,
        join: sql::Join,
    ) -> Result<QueryOperationResult> {
        select.joins.push(join);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_group_by(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        select: &mut sql::Select,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        select.group_bys.push(expr);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_order_by(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        select: &mut sql::Select,
        ordering: sql::Ordering,
    ) -> Result<QueryOperationResult> {
        select.order_bys.push(ordering);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn set_row_limit(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        select: &mut sql::Select,
        row_limit: u64,
    ) -> Result<QueryOperationResult> {
        select.row_limit = Some(row_limit);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn set_rows_to_skip(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        select: &mut sql::Select,
        row_skip: u64,
    ) -> Result<QueryOperationResult> {
        select.row_skip = row_skip;
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }
}

// TODO: tests
