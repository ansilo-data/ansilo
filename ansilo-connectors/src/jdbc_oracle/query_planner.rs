use ansilo_core::{
    common::data::DataValue,
    err::{bail, Context, Result},
    sqlil as sql,
};

use crate::{
    common::{
        data::ResultSetReader,
        entity::{ConnectorEntityConfig, EntitySource},
    },
    interface::{
        Connection, EntitySizeEstimate, OperationCost, QueryHandle, QueryOperationResult,
        QueryPlanner, SelectQueryOperation,
    },
    jdbc::{JdbcConnection, JdbcQuery},
};

use super::{
    OracleJdbcConnectorEntityConfig, OracleJdbcEntitySourceConfig, OracleJdbcQueryCompiler,
};

/// Query planner for Oracle JDBC driver
pub struct OracleJdbcQueryPlanner {}

impl QueryPlanner for OracleJdbcQueryPlanner {
    type TConnection = JdbcConnection;
    type TQuery = JdbcQuery;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

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
        entity: &EntitySource<OracleJdbcEntitySourceConfig>,
    ) -> Result<(OperationCost, sql::Select)> {
        // TODO: costs
        let select = sql::Select::new(sql::entity(
            entity.conf.id.as_str(),
            entity.version_id.as_str(),
        ));
        let costs = OperationCost::new(None, None, None);
        Ok((costs, select))
    }

    fn apply_select_operation(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        select: &mut sql::Select,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            SelectQueryOperation::AddColumn((alias, expr)) => {
                Self::add_col_expr(select, expr, alias)
            }
            SelectQueryOperation::AddWhere(expr) => Self::add_where_clause(select, expr),
            SelectQueryOperation::AddJoin(join) => Self::add_join(select, join),
            SelectQueryOperation::AddGroupBy(expr) => Self::add_group_by(select, expr),
            SelectQueryOperation::AddOrderBy(ordering) => Self::add_order_by(select, ordering),
            SelectQueryOperation::SetRowLimit(limit) => Self::set_row_limit(select, limit),
            SelectQueryOperation::SetRowOffset(offset) => Self::set_rows_to_skip(select, offset),
        }
    }
}

impl OracleJdbcQueryPlanner {
    fn add_col_expr(
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        select.cols.push((alias, expr));
        // TODO: costs
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_where_clause(select: &mut sql::Select, expr: sql::Expr) -> Result<QueryOperationResult> {
        select.r#where.push(expr);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_join(select: &mut sql::Select, join: sql::Join) -> Result<QueryOperationResult> {
        select.joins.push(join);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_group_by(select: &mut sql::Select, expr: sql::Expr) -> Result<QueryOperationResult> {
        select.group_bys.push(expr);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn add_order_by(
        select: &mut sql::Select,
        ordering: sql::Ordering,
    ) -> Result<QueryOperationResult> {
        select.order_bys.push(ordering);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn set_row_limit(select: &mut sql::Select, row_limit: u64) -> Result<QueryOperationResult> {
        select.row_limit = Some(row_limit);
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }

    fn set_rows_to_skip(select: &mut sql::Select, row_skip: u64) -> Result<QueryOperationResult> {
        select.row_skip = row_skip;
        Ok(QueryOperationResult::PerformedRemotely(OperationCost::new(
            None, None, None,
        )))
    }
}

// TODO: tests
