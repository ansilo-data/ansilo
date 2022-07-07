use ansilo_core::{
    common::data::DataValue,
    err::{bail, Context, Error, Result},
    sqlil as sql,
};

use crate::{
    common::{
        data::ResultSetReader,
        entity::{ConnectorEntityConfig, EntitySource},
    },
    interface::{
        EntitySizeEstimate, OperationCost, QueryOperationResult, QueryPlanner, SelectQueryOperation,
    },
};

use super::{MemoryConnection, MemoryQuery};

pub struct MemoryQueryPlanner {}

impl QueryPlanner for MemoryQueryPlanner {
    type TConnection = MemoryConnection;
    type TQuery = MemoryQuery;
    type TEntitySourceConfig = ();

    fn estimate_size(
        connection: &MemoryConnection,
        entity: &EntitySource<()>,
    ) -> Result<EntitySizeEstimate> {
        Ok(EntitySizeEstimate::new(
            Some(
                connection
                    .0
                    .get_entity_data(entity)
                    .ok_or(Error::msg("Could not find entity"))?
                    .len() as _,
            ),
            Some(entity.version().attributes.len() as _),
        ))
    }

    fn create_base_select(
        _connection: &MemoryConnection,
        _conf: &ConnectorEntityConfig<()>,
        entity: &EntitySource<()>,
    ) -> Result<(OperationCost, sql::Select)> {
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

impl MemoryQueryPlanner {
    fn add_col_expr(
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        select.cols.insert(alias, expr);
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
        Ok(QueryOperationResult::PerformedLocally)
    }

    fn add_group_by(select: &mut sql::Select, expr: sql::Expr) -> Result<QueryOperationResult> {
        Ok(QueryOperationResult::PerformedLocally)
    }

    fn add_order_by(
        select: &mut sql::Select,
        ordering: sql::Ordering,
    ) -> Result<QueryOperationResult> {
        Ok(QueryOperationResult::PerformedLocally)
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
