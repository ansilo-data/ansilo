use ansilo_core::{
    err::{Error, Result},
    sqlil as sql,
};

use crate::{
    common::entity::{ConnectorEntityConfig, EntitySource},
    interface::{
        OperationCost, QueryCompiler, QueryOperationResult, QueryPlanner, SelectQueryOperation,
    },
};

use super::{
    MemoryConnection, MemoryConnectorEntitySourceConfig, MemoryQuery, MemoryQueryCompiler,
};

pub struct MemoryQueryPlanner {}

impl QueryPlanner for MemoryQueryPlanner {
    type TConnection = MemoryConnection;
    type TQuery = MemoryQuery;
    type TEntitySourceConfig = MemoryConnectorEntitySourceConfig;

    fn estimate_size(
        connection: &MemoryConnection,
        entity: &EntitySource<MemoryConnectorEntitySourceConfig>,
    ) -> Result<OperationCost> {
        if let Some(mock_size) = &entity.source_conf.mock_entity_size {
            return Ok(mock_size.clone());
        }

        Ok(OperationCost::new(
            Some(
                connection
                    .0
                    .with_data(&entity.conf.id, &entity.version_id, |rows| rows.len())
                    .ok_or(Error::msg("Could not find entity"))? as _,
            ),
            None,
            None,
            None,
        ))
    }

    fn create_base_select(
        _connection: &MemoryConnection,
        _conf: &ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        _entity: &EntitySource<MemoryConnectorEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)> {
        let select = sql::Select::new(source.clone());

        Ok((OperationCost::default(), select))
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

    fn create_base_insert(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<MemoryConnectorEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        todo!()
    }

    fn create_base_update(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<MemoryConnectorEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)> {
        todo!()
    }

    fn create_base_delete(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<MemoryConnectorEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)> {
        todo!()
    }

    fn apply_insert_operation(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        insert: &mut sql::Insert,
        op: crate::interface::InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn apply_update_operation(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        update: &mut sql::Update,
        op: crate::interface::UpdateQueryOperation,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn apply_delete_operation(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        delete: &mut sql::Delete,
        op: crate::interface::DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        todo!()
    }

    fn explain_query(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        query: &sql::Query,
        verbose: bool,
    ) -> Result<serde_json::Value> {
        let compiled = MemoryQueryCompiler::compile_query(connection, conf, query.clone())?;

        Ok(if verbose {
            serde_json::to_value(compiled)
        } else {
            serde_json::to_value(compiled.query)
        }?)
    }
}

impl MemoryQueryPlanner {
    fn add_col_expr(
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        select.cols.push((alias, expr));
        Ok(QueryOperationResult::PerformedRemotely(
            OperationCost::default(),
        ))
    }

    fn add_where_clause(select: &mut sql::Select, expr: sql::Expr) -> Result<QueryOperationResult> {
        select.r#where.push(expr);
        Ok(QueryOperationResult::PerformedRemotely(
            OperationCost::default(),
        ))
    }

    fn add_join(select: &mut sql::Select, join: sql::Join) -> Result<QueryOperationResult> {
        select.joins.push(join);
        Ok(QueryOperationResult::PerformedRemotely(
            OperationCost::default(),
        ))
    }

    fn add_group_by(select: &mut sql::Select, expr: sql::Expr) -> Result<QueryOperationResult> {
        select.group_bys.push(expr);
        Ok(QueryOperationResult::PerformedRemotely(
            OperationCost::default(),
        ))
    }

    fn add_order_by(
        select: &mut sql::Select,
        ordering: sql::Ordering,
    ) -> Result<QueryOperationResult> {
        select.order_bys.push(ordering);
        Ok(QueryOperationResult::PerformedRemotely(
            OperationCost::default(),
        ))
    }

    fn set_row_limit(select: &mut sql::Select, row_limit: u64) -> Result<QueryOperationResult> {
        select.row_limit = Some(row_limit);
        Ok(QueryOperationResult::PerformedRemotely(
            OperationCost::default(),
        ))
    }

    fn set_rows_to_skip(select: &mut sql::Select, row_skip: u64) -> Result<QueryOperationResult> {
        select.row_skip = row_skip;
        Ok(QueryOperationResult::PerformedRemotely(
            OperationCost::default(),
        ))
    }
}
