use ansilo_core::{
    data::DataType,
    err::{Error, Result},
    sqlil as sql,
};

use crate::{
    common::entity::{ConnectorEntityConfig, EntitySource},
    interface::{
        DeleteQueryOperation, InsertQueryOperation, OperationCost, QueryCompiler,
        QueryOperationResult, QueryPlanner, SelectQueryOperation, UpdateQueryOperation,
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

    fn get_row_id_exprs(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<Vec<(sql::Expr, DataType)>> {
        Ok(vec![(
            sql::Expr::attr(source.alias.clone(), "ROWIDX"),
            DataType::UInt64,
        )])
    }

    fn create_base_select(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)> {
        let select = sql::Select::new(source.clone());
        Ok((OperationCost::default(), select))
    }

    fn create_base_insert(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        Ok((OperationCost::default(), sql::Insert::new(source.clone())))
    }

    fn create_base_update(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)> {
        Ok((OperationCost::default(), sql::Update::new(source.clone())))
    }

    fn create_base_delete(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)> {
        Ok((OperationCost::default(), sql::Delete::new(source.clone())))
    }

    fn apply_select_operation(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        select: &mut sql::Select,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            SelectQueryOperation::AddColumn((alias, expr)) => {
                Self::select_add_col(select, expr, alias)
            }
            SelectQueryOperation::AddWhere(expr) => Self::select_add_where(select, expr),
            SelectQueryOperation::AddJoin(join) => Self::select_add_join(select, join),
            SelectQueryOperation::AddGroupBy(expr) => Self::select_add_group_by(select, expr),
            SelectQueryOperation::AddOrderBy(ordering) => {
                Self::select_add_ordering(select, ordering)
            }
            SelectQueryOperation::SetRowLimit(limit) => Self::select_set_row_limit(select, limit),
            SelectQueryOperation::SetRowOffset(offset) => {
                Self::select_set_rows_to_skip(select, offset)
            }
        }
    }

    fn apply_insert_operation(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        insert: &mut sql::Insert,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            InsertQueryOperation::AddColumn((col, expr)) => Self::insert_add_col(insert, col, expr),
        }
    }

    fn apply_update_operation(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        update: &mut sql::Update,
        op: UpdateQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            UpdateQueryOperation::AddSet((col, expr)) => Self::update_add_set(update, col, expr),
            UpdateQueryOperation::AddWhere(cond) => Self::update_add_where(update, cond),
        }
    }

    fn apply_delete_operation(
        _connection: &Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        delete: &mut sql::Delete,
        op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            DeleteQueryOperation::AddWhere(cond) => Self::delete_add_where(delete, cond),
        }
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
    fn select_add_col(
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        select.cols.push((alias, expr));
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_add_where(select: &mut sql::Select, expr: sql::Expr) -> Result<QueryOperationResult> {
        select.r#where.push(expr);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_add_join(select: &mut sql::Select, join: sql::Join) -> Result<QueryOperationResult> {
        select.joins.push(join);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_add_group_by(
        select: &mut sql::Select,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        select.group_bys.push(expr);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_add_ordering(
        select: &mut sql::Select,
        ordering: sql::Ordering,
    ) -> Result<QueryOperationResult> {
        select.order_bys.push(ordering);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_set_row_limit(
        select: &mut sql::Select,
        row_limit: u64,
    ) -> Result<QueryOperationResult> {
        select.row_limit = Some(row_limit);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_set_rows_to_skip(
        select: &mut sql::Select,
        row_skip: u64,
    ) -> Result<QueryOperationResult> {
        select.row_skip = row_skip;
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn insert_add_col(
        insert: &mut sql::Insert,
        col: String,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        insert.cols.push((col, expr));
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn update_add_set(
        update: &mut sql::Update,
        col: String,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        update.cols.push((col, expr));
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn update_add_where(update: &mut sql::Update, cond: sql::Expr) -> Result<QueryOperationResult> {
        update.r#where.push(cond);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn delete_add_where(delete: &mut sql::Delete, cond: sql::Expr) -> Result<QueryOperationResult> {
        delete.r#where.push(cond);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }
}
