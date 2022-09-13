use std::{marker::PhantomData, ops::DerefMut};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, ensure, Context, Result},
    sqlil as sql,
};

use ansilo_connectors_base::{
    common::entity::EntitySource,
    interface::{
        BulkInsertQueryOperation, Connection, DeleteQueryOperation, InsertQueryOperation,
        OperationCost, QueryCompiler, QueryHandle, QueryOperationResult, QueryPlanner, ResultSet,
        SelectQueryOperation, UpdateQueryOperation,
    },
};

use tokio_postgres::Client;

use crate::{
    PostgresConnection, PostgresConnectorEntityConfig, PostgresEntitySourceConfig, PostgresQuery,
    PostgresQueryCompiler,
};

/// Maximum query params supported in a single query
const MAX_PARAMS: u16 = u16::MAX;

/// Query planner for Postgres driver
pub struct PostgresQueryPlanner<T> {
    _t: PhantomData<T>,
}

impl<T: DerefMut<Target = Client>> QueryPlanner for PostgresQueryPlanner<T> {
    type TConnection = PostgresConnection<T>;
    type TQuery = PostgresQuery;
    type TEntitySourceConfig = PostgresEntitySourceConfig;

    fn estimate_size(
        connection: &mut Self::TConnection,
        entity: &EntitySource<PostgresEntitySourceConfig>,
    ) -> Result<OperationCost> {
        let mut query = connection.prepare(PostgresQuery::new(
            format!(
                r#"EXPLAIN (FORMAT JSON) SELECT * FROM {}"#,
                PostgresQueryCompiler::<T>::compile_source_identifier(&entity.source)?
            ),
            vec![],
        ))?;

        let mut result_set = query.execute_query()?.reader()?;
        let value = result_set
            .read_data_value()?
            .context("Unexpected empty result set")?;

        let plan = match value.clone() {
            DataValue::JSON(plan) => plan,
            _ => bail!("Unexpected data value returned: {:?}", value),
        };

        let plan: serde_json::Value = serde_json::from_str(&plan)?;
        let plan = plan
            .as_array()
            .context("Expected array")?
            .get(0)
            .context("Expected not empty")?
            .as_object()
            .context("Expected object")?
            .get("Plan")
            .context("Expected Plan key")?
            .as_object()
            .context("Expected object")?;

        let num_rows = plan
            .get("Plan Rows")
            .context("Expected Plan Rows key")?
            .as_u64()
            .context("Expected row count integer")?;

        let width = plan
            .get("Plan Width")
            .context("Expected Plan Width key")?
            .as_u64()
            .context("Expected width integer")?;

        let startup_cost = plan
            .get("Startup Cost")
            .context("Expected Startup Cost key")?
            .as_f64()
            .context("Expected startup cost float64")?;

        let total_cost = plan
            .get("Total Cost")
            .context("Expected Total Cost key")?
            .as_f64()
            .context("Expected total cost float64")?;

        Ok(OperationCost::new(
            Some(num_rows as _),
            Some(width as _),
            Some(startup_cost),
            Some(total_cost),
        ))
    }

    fn get_row_id_exprs(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        _entity: &EntitySource<PostgresEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<Vec<(sql::Expr, DataType)>> {
        Ok(vec![(
            sql::Expr::attr(source.alias.clone(), "ctid"),
            DataType::Binary,
        )])
    }

    fn create_base_select(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        _entity: &EntitySource<PostgresEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)> {
        let select = sql::Select::new(source.clone());
        Ok((OperationCost::default(), select))
    }

    fn apply_select_operation(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
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
            SelectQueryOperation::SetRowLockMode(mode) => {
                Self::select_set_row_lock_mode(select, mode)
            }
        }
    }

    fn create_base_insert(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        _entity: &EntitySource<PostgresEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        Ok((OperationCost::default(), sql::Insert::new(source.clone())))
    }

    fn create_base_bulk_insert(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        _entity: &EntitySource<PostgresEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::BulkInsert)> {
        Ok((
            OperationCost::default(),
            sql::BulkInsert::new(source.clone()),
        ))
    }

    fn create_base_update(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        _entity: &EntitySource<PostgresEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)> {
        Ok((OperationCost::default(), sql::Update::new(source.clone())))
    }

    fn create_base_delete(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        _entity: &EntitySource<PostgresEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)> {
        Ok((OperationCost::default(), sql::Delete::new(source.clone())))
    }

    fn get_insert_max_batch_size(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        insert: &sql::Insert,
    ) -> Result<u32> {
        // @see https://doxygen.postgresql.org/libpq-fe_8h.html#afcd90c8ad3fd816d18282eb622678c25
        let params: usize = insert
            .cols
            .iter()
            .map(|row| row.1.walk_count(|e| e.as_parameter().is_some()))
            .sum();

        if params == 0 {
            return Ok(u32::MAX);
        }

        Ok((MAX_PARAMS as f32 / params as f32).floor() as _)
    }

    fn apply_insert_operation(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        insert: &mut sql::Insert,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            InsertQueryOperation::AddColumn((col, expr)) => Self::insert_add_col(insert, col, expr),
        }
    }

    fn apply_bulk_insert_operation(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        bulk_insert: &mut sql::BulkInsert,
        op: BulkInsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            BulkInsertQueryOperation::SetBulkRows((cols, values)) => {
                Self::bulk_insert_add_rows(bulk_insert, cols, values)
            }
        }
    }

    fn apply_update_operation(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        update: &mut sql::Update,
        op: UpdateQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            UpdateQueryOperation::AddSet((col, expr)) => Self::update_add_set(update, col, expr),
            UpdateQueryOperation::AddWhere(cond) => Self::update_add_where(update, cond),
        }
    }

    fn apply_delete_operation(
        _connection: &mut Self::TConnection,
        _conf: &PostgresConnectorEntityConfig,
        delete: &mut sql::Delete,
        op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            DeleteQueryOperation::AddWhere(cond) => Self::delete_add_where(delete, cond),
        }
    }

    fn explain_query(
        connection: &mut Self::TConnection,
        conf: &PostgresConnectorEntityConfig,
        query: &sql::Query,
        verbose: bool,
    ) -> Result<serde_json::Value> {
        let compiled = PostgresQueryCompiler::<T>::compile_query(connection, conf, query.clone())?;

        Ok(if verbose {
            serde_json::to_value(compiled)
        } else {
            serde_json::to_value(compiled.sql)
        }?)
    }
}

impl<T: DerefMut<Target = Client>> PostgresQueryPlanner<T> {
    fn select_add_col(
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        if !Self::expr_supported(&expr) {
            return Ok(QueryOperationResult::Unsupported);
        }

        select.cols.push((alias, expr));
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_add_where(select: &mut sql::Select, expr: sql::Expr) -> Result<QueryOperationResult> {
        if !Self::expr_supported(&expr) {
            return Ok(QueryOperationResult::Unsupported);
        }

        select.r#where.push(expr);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_add_join(select: &mut sql::Select, join: sql::Join) -> Result<QueryOperationResult> {
        if !Self::exprs_supported(&join.conds[..]) {
            return Ok(QueryOperationResult::Unsupported);
        }

        select.joins.push(join);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_add_group_by(
        select: &mut sql::Select,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        if !Self::expr_supported(&expr) {
            return Ok(QueryOperationResult::Unsupported);
        }

        select.group_bys.push(expr);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn select_add_ordering(
        select: &mut sql::Select,
        ordering: sql::Ordering,
    ) -> Result<QueryOperationResult> {
        if !Self::expr_supported(&ordering.expr) {
            return Ok(QueryOperationResult::Unsupported);
        }

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

    fn select_set_row_lock_mode(
        select: &mut sql::Select,
        mode: sql::SelectRowLockMode,
    ) -> Result<QueryOperationResult> {
        select.row_lock = mode;
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn insert_add_col(
        insert: &mut sql::Insert,
        col: String,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        if !Self::expr_supported(&expr) {
            return Ok(QueryOperationResult::Unsupported);
        }

        insert.cols.push((col, expr));
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn bulk_insert_add_rows(
        bulk_insert: &mut sql::BulkInsert,
        cols: Vec<String>,
        values: Vec<sql::Expr>,
    ) -> Result<QueryOperationResult> {
        if !Self::exprs_supported(&values) {
            return Ok(QueryOperationResult::Unsupported);
        }

        let params = values
            .iter()
            .map(|e| e.walk_count(|e| e.as_parameter().is_some()))
            .sum::<usize>();

        if params > MAX_PARAMS as _ {
            return Ok(QueryOperationResult::Unsupported);
        }

        ensure!(values.len() % cols.len() == 0);

        bulk_insert.cols = cols;
        bulk_insert.values = values;
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn update_add_set(
        update: &mut sql::Update,
        col: String,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        if !Self::expr_supported(&expr) {
            return Ok(QueryOperationResult::Unsupported);
        }

        update.cols.push((col, expr));
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn update_add_where(update: &mut sql::Update, cond: sql::Expr) -> Result<QueryOperationResult> {
        if !Self::expr_supported(&cond) {
            return Ok(QueryOperationResult::Unsupported);
        }

        update.r#where.push(cond);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn delete_add_where(delete: &mut sql::Delete, cond: sql::Expr) -> Result<QueryOperationResult> {
        if !Self::expr_supported(&cond) {
            return Ok(QueryOperationResult::Unsupported);
        }

        delete.r#where.push(cond);
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn expr_supported(expr: &sql::Expr) -> bool {
        expr.walk_all(|e| match e {
            _ => true,
        })
    }

    fn exprs_supported(expr: &[sql::Expr]) -> bool {
        expr.iter().all(Self::expr_supported)
    }
}

// TODO: tests
