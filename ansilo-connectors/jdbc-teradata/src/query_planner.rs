use ansilo_core::{
    data::{rust_decimal::prelude::ToPrimitive, DataType, DataValue},
    err::{bail, ensure, Context, Result},
    sqlil::{self as sql, AggregateCall},
};

use ansilo_connectors_base::{
    common::{entity::EntitySource, query::QueryParam},
    interface::{
        BulkInsertQueryOperation, Connection, DeleteQueryOperation, InsertQueryOperation,
        OperationCost, QueryCompiler, QueryHandle, QueryOperationResult, QueryPlanner, ResultSet,
        SelectQueryOperation, UpdateQueryOperation,
    },
};

use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcQuery};

use crate::TeradataJdbcTableOptions;

use super::{
    TeradataJdbcConnectorEntityConfig, TeradataJdbcEntitySourceConfig, TeradataJdbcQueryCompiler,
};

// Maximum query params supported by teradata in a single query
// @see https://www.docs.teradata.com/r/bBJcqMYyoxECDlJRAz9Dgw/8OqmibJKUccjW6Sb4d4CNQ
const MAX_PARAMS: u32 = 2500;

/// Query planner for Teradata JDBC driver
pub struct TeradataJdbcQueryPlanner {}

impl QueryPlanner for TeradataJdbcQueryPlanner {
    type TConnection = JdbcConnection;
    type TQuery = JdbcQuery;
    type TEntitySourceConfig = TeradataJdbcEntitySourceConfig;

    fn estimate_size(
        connection: &mut Self::TConnection,
        entity: &EntitySource<TeradataJdbcEntitySourceConfig>,
    ) -> Result<OperationCost> {
        let table = match &entity.source {
            TeradataJdbcEntitySourceConfig::Table(t) => t,
        };

        let value = Self::estimate_row_size_using_table_stats(connection, table)
            .or_else(|_| Self::estimate_row_size_using_count(connection, &entity.source))?;

        let num_rows = match value {
            DataValue::Float64(count) => count.ceil().to_u64().unwrap_or(0),
            DataValue::Int64(count) => count as _,
            DataValue::Int32(count) => count as _,
            _ => bail!("Unexpected data value returned: {:?}", value),
        };

        Ok(OperationCost::new(Some(num_rows as _), None, None, None))
    }

    fn get_row_id_exprs(
        _connection: &mut Self::TConnection,
        _conf: &TeradataJdbcConnectorEntityConfig,
        entity: &EntitySource<TeradataJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<Vec<(sql::Expr, DataType)>> {
        let primary_keys = entity.conf.primary_keys();

        if primary_keys.is_empty() {
            bail!("Cannot perform operation on table without primary keys");
        }

        Ok(primary_keys
            .into_iter()
            .map(|a| {
                (
                    sql::Expr::attr(source.alias.clone(), &a.id),
                    a.r#type.clone(),
                )
            })
            .collect())
    }

    fn create_base_select(
        _connection: &mut Self::TConnection,
        _conf: &TeradataJdbcConnectorEntityConfig,
        _entity: &EntitySource<TeradataJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)> {
        let select = sql::Select::new(source.clone());
        Ok((OperationCost::default(), select))
    }

    fn apply_select_operation(
        _connection: &mut Self::TConnection,
        _conf: &TeradataJdbcConnectorEntityConfig,
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
        _conf: &TeradataJdbcConnectorEntityConfig,
        _entity: &EntitySource<TeradataJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        Ok((OperationCost::default(), sql::Insert::new(source.clone())))
    }

    fn create_base_bulk_insert(
        _connection: &mut Self::TConnection,
        _conf: &TeradataJdbcConnectorEntityConfig,
        _entity: &EntitySource<TeradataJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::BulkInsert)> {
        Ok((
            OperationCost::default(),
            sql::BulkInsert::new(source.clone()),
        ))
    }

    fn create_base_update(
        _connection: &mut Self::TConnection,
        _conf: &TeradataJdbcConnectorEntityConfig,
        _entity: &EntitySource<TeradataJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)> {
        Ok((OperationCost::default(), sql::Update::new(source.clone())))
    }

    fn create_base_delete(
        _connection: &mut Self::TConnection,
        _conf: &TeradataJdbcConnectorEntityConfig,
        _entity: &EntitySource<TeradataJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)> {
        Ok((OperationCost::default(), sql::Delete::new(source.clone())))
    }

    fn get_insert_max_batch_size(
        _connection: &mut Self::TConnection,
        _conf: &TeradataJdbcConnectorEntityConfig,
        insert: &sql::Insert,
    ) -> Result<u32> {
        // @see https://docs.teradata.com/cd/B10501_01/appdev.920/a96624/e_limits.htm#LNPLS018
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
        _conf: &TeradataJdbcConnectorEntityConfig,
        insert: &mut sql::Insert,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            InsertQueryOperation::AddColumn((col, expr)) => Self::insert_add_col(insert, col, expr),
        }
    }

    fn apply_bulk_insert_operation(
        _connection: &mut Self::TConnection,
        _conf: &TeradataJdbcConnectorEntityConfig,
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
        _conf: &TeradataJdbcConnectorEntityConfig,
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
        _conf: &TeradataJdbcConnectorEntityConfig,
        delete: &mut sql::Delete,
        op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            DeleteQueryOperation::AddWhere(cond) => Self::delete_add_where(delete, cond),
        }
    }

    fn explain_query(
        connection: &mut Self::TConnection,
        conf: &TeradataJdbcConnectorEntityConfig,
        query: &sql::Query,
        verbose: bool,
    ) -> Result<serde_json::Value> {
        let compiled = TeradataJdbcQueryCompiler::compile_query(connection, conf, query.clone())?;

        Ok(if verbose {
            serde_json::to_value(compiled)
        } else {
            serde_json::to_value(compiled.query)
        }?)
    }
}

impl TeradataJdbcQueryPlanner {
    fn estimate_row_size_using_table_stats(
        connection: &mut JdbcConnection,
        table: &TeradataJdbcTableOptions,
    ) -> Result<DataValue> {
        let mut query = connection.prepare(JdbcQuery::new(
            r#"
            SELECT RowCount FROM DBC.TableStatsV
            WHERE DatabaseName = ? AND TableName = ?
            "#,
            vec![
                QueryParam::Constant(DataValue::Utf8String(table.database_name.clone())),
                QueryParam::Constant(DataValue::Utf8String(table.table_name.clone())),
            ],
        ))?;

        let mut result_set = query.execute_query()?.reader()?;

        let value = result_set
            .read_data_value()?
            .context("Unexpected empty result set")?;

        Ok(value)
    }

    fn estimate_row_size_using_count(
        connection: &mut JdbcConnection,
        source: &TeradataJdbcEntitySourceConfig,
    ) -> Result<DataValue> {
        let table = TeradataJdbcQueryCompiler::compile_source_identifier(source)?;

        let mut query = connection.prepare(JdbcQuery::new(
            format!(r#"SELECT COUNT(*) FROM {table}"#),
            vec![],
        ))?;

        let mut result_set = query.execute_query()?.reader()?;

        let value = result_set
            .read_data_value()?
            .context("Unexpected empty result set")?;

        Ok(value)
    }

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
        _select: &mut sql::Select,
        _row_skip: u64,
    ) -> Result<QueryOperationResult> {
        Ok(QueryOperationResult::Unsupported)
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
            sql::Expr::BinaryOp(op) => match &op.r#type {
                sql::BinaryOpType::Regexp => false,
                sql::BinaryOpType::NullSafeEqual => false,
                _ => true,
            },
            sql::Expr::Cast(cast) => match &cast.r#type {
                DataType::Float32 => false,
                DataType::Uuid => false,
                DataType::Time => false,
                DataType::UInt8 => false,
                DataType::UInt16 => false,
                DataType::UInt32 => false,
                DataType::UInt64 => false,
                _ => true,
            },
            sql::Expr::AggregateCall(call) => match call {
                AggregateCall::StringAgg(_) => false,
                _ => true,
            },
            _ => true,
        })
    }

    fn exprs_supported(expr: &[sql::Expr]) -> bool {
        expr.iter().all(Self::expr_supported)
    }
}
