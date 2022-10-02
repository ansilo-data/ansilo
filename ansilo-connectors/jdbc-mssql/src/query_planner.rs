use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, ensure, Context, Result},
    sqlil as sql,
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

use super::{MssqlJdbcConnectorEntityConfig, MssqlJdbcEntitySourceConfig, MssqlJdbcQueryCompiler};

/// Maximum query params supported in a single query
const MAX_PARAMS: u16 = u16::MAX;

/// Query planner for Mssql JDBC driver
pub struct MssqlJdbcQueryPlanner {}

impl QueryPlanner for MssqlJdbcQueryPlanner {
    type TConnection = JdbcConnection;
    type TQuery = JdbcQuery;
    type TEntitySourceConfig = MssqlJdbcEntitySourceConfig;

    fn estimate_size(
        connection: &mut Self::TConnection,
        entity: &EntitySource<MssqlJdbcEntitySourceConfig>,
    ) -> Result<OperationCost> {
        // TODO: multiple sample options

        let tab = match &entity.source {
            MssqlJdbcEntitySourceConfig::Table(tab) => tab,
        };

        let mut query = connection.prepare(JdbcQuery::new(
            r#"
            SELECT SUM(p.rows) as row_count
            FROM sys.partitions AS p
            INNER JOIN sys.tables AS t ON p.[object_id] = t.[object_id]
            INNER JOIN sys.schemas AS s ON s.[schema_id] = t.[schema_id]
            WHERE s.name = ? 
            AND t.name = ? 
            AND p.index_id IN (0,1); -- 0:Heap, 1:Clustered
            "#,
            vec![
                QueryParam::constant(DataValue::Utf8String(tab.schema_name.clone())),
                QueryParam::constant(DataValue::Utf8String(tab.table_name.clone())),
            ],
        ))?;

        let mut result_set = query.execute_query()?.reader()?;
        let value = result_set
            .read_data_value()?
            .context("Unexpected empty result set")?;

        let num_rows = match value.clone().try_coerce_into(&DataType::UInt64) {
            Ok(DataValue::UInt64(num)) => Some(num),
            _ if value.is_null() => None,
            _ => bail!("Unexpected data value returned: {:?}", value),
        };

        let num_rows = if num_rows.is_none() {
            // If could not determine from information schema, fallback to COUNT(*)
            let table = MssqlJdbcQueryCompiler::compile_source_identifier(&entity.source)?;

            let mut query = connection.prepare(JdbcQuery::new(
                format!(r#"SELECT COUNT(*) FROM {}"#, table),
                vec![],
            ))?;

            let mut result_set = query.execute_query()?.reader()?;
            let value = result_set
                .read_data_value()?
                .context("Unexpected empty result set")?;

            match value.clone().try_coerce_into(&DataType::UInt64) {
                Ok(DataValue::UInt64(num)) => num,
                _ => bail!("Unexpected data value returned: {:?}", value),
            }
        } else {
            num_rows.unwrap()
        };

        Ok(OperationCost::new(Some(num_rows as _), None, None, None))
    }

    fn get_row_id_exprs(
        _connection: &mut Self::TConnection,
        _conf: &MssqlJdbcConnectorEntityConfig,
        entity: &EntitySource<MssqlJdbcEntitySourceConfig>,
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
        _conf: &MssqlJdbcConnectorEntityConfig,
        _entity: &EntitySource<MssqlJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)> {
        let select = sql::Select::new(source.clone());
        Ok((OperationCost::default(), select))
    }

    fn apply_select_operation(
        _connection: &mut Self::TConnection,
        _conf: &MssqlJdbcConnectorEntityConfig,
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
        _conf: &MssqlJdbcConnectorEntityConfig,
        _entity: &EntitySource<MssqlJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        Ok((OperationCost::default(), sql::Insert::new(source.clone())))
    }

    fn create_base_bulk_insert(
        _connection: &mut Self::TConnection,
        _conf: &MssqlJdbcConnectorEntityConfig,
        _entity: &EntitySource<MssqlJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::BulkInsert)> {
        Ok((
            OperationCost::default(),
            sql::BulkInsert::new(source.clone()),
        ))
    }

    fn create_base_update(
        _connection: &mut Self::TConnection,
        _conf: &MssqlJdbcConnectorEntityConfig,
        _entity: &EntitySource<MssqlJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)> {
        Ok((OperationCost::default(), sql::Update::new(source.clone())))
    }

    fn create_base_delete(
        _connection: &mut Self::TConnection,
        _conf: &MssqlJdbcConnectorEntityConfig,
        _entity: &EntitySource<MssqlJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)> {
        Ok((OperationCost::default(), sql::Delete::new(source.clone())))
    }

    fn get_insert_max_batch_size(
        _connection: &mut Self::TConnection,
        _conf: &MssqlJdbcConnectorEntityConfig,
        insert: &sql::Insert,
    ) -> Result<u32> {
        // @see https://dev.mssql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
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
        _conf: &MssqlJdbcConnectorEntityConfig,
        insert: &mut sql::Insert,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            InsertQueryOperation::AddColumn((col, expr)) => Self::insert_add_col(insert, col, expr),
        }
    }

    fn apply_bulk_insert_operation(
        _connection: &mut Self::TConnection,
        _conf: &MssqlJdbcConnectorEntityConfig,
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
        _conf: &MssqlJdbcConnectorEntityConfig,
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
        _conf: &MssqlJdbcConnectorEntityConfig,
        delete: &mut sql::Delete,
        op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            DeleteQueryOperation::AddWhere(cond) => Self::delete_add_where(delete, cond),
        }
    }

    fn explain_query(
        connection: &mut Self::TConnection,
        conf: &MssqlJdbcConnectorEntityConfig,
        query: &sql::Query,
        verbose: bool,
    ) -> Result<serde_json::Value> {
        let compiled = MssqlJdbcQueryCompiler::compile_query(connection, conf, query.clone())?;

        Ok(if verbose {
            serde_json::to_value(compiled)
        } else {
            serde_json::to_value(compiled.query)
        }?)
    }
}

impl MssqlJdbcQueryPlanner {
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
        if join.r#type == sql::JoinType::Full {
            return Ok(QueryOperationResult::Unsupported);
        }

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
            sql::Expr::BinaryOp(op) => match op.r#type {
                sql::BinaryOpType::Regexp => false,
                _ => true,
            },
            sql::Expr::Cast(cast) => match cast.r#type {
                DataType::Int8 => false,
                DataType::UInt16 => false,
                DataType::UInt32 => false,
                DataType::UInt64 => false,
                DataType::JSON => false,
                DataType::Uuid => false,
                _ => true,
            },
            _ => true,
        })
    }

    fn exprs_supported(expr: &[sql::Expr]) -> bool {
        expr.iter().all(Self::expr_supported)
    }
}

