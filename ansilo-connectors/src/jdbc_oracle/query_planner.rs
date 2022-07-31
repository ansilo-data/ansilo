use ansilo_core::{
    data::{DataType, DataValue, StringOptions},
    err::{bail, Context, Result},
    sqlil as sql,
};

use crate::{
    common::{data::ResultSetReader, entity::EntitySource},
    interface::{
        Connection, DeleteQueryOperation, InsertQueryOperation, OperationCost, QueryCompiler,
        QueryHandle, QueryOperationResult, QueryPlanner, SelectQueryOperation,
        UpdateQueryOperation,
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
    ) -> Result<OperationCost> {
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

        Ok(OperationCost::new(Some(num_rows as _), None, None, None))
    }

    fn get_row_id_exprs(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        _entity: &EntitySource<OracleJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<Vec<(sql::Expr, DataType)>> {
        Ok(vec![(
            sql::Expr::attr(source.alias.clone(), "ROWID"),
            DataType::Utf8String(StringOptions::default()),
        )])
    }

    fn create_base_select(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        _entity: &EntitySource<OracleJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)> {
        let select = sql::Select::new(source.clone());
        Ok((OperationCost::default(), select))
    }

    fn apply_select_operation(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
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

    fn create_base_insert(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        entity: &EntitySource<OracleJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        match &entity.source_conf {
            OracleJdbcEntitySourceConfig::Table(_) => {}
            OracleJdbcEntitySourceConfig::CustomQueries(q) if q.insert_query.is_some() => {}
            _ => bail!(
                "Cannot perform insert on entity without source table or insert query defined"
            ),
        }

        Ok((OperationCost::default(), sql::Insert::new(source.clone())))
    }

    fn create_base_update(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        entity: &EntitySource<OracleJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)> {
        match &entity.source_conf {
            OracleJdbcEntitySourceConfig::Table(_) => {}
            OracleJdbcEntitySourceConfig::CustomQueries(q) if q.update_query.is_some() => {}
            _ => bail!(
                "Cannot perform update on entity without source table or update query defined"
            ),
        }

        Ok((OperationCost::default(), sql::Update::new(source.clone())))
    }

    fn create_base_delete(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        entity: &EntitySource<OracleJdbcEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)> {
        match &entity.source_conf {
            OracleJdbcEntitySourceConfig::Table(_) => {}
            OracleJdbcEntitySourceConfig::CustomQueries(q) if q.delete_query.is_some() => {}
            _ => bail!(
                "Cannot perform delete on entity without source table or delete query defined"
            ),
        }

        Ok((OperationCost::default(), sql::Delete::new(source.clone())))
    }

    fn apply_insert_operation(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        insert: &mut sql::Insert,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            InsertQueryOperation::AddColumn((col, expr)) => Self::insert_add_col(insert, col, expr),
        }
    }

    fn apply_update_operation(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        update: &mut sql::Update,
        op: UpdateQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            UpdateQueryOperation::AddSet((col, expr)) => Self::update_add_set(update, col, expr),
            UpdateQueryOperation::AddWhere(cond) => Self::update_add_where(update, cond),
        }
    }

    fn apply_delete_operation(
        _connection: &JdbcConnection,
        _conf: &OracleJdbcConnectorEntityConfig,
        delete: &mut sql::Delete,
        op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            DeleteQueryOperation::AddWhere(cond) => Self::delete_add_where(delete, cond),
        }
    }

    fn explain_query(
        connection: &JdbcConnection,
        conf: &OracleJdbcConnectorEntityConfig,
        query: &sql::Query,
        verbose: bool,
    ) -> Result<serde_json::Value> {
        let compiled = OracleJdbcQueryCompiler::compile_query(connection, conf, query.clone())?;

        Ok(if verbose {
            serde_json::to_value(compiled)
        } else {
            serde_json::to_value(compiled.query)
        }?)
    }
}

impl OracleJdbcQueryPlanner {
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

// TODO: tests
