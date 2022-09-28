use std::fs;

use ansilo_core::{
    data::DataType,
    err::{bail, Result},
    sqlil as sql,
};

use ansilo_connectors_base::{
    common::entity::{ConnectorEntityConfig, EntitySource},
    interface::{
        BulkInsertQueryOperation, DeleteQueryOperation, InsertQueryOperation, OperationCost,
        QueryCompiler, QueryOperationResult, QueryPlanner, SelectQueryOperation,
        UpdateQueryOperation,
    },
};
use apache_avro::Schema;

use super::{ArvoConnection, ArvoFile, ArvoQuery, ArvoQueryCompiler};

pub struct ArvoQueryPlanner {}

impl QueryPlanner for ArvoQueryPlanner {
    type TConnection = ArvoConnection;
    type TQuery = ArvoQuery;
    type TEntitySourceConfig = ArvoFile;

    fn estimate_size(
        _connection: &mut ArvoConnection,
        entity: &EntitySource<ArvoFile>,
    ) -> Result<OperationCost> {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(entity.source.path())?;
        let total_len = file.metadata()?.len();
        let reader = apache_avro::Reader::new(file)?;
        let schema = reader.writer_schema();
        let row_len = Self::estimate_bytes(schema);

        Ok(OperationCost::new(
            Some(total_len / row_len),
            Some(row_len as _),
            None,
            None,
        ))
    }

    fn get_row_id_exprs(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        _source: &sql::EntitySource,
    ) -> Result<Vec<(sql::Expr, DataType)>> {
        bail!("Unsupported");
    }

    fn create_base_select(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)> {
        let select = sql::Select::new(source.clone());
        Ok((OperationCost::default(), select))
    }

    fn create_base_insert(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        Ok((OperationCost::default(), sql::Insert::new(source.clone())))
    }

    fn create_base_bulk_insert(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::BulkInsert)> {
        Ok((
            OperationCost::default(),
            sql::BulkInsert::new(source.clone()),
        ))
    }

    fn create_base_update(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        _source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Update)> {
        bail!("Unsupported")
    }

    fn create_base_delete(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        _source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)> {
        bail!("Unsupported")
    }

    fn apply_select_operation(
        _con: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        select: &mut sql::Select,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            SelectQueryOperation::AddColumn((alias, expr)) => {
                Self::select_add_col(select, expr, alias)
            }
            _ => Ok(QueryOperationResult::Unsupported),
        }
    }

    fn get_insert_max_batch_size(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _insert: &sql::Insert,
    ) -> Result<u32> {
        Ok(1000)
    }

    fn apply_insert_operation(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        insert: &mut sql::Insert,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            InsertQueryOperation::AddColumn((col, expr)) => Self::insert_add_col(insert, col, expr),
        }
    }

    fn apply_bulk_insert_operation(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
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
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _update: &mut sql::Update,
        _op: UpdateQueryOperation,
    ) -> Result<QueryOperationResult> {
        bail!("Unsupported")
    }

    fn apply_delete_operation(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _delete: &mut sql::Delete,
        _op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        bail!("Unsupported")
    }

    fn explain_query(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        query: &sql::Query,
        _verbose: bool,
    ) -> Result<serde_json::Value> {
        let compiled = ArvoQueryCompiler::compile_query(connection, conf, query.clone())?;

        Ok(serde_json::to_value(compiled)?)
    }
}

impl ArvoQueryPlanner {
    fn estimate_bytes(schema: &Schema) -> u64 {
        match &schema {
            Schema::Null => 1,
            Schema::Boolean => 1,
            Schema::Int => 4,
            Schema::Long => 8,
            Schema::Float => 4,
            Schema::Double => 8,
            Schema::Bytes => 50,
            Schema::String => 50,
            Schema::Array(_) => 100,
            Schema::Map(_) => 200,
            Schema::Union(u) => u
                .variants()
                .iter()
                .map(|s| Self::estimate_bytes(s))
                .max()
                .unwrap(),
            Schema::Record { fields, .. } => {
                fields.iter().map(|f| Self::estimate_bytes(&f.schema)).sum()
            }
            Schema::Enum { .. } => 20,
            Schema::Fixed { size, .. } => (*size) as _,
            Schema::Decimal { .. } => 10,
            Schema::Uuid => 16,
            Schema::Date => 12,
            Schema::TimeMillis => 8,
            Schema::TimeMicros => 8,
            Schema::TimestampMillis => 14,
            Schema::TimestampMicros => 18,
            Schema::Duration => 12,
            Schema::Ref { .. } => 10,
        }
    }

    fn select_add_col(
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult> {
        if expr.as_attribute().is_none() {
            return Ok(QueryOperationResult::Unsupported);
        }

        select.cols.push((alias, expr));
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }

    fn insert_add_col(
        insert: &mut sql::Insert,
        col: String,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult> {
        if expr.as_parameter().is_none() {
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
        for expr in values.iter() {
            if expr.as_parameter().is_none() {
                return Ok(QueryOperationResult::Unsupported);
            }
        }

        bulk_insert.cols = cols;
        bulk_insert.values = values;
        Ok(QueryOperationResult::Ok(OperationCost::default()))
    }
}
