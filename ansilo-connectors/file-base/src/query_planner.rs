use std::marker::PhantomData;

use ansilo_core::{
    data::DataType,
    err::{bail, ensure, Context, Result},
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

use crate::{FileIO, FileSourceConfig};

use super::{FileConnection, FileQuery, FileQueryCompiler};

pub struct FileQueryPlanner<F: FileIO> {
    _io: PhantomData<F>,
}

impl<F: FileIO> QueryPlanner for FileQueryPlanner<F> {
    type TConnection = FileConnection<F>;
    type TQuery = FileQuery;
    type TEntitySourceConfig = FileSourceConfig;

    fn estimate_size(
        con: &mut FileConnection<F>,
        entity: &EntitySource<FileSourceConfig>,
    ) -> Result<OperationCost> {
        let path = entity.source.path(con.conf());

        let row_count = if path
            .try_exists()
            .context("Failed to check if file exists")?
        {
            F::estimate_row_count(con.conf(), &path)?
        } else {
            Some(0)
        };

        Ok(OperationCost::new(row_count, None, None, None))
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
        con: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Select)> {
        ensure!(
            F::supports_reading(con.conf(), &entity.source.path(con.conf()))?,
            "Reading is not supported"
        );

        let select = sql::Select::new(source.clone());
        Ok((OperationCost::default(), select))
    }

    fn create_base_insert(
        con: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        ensure!(
            F::supports_writing(con.conf(), &entity.source.path(con.conf()))?,
            "Writing is not supported"
        );

        Ok((OperationCost::default(), sql::Insert::new(source.clone())))
    }

    fn create_base_bulk_insert(
        con: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::BulkInsert)> {
        ensure!(
            F::supports_writing(con.conf(), &entity.source.path(con.conf()))?,
            "Writing is not supported"
        );

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
        con: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
        source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Delete)> {
        ensure!(
            F::supports_truncating(con.conf(), &entity.source.path(con.conf()))?,
            "Truncating is not supported"
        );

        Ok((OperationCost::default(), sql::Delete::new(source.clone())))
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
        let compiled = FileQueryCompiler::compile_query(connection, conf, query.clone())?;

        Ok(serde_json::to_value(compiled)?)
    }
}

impl<F: FileIO> FileQueryPlanner<F> {
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
