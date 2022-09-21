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

use crate::InternalConnection;

use super::{InternalQuery, InternalQueryCompiler};

pub struct InternalQueryPlanner;

impl QueryPlanner for InternalQueryPlanner {
    type TConnection = InternalConnection;
    type TQuery = InternalQuery;
    type TEntitySourceConfig = ();

    fn estimate_size(
        _connection: &mut InternalConnection,
        _entity: &EntitySource<()>,
    ) -> Result<OperationCost> {
        Ok(OperationCost::new(Some(100), None, None, None))
    }

    fn get_row_id_exprs(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        _source: &sql::EntitySource,
    ) -> Result<Vec<(sql::Expr, DataType)>> {
        bail!("Unsupported")
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
        _source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::Insert)> {
        bail!("Unsupported")
    }

    fn create_base_bulk_insert(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _entity: &EntitySource<Self::TEntitySourceConfig>,
        _source: &sql::EntitySource,
    ) -> Result<(OperationCost, sql::BulkInsert)> {
        bail!("Unsupported")
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
            SelectQueryOperation::AddColumn((alias, sql::Expr::Attribute(att))) => {
                select.cols.push((alias, sql::Expr::Attribute(att)));
                Ok(QueryOperationResult::Ok(OperationCost::default()))
            }
            _ => Ok(QueryOperationResult::Unsupported),
        }
    }

    fn get_insert_max_batch_size(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _insert: &sql::Insert,
    ) -> Result<u32> {
        bail!("Unsupported")
    }

    fn apply_insert_operation(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _insert: &mut sql::Insert,
        _op: InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        Ok(QueryOperationResult::Unsupported)
    }

    fn apply_bulk_insert_operation(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _bulk_insert: &mut sql::BulkInsert,
        _op: BulkInsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        Ok(QueryOperationResult::Unsupported)
    }

    fn apply_update_operation(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _update: &mut sql::Update,
        _op: UpdateQueryOperation,
    ) -> Result<QueryOperationResult> {
        Ok(QueryOperationResult::Unsupported)
    }

    fn apply_delete_operation(
        _connection: &mut Self::TConnection,
        _conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        _delete: &mut sql::Delete,
        _op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        Ok(QueryOperationResult::Unsupported)
    }

    fn explain_query(
        connection: &mut Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        query: &sql::Query,
        _verbose: bool,
    ) -> Result<serde_json::Value> {
        let compiled = InternalQueryCompiler::compile_query(connection, conf, query.clone())?;

        Ok(serde_json::to_value(compiled)?)
    }
}
