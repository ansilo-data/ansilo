use std::sync::Arc;

use ansilo_core::{
    config::EntityConfig,
    data::DataType,
    err::{anyhow, Context, Error, Result},
    sqlil,
};
use ansilo_pg::fdw::proto::{ClientMessage, OperationCost, ServerMessage};
use pgx::pg_sys::Oid;

use crate::{fdw::common::FdwIpcConnection, sqlil::ConversionContext};

use super::{
    FdwDeleteQuery, FdwInsertQuery, FdwQueryContext, FdwQueryType, FdwSelectQuery, FdwUpdateQuery,
};

/// Context data for query planning
pub struct FdwContext {
    /// The connection state to ansilo
    pub connection: Arc<FdwIpcConnection>,
    /// The ID of the data source for this FDW connection
    pub data_source_id: String,
    /// The initial entity of fdw context
    pub entity: sqlil::EntityId,
}

impl FdwContext {
    pub fn new(connection: Arc<FdwIpcConnection>, entity: sqlil::EntityId) -> Self {
        let data_source_id = connection.data_source_id.clone();

        Self {
            connection,
            data_source_id,
            entity,
        }
    }

    fn send(&mut self, req: ClientMessage) -> Result<ServerMessage> {
        self.connection.send(req)
    }

    pub fn discover_entities(&mut self) -> Result<Vec<EntityConfig>> {
        let res = self.send(ClientMessage::DiscoverEntities).unwrap();

        let entities = match res {
            ServerMessage::DiscoveredEntitiesResult(e) => e,
            _ => return Err(unexpected_response(res).context("Discover Entities")),
        };

        Ok(entities)
    }

    pub fn estimate_size(&mut self) -> Result<OperationCost> {
        let res = self
            .send(ClientMessage::EstimateSize(self.entity.clone()))
            .unwrap();

        let base_cost = match res {
            ServerMessage::EstimatedSizeResult(e) => e,
            _ => return Err(unexpected_response(res).context("Estimate Size")),
        };

        Ok(base_cost)
    }

    pub fn get_row_id_exprs(&mut self, alias: &str) -> Result<Vec<(sqlil::Expr, DataType)>> {
        let res = self
            .send(ClientMessage::GetRowIds(sqlil::EntitySource::new(
                self.entity.clone(),
                alias,
            )))
            .unwrap();

        let row_ids = match res {
            ServerMessage::RowIds(e) => e,
            _ => return Err(unexpected_response(res).context("Getting row id's")),
        };

        Ok(row_ids)
    }

    pub fn create_query(
        &mut self,
        varno: Oid,
        r#type: sqlil::QueryType,
    ) -> Result<FdwQueryContext> {
        let mut cvt = ConversionContext::new();
        let alias = cvt.register_alias(varno);

        let (query_id, cost) = self
            .send(ClientMessage::CreateQuery(
                sqlil::EntitySource::new(self.entity.clone(), alias),
                r#type,
            ))
            .and_then(|res| match res {
                ServerMessage::QueryCreated(query_id, cost) => Ok((query_id, cost)),
                _ => return Err(unexpected_response(res)),
            })
            .context("Creating query")
            .unwrap();

        let query = match r#type {
            sqlil::QueryType::Select => FdwQueryType::Select(FdwSelectQuery::default()),
            sqlil::QueryType::Insert => FdwQueryType::Insert(FdwInsertQuery::default()),
            sqlil::QueryType::Update => FdwQueryType::Update(FdwUpdateQuery::default()),
            sqlil::QueryType::Delete => FdwQueryType::Delete(FdwDeleteQuery::default()),
        };

        let query = FdwQueryContext::new(
            Arc::clone(&self.connection),
            query_id,
            varno,
            query,
            cost,
            cvt,
        );

        Ok(query)
    }
}

fn unexpected_response(response: ServerMessage) -> Error {
    if let ServerMessage::Error(message) = response {
        anyhow!("Error from server: {message}")
    } else {
        anyhow!("Unexpected response {:?}", response)
    }
}
