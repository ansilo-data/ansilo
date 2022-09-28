use ansilo_core::{
    err::{bail, Result},
    sqlil::{self as sql, Query},
};

use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::QueryCompiler};

use crate::ArvoQueryType;

use super::{ArvoConnection, ArvoFile, ArvoQuery};

pub struct ArvoQueryCompiler;

impl QueryCompiler for ArvoQueryCompiler {
    type TConnection = ArvoConnection;
    type TQuery = ArvoQuery;
    type TEntitySourceConfig = ArvoFile;

    fn compile_query(
        _con: &mut ArvoConnection,
        conf: &ConnectorEntityConfig<ArvoFile>,
        query: sql::Query,
    ) -> Result<ArvoQuery> {
        let (e, q) = match query {
            Query::Select(q) => (
                q.from,
                ArvoQueryType::ReadAll(
                    q.cols
                        .iter()
                        .map(|(alias, expr)| {
                            (
                                alias.clone(),
                                expr.as_attribute().unwrap().attribute_id.clone(),
                            )
                        })
                        .collect(),
                ),
            ),
            Query::Insert(q) => (
                q.target,
                ArvoQueryType::InsertBatch(
                    q.cols
                        .iter()
                        .map(|(col, expr)| (col.clone(), expr.as_parameter().unwrap().clone()))
                        .collect(),
                ),
            ),
            Query::BulkInsert(q) => (
                q.target,
                ArvoQueryType::InsertBatch(
                    q.values
                        .iter()
                        .enumerate()
                        .map(|(idx, expr)| {
                            (
                                q.cols[idx % q.cols.len()].clone(),
                                expr.as_parameter().unwrap().clone(),
                            )
                        })
                        .collect(),
                ),
            ),
            _ => bail!("Unsupported"),
        };

        let entity = conf.get(&e.entity)?;

        Ok(ArvoQuery::new(
            entity.conf.clone(),
            entity.source.clone(),
            q,
        ))
    }

    fn query_from_string(
        _connection: &mut Self::TConnection,
        _query: String,
        _params: Vec<sql::Parameter>,
    ) -> Result<Self::TQuery> {
        bail!("Unsupported")
    }
}
