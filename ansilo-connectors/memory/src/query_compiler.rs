use ansilo_core::{err::{Result, bail}, sqlil as sql};

use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::QueryCompiler};

use super::{MemoryConnection, MemoryConnectorEntitySourceConfig, MemoryQuery};

pub struct MemoryQueryCompiler;

impl QueryCompiler for MemoryQueryCompiler {
    type TConnection = MemoryConnection;
    type TQuery = MemoryQuery;
    type TEntitySourceConfig = MemoryConnectorEntitySourceConfig;

    fn compile_query(
        _con: &mut MemoryConnection,
        _conf: &ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        query: sql::Query,
    ) -> Result<MemoryQuery> {
        let mut params = vec![];

        query.exprs().for_each(|e| {
            e.walk(&mut |e| {
                if let sql::Expr::Parameter(p) = e {
                    params.push((p.id, p.r#type.clone()))
                }
            })
        });

        Ok(MemoryQuery::new(query, params))
    }

    fn query_from_string(
        _connection: &mut Self::TConnection,
        _query: String,
        _params: Vec<sql::Parameter>,
    ) -> Result<Self::TQuery> {
        bail!("Unsupported")
    }
}
