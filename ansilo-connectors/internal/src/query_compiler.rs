use std::str::FromStr;

use ansilo_core::{
    err::{bail, Context, Error, Result},
    sqlil as sql,
};

use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::QueryCompiler};

use crate::{InternalConnection, InternalQuery, InternalQueryType};

pub struct InternalQueryCompiler;

impl QueryCompiler for InternalQueryCompiler {
    type TConnection = InternalConnection;
    type TQuery = InternalQuery;
    type TEntitySourceConfig = ();

    fn compile_query(
        con: &mut InternalConnection,
        _conf: &ConnectorEntityConfig<()>,
        query: sql::Query,
    ) -> Result<InternalQuery> {
        let select = match query.into_select() {
            Ok(s) => s,
            _ => bail!("Unsupported"),
        };

        let query = match select.from.entity.entity_id.as_str() {
            "jobs" => InternalQueryType::Job(parse_cols(select.cols)?),
            "job_triggers" => InternalQueryType::JobTrigger(parse_cols(select.cols)?),
            "service_users" => InternalQueryType::ServiceUser(parse_cols(select.cols)?),
            _ => bail!("Unsupported"),
        };

        let query = InternalQuery { nc: con.0, query };

        Ok(query)
    }

    fn query_from_string(
        _connection: &mut Self::TConnection,
        _query: String,
        _params: Vec<sql::Parameter>,
    ) -> Result<Self::TQuery> {
        bail!("Unsupported")
    }
}

fn parse_cols<T: FromStr<Err = Error>>(cols: Vec<(String, sql::Expr)>) -> Result<Vec<(String, T)>> {
    cols.into_iter()
        .map(|(alias, col)| {
            col.as_attribute()
                .context("Unsupported")
                .and_then(|att| att.attribute_id.parse::<T>())
                .map(|c| (alias, c))
        })
        .collect()
}
