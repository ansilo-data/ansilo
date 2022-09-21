use std::str::FromStr;

use ansilo_core::{
    err::{bail, Context, Error, Result},
    sqlil as sql,
};

use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::QueryCompiler};

use crate::{InternalConnection, InternalQuery};

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
            "jobs" => InternalQuery::Job(con.0, parse_cols(select.cols)?),
            "job_triggers" => InternalQuery::JobTrigger(con.0, parse_cols(select.cols)?),
            "service_users" => InternalQuery::ServiceUser(con.0, parse_cols(select.cols)?),
            _ => bail!("Unsupported"),
        };

        Ok(query)
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
