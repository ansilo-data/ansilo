use std::marker::PhantomData;

use ansilo_core::{
    err::{bail, Result},
    sqlil::{self as sql, Query},
};

use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::QueryCompiler};

use crate::{
    FileConnection, FileIO, FileQuery, FileQueryType, FileSourceConfig, InsertRowsQuery,
    ReadColumnsQuery,
};

pub struct FileQueryCompiler<F: FileIO> {
    _io: PhantomData<F>,
}

impl<F: FileIO> QueryCompiler for FileQueryCompiler<F> {
    type TConnection = FileConnection<F>;
    type TQuery = FileQuery;
    type TEntitySourceConfig = FileSourceConfig;

    fn compile_query(
        con: &mut FileConnection<F>,
        conf: &ConnectorEntityConfig<FileSourceConfig>,
        query: sql::Query,
    ) -> Result<FileQuery> {
        let (e, q) = match query {
            Query::Select(q) => (
                q.from,
                FileQueryType::ReadColumns(ReadColumnsQuery::new(
                    q.cols
                        .iter()
                        .map(|(alias, expr)| {
                            (
                                alias.clone(),
                                expr.as_attribute().unwrap().attribute_id.clone(),
                            )
                        })
                        .collect(),
                )),
            ),
            Query::Insert(q) => (
                q.target,
                FileQueryType::InsertRows(InsertRowsQuery::new(
                    q.cols.iter().map(|(col, _)| col.clone()).collect(),
                    q.cols
                        .iter()
                        .map(|(_, expr)| expr.as_parameter().unwrap().clone())
                        .collect(),
                )),
            ),
            Query::BulkInsert(q) => (
                q.target,
                FileQueryType::InsertRows(InsertRowsQuery::new(
                    q.cols.iter().cloned().collect(),
                    q.values
                        .iter()
                        .map(|expr| expr.as_parameter().unwrap().clone())
                        .collect(),
                )),
            ),
            _ => bail!("Unsupported"),
        };

        let entity = conf.get(&e.entity)?;

        Ok(FileQuery::new(
            entity.conf.clone(),
            entity.source.path(con.conf()),
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
