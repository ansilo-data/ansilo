use std::{io::Write, ops::DerefMut, sync::Arc};

use ansilo_connectors_base::{
    common::data::DataSink,
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};
use ansilo_core::{
    data::{DataType, DataValue},
    err::{ensure, Context, Result},
    sqlil::{self},
};
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use serde::Serialize;
use tokio_postgres::{types::Type, Client, Statement};

use crate::{
    data::{from_pg_type, to_pg},
    result_set::PostgresResultSet,
    runtime::runtime,
};

/// Postgres query
#[derive(Debug, Clone, PartialEq, Serialize, EnumAsInner)]
pub enum QueryParam {
    Dynamic(sqlil::Parameter),
    Constant(DataValue),
}

impl QueryParam {
    pub(crate) fn r#type(&self) -> DataType {
        match self {
            QueryParam::Dynamic(p) => p.r#type.clone(),
            QueryParam::Constant(c) => c.r#type(),
        }
    }
}

/// Postgres query
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PostgresQuery {
    /// The postgres SQL query
    pub sql: String,
    /// List of parameters expected by the query
    pub params: Vec<QueryParam>,
}

impl PostgresQuery {
    pub fn new(sql: impl Into<String>, params: Vec<QueryParam>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

/// Postgres prepared query
pub struct PostgresPreparedQuery<T> {
    /// The postgres client
    client: Arc<T>,
    /// The postgres SQL query
    sql: String,
    /// The prepared postgres query
    statement: Statement,
    /// List of parameters expected by the query
    params: Vec<(QueryParam, Type)>,
    /// Dynamic param values
    values: Vec<DataValue>,
    /// Number of dynamic params
    num_dyn_params: usize,
    /// Logged params
    logged_params: Vec<DataValue>,
    /// Buffer for storing query params
    sink: DataSink,
}

impl<T> PostgresPreparedQuery<T> {
    pub fn new(
        client: Arc<T>,
        statement: Statement,
        query: String,
        params: Vec<QueryParam>,
    ) -> Self {
        let params = params
            .into_iter()
            .zip_eq(statement.params().into_iter().cloned())
            .collect_vec();

        let sink = DataSink::new(
            params
                .iter()
                .filter(|(p, _)| p.as_dynamic().is_some())
                .map(|(p, _)| p.as_dynamic().unwrap().r#type.clone())
                .collect(),
        );

        let num_dyn_params = params
            .iter()
            .filter(|(p, _)| p.as_dynamic().is_some())
            .count();

        Self {
            client,
            sql: query,
            statement,
            sink,
            params,
            num_dyn_params,
            logged_params: vec![],
            values: vec![],
        }
    }
}

impl<T: DerefMut<Target = Client>> QueryHandle for PostgresPreparedQuery<T> {
    type TResultSet = PostgresResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(QueryInputStructure::new(
            self.params
                .iter()
                .filter(|(p, _)| p.as_dynamic().is_some())
                .map(|(p, _)| p.as_dynamic().map(|p| (p.id, p.r#type.clone())).unwrap())
                .collect(),
        ))
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        self.sink.write_all(buff)?;

        while let Some(val) = self.sink.read_data_value()? {
            self.values.push(val);
        }

        ensure!(
            self.values.len() <= self.num_dyn_params,
            "Written passed end of query"
        );

        Ok(buff.len())
    }

    fn restart(&mut self) -> Result<()> {
        self.sink.clear();
        self.values.clear();
        self.logged_params.clear();
        Ok(())
    }

    fn execute(&mut self) -> Result<Self::TResultSet> {
        ensure!(
            self.values.len() == self.num_dyn_params,
            "Must write all query params"
        );

        let mut dyn_param_idx = 0;
        let mut params = vec![];

        for (param, pg_t) in self.params.iter() {
            let val = match param {
                QueryParam::Dynamic(_) => {
                    let val = &self.values[dyn_param_idx];
                    dyn_param_idx += 1;
                    val
                }
                QueryParam::Constant(cnst) => &cnst,
            };

            params.push(to_pg(val.clone(), pg_t)?);
            self.logged_params.push(val.clone());
        }

        let stream = runtime()
            .block_on(
                self.client
                    .query_raw(&self.statement, params.iter().map(|p| &**p)),
            )
            .context("Failed to execute query")?;

        let cols = self
            .statement
            .columns()
            .iter()
            .map(|c| Ok((c.name().to_string(), from_pg_type(c.type_())?)))
            .collect::<Result<_>>()?;

        Ok(PostgresResultSet::new(stream, cols))
    }

    fn logged(&self) -> Result<LoggedQuery> {
        Ok(LoggedQuery::new(
            &self.sql,
            self.logged_params
                .iter()
                .map(|val| format!("value={:?}", val))
                .collect(),
            None,
        ))
    }
}
