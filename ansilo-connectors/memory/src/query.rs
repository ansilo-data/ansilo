use std::{collections::HashMap, io, sync::Arc};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, ensure, Context, Result},
    sqlil,
};
use serde::Serialize;

use ansilo_connectors_base::{
    common::{data::DataReader, entity::ConnectorEntityConfig},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};

use super::{
    executor::MemoryQueryExecutor, MemoryConnectorEntitySourceConfig, MemoryDatabase,
    MemoryResultSet,
};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MemoryQuery {
    pub query: sqlil::Query,
    pub params: Vec<(u32, DataType)>,
}

impl MemoryQuery {
    pub fn new(query: sqlil::Query, params: Vec<(u32, DataType)>) -> Self {
        Self { query, params }
    }
}

pub struct MemoryQueryHandle {
    query: MemoryQuery,
    batch: Vec<HashMap<u32, DataValue>>,
    data: Arc<MemoryDatabase>,
    entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    param_buff: Vec<u8>,
    reset: bool,
}

impl MemoryQueryHandle {
    pub fn new(
        query: MemoryQuery,
        data: Arc<MemoryDatabase>,
        entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ) -> Self {
        Self {
            query,
            data,
            batch: vec![],
            entities,
            param_buff: vec![],
            reset: false,
        }
    }
}

impl QueryHandle for MemoryQueryHandle {
    type TResultSet = MemoryResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(QueryInputStructure::new(self.query.params.clone()))
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        if self.reset {
            self.param_buff.clear();
            self.reset = false;
        }
        self.param_buff.extend_from_slice(buff);
        Ok(buff.len())
    }

    fn restart(&mut self) -> Result<()> {
        self.reset = true;
        self.batch.clear();
        Ok(())
    }

    fn execute_query(&mut self) -> Result<MemoryResultSet> {
        if !self.batch.is_empty() {
            bail!("Batching is not supported on result set queries");
        }
        let params = self.parse_params()?;

        let executor = MemoryQueryExecutor::new(
            Arc::clone(&self.data),
            self.entities.clone(),
            self.query.query.clone(),
            params,
        );

        executor.run()
    }

    fn execute_modify(&mut self) -> Result<Option<u64>> {
        if self.batch.is_empty() {
            let params = self.parse_params()?;
            self.batch.push(params);
        } else {
            ensure!(self.param_buff.is_empty(), "Partial query input written")
        }

        let mut totals = vec![];

        for params in &self.batch {
            let executor = MemoryQueryExecutor::new(
                Arc::clone(&self.data),
                self.entities.clone(),
                self.query.query.clone(),
                params.clone(),
            );

            totals.push(executor.run_modify()?);
        }

        Ok(if totals.contains(&None) {
            None
        } else {
            Some(totals.into_iter().map(|i| i.unwrap()).sum())
        })
    }

    fn logged(&self) -> Result<LoggedQuery> {
        Ok(LoggedQuery::new(
            format!("{:?}", self.query),
            self.parse_params()?
                .into_iter()
                .map(|p| format!("{:?}", p))
                .collect(),
            None,
        ))
    }

    fn supports_batching(&self) -> bool {
        true
    }

    fn add_to_batch(&mut self) -> Result<()> {
        let params = self.parse_params()?;
        self.batch.push(params);
        self.param_buff.clear();
        Ok(())
    }
}

impl MemoryQueryHandle {
    fn parse_params(&self) -> Result<HashMap<u32, DataValue>> {
        eprintln!("Params: {:?}", self.param_buff);
        let mut params = HashMap::new();
        let mut param_reader = DataReader::new(
            io::Cursor::new(self.param_buff.clone()),
            self.get_structure().unwrap().types(),
        );

        for (id, _) in self.query.params.iter() {
            params.insert(
                *id,
                param_reader
                    .read_data_value()?
                    .context("Not all query parameters have been written")?,
            );
        }

        Ok(params)
    }
}
