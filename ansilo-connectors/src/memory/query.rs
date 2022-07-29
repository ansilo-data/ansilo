use std::{collections::HashMap, io, sync::Arc};

use ansilo_core::{
    data::DataType,
    err::{bail, Context, Result},
    sqlil,
};
use serde::Serialize;

use crate::{
    common::{data::DataReader, entity::ConnectorEntityConfig},
    interface::{QueryHandle, QueryInputStructure},
};

use super::{executor::MemoryQueryExecutor, MemoryConnectionConfig, MemoryResultSet, MemoryConnectorEntitySourceConfig};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MemoryQuery {
    pub select: sqlil::Select,
    pub params: Vec<(u32, DataType)>,
}

impl MemoryQuery {
    pub fn new(select: sqlil::Select, params: Vec<(u32, DataType)>) -> Self {
        Self { select, params }
    }
}

pub struct MemoryQueryHandle {
    query: MemoryQuery,
    data: Arc<MemoryConnectionConfig>,
    entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    param_buff: Vec<u8>,
    reset: bool,
}

impl MemoryQueryHandle {
    pub fn new(
        query: MemoryQuery,
        data: Arc<MemoryConnectionConfig>,
        entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ) -> Self {
        Self {
            query,
            data,
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
        Ok(())
    }

    fn execute(&mut self) -> Result<MemoryResultSet> {
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

        let executor = MemoryQueryExecutor::new(
            Arc::clone(&self.data),
            self.entities.clone(),
            self.query.select.clone(),
            params,
        );

        executor.run()
    }
}
