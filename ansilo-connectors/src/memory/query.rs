use std::{io, sync::Arc};

use ansilo_core::{
    common::data::{DataType, DataValue},
    err::Result,
    sqlil,
};

use crate::{
    common::{data::DataReader, entity::ConnectorEntityConfig},
    interface::{QueryHandle, QueryInputStructure},
};

use super::{executor::MemoryQueryExecutor, MemoryConnectionConfig, MemoryResultSet};

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryQuery {
    pub select: sqlil::Select,
    pub params: Vec<DataType>,
}

impl MemoryQuery {
    pub fn new(select: sqlil::Select, params: Vec<DataType>) -> Self {
        Self { select, params }
    }
}

pub struct MemoryQueryHandle {
    query: MemoryQuery,
    data: Arc<MemoryConnectionConfig>,
    entities: ConnectorEntityConfig<()>,
    param_buff: Vec<u8>,
    params: Vec<DataValue>,
}

impl MemoryQueryHandle {
    pub fn new(
        query: MemoryQuery,
        data: Arc<MemoryConnectionConfig>,
        entities: ConnectorEntityConfig<()>,
    ) -> Self {
        Self {
            query,
            data,
            entities,
            param_buff: vec![],
            params: vec![],
        }
    }
}

impl QueryHandle for MemoryQueryHandle {
    type TResultSet = MemoryResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(QueryInputStructure::new(self.query.params.clone()))
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        self.param_buff.extend_from_slice(buff);

        let mut reader = DataReader::new(
            io::Cursor::new(self.param_buff.as_slice()),
            self.query.params.clone(),
        );

        for param in reader.read_data_value()? {
            self.params.push(param);
        }

        let cursor = reader.inner();
        let read = cursor.position() as usize;
        self.param_buff.drain(..read);

        Ok(read)
    }

    fn execute(&mut self) -> Result<MemoryResultSet> {
        let executor = MemoryQueryExecutor::new(
            Arc::clone(&self.data),
            self.entities.clone(),
            self.query.select.clone(),
            self.params.clone(),
        );

        executor.run()
    }
}
