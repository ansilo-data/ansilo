use std::sync::Arc;

use ansilo_core::{common::data::DataType, err::Result, sqlil};

use crate::{
    common::entity::ConnectorEntityConfig,
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
        }
    }
}

impl QueryHandle for MemoryQueryHandle {
    type TResultSet = MemoryResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(QueryInputStructure::new(self.query.params.clone()))
    }

    fn write(&mut self, _buff: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn execute(&mut self) -> Result<MemoryResultSet> {
        let executor = MemoryQueryExecutor::new(
            Arc::clone(&self.data),
            self.entities.clone(),
            self.query.select.clone(),
            vec![],
        );

        executor.run()
    }
}
