use std::{
    io::{Cursor, Read},
    sync::Arc,
};

use ansilo_core::{
    common::data::{DataType, DataValue},
    err::{Context, Result},
};

use crate::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};

pub struct MemoryResultSet {
    pub cols: Vec<(String, DataType)>,
    pub data: Vec<Vec<DataValue>>,
    pub data_buff: Cursor<Vec<u8>>,
}

impl MemoryResultSet {
    pub fn new(cols: Vec<(String, DataType)>, data: Vec<Vec<DataValue>>) -> Result<Self> {
        let mut writer = DataWriter::new(
            Cursor::new(Vec::<u8>::new()),
            Some(cols.iter().map(|i| i.1.clone()).collect()),
        );

        for row in data.iter() {
            for cell in row.iter() {
                writer.write_data_value(cell.clone())?;
            }
        }

        Ok(Self {
            cols,
            data,
            data_buff: writer.inner(),
        })
    }
}

impl ResultSet for MemoryResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(RowStructure::new(self.cols.clone()))
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        self.data_buff.read(buff).context("Failed to read")
    }
}

#[cfg(test)]
mod tests {}
