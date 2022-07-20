use std::io::{Cursor, Read, Seek};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{Context, Result},
};

use crate::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryResultSet {
    pub cols: Vec<(String, DataType)>,
    pub data: Vec<Vec<DataValue>>,
    pub buff: Cursor<Vec<u8>>,
}

impl MemoryResultSet {
    pub fn new(cols: Vec<(String, DataType)>, data: Vec<Vec<DataValue>>) -> Result<Self> {
        let mut writer = DataWriter::new(Cursor::new(Vec::<u8>::new()), None);

        for row in data.iter() {
            for cell in row.iter() {
                writer.write_data_value(cell.clone())?;
            }
        }

        let mut buff = writer.inner();
        buff.rewind().context("Failed to rewind cursor")?;

        Ok(Self { cols, data, buff })
    }
}

impl ResultSet for MemoryResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(RowStructure::new(self.cols.clone()))
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        self.buff.read(buff).context("Failed to read")
    }
}

#[cfg(test)]
mod tests {
    use crate::common::data::ResultSetReader;

    use super::*;

    #[test]
    fn test_memory_connector_result_set() {
        let result_set = MemoryResultSet::new(
            vec![("col".to_string(), DataType::UInt32)],
            vec![vec![DataValue::UInt32(123)]],
        )
        .unwrap();

        assert_eq!(
            result_set.get_structure().unwrap(),
            RowStructure::new(vec![("col".to_string(), DataType::UInt32)])
        );

        let mut reader = ResultSetReader::new(result_set).unwrap();
        assert_eq!(
            reader.read_data_value().unwrap(),
            Some(DataValue::UInt32(123))
        );
        assert_eq!(reader.read_data_value().unwrap(), None);
    }
}
