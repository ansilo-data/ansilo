use std::io::{Cursor, Read, Seek};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, Context, Result},
};

use ansilo_connectors_base::{
    common::data::DataWriter,
    interface::{ResultSet, RowStructure},
};

#[derive(Debug, Clone, PartialEq)]
pub struct InternalResultSet {
    cols: Vec<(String, DataType)>,
    buff: Cursor<Vec<u8>>,
    data: Option<Vec<DataValue>>,
}

impl InternalResultSet {
    pub fn new(cols: Vec<(String, DataType)>, data: Vec<DataValue>) -> Result<Self> {
        let mut writer = DataWriter::new(Cursor::new(Vec::<u8>::new()), None);

        for row in data.chunks(cols.len()) {
            for (idx, cell) in row.iter().enumerate() {
                if !cell.is_null() && cell.r#type() != cols[idx].1 {
                    bail!(
                        "Expecting data type {:?} but found data value {:?} found with unexpected type",
                        cols[idx].1, cell.r#type()
                    );
                }
                writer.write_data_value(cell.clone())?;
            }
        }

        let mut buff = writer.inner();
        buff.rewind().context("Failed to rewind cursor")?;

        #[cfg(test)]
        let data = Some(data);
        #[cfg(not(test))]
        let data = None;

        Ok(Self { cols, data, buff })
    }
}

impl ResultSet for InternalResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(RowStructure::new(self.cols.clone()))
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        self.buff.read(buff).context("Failed to read")
    }
}

#[cfg(test)]
mod tests {
    use ansilo_connectors_base::common::data::ResultSetReader;

    use super::*;

    #[test]
    fn test_internal_result_set() {
        let result_set = InternalResultSet::new(
            vec![("col".to_string(), DataType::UInt32)],
            vec![DataValue::UInt32(123)],
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
