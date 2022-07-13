use std::io::Read;

use ansilo_core::{
    common::data::{DataType, DataValue},
    err::{bail, Context, Result},
};

/// Wraps a Read in order to parse and read the data as DataValue
pub struct DataReader<T>
where
    T: Read,
{
    /// The inner read
    inner: T,
    /// The data structure, loaded on first read
    structure: Vec<DataType>,
    /// The current row index
    row_idx: u64,
    /// The current column index
    col_idx: usize,
}

impl<T> DataReader<T>
where
    T: Read,
{
    pub fn new(inner: T, structure: Vec<DataType>) -> Self {
        Self {
            inner: inner,
            structure,
            row_idx: 0,
            col_idx: 0,
        }
    }

    pub fn inner(self) -> T {
        self.inner
    }

    /// Gets the data type structure of the rows returned in the result set
    pub fn get_structure(&self) -> &Vec<DataType> {
        &self.structure
    }

    /// Reads the next data value from the underlying Read
    /// Returns Ok(None) if there is no more data to read in the result set
    pub fn read_data_value(&mut self) -> Result<Option<DataValue>> {
        let not_null = self.read_byte().context("Failed to read null flag byte")?;

        // Check for EOF
        if not_null.is_none() {
            return if self.col_idx == 0 {
                Ok(None)
            } else {
                bail!("Unexpected EOF occurred while reading row")
            };
        }

        let res = if not_null.unwrap() != 0 {
            // TODO: data types
            match self.current_data_type() {
                DataType::Varchar(_) => DataValue::Varchar(self.read_stream()?),
                DataType::Binary => DataValue::Binary(self.read_stream()?),
                DataType::Boolean => DataValue::Boolean(self.read_exact::<1>()?[0] != 0),
                DataType::Int8 => todo!(),
                DataType::UInt8 => todo!(),
                DataType::Int16 => todo!(),
                DataType::UInt16 => todo!(),
                DataType::Int32 => DataValue::Int32(i32::from_ne_bytes(self.read_exact::<4>()?)),
                DataType::UInt32 => DataValue::UInt32(u32::from_ne_bytes(self.read_exact::<4>()?)),
                DataType::Int64 => todo!(),
                DataType::UInt64 => todo!(),
                DataType::Float32 => todo!(),
                DataType::Float64 => todo!(),
                DataType::Decimal(_) => todo!(),
                DataType::JSON => todo!(),
                DataType::Date => todo!(),
                DataType::Time => todo!(),
                DataType::DateTime => todo!(),
                DataType::DateTimeWithTZ => todo!(),
                DataType::Uuid => todo!(),
                DataType::Null => todo!(),
            }
        } else {
            DataValue::Null
        };

        self.advance();

        Ok(Some(res))
    }

    /// Reads a stream of data from the internal buf reader
    /// Each chunk is framed with the length of data to come
    fn read_stream(&mut self) -> Result<Vec<u8>> {
        let mut data = vec![];
        let mut read = 0usize;

        loop {
            let length = self
                .read_exact::<1>()
                .context("Failed to read stream chunk length")?[0];

            // Check for EOF
            if length <= 0 {
                break;
            }

            let length = length as usize;
            data.resize(data.len() + length, 0);
            self.inner
                .read_exact(&mut data[read..][..length])
                .context("Failed to read data from stream")?;
            read += length;
        }

        Ok(data)
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut buf = [0; N];
        self.inner.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_byte(&mut self) -> Result<Option<u8>> {
        let mut buf = [0; 1];
        let read = self.inner.read(&mut buf)?;

        Ok(if read == 0 { None } else { Some(buf[0]) })
    }

    fn current_data_type(&self) -> &DataType {
        &self.structure[self.col_idx]
    }

    fn num_cols(&self) -> usize {
        self.structure.len()
    }

    fn is_last_col(&self) -> bool {
        self.col_idx == self.num_cols() - 1
    }

    fn advance(&mut self) {
        if self.is_last_col() {
            self.col_idx = 0;
            self.row_idx += 1;
        } else {
            self.col_idx += 1;
        }
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use ansilo_core::common::data::{EncodingType, VarcharOptions};

    use super::*;

    fn create_data_reader(structure: Vec<DataType>, data: Vec<u8>) -> DataReader<Cursor<Vec<u8>>> {
        DataReader::new(Cursor::new(data), structure)
    }

    #[test]
    fn test_data_reader_no_cols() {
        let mut res = create_data_reader(vec![], vec![]);

        assert!(res.read_data_value().is_ok());
    }

    #[test]
    fn test_data_reader_empty() {
        let mut res = create_data_reader(vec![DataType::Int8], vec![]);

        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_int32() {
        let mut res = create_data_reader(
            vec![DataType::Int32],
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(123)));
    }

    #[test]
    fn test_data_reader_varchar() {
        let mut res = create_data_reader(
            vec![DataType::Varchar(VarcharOptions::new(
                None,
                EncodingType::Utf8,
            ))],
            [
                vec![1u8],                 // not null
                vec![3u8],                 // read length
                "abc".as_bytes().to_vec(), // data
                vec![0u8],                 // read length (eof)
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Varchar("abc".as_bytes().to_vec()))
        );
    }

    #[test]
    fn test_data_reader_varchar_multiple_reads() {
        let mut res = create_data_reader(
            vec![DataType::Varchar(VarcharOptions::new(
                None,
                EncodingType::Utf8,
            ))],
            [
                vec![1u8],                   // not null
                vec![3u8],                   // read length
                "abc".as_bytes().to_vec(),   // data
                vec![5u8],                   // read length
                "12345".as_bytes().to_vec(), // data
                vec![0u8],                   // read length (eof)
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Varchar("abc12345".as_bytes().to_vec()))
        );
    }

    #[test]
    fn test_data_reader_int32_multiple_rows() {
        let mut res = create_data_reader(
            vec![DataType::Int32],
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
                vec![1u8],                      // not null
                456_i32.to_ne_bytes().to_vec(), // data
                vec![1u8],                      // not null
                789_i32.to_ne_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(123)));
        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(456)));
        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(789)));
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_int32_with_nulls() {
        let mut res = create_data_reader(
            vec![DataType::Int32],
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
                vec![0u8],                      // not null
                vec![0u8],                      // not null
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(123)));
        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Null));
        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Null));
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_int32_and_varchar() {
        let mut res = create_data_reader(
            vec![
                DataType::Int32,
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
            ],
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
                vec![1u8],                      // not null
                vec![3u8],                      // read length
                "abc".as_bytes().to_vec(),      // data
                vec![0u8],                      // read length (eof)
                vec![1u8],                      // not null
                456_i32.to_ne_bytes().to_vec(), // data
                vec![1u8],                      // not null
                vec![3u8],                      // read length
                "123".as_bytes().to_vec(),      // data
                vec![0u8],                      // read length (eof)
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(123)));
        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Varchar("abc".as_bytes().to_vec()))
        );
        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(456)));
        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Varchar("123".as_bytes().to_vec()))
        );
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_end_mid_row() {
        let mut res = create_data_reader(
            vec![DataType::Int32, DataType::Int32],
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(123)));
        assert!(res.read_data_value().is_err());
    }
}
