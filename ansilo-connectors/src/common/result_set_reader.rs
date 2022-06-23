use std::{
    io::{self, BufReader, Read},
    marker::PhantomData,
};

use ansilo_core::{
    common::data::{DataType, DataValue},
    err::{bail, Context, Result},
};

use crate::interface::{ResultSet, RowStructure};

/// Wraps a result set in order to parse and read the data as rust values
pub struct ResultSetReader<'a, T>
where
    T: ResultSet<'a>,
{
    /// The inner result set
    /// We use a buf reader to ensure we dont call the underlying read impl
    /// too frequently as it could be expensive
    /// (eg across the JNI bridge)
    inner: BufReader<Reader<'a, T>>,
    /// The row structure
    structure: Option<RowStructure>,
    /// The current row index
    row_idx: u64,
    /// The current column index
    col_idx: usize,
}

/// Wrapper to implement io::Read for the ResultSet trait
struct Reader<'a, T>(pub T, PhantomData<&'a T>)
where
    T: ResultSet<'a>;

impl<'a, T> Read for Reader<'a, T>
where
    T: ResultSet<'a>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0
            .read(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

impl<'a, T> ResultSetReader<'a, T>
where
    T: ResultSet<'a>,
{
    pub fn new(inner: T) -> Self {
        Self {
            inner: BufReader::with_capacity(1024, Reader(inner, PhantomData)),
            structure: None,
            row_idx: 0,
            col_idx: 0,
        }
    }

    pub fn inner(self) -> T {
        self.inner.into_inner().0
    }

    /// Reads the next data value from the result set
    /// Returns Ok(None) if there is no more data to read in the result set
    pub fn read_data_value(&mut self) -> Result<Option<DataValue>> {
        if self.structure.is_none() {
            let structure = self.inner.get_ref().0.get_structure()?;

            // We don't allow no columns
            if structure.cols.is_empty() {
                bail!("At least one column must be present in the result set");
            }

            self.structure = Some(structure);
        }

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
                DataType::FloatSingle => todo!(),
                DataType::FloatDouble => todo!(),
                DataType::Decimal(_) => todo!(),
                DataType::JSON => todo!(),
                DataType::Date => todo!(),
                DataType::Time => todo!(),
                DataType::Timestamp => todo!(),
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

    /// Reads a stream of data from the internal buffer
    /// Each chunk is framed with the length of data to come
    fn read_stream(&mut self) -> Result<Vec<u8>> {
        let mut data = vec![];
        let mut read = 0usize;

        loop {
            let length = self
                .read_exact::<1>()
                .context("Failed to read stream length")?[0];

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

    fn current_data_type(&self) -> DataType {
        let structure = self.structure.as_ref().unwrap();
        let data_type = &structure.cols[self.col_idx].1;

        data_type.clone()
    }

    fn num_cols(&self) -> usize {
        self.structure.as_ref().unwrap().cols.len()
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

    use ansilo_core::common::data::{EncodingType, VarcharOptions};

    use super::*;

    struct MockResultSet(RowStructure, io::Cursor<Vec<u8>>);

    impl<'a> ResultSet<'a> for MockResultSet {
        fn get_structure(&self) -> Result<RowStructure> {
            Ok(self.0.clone())
        }

        fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
            self.1.read(buff).context("failed to read")
        }
    }

    impl MockResultSet {
        fn new<'a>(s: RowStructure, data: Vec<u8>) -> ResultSetReader<'a, Self> {
            ResultSetReader::new(Self(s, io::Cursor::new(data)))
        }
    }

    #[test]
    fn test_result_set_reader_no_cols() {
        let mut res = MockResultSet::new(RowStructure::new(vec![]), vec![]);

        assert!(res.read_data_value().is_err());
    }

    #[test]
    fn test_result_set_reader_empty() {
        let mut res = MockResultSet::new(
            RowStructure::new(vec![("a".to_string(), DataType::Int8)]),
            vec![],
        );

        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_result_set_reader_int32() {
        let mut res = MockResultSet::new(
            RowStructure::new(vec![("a".to_string(), DataType::Int32)]),
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(123)));
    }

    #[test]
    fn test_result_set_reader_varchar() {
        let mut res = MockResultSet::new(
            RowStructure::new(vec![(
                "a".to_string(),
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
            )]),
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
    fn test_result_set_reader_varchar_multiple_reads() {
        let mut res = MockResultSet::new(
            RowStructure::new(vec![(
                "a".to_string(),
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
            )]),
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
    fn test_result_set_reader_int32_multiple_rows() {
        let mut res = MockResultSet::new(
            RowStructure::new(vec![("a".to_string(), DataType::Int32)]),
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
    fn test_result_set_reader_int32_with_nulls() {
        let mut res = MockResultSet::new(
            RowStructure::new(vec![("a".to_string(), DataType::Int32)]),
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
    fn test_result_set_reader_int32_and_varchar() {
        let mut res = MockResultSet::new(
            RowStructure::new(vec![
                ("a".to_string(), DataType::Int32),
                (
                    "b".to_string(),
                    DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
                ),
            ]),
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
    fn test_result_set_reader_end_mid_row() {
        let mut res = MockResultSet::new(
            RowStructure::new(vec![
                ("a".to_string(), DataType::Int32),
                ("b".to_string(), DataType::Int32),
            ]),
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
