use std::io::{self, BufReader, Read};

use ansilo_core::{data::DataValue, err::Result};

use crate::interface::{ResultSet, RowStructure};

use super::DataReader;

/// Wraps a result set in order to parse and read the data as rust values
pub struct ResultSetReader<T>
where
    T: ResultSet,
{
    /// The inner result set
    /// We use a buf reader to ensure we dont call the underlying read impl
    /// too frequently as it could be expensive
    /// (eg across the JNI bridge)
    inner: DataReader<BufReader<ResultSetRead<T>>>,
    /// The row structure
    structure: RowStructure,
}

/// Wrapper to implement io::Read for the ResultSet trait
pub struct ResultSetRead<T>(pub T)
where
    T: ResultSet;

impl<T> Read for ResultSetRead<T>
where
    T: ResultSet,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0
            .read(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

impl<T> ResultSetReader<T>
where
    T: ResultSet,
{
    pub fn new(inner: T) -> Result<Self> {
        let structure = inner.get_structure()?;

        Ok(Self {
            inner: DataReader::new(
                BufReader::with_capacity(1024, ResultSetRead(inner)),
                structure.cols.iter().map(|i| i.1.clone()).collect(),
            ),
            structure,
        })
    }

    pub fn inner(self) -> T {
        self.inner.inner().into_inner().0
    }

    /// Gets the data type structure of the rows returned in the result set
    pub fn get_structure(&mut self) -> &RowStructure {
        &self.structure
    }

    /// Reads the next data value from the result set
    /// Returns Ok(None) if there is no more data to read in the result set
    pub fn read_data_value(&mut self) -> Result<Option<DataValue>> {
        self.inner.read_data_value()
    }
}

#[cfg(test)]
pub(super) mod rs_tests {

    use ansilo_core::{data::DataType, err::Context};

    use super::*;

    pub(crate) struct MockResultSet(RowStructure, io::Cursor<Vec<u8>>);

    impl ResultSet for MockResultSet {
        fn get_structure(&self) -> Result<RowStructure> {
            Ok(self.0.clone())
        }

        fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
            self.1.read(buff).context("failed to read")
        }
    }

    impl MockResultSet {
        fn new(s: RowStructure, data: Vec<u8>) -> ResultSetReader<Self> {
            ResultSetReader::new(Self(s, io::Cursor::new(data))).unwrap()
        }
    }

    #[test]
    fn test_result_set_reader_get_structure() {
        let structure = RowStructure::new(vec![("test".to_string(), DataType::Int32)]);
        let mut res = MockResultSet::new(structure.clone(), vec![]);

        assert_eq!(res.get_structure(), &structure);
    }

    #[test]
    fn test_result_set_reader_inner() {
        let structure = RowStructure::new(vec![("test".to_string(), DataType::Int32)]);
        let buff = vec![1, 2, 3];
        let res = MockResultSet::new(structure, buff.clone());

        assert_eq!(res.inner().1.into_inner(), buff);
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
}
