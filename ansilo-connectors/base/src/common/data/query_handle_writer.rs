use std::io::{self, BufWriter, Write};

use ansilo_core::{
    data::DataValue,
    err::{Context, Error, Result},
};

use crate::interface::{QueryHandle, QueryInputStructure};

use super::DataWriter;

/// Wraps a query handle in order to provide a higher level interface to write data
/// to the query
pub struct QueryHandleWriter<T>
where
    T: QueryHandle,
{
    /// The inner query handle
    /// We use a buf writer to ensure we dont call the underlying write impl
    /// too frequently as it could be expensive
    /// (eg across the JNI bridge)
    inner: DataWriter<BufWriter<QueryHandleWrite<T>>>,
    /// The query input structure
    structure: QueryInputStructure,
}

/// Wrapper to implement io::Read for the ResultSet trait
pub struct QueryHandleWrite<T>(pub T)
where
    T: QueryHandle;

impl<T> Write for QueryHandleWrite<T>
where
    T: QueryHandle,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .write(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<T> QueryHandleWriter<T>
where
    T: QueryHandle,
{
    pub fn new(inner: T) -> Result<Self> {
        let structure = inner.get_structure()?;
        let param_types = structure.params.iter().map(|i| i.1.clone()).collect();

        Ok(Self {
            inner: DataWriter::new(
                BufWriter::with_capacity(1024, QueryHandleWrite(inner)),
                Some(param_types),
            ),
            structure,
        })
    }

    /// Returns the underlying query handle.
    /// Flushes any buffered data.
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner.inner_mut().get_mut().0
    }

    /// Flushes the written params to the underlying query
    pub fn flush(&mut self) -> Result<()> {
        self.inner
            .inner_mut()
            .flush()
            .context("Failed to flush the buffer")
    }

    /// Returns the underlying query handle.
    /// Flushes any buffered data.
    pub fn inner(mut self) -> Result<T> {
        self.flush()?;

        Ok(self
            .inner
            .inner()
            .into_inner()
            .map_err(|_| Error::msg("Failed to get inner query handle"))?
            .0)
    }

    /// Get the expected data types of the query parameters
    pub fn get_structure(&self) -> &QueryInputStructure {
        &self.structure
    }

    /// Restarts the inner query
    pub fn restart(&mut self) -> Result<()> {
        self.inner.restart()?;
        self.inner_mut().restart()?;
        Ok(())
    }

    /// Writes the supplied data value to the underlying query handle
    pub fn write_data_value(&mut self, data: DataValue) -> Result<()> {
        self.inner.write_data_value(data)
    }
}

#[cfg(test)]
mod tests {

    use ansilo_core::data::DataType;

    use crate::common::data::rs_tests::MockResultSet;

    use super::*;

    pub(super) struct MockQueryHandle(QueryInputStructure, io::Cursor<Vec<u8>>);

    impl QueryHandle for MockQueryHandle {
        type TResultSet = MockResultSet;

        fn get_structure(&self) -> Result<QueryInputStructure> {
            Ok(self.0.clone())
        }

        fn write(&mut self, buff: &[u8]) -> Result<usize> {
            Ok(self.1.write(buff)?)
        }

        fn restart(&mut self) -> Result<()> {
            unimplemented!()
        }

        fn execute(&mut self) -> Result<MockResultSet> {
            unimplemented!()
        }
    }

    impl MockQueryHandle {
        fn new(s: QueryInputStructure, capacity: usize) -> QueryHandleWriter<Self> {
            QueryHandleWriter::new(Self(s, io::Cursor::new(Vec::<u8>::with_capacity(capacity))))
                .unwrap()
        }
    }

    #[test]
    fn test_query_handle_writer_get_structure() {
        let structure = QueryInputStructure::new(vec![(1, DataType::Int32)]);
        let query = MockQueryHandle::new(structure.clone(), 1024);

        assert_eq!(query.get_structure(), &structure);
    }

    #[test]
    fn test_query_handle_writer_inner() {
        let structure = QueryInputStructure::new(vec![(1, DataType::Int32)]);
        let query = MockQueryHandle::new(structure.clone(), 1024);

        query.inner().unwrap();
    }

    #[test]
    fn test_query_handle_writer_write_value() {
        let structure = QueryInputStructure::new(vec![(1, DataType::Int32)]);
        let mut query = MockQueryHandle::new(structure.clone(), 1024);

        query.write_data_value(DataValue::Int32(123)).unwrap();

        let buff = query.inner().unwrap().1.into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // val
            ]
            .concat()
        );
    }

    #[test]
    fn test_query_handle_writer_write_invalid() {
        let structure = QueryInputStructure::new(vec![(1, DataType::Int32)]);
        let mut query = MockQueryHandle::new(structure.clone(), 1024);

        let res = query.write_data_value(DataValue::Utf8String("invalid".into()));

        assert!(res.is_err());
    }
}
