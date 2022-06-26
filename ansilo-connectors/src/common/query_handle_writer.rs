use std::{
    io::{self, BufWriter, Write},
    marker::PhantomData,
};

use ansilo_core::{
    common::data::{DataType, DataValue},
    err::{bail, Context, Error, Result},
};

use crate::interface::{QueryHandle, QueryInputStructure, ResultSet};

use super::DataWriter;

/// Wraps a query handle in order to provide a higher level interface to write data
/// to the query
pub struct QueryHandleWriter<'a, T, R>
where
    T: QueryHandle<'a, R>,
    R: ResultSet<'a>,
{
    /// The inner query handle
    /// We use a buf writer to ensure we dont call the underlying write impl
    /// too frequently as it could be expensive
    /// (eg across the JNI bridge)
    inner: DataWriter<BufWriter<Writer<'a, T, R>>>,
    /// The query input structure
    structure: QueryInputStructure,
}

/// Wrapper to implement io::Read for the ResultSet trait
struct Writer<'a, T, R>(pub T, PhantomData<&'a R>)
where
    T: QueryHandle<'a, R>,
    R: ResultSet<'a>;

impl<'a, T, R> Write for Writer<'a, T, R>
where
    T: QueryHandle<'a, R>,
    R: ResultSet<'a>,
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

impl<'a, T, R> QueryHandleWriter<'a, T, R>
where
    T: QueryHandle<'a, R>,
    R: ResultSet<'a>,
{
    pub fn new(inner: T) -> Result<Self> {
        let structure = inner.get_structure()?;
        Ok(Self {
            inner: DataWriter::new(
                BufWriter::with_capacity(1024, Writer(inner, PhantomData)),
                Some(structure.params.clone()),
            ),
            structure,
        })
    }

    pub fn inner(self) -> Result<T> {
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

    /// Writes the supplied data value to the underlying query handle
    pub fn write_data_value(&mut self, data: DataValue) -> Result<()> {
        self.inner.write_data_value(data)
    }
}

#[cfg(test)]
mod tests {

    use crate::common::rs_tests::MockResultSet;

    use super::*;

    pub(super) struct MockQueryHandle(QueryInputStructure, io::Cursor<Vec<u8>>);

    impl<'a> QueryHandle<'a, MockResultSet> for MockQueryHandle {
        fn get_structure(&self) -> Result<QueryInputStructure> {
            Ok(self.0.clone())
        }

        fn write(&mut self, buff: &[u8]) -> Result<usize> {
            Ok(self.1.write(buff)?)
        }

        fn execute(&mut self) -> Result<MockResultSet> {
            unimplemented!()
        }
    }

    impl MockQueryHandle {
        fn new<'a>(
            s: QueryInputStructure,
            capacity: usize,
        ) -> QueryHandleWriter<'a, Self, MockResultSet> {
            QueryHandleWriter::new(Self(s, io::Cursor::new(Vec::<u8>::with_capacity(capacity))))
                .unwrap()
        }
    }

    #[test]
    fn test_query_handle_writer_get_structure() {
        let structure = QueryInputStructure::new(vec![DataType::Int32]);
        let mut query = MockQueryHandle::new(structure.clone(), 1024);

        assert_eq!(query.get_structure(), &structure);
    }

    #[test]
    fn test_query_handle_writer_inner() {
        let structure = QueryInputStructure::new(vec![DataType::Int32]);
        let query = MockQueryHandle::new(structure.clone(), 1024);

        query.inner().unwrap();
    }
}
