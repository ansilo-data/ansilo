use std::{
    io::{self, BufWriter, Write},
    marker::PhantomData,
};

use ansilo_core::{
    common::data::{DataType, DataValue},
    err::{bail, Context, Error, Result},
};

use crate::interface::{QueryHandle, QueryInputStructure, ResultSet};

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
    inner: BufWriter<Writer<'a, T, R>>,
    /// The type of the query parameters, loaded on first write
    structure: Option<QueryInputStructure>,
    /// The current column index
    param_idx: usize,
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
    pub fn new(inner: T) -> Self {
        Self {
            inner: BufWriter::with_capacity(1024, Writer(inner, PhantomData)),
            structure: None,
            param_idx: 0,
        }
    }

    pub fn inner(self) -> Result<T> {
        Ok(self
            .inner
            .into_inner()
            .map_err(|_| Error::msg("Failed to get inner query handle"))?
            .0)
    }

    /// Get the expected data types of the query parameters
    pub fn get_structure(&mut self) -> Result<&QueryInputStructure> {
        if self.structure.is_none() {
            let structure = self.inner.get_ref().0.get_structure()?;

            self.structure = Some(structure);
        }

        Ok(self.structure.as_ref().unwrap())
    }

    /// Writes the supplied data value to the underlying query handle
    pub fn write_data_value(&mut self, data: DataValue) -> Result<()> {
        self.get_structure()?;

        if self.param_idx == self.num_params() {
            bail!("Already written all query parameters");
        }

        if let DataValue::Null = data {
            // Write non-null flag byte
            self.write(&[0])?;
        } else {
            // TODO: data types
            #[allow(unused_variables)]
            match (self.current_data_type(), data) {
                (DataType::Varchar(_), DataValue::Varchar(val)) => {
                    self.write(&[1])?;
                    self.write_stream(val.as_slice())?;
                }
                (DataType::Binary, DataValue::Binary(val)) => {
                    self.write(&[1])?;
                    self.write_stream(val.as_slice())?;
                }
                (DataType::Boolean, DataValue::Boolean(val)) => todo!(),
                (DataType::Int8, DataValue::Int8(val)) => todo!(),
                (DataType::UInt8, DataValue::UInt8(val)) => todo!(),
                (DataType::Int16, DataValue::Int16(val)) => todo!(),
                (DataType::UInt16, DataValue::UInt16(val)) => todo!(),
                (DataType::Int32, DataValue::Int32(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (DataType::UInt32, DataValue::UInt32(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (DataType::Int64, DataValue::Int64(val)) => todo!(),
                (DataType::UInt64, DataValue::UInt64(val)) => todo!(),
                (DataType::FloatSingle, DataValue::FloatSingle(val)) => todo!(),
                (DataType::FloatDouble, DataValue::FloatDouble(val)) => todo!(),
                (DataType::Decimal(_), DataValue::Decimal(val)) => todo!(),
                (DataType::JSON, DataValue::JSON(val)) => todo!(),
                (DataType::Date, DataValue::Date(val)) => todo!(),
                (DataType::Time, DataValue::Time(val)) => todo!(),
                (DataType::Timestamp, DataValue::Timestamp(val)) => todo!(),
                (DataType::DateTimeWithTZ, DataValue::DateTimeWithTZ(val)) => todo!(),
                (DataType::Uuid, DataValue::Uuid(val)) => todo!(),
                (r#type, data) => bail!(
                    "Data type mismatch on query param {}, expected {:?}, received {:?}",
                    self.param_idx,
                    r#type,
                    data
                ),
            };
        }

        self.advance();

        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.inner
            .write_all(data)
            .context("Failed to write to underlying query handle")
    }

    fn write_stream(&mut self, data: &[u8]) -> Result<()> {
        for chunk in data.chunks(255) {
            // Write chunk length
            self.write(&[chunk.len() as u8])?;
            self.write(chunk)?;
        }

        // Write EOF
        self.write(&[0])?;

        Ok(())
    }

    fn current_data_type(&self) -> &DataType {
        let structure = self.structure.as_ref().unwrap();
        let data_type = &structure.params[self.param_idx];

        data_type
    }

    fn num_params(&self) -> usize {
        self.structure.as_ref().unwrap().params.len()
    }

    fn advance(&mut self) {
        self.param_idx += 1;
    }
}

#[cfg(test)]
pub(super) mod qh_tests {

    use ansilo_core::common::data::{EncodingType, VarcharOptions};

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
        }
    }

    #[test]
    fn test_query_handle_writer_get_structure() {
        let structure = QueryInputStructure::new(vec![DataType::Int32]);
        let mut query = MockQueryHandle::new(structure.clone(), 1024);

        assert_eq!(query.get_structure().unwrap(), &structure);
    }

    #[test]
    fn test_query_handle_writer_no_params() {
        let mut query = MockQueryHandle::new(QueryInputStructure::new(vec![]), 1024);

        assert!(query.write_data_value(DataValue::Null).is_err());
    }

    #[test]
    fn test_query_handle_writer_write_int() {
        let mut query = MockQueryHandle::new(QueryInputStructure::new(vec![DataType::Int32]), 1024);

        query.write_data_value(DataValue::Int32(123)).unwrap();

        let buff = query.inner().unwrap().1.into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
            ]
            .concat()
        )
    }

    #[test]
    fn test_query_handle_writer_write_varchar() {
        let mut query = MockQueryHandle::new(
            QueryInputStructure::new(vec![DataType::Varchar(VarcharOptions::new(
                None,
                EncodingType::Utf8,
            ))]),
            1024,
        );

        query
            .write_data_value(DataValue::Varchar("abc".as_bytes().to_vec()))
            .unwrap();

        let buff = query.inner().unwrap().1.into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                 // not null
                vec![3u8],                 // chunk length
                "abc".as_bytes().to_vec(), // data
                vec![0u8],                 // eof
            ]
            .concat()
        )
    }

    #[test]
    fn test_query_handle_writer_write_long_varchar() {
        let mut query = MockQueryHandle::new(
            QueryInputStructure::new(vec![DataType::Varchar(VarcharOptions::new(
                None,
                EncodingType::Utf8,
            ))]),
            1024,
        );

        query
            .write_data_value(DataValue::Varchar("a".repeat(500).as_bytes().to_vec()))
            .unwrap();

        let buff = query.inner().unwrap().1.into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                           // not null
                vec![255u8],                         // chunk length
                "a".repeat(255).as_bytes().to_vec(), // data
                vec![245u8],                         // chunk length
                "a".repeat(245).as_bytes().to_vec(), // data
                vec![0u8],                           // eof
            ]
            .concat()
        )
    }

    #[test]
    fn test_query_handle_writer_write_null() {
        let mut query = MockQueryHandle::new(QueryInputStructure::new(vec![DataType::Int32]), 1024);

        query.write_data_value(DataValue::Null).unwrap();

        let buff = query.inner().unwrap().1.into_inner();

        assert_eq!(buff, vec![0u8])
    }

    #[test]
    fn test_query_handle_writer_mismatch_data_type() {
        let mut query = MockQueryHandle::new(QueryInputStructure::new(vec![DataType::Int32]), 1024);

        query.write_data_value(DataValue::UInt32(123)).unwrap_err();

        let buff = query.inner().unwrap().1.into_inner();

        assert!(buff.is_empty());
    }

    #[test]
    fn test_query_handle_writer_write_int_then_varchar() {
        let mut query = MockQueryHandle::new(
            QueryInputStructure::new(vec![
                DataType::Int32,
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
            ]),
            1024,
        );

        query.write_data_value(DataValue::Int32(123)).unwrap();
        query
            .write_data_value(DataValue::Varchar("abc".as_bytes().to_vec()))
            .unwrap();

        let buff = query.inner().unwrap().1.into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
                vec![1u8],                      // not null
                vec![3u8],                      // chunk length
                "abc".as_bytes().to_vec(),      // data
                vec![0u8],                      // eof
            ]
            .concat()
        )
    }

    #[test]
    fn test_query_handle_writer_write_excess_data() {
        let mut query = MockQueryHandle::new(QueryInputStructure::new(vec![DataType::Int32]), 1024);

        query.write_data_value(DataValue::Null).unwrap();
        query.write_data_value(DataValue::Null).unwrap_err();

        let buff = query.inner().unwrap().1.into_inner();

        assert_eq!(buff, vec![0u8])
    }
}
