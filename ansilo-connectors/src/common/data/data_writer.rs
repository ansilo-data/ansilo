use std::io::{self, Write};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, Context, Result},
};

/// Wraps an inner writer T providing an interface to serialise DataValue
/// to an underlying stream
#[derive(Clone)]
pub struct DataWriter<T>
where
    T: Write,
{
    /// The inner write
    inner: T,
    /// The expected type of data values if any
    structure: Option<Vec<DataType>>,
    /// The current parameter index
    param_idx: usize,
}

impl<T> DataWriter<T>
where
    T: Write,
{
    pub fn new(inner: T, structure: Option<Vec<DataType>>) -> Self {
        Self {
            inner,
            structure,
            param_idx: 0,
        }
    }

    pub fn inner(self) -> T {
        self.inner
    }

    pub fn get_structure(&mut self) -> Option<&Vec<DataType>> {
        self.structure.as_ref()
    }

    /// Writes the supplied data value to the underlying query handle
    pub fn write_data_value(&mut self, data: DataValue) -> Result<()> {
        if self.structure.is_some() && self.param_idx == self.num_params().unwrap() {
            bail!("Already written all query parameters");
        }

        if let DataValue::Null = data {
            // Write non-null flag byte
            self.write(&[0])?;
        } else {
            // TODO: data types
            #[allow(unused_variables)]
            match (self.current_data_type(), data) {
                (None | Some(DataType::Utf8String(_)), DataValue::Utf8String(val)) => {
                    self.write(&[1])?;
                    self.write_stream(val.as_slice())?;
                }
                (None | Some(DataType::Binary), DataValue::Binary(val)) => {
                    self.write(&[1])?;
                    self.write_stream(val.as_slice())?;
                }
                (None | Some(DataType::Boolean), DataValue::Boolean(val)) => todo!(),
                (None | Some(DataType::Int8), DataValue::Int8(val)) => todo!(),
                (None | Some(DataType::UInt8), DataValue::UInt8(val)) => {
                    self.write(&[1])?;
                    self.write(&[val])?;
                }
                (None | Some(DataType::Int16), DataValue::Int16(val)) => todo!(),
                (None | Some(DataType::UInt16), DataValue::UInt16(val)) => todo!(),
                (None | Some(DataType::Int32), DataValue::Int32(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::UInt32), DataValue::UInt32(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::Int64), DataValue::Int64(val)) => todo!(),
                (None | Some(DataType::UInt64), DataValue::UInt64(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::Float32), DataValue::Float32(val)) => todo!(),
                (None | Some(DataType::Float64), DataValue::Float64(val)) => todo!(),
                (None | Some(DataType::Decimal(_)), DataValue::Decimal(val)) => todo!(),
                (None | Some(DataType::JSON), DataValue::JSON(val)) => todo!(),
                (None | Some(DataType::Date), DataValue::Date(val)) => todo!(),
                (None | Some(DataType::Time), DataValue::Time(val)) => todo!(),
                (None | Some(DataType::DateTime), DataValue::DateTime(val)) => todo!(),
                (None | Some(DataType::DateTimeWithTZ), DataValue::DateTimeWithTZ(val)) => todo!(),
                (None | Some(DataType::Uuid), DataValue::Uuid(val)) => todo!(),
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

    fn current_data_type(&self) -> Option<&DataType> {
        self.structure.as_ref().map(|i| &i[self.param_idx])
    }

    fn num_params(&self) -> Option<usize> {
        self.structure.as_ref().map(|i| i.len())
    }

    fn advance(&mut self) {
        self.param_idx += 1;
    }
}

impl DataWriter<io::Cursor<Vec<u8>>> {
    /// Converts a vec of DataValue into a buffer
    pub fn to_vec(data: Vec<DataValue>) -> Result<Vec<u8>> {
        let mut writer = DataWriter::new(io::Cursor::new(vec![]), None);

        for val in data.into_iter() {
            writer
                .write_data_value(val)
                .context("Failed to write query parameter")?;
        }

        Ok(writer.inner().into_inner())
    }

    /// Converts a single DataValue into a buffer
    pub fn to_vec_one(data: DataValue) -> Result<Vec<u8>> {
        Self::to_vec(vec![data])
    }
}

#[cfg(test)]
mod tests {

    use std::io;

    use ansilo_core::data::StringOptions;

    use super::*;

    fn create_data_writer(structure: Option<Vec<DataType>>) -> DataWriter<io::Cursor<Vec<u8>>> {
        DataWriter::new(io::Cursor::new(vec![]), structure)
    }

    #[test]
    fn test_data_writer_get_structure() {
        let structure = vec![DataType::Int32];
        let mut writer = create_data_writer(Some(structure.clone()));

        assert_eq!(writer.get_structure().unwrap(), &structure);
    }

    #[test]
    fn test_data_writer_no_params() {
        let mut writer = create_data_writer(Some(vec![]));

        assert!(writer.write_data_value(DataValue::Null).is_err());
    }

    #[test]
    fn test_data_writer_write_int() {
        let mut writer = create_data_writer(Some(vec![DataType::Int32]));

        writer.write_data_value(DataValue::Int32(123)).unwrap();

        let buff = writer.inner().into_inner();

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
    fn test_data_writer_write_varchar() {
        let mut writer =
            create_data_writer(Some(vec![DataType::Utf8String(StringOptions::default())]));

        writer
            .write_data_value(DataValue::Utf8String("abc".as_bytes().to_vec()))
            .unwrap();

        let buff = writer.inner().into_inner();

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
    fn test_data_writer_write_long_varchar() {
        let mut writer =
            create_data_writer(Some(vec![DataType::Utf8String(StringOptions::default())]));

        writer
            .write_data_value(DataValue::Utf8String("a".repeat(500).as_bytes().to_vec()))
            .unwrap();

        let buff = writer.inner().into_inner();

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
    fn test_data_writer_write_null() {
        let mut writer = create_data_writer(Some(vec![DataType::Int32]));

        writer.write_data_value(DataValue::Null).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(buff, vec![0u8])
    }

    #[test]
    fn test_data_writer_mismatch_data_type() {
        let mut writer = create_data_writer(Some(vec![DataType::Int32]));

        writer.write_data_value(DataValue::UInt32(123)).unwrap_err();

        let buff = writer.inner().into_inner();

        assert!(buff.is_empty());
    }

    #[test]
    fn test_data_writer_write_int_then_varchar() {
        let mut writer = create_data_writer(Some(vec![
            DataType::Int32,
            DataType::Utf8String(StringOptions::default()),
        ]));

        writer.write_data_value(DataValue::Int32(123)).unwrap();
        writer
            .write_data_value(DataValue::Utf8String("abc".as_bytes().to_vec()))
            .unwrap();

        let buff = writer.inner().into_inner();

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
    fn test_data_writer_write_excess_data() {
        let mut writer = create_data_writer(Some(vec![DataType::Int32]));

        writer.write_data_value(DataValue::Null).unwrap();
        writer.write_data_value(DataValue::Null).unwrap_err();

        let buff = writer.inner().into_inner();

        assert_eq!(buff, vec![0u8])
    }

    #[test]
    fn test_data_writer_with_no_structure() {
        let mut writer = create_data_writer(None);

        writer.write_data_value(DataValue::Int32(123)).unwrap();
        writer
            .write_data_value(DataValue::Utf8String("abc".as_bytes().to_vec()))
            .unwrap();
        writer.write_data_value(DataValue::Int32(456)).unwrap();
        writer.write_data_value(DataValue::Null).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                      // not null
                123_i32.to_ne_bytes().to_vec(), // data
                vec![1u8],                      // not null
                vec![3u8],                      // chunk length
                "abc".as_bytes().to_vec(),      // data
                vec![0u8],                      // eof
                vec![1u8],                      // not null
                456_i32.to_ne_bytes().to_vec(), // data
                vec![0u8],                      // null
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_to_vec() {
        let buff = DataWriter::to_vec(vec![DataValue::UInt8(10), DataValue::UInt8(20)]).unwrap();

        assert_eq!(
            buff,
            [
                vec![1u8],  // not null
                vec![10u8], // val 1
                vec![1u8],  // not null
                vec![20u8], // val 2
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_to_vec_one() {
        let buff =
            DataWriter::to_vec_one(DataValue::Utf8String("abc".as_bytes().to_vec())).unwrap();

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
    fn test_data_writer_write_uint64() {
        let mut writer = create_data_writer(Some(vec![DataType::UInt64]));

        writer.write_data_value(DataValue::UInt64(1234)).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                       // not null
                1234_u64.to_ne_bytes().to_vec(), // data
            ]
            .concat()
        )
    }
}
