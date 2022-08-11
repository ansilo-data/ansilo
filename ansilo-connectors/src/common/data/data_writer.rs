use std::io::{self, Write};

use ansilo_core::{
    data::{
        chrono::{Datelike, Timelike},
        DataType, DataValue,
    },
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

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn get_structure(&mut self) -> Option<&Vec<DataType>> {
        self.structure.as_ref()
    }

    pub fn restart(&mut self) -> Result<()> {
        self.param_idx = 0;
        Ok(())
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
            match (self.current_data_type(), data) {
                (None | Some(DataType::Utf8String(_)), DataValue::Utf8String(val)) => {
                    self.write(&[1])?;
                    self.write_stream(val.as_slice())?;
                }
                (None | Some(DataType::Binary), DataValue::Binary(val)) => {
                    self.write(&[1])?;
                    self.write_stream(val.as_slice())?;
                }
                (None | Some(DataType::Boolean), DataValue::Boolean(val)) => {
                    self.write(&[1])?;
                    self.write(&[val as u8])?;
                }
                (None | Some(DataType::Int8), DataValue::Int8(val)) => {
                    self.write(&[1])?;
                    self.write(&[val as u8])?;
                }
                (None | Some(DataType::UInt8), DataValue::UInt8(val)) => {
                    self.write(&[1])?;
                    self.write(&[val])?;
                }
                (None | Some(DataType::Int16), DataValue::Int16(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::UInt16), DataValue::UInt16(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::Int32), DataValue::Int32(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::UInt32), DataValue::UInt32(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::Int64), DataValue::Int64(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::UInt64), DataValue::UInt64(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::Float32), DataValue::Float32(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::Float64), DataValue::Float64(val)) => {
                    self.write(&[1])?;
                    self.write(&val.to_ne_bytes())?;
                }
                (None | Some(DataType::Decimal(_)), DataValue::Decimal(val)) => {
                    self.write(&[1])?;
                    self.write_stream(val.to_string().as_bytes())?;
                }
                (None | Some(DataType::JSON), DataValue::JSON(val)) => {
                    self.write(&[1])?;
                    self.write_stream(val.as_bytes())?;
                }
                (None | Some(DataType::Date), DataValue::Date(val)) => {
                    self.write(&[1])?;
                    self.write_date(val)?;
                }
                (None | Some(DataType::Time), DataValue::Time(val)) => {
                    self.write(&[1])?;
                    self.write_time(val)?;
                }
                (None | Some(DataType::DateTime), DataValue::DateTime(val)) => {
                    self.write(&[1])?;
                    self.write_date_time(val)?;
                }
                (None | Some(DataType::DateTimeWithTZ), DataValue::DateTimeWithTZ(val)) => {
                    self.write(&[1])?;
                    // We write this as a stream type, so prefix the first chunk
                    // with its length
                    self.write(&[13])?;
                    self.write_date_time(val.dt)?;
                    self.write_stream(val.tz.name().as_bytes())?;
                }
                (None | Some(DataType::Uuid), DataValue::Uuid(val)) => {
                    self.write(&[1])?;
                    self.write(val.as_bytes())?;
                }
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

    fn write_date_time(&mut self, val: ansilo_core::data::chrono::NaiveDateTime) -> Result<()> {
        self.write_date(val.date())?;
        self.write_time(val.time())?;
        Ok(())
    }

    fn write_time(&mut self, val: ansilo_core::data::chrono::NaiveTime) -> Result<()> {
        self.write(&[val.hour() as u8, val.minute() as u8, val.second() as u8])?;
        self.write(&val.nanosecond().to_ne_bytes())?;
        Ok(())
    }

    fn write_date(&mut self, val: ansilo_core::data::chrono::NaiveDate) -> Result<()> {
        self.write(&val.year().to_ne_bytes())?;
        self.write(&[val.month() as u8, val.day() as u8])?;
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

    use ansilo_core::data::{
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        chrono_tz::Tz,
        rust_decimal::Decimal,
        DateTimeWithTZ, DecimalOptions, StringOptions,
    };

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
    fn test_data_writer_write_int64() {
        let mut writer = create_data_writer(Some(vec![DataType::Int64]));

        writer.write_data_value(DataValue::Int64(1234)).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                       // not null
                1234_i64.to_ne_bytes().to_vec(), // data
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

    #[test]
    fn test_data_writer_write_bool() {
        let mut writer = create_data_writer(Some(vec![DataType::Boolean]));

        writer.write_data_value(DataValue::Boolean(true)).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            vec![
                1u8, // not null
                1u8, // data
            ]
        );

        let mut writer = create_data_writer(Some(vec![DataType::Boolean]));

        writer.write_data_value(DataValue::Boolean(false)).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            vec![
                1u8, // not null
                0u8, // data
            ]
        )
    }

    #[test]
    fn test_data_writer_write_uint8() {
        let mut writer = create_data_writer(Some(vec![DataType::UInt8]));

        writer.write_data_value(DataValue::UInt8(234)).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            vec![
                1u8,   // not null
                234u8, // data
            ]
        );
    }

    #[test]
    fn test_data_writer_write_int8() {
        let mut writer = create_data_writer(Some(vec![DataType::Int8]));

        writer.write_data_value(DataValue::Int8(-120)).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            vec![
                1u8,   // not null
                136u8, // data
            ]
        );
    }

    #[test]
    fn test_data_writer_write_uint16() {
        let mut writer = create_data_writer(Some(vec![DataType::UInt16]));

        writer.write_data_value(DataValue::UInt16(1234)).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                       // not null
                1234_u16.to_ne_bytes().to_vec(), // data
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_int16() {
        let mut writer = create_data_writer(Some(vec![DataType::Int16]));

        writer.write_data_value(DataValue::Int16(1234)).unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                       // not null
                1234_i16.to_ne_bytes().to_vec(), // data
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_float32() {
        let mut writer = create_data_writer(Some(vec![DataType::Float32]));

        writer
            .write_data_value(DataValue::Float32(1234.567))
            .unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                           // not null
                1234.567_f32.to_ne_bytes().to_vec(), // data
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_float64() {
        let mut writer = create_data_writer(Some(vec![DataType::Float64]));

        writer
            .write_data_value(DataValue::Float64(1234.567))
            .unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                           // not null
                1234.567_f64.to_ne_bytes().to_vec(), // data
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_decimal() {
        let mut writer =
            create_data_writer(Some(vec![DataType::Decimal(DecimalOptions::default())]));

        writer
            .write_data_value(DataValue::Decimal(Decimal::ONE_THOUSAND))
            .unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                  // not null
                vec![4u8],                  // len
                "1000".as_bytes().to_vec(), // str
                vec![0u8],                  // eof
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_json() {
        let mut writer = create_data_writer(Some(vec![DataType::JSON]));

        writer
            .write_data_value(DataValue::JSON("{}".to_string()))
            .unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                // not null
                vec![2u8],                // chunk length
                "{}".as_bytes().to_vec(), // data
                vec![0u8],                // eof
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_date() {
        let mut writer = create_data_writer(Some(vec![DataType::Date]));

        writer
            .write_data_value(DataValue::Date(NaiveDate::from_ymd(2000, 10, 24)))
            .unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                       // not null
                2000_u32.to_ne_bytes().to_vec(), // year
                vec![10u8],                      // month
                vec![24u8],                      // day
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_time() {
        let mut writer = create_data_writer(Some(vec![DataType::Time]));

        writer
            .write_data_value(DataValue::Time(NaiveTime::from_hms_nano(6, 45, 21, 12345)))
            .unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                        // not null
                vec![6u8],                        // hour
                vec![45u8],                       // min
                vec![21u8],                       // sec
                12345_u32.to_ne_bytes().to_vec(), // nano
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_date_time() {
        let mut writer = create_data_writer(Some(vec![DataType::DateTime]));

        writer
            .write_data_value(DataValue::DateTime(NaiveDateTime::new(
                NaiveDate::from_ymd(2000, 10, 24),
                NaiveTime::from_hms_nano(6, 45, 21, 12345),
            )))
            .unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                        // not null
                2000_u32.to_ne_bytes().to_vec(),  // year
                vec![10u8],                       // month
                vec![24u8],                       // day
                vec![6u8],                        // hour
                vec![45u8],                       // min
                vec![21u8],                       // sec
                12345_u32.to_ne_bytes().to_vec(), // nano
            ]
            .concat()
        )
    }

    #[test]
    fn test_data_writer_write_date_time_with_tz() {
        let mut writer = create_data_writer(Some(vec![DataType::DateTimeWithTZ]));

        writer
            .write_data_value(DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2000, 10, 24),
                    NaiveTime::from_hms_nano(6, 45, 21, 12345),
                ),
                Tz::Australia__Melbourne,
            )))
            .unwrap();

        let buff = writer.inner().into_inner();

        assert_eq!(
            buff,
            [
                vec![1u8],                                 // not null
                vec![13u8],                                // dt len
                2000_u32.to_ne_bytes().to_vec(),           // year
                vec![10u8],                                // month
                vec![24u8],                                // day
                vec![6u8],                                 // hour
                vec![45u8],                                // min
                vec![21u8],                                // sec
                12345_u32.to_ne_bytes().to_vec(),          // nano
                vec![19u8],                                // tz len
                "Australia/Melbourne".as_bytes().to_vec(), // tz name
                vec![0u8],                                 // tz end
            ]
            .concat()
        )
    }
}
