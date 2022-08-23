use std::{io::Read, str::FromStr};

use ansilo_core::{
    data::{
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        chrono_tz::Tz,
        rust_decimal::Decimal,
        uuid::Uuid,
        DataType, DataValue, DateTimeWithTZ,
    },
    err::{bail, Context, Error, Result},
};

/// Wraps a Read in order to parse and read the data as DataValue
#[derive(Clone)]
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
            match self.current_data_type() {
                DataType::Utf8String(_) => DataValue::Utf8String(self.read_string()?),
                DataType::Binary => DataValue::Binary(self.read_stream()?),
                DataType::Boolean => DataValue::Boolean(self.read_exact::<1>()?[0] != 0),
                DataType::Int8 => DataValue::Int8(self.read_exact::<1>()?[0] as i8),
                DataType::UInt8 => DataValue::UInt8(self.read_exact::<1>()?[0]),
                DataType::Int16 => DataValue::Int16(i16::from_be_bytes(self.read_exact::<2>()?)),
                DataType::UInt16 => DataValue::UInt16(u16::from_be_bytes(self.read_exact::<2>()?)),
                DataType::Int32 => DataValue::Int32(i32::from_be_bytes(self.read_exact::<4>()?)),
                DataType::UInt32 => DataValue::UInt32(u32::from_be_bytes(self.read_exact::<4>()?)),
                DataType::Int64 => DataValue::Int64(i64::from_be_bytes(self.read_exact::<8>()?)),
                DataType::UInt64 => DataValue::UInt64(u64::from_be_bytes(self.read_exact::<8>()?)),
                DataType::Float32 => {
                    DataValue::Float32(f32::from_be_bytes(self.read_exact::<4>()?))
                }
                DataType::Float64 => {
                    DataValue::Float64(f64::from_be_bytes(self.read_exact::<8>()?))
                }
                DataType::Decimal(_) => DataValue::Decimal(
                    Decimal::from_str_exact(&self.read_string()?)
                        .context("Failed to parse decimal value")?,
                ),
                DataType::JSON => DataValue::JSON(self.read_string()?),
                DataType::Date => DataValue::Date(Self::read_date(self.read_exact()?)?),
                DataType::Time => DataValue::Time(Self::read_time(self.read_exact()?)?),
                DataType::DateTime => DataValue::DateTime(Self::read_date_time(self.read_exact()?)?),
                DataType::DateTimeWithTZ => {
                    let buff = self.read_stream()?;
                    let (dt, tz) = buff.split_at(13);
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        Self::read_date_time(dt.try_into().unwrap())?,
                        Tz::from_str(
                            &String::from_utf8(tz.to_vec())
                                .context("Failed to parse tz as UTF8")?,
                        )
                        .map_err(|tz| Error::msg(format!("Unknown timezone: {tz}")))?,
                    ))
                }
                DataType::Uuid => DataValue::Uuid(Uuid::from_bytes(self.read_exact::<16>()?)),
                DataType::Null => bail!("Found null data type with non-null byte"),
            }
        } else {
            DataValue::Null
        };

        self.advance();

        Ok(Some(res))
    }

    fn read_string(&mut self) -> Result<String> {
        Ok(String::from_utf8(self.read_stream()?).context("Failed to parse bytes as UTF8")?)
    }

    fn read_date_time(buff: [u8; 13]) -> Result<NaiveDateTime> {
        let date = Self::read_date(buff[..6].try_into().unwrap())?;
        let time = Self::read_time(buff[6..].try_into().unwrap())?;
        Ok(NaiveDateTime::new(date, time))
    }

    fn read_time(d: [u8; 7]) -> Result<NaiveTime> {
        Ok(NaiveTime::from_hms_nano(
            d[0] as _,
            d[1] as _,
            d[2] as _,
            u32::from_be_bytes([d[3], d[4], d[5], d[6]]),
        ))
    }

    fn read_date(d: [u8; 6]) -> Result<NaiveDate> {
        Ok(NaiveDate::from_ymd(
            i32::from_be_bytes([d[0], d[1], d[2], d[3]]),
            d[4] as _,
            d[5] as _,
        ))
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

    use ansilo_core::data::{uuid, DecimalOptions, StringOptions};

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
                123_i32.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(123)));
    }

    #[test]
    fn test_data_reader_varchar() {
        let mut res = create_data_reader(
            vec![DataType::Utf8String(StringOptions::default())],
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
            Some(DataValue::Utf8String("abc".into()))
        );
    }

    #[test]
    fn test_data_reader_varchar_multiple_reads() {
        let mut res = create_data_reader(
            vec![DataType::Utf8String(StringOptions::default())],
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
            Some(DataValue::Utf8String("abc12345".into()))
        );
    }

    #[test]
    fn test_data_reader_int32_multiple_rows() {
        let mut res = create_data_reader(
            vec![DataType::Int32],
            [
                vec![1u8],                      // not null
                123_i32.to_be_bytes().to_vec(), // data
                vec![1u8],                      // not null
                456_i32.to_be_bytes().to_vec(), // data
                vec![1u8],                      // not null
                789_i32.to_be_bytes().to_vec(), // data
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
                123_i32.to_be_bytes().to_vec(), // data
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
                DataType::Utf8String(StringOptions::default()),
            ],
            [
                vec![1u8],                      // not null
                123_i32.to_be_bytes().to_vec(), // data
                vec![1u8],                      // not null
                vec![3u8],                      // read length
                "abc".as_bytes().to_vec(),      // data
                vec![0u8],                      // read length (eof)
                vec![1u8],                      // not null
                456_i32.to_be_bytes().to_vec(), // data
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
            Some(DataValue::Utf8String("abc".into()))
        );
        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(456)));
        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Utf8String("123".into()))
        );
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_int64() {
        let mut res = create_data_reader(
            vec![DataType::Int64],
            [
                vec![1u8],                       // not null
                1234_i64.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int64(1234)));
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_uint64() {
        let mut res = create_data_reader(
            vec![DataType::UInt64],
            [
                vec![1u8],                       // not null
                1234_u64.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::UInt64(1234))
        );
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_end_mid_row() {
        let mut res = create_data_reader(
            vec![DataType::Int32, DataType::Int32],
            [
                vec![1u8],                      // not null
                123_i32.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int32(123)));
        assert!(res.read_data_value().is_err());
    }

    #[test]
    fn test_data_reader_uint8() {
        let mut res = create_data_reader(
            vec![DataType::UInt8],
            [
                vec![1u8],   // not null
                vec![123u8], // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::UInt8(123)));
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_int8() {
        let mut res = create_data_reader(
            vec![DataType::Int8],
            [
                vec![1u8],          // not null
                vec![-123i8 as u8], // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int8(-123)));
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_uint16() {
        let mut res = create_data_reader(
            vec![DataType::UInt16],
            [
                vec![1u8],                       // not null
                1234_u16.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::UInt16(1234))
        );
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_int16() {
        let mut res = create_data_reader(
            vec![DataType::Int16],
            [
                vec![1u8],                       // not null
                1234_i16.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Int16(1234)));
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_float32() {
        let mut res = create_data_reader(
            vec![DataType::Float32],
            [
                vec![1u8],                          // not null
                123.456_f32.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Float32(123.456))
        );
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_float64() {
        let mut res = create_data_reader(
            vec![DataType::Float64],
            [
                vec![1u8],                          // not null
                123.456_f64.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Float64(123.456))
        );
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_decimal() {
        let mut res = create_data_reader(
            vec![DataType::Decimal(DecimalOptions::default())],
            [
                vec![1u8],                     // not null
                vec![7u8],                     // len
                "100.123".as_bytes().to_vec(), // str
                vec![0u8],                     // eof
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Decimal(Decimal::new(100123, 3)))
        );
        assert_eq!(res.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_reader_json() {
        let mut res = create_data_reader(
            vec![DataType::JSON],
            [
                vec![1u8],                // not null
                vec![2u8],                // read length
                "{}".as_bytes().to_vec(), // data
                vec![0u8],                // read length (eof)
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::JSON("{}".to_string()))
        );
    }

    #[test]
    fn test_data_reader_date() {
        let mut res = create_data_reader(
            vec![DataType::Date],
            [
                vec![1u8],                       // not null
                2020_i32.to_be_bytes().to_vec(), // year
                vec![10u8],                      // month
                vec![21u8],                      // day
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Date(NaiveDate::from_ymd(2020, 10, 21)))
        );
    }

    #[test]
    fn test_data_reader_time() {
        let mut res = create_data_reader(
            vec![DataType::Time],
            [
                vec![1u8],                        // not null
                vec![12u8],                       // hour
                vec![45u8],                       // minute
                vec![23u8],                       // second
                12345_u32.to_be_bytes().to_vec(), // nanosec
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Time(NaiveTime::from_hms_nano(12, 45, 23, 12345)))
        );
    }

    #[test]
    fn test_data_reader_date_time() {
        let mut res = create_data_reader(
            vec![DataType::DateTime],
            [
                vec![1u8],                        // not null
                2020_i32.to_be_bytes().to_vec(),  // year
                vec![10u8],                       // month
                vec![21u8],                       // day
                vec![12u8],                       // hour
                vec![45u8],                       // minute
                vec![23u8],                       // second
                12345_u32.to_be_bytes().to_vec(), // nanosec
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::DateTime(NaiveDateTime::new(
                NaiveDate::from_ymd(2020, 10, 21),
                NaiveTime::from_hms_nano(12, 45, 23, 12345)
            )))
        );
    }

    #[test]
    fn test_data_reader_date_time_with_tz() {
        let mut res = create_data_reader(
            vec![DataType::DateTimeWithTZ],
            [
                vec![1u8],                                 // not null
                vec![13u8],                                // dt (len)
                2020_i32.to_be_bytes().to_vec(),           // year
                vec![10u8],                                // month
                vec![21u8],                                // day
                vec![12u8],                                // hour
                vec![45u8],                                // minute
                vec![23u8],                                // second
                12345_u32.to_be_bytes().to_vec(),          // nanosec
                vec![19u8],                                // tz (len)
                "Australia/Melbourne".as_bytes().to_vec(), // tz (name)
                vec![0u8],                                 // tz (eof)
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2020, 10, 21),
                    NaiveTime::from_hms_nano(12, 45, 23, 12345)
                ),
                Tz::Australia__Melbourne
            )))
        );
    }

    #[test]
    fn test_data_reader_date_time_with_tz_one_chunk() {
        let mut res = create_data_reader(
            vec![DataType::DateTimeWithTZ],
            [
                vec![1u8],                                 // not null
                vec![13u8 + 19u8],                         // dt + tz (len)
                2020_i32.to_be_bytes().to_vec(),           // year
                vec![10u8],                                // month
                vec![21u8],                                // day
                vec![12u8],                                // hour
                vec![45u8],                                // minute
                vec![23u8],                                // second
                12345_u32.to_be_bytes().to_vec(),          // nanosec
                "Australia/Melbourne".as_bytes().to_vec(), // tz (name)
                vec![0u8],                                 // tz (eof)
            ]
            .concat(),
        );

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2020, 10, 21),
                    NaiveTime::from_hms_nano(12, 45, 23, 12345)
                ),
                Tz::Australia__Melbourne
            )))
        );
    }

    #[test]
    fn test_data_reader_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let mut res = create_data_reader(
            vec![DataType::Uuid],
            [
                vec![1u8], // not null
                uuid.as_bytes().to_vec(),
            ]
            .concat(),
        );

        assert_eq!(res.read_data_value().unwrap(), Some(DataValue::Uuid(uuid)));
    }

    #[test]
    fn test_data_reader_null_with_invalid_non_null_byte() {
        let mut res = create_data_reader(
            vec![DataType::Null],
            [
                vec![1u8], // not null
            ]
            .concat(),
        );

        res.read_data_value().unwrap_err();
    }
}
