use std::{collections::VecDeque, io::Write};

use ansilo_core::{
    data::{DataType, DataValue},
    err::Result,
};

use super::DataReader;

/// A writable sink that parses the incoming byte stream as a sequence
/// of `DataValue`
///
/// We use this to write query input data then read out the DataValue's
/// and convert them to the data source format.
#[derive(Clone)]
pub struct DataSink {
    /// The data types being read from the input stream
    structure: Vec<DataType>,
    /// Temporary data buffer for partial data values
    buf: VecDeque<u8>,
    /// The buffer to hold incoming data for streamed types
    next_chunk_idx: Option<usize>,
    /// The current row index
    row_idx: u64,
    /// The current column index
    col_idx: usize,
}

impl DataSink {
    pub fn new(structure: Vec<DataType>) -> Self {
        Self {
            structure,
            buf: VecDeque::new(),
            next_chunk_idx: None,
            row_idx: 0,
            col_idx: 0,
        }
    }

    /// Gets the data type structure expected by the data sink
    pub fn get_structure(&self) -> &Vec<DataType> {
        &self.structure
    }

    /// Parses and returns the next data value from the underlying buffer.
    /// Returns Ok(None) if the buffer is empty or contains a partial data value.
    pub fn read_data_value(&mut self) -> Result<Option<DataValue>> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        let not_null = self.buf.front().unwrap();

        if *not_null == 0 {
            let _ = self.buf.pop_front();
            return Ok(Some(DataValue::Null));
        }

        if self.buf.len() == 1 {
            return Ok(None);
        }

        let data_val = if let Some(size) = self.current_data_type_fixed_size() {
            if self.buf.len() < 1 + size {
                return Ok(None);
            }

            let buf = self.buf.drain(..(1 + size)).collect();
            DataReader::read_one(buf, self.current_data_type())
        } else {
            if self.next_chunk_idx.is_none() {
                // Current buffer structure is [(not null)u8, (chunk length)u8, (data)n....]
                self.next_chunk_idx = Some(2 + self.buf[1] as usize);
            }

            loop {
                if self.buf.len() <= self.next_chunk_idx.unwrap() as _ {
                    return Ok(None);
                }

                let chunk_len = self.buf[self.next_chunk_idx.unwrap()];

                if chunk_len == 0 {
                    let buf = self.buf.drain(..=self.next_chunk_idx.unwrap()).collect();
                    let data_val = DataReader::read_one(buf, self.current_data_type());
                    self.next_chunk_idx = None;

                    break data_val;
                }

                // Current buffer structure is [..., (chunk len)u8, (data)n, ...]
                self.next_chunk_idx = Some(1 + self.next_chunk_idx.unwrap() + chunk_len as usize);
            }
        };

        self.advance();

        return data_val.map(|v| Some(v));
    }

    fn current_data_type_fixed_size(&self) -> Option<usize> {
        Some(match self.current_data_type() {
            DataType::Boolean => 1,
            DataType::Int8 => 1,
            DataType::UInt8 => 1,
            DataType::Int16 => 2,
            DataType::UInt16 => 2,
            DataType::Int32 => 4,
            DataType::UInt32 => 4,
            DataType::Int64 => 8,
            DataType::UInt64 => 8,
            DataType::Float32 => 4,
            DataType::Float64 => 8,
            DataType::Date => 6,
            DataType::Time => 7,
            DataType::DateTime => 13,
            DataType::Uuid => 16,
            DataType::Null => 0,
            _ => return None,
        })
    }

    fn current_data_type(&self) -> &DataType {
        &self.structure[self.col_idx]
    }

    pub fn num_cols(&self) -> usize {
        self.structure.len()
    }

    pub fn is_last_col(&self) -> bool {
        self.col_idx == self.num_cols() - 1
    }

    pub fn col_idx(&self) -> usize {
        self.col_idx
    }

    /// Clears the sink
    pub fn clear(&mut self) {
        self.row_idx = 0;
        self.col_idx = 0;
        self.buf.clear();
    }

    pub(crate) fn buf_len(&self) -> usize {
        self.buf.len()
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

impl Write for DataSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend(buf.iter());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ansilo_core::data::{
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        chrono_tz::Tz,
        rust_decimal::Decimal,
        uuid::Uuid,
        DateTimeWithTZ,
    };

    use crate::common::data::DataWriter;

    use super::*;

    #[test]
    fn test_read_data_types_serialised_by_writer() {
        let cases = [
            DataValue::Utf8String("A".into()),
            DataValue::Utf8String("🔥".into()),
            DataValue::Utf8String("foobar".into()),
            DataValue::Utf8String("🥑🚀".into()),
            DataValue::Decimal(Decimal::new(12345600, 5)),
            DataValue::Int8(88),
            DataValue::Int16(5432),
            DataValue::Int32(123456),
            DataValue::Int64(-9876543210i64),
            DataValue::UInt8(188),
            DataValue::UInt16(55432),
            DataValue::UInt32(1123456),
            DataValue::UInt64(19876543210),
            DataValue::Float32(11.22),
            DataValue::Float64(33.44),
            DataValue::Binary(b"BLOB".to_vec()),
            DataValue::JSON("{\"foo\": \"bar\"}".into()),
            DataValue::Date(NaiveDate::from_ymd(2020, 12, 23)),
            DataValue::Time(NaiveTime::from_hms(1, 2, 3)),
            DataValue::DateTime(NaiveDateTime::from_str("2018-02-01T01:02:03").unwrap()),
            DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap(),
                Tz::UTC,
            )),
            DataValue::Uuid(Uuid::new_v4()),
            DataValue::Null,
        ];

        let types = cases.iter().map(|d| d.r#type()).collect();
        let mut sink = DataSink::new(types);

        for case in cases {
            let buf = DataWriter::to_vec_one(case.clone()).unwrap();

            // Write byte-by-byte to ensure we support partial writes in all cases
            for (idx, byte) in buf.iter().cloned().enumerate() {
                sink.write(&[byte]).unwrap();

                if idx == buf.len() - 1 {
                    assert_eq!(sink.read_data_value().unwrap(), Some(case.clone()))
                } else {
                    assert_eq!(sink.read_data_value().unwrap(), None)
                }
            }
        }
    }

    #[test]
    fn test_data_sink_read_empty() {
        let mut sink = DataSink::new(vec![DataType::UInt32]);

        assert_eq!(sink.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_sink_read_partial() {
        let mut sink = DataSink::new(vec![DataType::UInt32]);

        sink.write_all(&[1, 1, 2, 3]).unwrap();

        assert_eq!(sink.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_sink_read_null() {
        let mut sink = DataSink::new(vec![DataType::UInt32]);

        sink.write_all(&[0]).unwrap();

        assert_eq!(sink.read_data_value().unwrap(), Some(DataValue::Null));
        assert_eq!(sink.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_sink_read_uint32() {
        let mut sink = DataSink::new(vec![DataType::UInt32]);

        sink.write_all(
            &[
                vec![1u8],                      // not null
                1234u32.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::UInt32(1234))
        );
        assert_eq!(sink.read_data_value().unwrap(), None,);
    }

    #[test]
    fn test_data_sink_write_partial_then_remainder_read_uint32() {
        let mut sink = DataSink::new(vec![DataType::UInt32]);

        sink.write_all(&[
            1u8, // not null
            0, 0, // partial data
        ])
        .unwrap();

        assert_eq!(sink.read_data_value().unwrap(), None);

        sink.write_all(&1234u16.to_be_bytes()).unwrap();

        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::UInt32(1234))
        );
        assert_eq!(sink.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_sink_read_string() {
        let mut sink = DataSink::new(vec![DataType::rust_string()]);

        sink.write_all(
            &[
                vec![1u8],                   // not null
                vec![5u8],                   // chunk len
                "hello".as_bytes().to_vec(), // data
                vec![0u8],                   // eof
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::Utf8String("hello".into()))
        );
        assert_eq!(sink.read_data_value().unwrap(), None,);
    }

    #[test]
    fn test_data_sink_read_string_multiple_chunks() {
        let mut sink = DataSink::new(vec![DataType::rust_string()]);

        sink.write_all(
            &[
                vec![1u8],                   // not null
                vec![5u8],                   // chunk len
                "hello".as_bytes().to_vec(), // data
                vec![1u8],                   // chunk len
                " ".as_bytes().to_vec(),     // data
                vec![5u8],                   // chunk len
                "world".as_bytes().to_vec(), // data
                vec![0u8],                   // eof
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::Utf8String("hello world".into()))
        );
        assert_eq!(sink.read_data_value().unwrap(), None,);
    }

    #[test]
    fn test_data_sink_read_string_multiple_chunks_with_partial_writes() {
        let mut sink = DataSink::new(vec![DataType::rust_string()]);

        sink.write_all(
            &[
                vec![1u8],                   // not null
                vec![5u8],                   // chunk len
                "hello".as_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.read_data_value().unwrap(), None);

        sink.write_all(
            &[
                vec![1u8],               // chunk len
                " ".as_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.read_data_value().unwrap(), None);

        sink.write_all(
            &[
                vec![5u8],                   // chunk len
                "world".as_bytes().to_vec(), // data
                vec![0u8],                   // eof
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::Utf8String("hello world".into()))
        );
        assert_eq!(sink.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_sink_write_multiple_uint32() {
        let mut sink = DataSink::new(vec![DataType::UInt32]);

        sink.write_all(
            &[
                vec![1u8],                     // not null
                123u32.to_be_bytes().to_vec(), // data
                vec![1u8],                     // not null
                456u32.to_be_bytes().to_vec(), // data
                vec![1u8],                     // not null
                789u32.to_be_bytes().to_vec(), // data
                vec![0u8],                     // null
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::UInt32(123))
        );
        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::UInt32(456))
        );
        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::UInt32(789))
        );
        assert_eq!(sink.read_data_value().unwrap(), Some(DataValue::Null));
        assert_eq!(sink.read_data_value().unwrap(), None,);
    }

    #[test]
    fn test_data_sink_uint32_string_and_int16() {
        let mut sink = DataSink::new(vec![
            DataType::UInt32,
            DataType::rust_string(),
            DataType::Int16,
        ]);

        sink.write_all(
            &[
                vec![1u8],                        // not null
                123u32.to_be_bytes().to_vec(),    // data
                vec![1u8],                        // not null
                vec![3u8],                        // chunk len
                "abc".as_bytes().to_vec(),        // data
                vec![0u8],                        // eof
                vec![1u8],                        // not null
                (-456i16).to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::UInt32(123))
        );
        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::Utf8String("abc".into()))
        );
        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::Int16(-456))
        );
        assert_eq!(sink.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_data_sink_partial_write_then_restart() {
        let mut sink = DataSink::new(vec![DataType::rust_string()]);

        sink.write_all(
            &[
                vec![1u8],                 // not null
                vec![5u8],                 // chunk len
                "hel".as_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.read_data_value().unwrap(), None,);

        sink.clear();

        sink.write_all(
            &[
                vec![1u8],                   // not null
                vec![5u8],                   // chunk len
                "hello".as_bytes().to_vec(), // data
                vec![0u8],                   // eof
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(
            sink.read_data_value().unwrap(),
            Some(DataValue::Utf8String("hello".into()))
        );
        assert_eq!(sink.read_data_value().unwrap(), None,);
    }
}
