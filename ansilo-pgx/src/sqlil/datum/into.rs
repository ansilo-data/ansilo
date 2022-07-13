use ansilo_core::{
    common::data::{
        chrono::{
            Datelike, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Timelike, Weekday,
        },
        chrono_tz::Tz,
        uuid, DataType, DataValue, EncodingType, VarcharOptions,
    },
    err::{bail, Result},
};
use pgx::{
    pg_schema,
    pg_sys::{self, Oid},
    IntoDatum, PgBox,
};

/// Attempt to convert an ansilo DataValue into a postgres Datum type
///
/// This is a hot code path. We need to take good care in optimising this.
pub unsafe fn into_datum(
    type_oid: Oid,
    r#type: &DataType,
    val: DataValue,
    is_null: *mut bool,
    datum: *mut pg_sys::Datum,
) -> Result<()> {
    *is_null = false;

    *datum = match (type_oid, r#type, val) {
        (_, _, DataValue::Null) => {
            *is_null = true;
            0i8.into_datum().unwrap()
        }
        //
        (
            pg_sys::VARCHAROID | pg_sys::TEXTOID,
            DataType::Varchar(opts),
            DataValue::Varchar(data),
        ) => into_string(data, opts)?.into_datum().unwrap(),
        //
        (pg_sys::BYTEAOID, DataType::Varchar(_), DataValue::Varchar(data)) => {
            data.into_datum().unwrap()
        }
        (pg_sys::BYTEAOID, DataType::Binary, DataValue::Binary(data)) => data.into_datum().unwrap(),
        //
        (pg_sys::BOOLOID, DataType::Boolean, DataValue::Boolean(data)) => {
            data.into_datum().unwrap()
        }
        // Postgres doesn't have an unsigned byte type and "char" is limited to [0, 127]
        (pg_sys::INT2OID, DataType::Int8, DataValue::Int8(data)) => {
            (data as i16).into_datum().unwrap()
        }
        (pg_sys::INT2OID, DataType::Int16, DataValue::Int16(data)) => data.into_datum().unwrap(),
        (pg_sys::INT4OID, DataType::Int32, DataValue::Int32(data)) => data.into_datum().unwrap(),
        (pg_sys::INT8OID, DataType::Int64, DataValue::Int64(data)) => data.into_datum().unwrap(),
        // For our unsigned integer types, pg doesn't have native types to match
        // We err on the side of caution and preserve the value rather than the type
        (pg_sys::INT2OID, DataType::UInt8, DataValue::UInt8(data)) => {
            (data as i16).into_datum().unwrap()
        }
        (pg_sys::INT4OID, DataType::UInt16, DataValue::UInt16(data)) => {
            (data as i32).into_datum().unwrap()
        }
        (pg_sys::INT8OID, DataType::UInt32, DataValue::UInt32(data)) => {
            (data as i64).into_datum().unwrap()
        }
        (pg_sys::NUMERICOID, DataType::UInt64, DataValue::UInt64(data)) => {
            pgx::Numeric(data.to_string()).into_datum().unwrap()
        }
        //
        (pg_sys::FLOAT4OID, DataType::Float32, DataValue::Float32(data)) => {
            data.into_datum().unwrap()
        }
        (pg_sys::FLOAT8OID, DataType::Float64, DataValue::Float64(data)) => {
            data.into_datum().unwrap()
        }
        (pg_sys::NUMERICOID, DataType::Decimal(_), DataValue::Decimal(data)) => {
            pgx::Numeric(data.to_string()).into_datum().unwrap()
        }
        //
        (pg_sys::JSONOID, DataType::JSON, DataValue::JSON(data)) => {
            pgx::JsonString(data).into_datum().unwrap()
        }
        (pg_sys::JSONBOID, DataType::JSON, DataValue::JSON(data)) => {
            pgx::JsonB(serde_json::from_str(&data)?)
                .into_datum()
                .unwrap()
        }
        //
        (pg_sys::DATEOID, DataType::Date, DataValue::Date(data)) => {
            into_date(data).into_datum().unwrap()
        }
        (pg_sys::TIMEOID, DataType::Time, DataValue::Time(data)) => {
            into_time(data).into_datum().unwrap()
        }
        (pg_sys::TIMESTAMPOID, DataType::DateTime, DataValue::DateTime(data)) => {
            into_date_time(data).into_datum().unwrap()
        }
        (pg_sys::TIMESTAMPTZOID, DataType::DateTimeWithTZ, DataValue::DateTimeWithTZ(data)) => {
            into_date_time_tz(data).into_datum().unwrap()
        }
        //
        (pg_sys::UUIDOID, DataType::Uuid, DataValue::Uuid(data)) => {
            into_uuid(data).into_datum().unwrap()
        }
        (type_oid, r#type, _) => bail!(
            "Type mismatch between underlying {:?} type and postgres type {:?}",
            r#type,
            type_oid
        ),
    };

    Ok(())
}

fn into_string(data: Vec<u8>, opts: &VarcharOptions) -> Result<String> {
    Ok(match opts.encoding {
        // ASCII should be interpretable as UTF-8
        EncodingType::Ascii => String::from_utf8(data)?,
        EncodingType::Utf8 => String::from_utf8(data)?,
        EncodingType::Utf16 if data.len() % 2 == 0 => String::from_utf16(
            data.chunks(2)
                .into_iter()
                .map(|i| u16::from_ne_bytes([i[0], i[1]]))
                .collect::<Vec<u16>>()
                .as_slice(),
        )?,
        _ => bail!("Invalid string data found"),
    })
}

/// Converts the supplied DataValue into a pgalloc'd Datum
pub unsafe fn into_datum_pg_alloc(
    type_oid: Oid,
    r#type: &DataType,
    val: DataValue,
) -> Result<(bool, PgBox<pg_sys::Datum, pgx::AllocatedByRust>)> {
    let datum = PgBox::<pg_sys::Datum>::alloc();
    let mut is_null = false;
    into_datum(
        type_oid,
        r#type,
        val,
        &mut is_null as *mut _,
        datum.as_ptr(),
    )?;

    Ok((is_null, datum))
}

fn into_date(data: NaiveDate) -> pgx::Date {
    pgx::Date::new(
        time::Date::from_iso_week_date(
            data.year() as _,
            data.iso_week().week() as _,
            match data.weekday() {
                Weekday::Mon => time::Weekday::Monday,
                Weekday::Tue => time::Weekday::Tuesday,
                Weekday::Wed => time::Weekday::Wednesday,
                Weekday::Thu => time::Weekday::Thursday,
                Weekday::Fri => time::Weekday::Friday,
                Weekday::Sat => time::Weekday::Saturday,
                Weekday::Sun => time::Weekday::Sunday,
            },
        )
        .unwrap(),
    )
}

fn into_time(data: NaiveTime) -> pgx::Time {
    pgx::Time::new(
        time::Time::from_hms_nano(
            data.hour() as _,
            data.minute() as _,
            data.second() as _,
            data.nanosecond() as _,
        )
        .unwrap(),
    )
}

fn into_date_time(data: NaiveDateTime) -> pgx::Timestamp {
    pgx::Timestamp::new(time::PrimitiveDateTime::new(
        *into_date(data.date()),
        *into_time(data.time()),
    ))
}

fn into_date_time_tz(data: (NaiveDateTime, Tz)) -> pgx::TimestampWithTimeZone {
    pgx::TimestampWithTimeZone::new(
        *into_date_time(data.0),
        time::UtcOffset::from_whole_seconds(
            data.1
                .offset_from_local_datetime(&data.0)
                .unwrap()
                .fix()
                .local_minus_utc(),
        )
        .unwrap(),
    )
}

fn into_uuid(data: uuid::Uuid) -> pgx::Uuid {
    pgx::Uuid::from_bytes(data.into_bytes())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use ansilo_core::common::data::{rust_decimal::Decimal, DecimalOptions};
    use pgx::*;

    use super::*;

    unsafe fn into_datum_owned(
        type_oid: Oid,
        r#type: DataType,
        val: DataValue,
    ) -> Result<(bool, pg_sys::Datum)> {
        let (is_null, datum) = into_datum_pg_alloc(type_oid, &r#type, val)?;
        let datum = datum.into_pg_boxed();

        Ok((is_null, *datum))
    }

    #[pg_test]
    fn test_into_datum_null() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::INT2OID, DataType::Int32, DataValue::Null).unwrap(),
                (true, pgx::Datum::from(0u8))
            );
        }
    }

    #[pg_test]
    fn test_into_datum_i8() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::INT2OID, DataType::Int8, DataValue::Int8(-123)).unwrap(),
                (false, pgx::Datum::from(-123i16))
            );
        }
    }

    #[pg_test]
    fn test_into_datum_u8() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::INT2OID, DataType::UInt8, DataValue::UInt8(255)).unwrap(),
                (false, pgx::Datum::from(255i16))
            );
        }
    }

    #[pg_test]
    fn test_into_datum_i16() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::INT2OID, DataType::Int16, DataValue::Int16(123)).unwrap(),
                (false, pgx::Datum::from(123i16))
            );
        }
    }

    #[pg_test]
    fn test_into_datum_u16() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::INT4OID, DataType::UInt16, DataValue::UInt16(1234))
                    .unwrap(),
                (false, pgx::Datum::from(1234i32))
            );
        }
    }

    #[pg_test]
    fn test_into_datum_i32() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::INT4OID, DataType::Int32, DataValue::Int32(123)).unwrap(),
                (false, pgx::Datum::from(123i32))
            );
        }
    }

    #[pg_test]
    fn test_into_datum_u32() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::INT8OID, DataType::UInt32, DataValue::UInt32(1234))
                    .unwrap(),
                (false, pgx::Datum::from(1234i32))
            );
        }
    }

    #[pg_test]
    fn test_into_datum_i64() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::INT8OID, DataType::Int64, DataValue::Int64(-123456))
                    .unwrap(),
                (false, pgx::Datum::from(-123456i64))
            );
        }
    }

    #[pg_test]
    fn test_into_datum_u64() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::NUMERICOID,
                DataType::UInt64,
                DataValue::UInt64(12345678987654321),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                Numeric::from_datum(datum, false).unwrap().0,
                "12345678987654321".to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_f32() {
        unsafe {
            assert_eq!(
                into_datum_owned(
                    pg_sys::FLOAT4OID,
                    DataType::Float32,
                    DataValue::Float32(123.456)
                )
                .unwrap(),
                (false, 123.456f32.into_datum().unwrap())
            );
        }
    }

    #[pg_test]
    fn test_into_datum_f64() {
        unsafe {
            assert_eq!(
                into_datum_owned(
                    pg_sys::FLOAT8OID,
                    DataType::Float64,
                    DataValue::Float64(123.456)
                )
                .unwrap(),
                (false, 123.456f64.into_datum().unwrap())
            );
        }
    }

    #[pg_test]
    fn test_into_datum_numeric() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::NUMERICOID,
                DataType::Decimal(DecimalOptions::default()),
                DataValue::Decimal(Decimal::new(987654321, 3)),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                Numeric::from_datum(datum, false).unwrap().0,
                "987654.321".to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_varchar_utf8() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::VARCHAROID,
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
                DataValue::Varchar("Hello world".as_bytes().to_vec()),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                String::from_datum(datum, false).unwrap(),
                "Hello world".to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_varchar_utf16() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::VARCHAROID,
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf16)),
                DataValue::Varchar(
                    "Hello world"
                        .encode_utf16()
                        .flat_map(|i| i.to_ne_bytes())
                        .collect::<Vec<u8>>(),
                ),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                String::from_datum(datum, false).unwrap(),
                "Hello world".to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_varchar_ascii() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::VARCHAROID,
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Ascii)),
                DataValue::Varchar(b"Hello world".to_vec()),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                String::from_datum(datum, false).unwrap(),
                "Hello world".to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_text() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::TEXTOID,
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
                DataValue::Varchar("Hello world".as_bytes().to_vec()),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                String::from_datum(datum, false).unwrap(),
                "Hello world".to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_bytea() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::BYTEAOID,
                DataType::Binary,
                DataValue::Binary("Hello world".as_bytes().to_vec()),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                String::from_utf8(Vec::<u8>::from_datum(datum, false).unwrap()).unwrap(),
                "Hello world".to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_bool() {
        unsafe {
            assert_eq!(
                into_datum_owned(pg_sys::BOOLOID, DataType::Boolean, DataValue::Boolean(true))
                    .unwrap(),
                (false, true.into_datum().unwrap())
            );
        }
    }

    #[pg_test]
    fn test_into_datum_json() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::JSONOID,
                DataType::JSON,
                DataValue::JSON(r#"{"hello":"json"}"#.to_string()),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                JsonString::from_datum(datum, false).unwrap().0,
                r#"{"hello":"json"}"#.to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_jsonb() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::JSONBOID,
                DataType::JSON,
                DataValue::JSON(r#"{"hello":"jsonb"}"#.to_string()),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                JsonB::from_datum(datum, false).unwrap().0.to_string(),
                r#"{"hello":"jsonb"}"#.to_string()
            );
        }
    }

    #[pg_test]
    fn test_into_datum_date() {
        unsafe {
            assert_eq!(
                into_datum_owned(
                    pg_sys::DATEOID,
                    DataType::Date,
                    DataValue::Date(NaiveDate::from_ymd(2020, 1, 5))
                )
                .unwrap(),
                (
                    false,
                    pgx::Date::new(
                        time::Date::from_calendar_date(2020, time::Month::January, 5).unwrap()
                    )
                    .into_datum()
                    .unwrap()
                )
            );
        }
    }

    #[pg_test]
    fn test_into_datum_time() {
        unsafe {
            assert_eq!(
                into_datum_owned(
                    pg_sys::TIMEOID,
                    DataType::Time,
                    DataValue::Time(NaiveTime::from_hms_milli(7, 43, 11, 123))
                )
                .unwrap(),
                (
                    false,
                    pgx::Time::new(time::Time::from_hms_milli(7, 43, 11, 123).unwrap())
                        .into_datum()
                        .unwrap()
                )
            );
        }
    }

    #[pg_test]
    fn test_into_datum_timestamp() {
        unsafe {
            assert_eq!(
                into_datum_owned(
                    pg_sys::TIMESTAMPOID,
                    DataType::DateTime,
                    DataValue::DateTime(NaiveDateTime::new(
                        NaiveDate::from_ymd(2020, 1, 5),
                        NaiveTime::from_hms_milli(7, 43, 11, 123)
                    ))
                )
                .unwrap(),
                (
                    false,
                    pgx::Timestamp::new(time::PrimitiveDateTime::new(
                        time::Date::from_calendar_date(2020, time::Month::January, 5).unwrap(),
                        time::Time::from_hms_milli(7, 43, 11, 123).unwrap()
                    ))
                    .into_datum()
                    .unwrap()
                )
            );
        }
    }

    #[pg_test]
    fn test_into_datum_timestamp_tz() {
        unsafe {
            assert_eq!(
                into_datum_owned(
                    pg_sys::TIMESTAMPTZOID,
                    DataType::DateTimeWithTZ,
                    DataValue::DateTimeWithTZ((
                        NaiveDateTime::new(
                            NaiveDate::from_ymd(2020, 1, 5),
                            NaiveTime::from_hms_milli(7, 43, 11, 123)
                        ),
                        Tz::UTC
                    ))
                )
                .unwrap(),
                (
                    false,
                    pgx::TimestampWithTimeZone::new(
                        time::PrimitiveDateTime::new(
                            time::Date::from_calendar_date(2020, time::Month::January, 5).unwrap(),
                            time::Time::from_hms_milli(7, 43, 11, 123).unwrap()
                        ),
                        time::UtcOffset::UTC
                    )
                    .into_datum()
                    .unwrap()
                )
            );
        }
    }

    #[pg_test]
    fn test_into_datum_varchar_uuid() {
        unsafe {
            let uuid = uuid::Uuid::new_v4();
            let (is_null, datum) = into_datum_owned(
                pg_sys::UUIDOID,
                DataType::Uuid,
                DataValue::Uuid(uuid.clone()),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                pgx::Uuid::from_datum(datum, false).unwrap().as_bytes(),
                uuid.as_bytes()
            );
        }
    }
}
