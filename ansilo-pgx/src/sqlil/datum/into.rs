use ansilo_core::{
    data::{
        chrono::{
            Datelike, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Timelike, Weekday,
        },
        uuid, DataType, DataValue, DateTimeWithTZ,
    },
    err::{bail, Result},
};
use pgx::{
    pg_schema,
    pg_sys::{self, Oid},
    IntoDatum, PgBox,
};

use super::from_pg_type;

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
            DataType::Utf8String(_),
            DataValue::Utf8String(data),
        ) => data.into_datum().unwrap(),
        //
        (pg_sys::BYTEAOID, DataType::Utf8String(_), DataValue::Utf8String(data)) => {
            data.as_bytes().to_vec().into_datum().unwrap()
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
        (type_oid, r#type, data) => {
            // If we fail on the strict conversion path we try to coerce the type before giving up
            if let Ok(_) = from_pg_type(type_oid)
                .and_then(|r#type| Ok((data.try_coerce_into(&r#type)?, r#type)))
                .and_then(|(coerced, r#type)| {
                    into_datum(type_oid, &r#type, coerced, is_null, datum)
                })
            {
                return Ok(());
            }

            bail!(
                "Type mismatch between underlying {:?} type and postgres type {:?}",
                r#type,
                type_oid
            )
        }
    };

    Ok(())
}

/// Converts the supplied DataValue into a pgalloc'd Datum
pub(crate) unsafe fn into_datum_pg_alloc(
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

fn into_date_time_tz(data: DateTimeWithTZ) -> pgx::TimestampWithTimeZone {
    pgx::TimestampWithTimeZone::new(
        *into_date_time(data.dt),
        time::UtcOffset::from_whole_seconds(
            data.tz
                .offset_from_local_datetime(&data.dt)
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
    use ansilo_core::data::{chrono_tz::Tz, rust_decimal::Decimal, DecimalOptions, StringOptions};
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
                DataType::Utf8String(StringOptions::default()),
                DataValue::Utf8String("Hello world".into()),
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
                DataType::Utf8String(StringOptions::default()),
                DataValue::Utf8String("Hello world".into()),
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
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
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

    #[pg_test]
    fn test_into_datum_text_from_binary_coerces_type() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::TEXTOID,
                DataType::Binary,
                DataValue::Binary("Hello world".as_bytes().to_vec()),
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
    fn test_into_datum_bytea_from_utf8_string_coerces_type() {
        unsafe {
            let (is_null, datum) = into_datum_owned(
                pg_sys::BYTEAOID,
                DataType::Utf8String(StringOptions::default()),
                DataValue::Utf8String("Hello world".into()),
            )
            .unwrap();
            assert_eq!(is_null, false);
            assert_eq!(
                Vec::<u8>::from_datum(datum, false).unwrap(),
                "Hello world".as_bytes().to_vec()
            );
        }
    }
}
