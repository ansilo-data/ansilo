use std::{any::TypeId, str::FromStr};

use ansilo_core::{
    data::{
        chrono::{NaiveDate, NaiveDateTime, NaiveTime, Weekday},
        chrono_tz::Tz,
        rust_decimal::{prelude::FromPrimitive, Decimal},
        uuid::Uuid,
        DataValue, DateTimeWithTZ,
    },
    err::{bail, Context, Error, Result},
};
use pgx::{pg_sys::Oid, *};

/// Attempt to convert a postgres datum union type to ansilo's DataValue
///
/// NOTE: This cannot be called with a NULL value, doing so will result in Bad Things (tm)
///
/// This is a hot code path. We need to take good care in optimising this.
pub unsafe fn from_datum(type_oid: Oid, datum: pg_sys::Datum) -> Result<DataValue> {
    match type_oid {
        // @see https://github.com/postgres/postgres/blob/REL_14_4/src/include/postgres.h
        pg_sys::INT2OID => Ok(DataValue::Int16(i16::parse(datum)?)),
        pg_sys::INT4OID => Ok(DataValue::Int32(i32::parse(datum)?)),
        pg_sys::INT8OID => Ok(DataValue::Int64(i64::parse(datum)?)),
        // @see https://github.com/postgres/postgres/blob/REL_14_4/src/include/postgres.h#L707
        pg_sys::FLOAT4OID => Ok(DataValue::Float32(f32::parse(datum)?)),
        pg_sys::FLOAT8OID => Ok(DataValue::Float64(f64::parse(datum)?)),
        pg_sys::NUMERICOID => Ok(from_numeric(datum)),
        // We assume UTF8 as we hard code this configuration during initdb
        pg_sys::TEXTOID | pg_sys::VARCHAROID => Ok(DataValue::Utf8String(String::parse(datum)?)),
        // char is an internal type (i8) used by postgres, likely not portable
        // and should not be used across db's
        pg_sys::CHAROID => {
            bail!("Postgres CHAR types are not supported, use another integer or character type")
        }
        //
        pg_sys::BYTEAOID => Ok(DataValue::Binary(Vec::<u8>::parse(datum)?)),
        //
        pg_sys::BOOLOID => Ok(DataValue::Boolean(bool::parse(datum)?)),
        //
        pg_sys::JSONOID => Ok(DataValue::JSON(pgx::JsonString::parse(datum)?.0)),
        pg_sys::JSONBOID => Ok(DataValue::JSON(pgx::JsonB::parse(datum)?.0.to_string())),
        //
        pg_sys::DATEOID => Ok(DataValue::Date(from_date(pgx::Date::parse(datum)?))),
        pg_sys::TIMEOID => Ok(DataValue::Time(from_time(pgx::Time::parse(datum)?))),
        pg_sys::TIMESTAMPOID => Ok(DataValue::DateTime(from_date_time(pgx::Timestamp::parse(
            datum,
        )?))),
        pg_sys::TIMESTAMPTZOID => Ok(DataValue::DateTimeWithTZ(from_date_time_tz(
            pgx::TimestampWithTimeZone::parse(datum)?,
        ))),
        //
        pg_sys::UUIDOID => Ok(DataValue::Uuid(to_uuid(pgx::Uuid::parse(datum)?))),
        _ => bail!("Unknown type oid: {type_oid}"),
    }
}

trait ParseDatum<T>: FromDatum {
    unsafe fn parse(datum: pg_sys::Datum) -> Result<T>;
}

impl<T: FromDatum + 'static> ParseDatum<T> for T {
    unsafe fn parse(datum: pg_sys::Datum) -> Result<T> {
        T::from_datum(datum, false).ok_or_else(|| {
            Error::msg(format!(
                "Failed to parse datum as type {:?}",
                TypeId::of::<T>()
            ))
        })
    }
}

/// TODO[low]: implement faster conversion using bit manipulation to translate across formats
unsafe fn from_numeric(datum: pg_sys::Datum) -> DataValue {
    // @see https://doxygen.postgresql.org/numeric_8h_source.html#l00059
    let numeric = pgx::Numeric::from_datum(datum, false).unwrap();
    let num_str = numeric.0;

    // Convert +/-Infinity and NaN's to null
    if num_str.starts_with("I") || num_str.starts_with("-I") || num_str.starts_with("N") {
        return DataValue::Null;
    }

    let dec = Decimal::from_str(&num_str)
        .with_context(|| format!("Failed to parse '{}' as decimal", num_str))
        .unwrap();
    DataValue::Decimal(dec)
}

fn from_date(datum: pgx::Date) -> NaiveDate {
    let (y, w, d) = datum.to_iso_week_date();
    NaiveDate::from_isoywd(
        y as _,
        w as _,
        Weekday::from_u8(d.number_days_from_monday()).unwrap(),
    )
}

fn from_time(datum: pgx::Time) -> NaiveTime {
    let (h, m, s, p) = datum.as_hms_micro();
    NaiveTime::from_hms_micro(h as _, m as _, s as _, p as _)
}

fn from_date_time(datum: pgx::Timestamp) -> NaiveDateTime {
    NaiveDateTime::new(
        from_date(pgx::Date::new(datum.date())),
        from_time(pgx::Time::new(datum.time())),
    )
}

fn from_date_time_tz(datum: pgx::TimestampWithTimeZone) -> DateTimeWithTZ {
    let ts = datum.unix_timestamp();
    let ns = datum.nanosecond();
    // TODO: do we need timezones here?, dont think so. maybe just have UtcTimestamp type
    DateTimeWithTZ::new(NaiveDateTime::from_timestamp(ts, ns), Tz::UTC)
}

fn to_uuid(datum: pgx::Uuid) -> Uuid {
    Uuid::from_slice(datum.as_bytes()).unwrap()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use ansilo_core::data::uuid;

    use super::*;

    fn datum_from_query<T: FromDatum + IntoDatum>(query: &'static str) -> Datum {
        let res = Spi::connect(|client| {
            let data = Box::new(client.select(query, None, None).next().unwrap());
            let datum = data.by_ordinal(1).unwrap().value::<T>().unwrap();

            Ok(Some(datum))
        })
        .unwrap();

        res.into_datum().unwrap()
    }

    #[pg_test]
    fn test_from_datum_i16() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::INT2OID, pgx::Datum::from(123i16)).unwrap(),
                DataValue::Int16(123)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_i16_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::INT2OID,
                    datum_from_query::<i16>("SELECT 234::smallint")
                )
                .unwrap(),
                DataValue::Int16(234)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_i32() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::INT4OID, pgx::Datum::from(123i32)).unwrap(),
                DataValue::Int32(123)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_i32_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::INT4OID,
                    datum_from_query::<i32>("SELECT 2147483647::integer")
                )
                .unwrap(),
                DataValue::Int32(2147483647)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_i64() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::INT8OID, pgx::Datum::from(123i64)).unwrap(),
                DataValue::Int64(123)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_i64_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::INT8OID,
                    datum_from_query::<i64>("SELECT 9223372036854775807::bigint")
                )
                .unwrap(),
                DataValue::Int64(9223372036854775807)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_f32() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::FLOAT4OID, 123.456f32.into_datum().unwrap()).unwrap(),
                DataValue::Float32(123.456)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_f32_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::FLOAT4OID,
                    datum_from_query::<f32>("SELECT 987.654::real")
                )
                .unwrap(),
                DataValue::Float32(987.654)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_f64() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::FLOAT8OID, 123.456f64.into_datum().unwrap()).unwrap(),
                DataValue::Float64(123.456)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_f64_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::FLOAT8OID,
                    datum_from_query::<f64>("SELECT 987.654::double precision")
                )
                .unwrap(),
                DataValue::Float64(987.654)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_numeric() {
        fn make_numeric(num: impl Into<String>) -> Datum {
            let num = pgx::Numeric(num.into());
            let res = num.into_datum().unwrap();
            res
        }

        unsafe {
            assert_eq!(
                from_datum(pg_sys::NUMERICOID, make_numeric("123.456")).unwrap(),
                DataValue::Decimal(Decimal::from_f64(123.456).unwrap())
            );
            assert_eq!(
                from_datum(pg_sys::NUMERICOID, make_numeric("Infinity")).unwrap(),
                DataValue::Null
            );
            assert_eq!(
                from_datum(pg_sys::NUMERICOID, make_numeric("-Infinity")).unwrap(),
                DataValue::Null
            );
            assert_eq!(
                from_datum(pg_sys::NUMERICOID, make_numeric("NaN")).unwrap(),
                DataValue::Null
            );
        }
    }

    #[pg_test]
    fn test_from_datum_numeric_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::NUMERICOID,
                    datum_from_query::<Numeric>("SELECT 987.654::numeric")
                )
                .unwrap(),
                DataValue::Decimal(Decimal::from_f64(987.654).unwrap())
            );
            assert_eq!(
                from_datum(
                    pg_sys::NUMERICOID,
                    datum_from_query::<Numeric>("SELECT 'Infinity'::numeric")
                )
                .unwrap(),
                DataValue::Null
            );
            assert_eq!(
                from_datum(
                    pg_sys::NUMERICOID,
                    datum_from_query::<Numeric>("SELECT '-Infinity'::numeric")
                )
                .unwrap(),
                DataValue::Null
            );
            assert_eq!(
                from_datum(
                    pg_sys::NUMERICOID,
                    datum_from_query::<Numeric>("SELECT 'NaN'::numeric")
                )
                .unwrap(),
                DataValue::Null
            );
        }
    }

    #[pg_test]
    fn test_from_datum_varchar() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::VARCHAROID, "Example String".into_datum().unwrap()).unwrap(),
                DataValue::Utf8String("Example String".into())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_varchar_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::VARCHAROID,
                    datum_from_query::<String>("SELECT 'Example String'")
                )
                .unwrap(),
                DataValue::Utf8String("Example String".into())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_text() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::TEXTOID, "Example Text".into_datum().unwrap()).unwrap(),
                DataValue::Utf8String("Example Text".into())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_text_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::TEXTOID,
                    datum_from_query::<String>("SELECT 'Example Text'::text")
                )
                .unwrap(),
                DataValue::Utf8String("Example Text".into())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_bytea() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::BYTEAOID, [1u8, 123, 0, 5].into_datum().unwrap()).unwrap(),
                DataValue::Binary(vec![1u8, 123, 0, 5])
            );
        }
    }

    #[pg_test]
    fn test_from_datum_bytea_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::BYTEAOID,
                    datum_from_query::<Vec<u8>>("SELECT 'hello binary'::bytea")
                )
                .unwrap(),
                DataValue::Binary("hello binary".as_bytes().to_vec())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_bool() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::BOOLOID, true.into_datum().unwrap()).unwrap(),
                DataValue::Boolean(true)
            );
            assert_eq!(
                from_datum(pg_sys::BOOLOID, false.into_datum().unwrap()).unwrap(),
                DataValue::Boolean(false)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_bool_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::BOOLOID,
                    datum_from_query::<bool>("SELECT TRUE::bool")
                )
                .unwrap(),
                DataValue::Boolean(true)
            );
            assert_eq!(
                from_datum(
                    pg_sys::BOOLOID,
                    datum_from_query::<bool>("SELECT FALSE::bool")
                )
                .unwrap(),
                DataValue::Boolean(false)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_json() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::JSONOID,
                    pgx::JsonString(r#"{"hello":"json"}"#.to_string())
                        .into_datum()
                        .unwrap()
                )
                .unwrap(),
                DataValue::JSON(r#"{"hello":"json"}"#.to_string())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_json_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::JSONOID,
                    datum_from_query::<pgx::JsonString>(
                        r#"SELECT '{"hello":"postgres json"}'::json"#
                    ),
                )
                .unwrap(),
                DataValue::JSON(r#"{"hello":"postgres json"}"#.to_string())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_jsonb() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::JSONBOID,
                    pgx::JsonB(serde_json::from_str(r#"{"hello":"jsonb"}"#).unwrap())
                        .into_datum()
                        .unwrap()
                )
                .unwrap(),
                DataValue::JSON(r#"{"hello":"jsonb"}"#.to_string())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_jsonb_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::JSONBOID,
                    datum_from_query::<pgx::JsonB>(r#"SELECT '{"hello":"postgres jsonb"}'::jsonb"#),
                )
                .unwrap(),
                DataValue::JSON(r#"{"hello":"postgres jsonb"}"#.to_string())
            );
        }
    }

    #[pg_test]
    fn test_from_datum_date() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::DATEOID,
                    pgx::Date::new(
                        time::Date::from_calendar_date(2020, time::Month::January, 5).unwrap()
                    )
                    .into_datum()
                    .unwrap()
                )
                .unwrap(),
                DataValue::Date(NaiveDate::from_ymd(2020, 1, 5))
            );
        }
    }

    #[pg_test]
    fn test_from_datum_date_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::DATEOID,
                    datum_from_query::<pgx::Date>("SELECT '2020-03-18'::date")
                )
                .unwrap(),
                DataValue::Date(NaiveDate::from_ymd(2020, 3, 18))
            );
        }
    }

    #[pg_test]
    fn test_from_datum_time() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::TIMEOID,
                    pgx::Time::new(time::Time::from_hms_milli(7, 43, 11, 123).unwrap())
                        .into_datum()
                        .unwrap()
                )
                .unwrap(),
                DataValue::Time(NaiveTime::from_hms_milli(7, 43, 11, 123))
            );
        }
    }

    #[pg_test]
    fn test_from_datum_time_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::TIMEOID,
                    datum_from_query::<pgx::Time>("SELECT '23:50:42.123456'::time")
                )
                .unwrap(),
                DataValue::Time(NaiveTime::from_hms_micro(23, 50, 42, 123456))
            );
        }
    }

    #[pg_test]
    fn test_from_datum_timestamp() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::TIMESTAMPOID,
                    pgx::Timestamp::new(time::PrimitiveDateTime::new(
                        time::Date::from_calendar_date(2020, time::Month::January, 5).unwrap(),
                        time::Time::from_hms_milli(7, 43, 11, 123).unwrap()
                    ))
                    .into_datum()
                    .unwrap()
                )
                .unwrap(),
                DataValue::DateTime(NaiveDateTime::new(
                    NaiveDate::from_ymd(2020, 1, 5),
                    NaiveTime::from_hms_milli(7, 43, 11, 123)
                ))
            );
        }
    }

    #[pg_test]
    fn test_from_datum_timestamp_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::TIMESTAMPOID,
                    datum_from_query::<pgx::Timestamp>(
                        "SELECT TIMESTAMP '2020-01-22 23:50:42.123456'"
                    )
                )
                .unwrap(),
                DataValue::DateTime(NaiveDateTime::new(
                    NaiveDate::from_ymd(2020, 1, 22),
                    NaiveTime::from_hms_micro(23, 50, 42, 123456)
                ))
            );
        }
    }

    #[pg_test]
    fn test_from_datum_timestamp_tz() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::TIMESTAMPTZOID,
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
                .unwrap(),
                DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                    NaiveDateTime::new(
                        NaiveDate::from_ymd(2020, 1, 5),
                        NaiveTime::from_hms_milli(7, 43, 11, 123)
                    ),
                    Tz::UTC
                ))
            );
        }
    }

    #[pg_test]
    fn test_from_datum_timestamp_tz_query() {
        unsafe {
            assert_eq!(
                from_datum(
                    pg_sys::TIMESTAMPTZOID,
                    datum_from_query::<pgx::TimestampWithTimeZone>(
                        "SELECT TIMESTAMP WITH TIME ZONE '2020-01-22 23:50:42.123456+05'"
                    )
                )
                .unwrap(),
                DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                    NaiveDateTime::new(
                        NaiveDate::from_ymd(2020, 1, 22),
                        NaiveTime::from_hms_micro(18, 50, 42, 123456)
                    ),
                    Tz::UTC
                ))
            );
        }
    }

    #[pg_test]
    fn test_from_datum_uuid() {
        unsafe {
            let uuid = uuid::Uuid::new_v4();
            assert_eq!(
                from_datum(
                    pg_sys::UUIDOID,
                    pgx::Uuid::from_bytes(*uuid.clone().as_bytes())
                        .into_datum()
                        .unwrap()
                )
                .unwrap(),
                DataValue::Uuid(uuid)
            );
        }
    }
}
