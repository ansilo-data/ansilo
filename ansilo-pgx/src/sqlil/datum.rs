use std::{any::TypeId, str::FromStr};

use ansilo_core::{
    common::data::{
        chrono::{NaiveDate, NaiveDateTime, NaiveTime, Weekday},
        chrono_tz::Tz,
        rust_decimal::{prelude::FromPrimitive, Decimal},
        uuid::Uuid,
        DataValue,
    },
    err::{bail, Context, Error, Result},
};
use pgx::{
    pg_schema,
    pg_sys::{self, Oid},
    FromDatum,
};

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
        pg_sys::FLOAT4OID => Ok(DataValue::FloatSingle(f32::parse(datum)?)),
        pg_sys::FLOAT8OID => Ok(DataValue::FloatDouble(f64::parse(datum)?)),
        pg_sys::NUMERICOID => Ok(from_numeric(datum)),
        // TODO: verify this is encoding safe? (should be as we only support a UTF8 postgres)
        pg_sys::VARCHAROID => Ok(DataValue::Varchar(
            String::parse(datum)?.as_bytes().to_vec(),
        )),
        pg_sys::TEXTOID => Ok(DataValue::Varchar(
            String::parse(datum)?.as_bytes().to_vec(),
        )),
        //
        pg_sys::BYTEAOID => Ok(DataValue::Binary(Vec::<u8>::parse(datum)?)),
        //
        pg_sys::BOOLOID => Ok(DataValue::Boolean(bool::parse(datum)?)),
        pg_sys::BITOID => Ok(DataValue::Boolean(bool::parse(datum)?)),
        pg_sys::VARBITOID => Ok(DataValue::Boolean(bool::parse(datum)?)),
        //
        pg_sys::JSONOID => Ok(DataValue::JSON(pgx::Json::parse(datum)?.0.to_string())),
        pg_sys::JSONBOID => Ok(DataValue::JSON(pgx::JsonB::parse(datum)?.0.to_string())),
        //
        pg_sys::DATEOID => Ok(DataValue::Date(to_date(pgx::Date::parse(datum)?))),
        pg_sys::TIMEOID => Ok(DataValue::Time(to_time(pgx::Time::parse(datum)?))),
        pg_sys::TIMESTAMPOID => Ok(DataValue::DateTime(to_date_time(pgx::Timestamp::parse(
            datum,
        )?))),
        pg_sys::TIMESTAMPTZOID => Ok(DataValue::DateTimeWithTZ(to_date_time_tz(
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
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct NumericVar {
    ndigits: isize,
    weight: isize,   /* weight of first digit */
    sign: isize,     /* weight of first digit */
    dscale: isize,   /* weight of first digit */
    buf: *mut i8,    /* start of palloc'd space for digits[] */
    digits: *mut i8, /* base-NBASE digits */
}
/// TODO: implement faster conversion using bit manipulation to translate across formats
unsafe fn from_numeric(datum: pg_sys::Datum) -> DataValue {
    // @see https://doxygen.postgresql.org/numeric_8h_source.html#l00059
    {
        let datum = datum.ptr_cast::<NumericVar>();
        pgx::log!("from_numeric ptr: {:?}", datum);
        pgx::log!("from_numeric data: {:?}", *datum);
    }
    //
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

fn to_date(datum: pgx::Date) -> NaiveDate {
    let (y, m, d) = datum.to_iso_week_date();
    NaiveDate::from_isoywd(
        y as _,
        m as _,
        Weekday::from_u8(d.number_days_from_monday()).unwrap(),
    )
}

fn to_time(datum: pgx::Time) -> NaiveTime {
    let (h, m, s, p) = datum.as_hms_micro();
    NaiveTime::from_hms_micro(h as _, m as _, s as _, p as _)
}

fn to_date_time(datum: pgx::Timestamp) -> NaiveDateTime {
    NaiveDateTime::new(
        to_date(pgx::Date::new(datum.date())),
        to_time(pgx::Time::new(datum.time())),
    )
}

fn to_date_time_tz(datum: pgx::TimestampWithTimeZone) -> (NaiveDateTime, Tz) {
    let ts = datum.unix_timestamp();
    let ns = datum.nanosecond();
    // TODO: do we need timezones here?, dont think so. maybe just have UtcTimestamp type
    (NaiveDateTime::from_timestamp(ts, ns), Tz::UTC)
}

fn to_uuid(datum: pgx::Uuid) -> Uuid {
    Uuid::from_slice(datum.as_bytes()).unwrap()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;

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
                DataValue::FloatSingle(123.456)
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
                DataValue::FloatSingle(987.654)
            );
        }
    }

    #[pg_test]
    fn test_from_datum_f64() {
        unsafe {
            assert_eq!(
                from_datum(pg_sys::FLOAT8OID, 123.456f64.into_datum().unwrap()).unwrap(),
                DataValue::FloatDouble(123.456)
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
                DataValue::FloatDouble(987.654)
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
}
