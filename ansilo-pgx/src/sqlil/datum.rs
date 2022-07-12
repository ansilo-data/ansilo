use std::{any::TypeId, mem, str::FromStr};

use ansilo_core::{
    common::data::{
        chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Weekday},
        chrono_tz::Tz,
        rust_decimal::{Decimal, prelude::FromPrimitive},
        uuid::Uuid,
        DataValue,
    },
    err::{bail, Error, Result},
};
use pgx::{
    pg_sys::{self, Node, Oid},
    FromDatum,
};

use crate::util::string::parse_to_owned_utf8_string;

/// Attempt to convert a postgres datum union type to ansilo's DataValue
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

/// TODO: implement faster conversion using bit manipulation to translate across formats
unsafe fn from_numeric(datum: pg_sys::Datum) -> DataValue {
    // @see https://doxygen.postgresql.org/numeric_8h_source.html#l00059
    let datum = pg_sys::pg_detoast_datum(datum.to_void() as *mut _);
    let datum = datum as pg_sys::Numeric;
    let num_str = parse_to_owned_utf8_string(pg_sys::numeric_normalize(datum)).unwrap();

    // Convert +/-Infinity and NaN's to null
    if num_str.starts_with("I") || num_str.starts_with("-I") || num_str.starts_with("N") {
        return DataValue::Null;
    }

    let dec = Decimal::from_str(&num_str).unwrap();
    DataValue::Decimal(dec)
}

fn to_date(datum: pgx::Date) -> NaiveDate {
    let (y, m, d) = datum.to_iso_week_date();
    NaiveDate::from_isoywd(y as _, m as _, Weekday::from_u8(d.number_days_from_monday()).unwrap())
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
