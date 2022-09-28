use ansilo_core::{
    data::{
        chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike},
        chrono_tz::Tz,
        DataType, DataValue, DateTimeWithTZ, StringOptions,
    },
    err::{bail, Context, Result},
};
use apache_avro::{schema::UnionSchema, types::Value as ArvoValue, Schema};

pub fn from_arvo_type(schema: &Schema) -> Result<(DataType, bool)> {
    let (schema, nullable) = if let Schema::Union(union) = schema {
        if union.variants().len() == 2 && union.variants().contains(&Schema::Null) {
            (
                union
                    .variants()
                    .iter()
                    .find(|d| *d != &Schema::Null)
                    .context("Invalid union")?,
                true,
            )
        } else {
            (schema, false)
        }
    } else {
        (schema, false)
    };

    let r#type = match schema {
        Schema::Null => DataType::Null,
        Schema::Boolean => DataType::Boolean,
        Schema::Int => DataType::Int32,
        Schema::Long => DataType::Int64,
        Schema::Float => DataType::Float32,
        Schema::Double => DataType::Float64,
        Schema::Bytes => DataType::Binary,
        Schema::String => DataType::Utf8String(StringOptions::default()),
        Schema::Fixed { .. } => DataType::Binary,
        Schema::Uuid => DataType::Uuid,
        Schema::Date => DataType::Date,
        Schema::TimeMillis => DataType::Time,
        Schema::TimeMicros => DataType::Time,
        Schema::TimestampMillis => DataType::DateTime,
        Schema::TimestampMicros => DataType::DateTime,
        Schema::Enum { .. } => DataType::Utf8String(StringOptions::default()),
        _ => bail!("Unsupported arvo type: {:?}", schema),
    };

    Ok((r#type, nullable))
}

pub fn into_arvo_type(r#type: &DataType, nullable: bool) -> Result<Schema> {
    let mut schema = match r#type {
        DataType::Utf8String(_) => Schema::String,
        DataType::Binary => Schema::Bytes,
        DataType::Boolean => Schema::Boolean,
        DataType::Int8 => Schema::Int,
        DataType::UInt8 => Schema::Int,
        DataType::Int16 => Schema::Int,
        DataType::UInt16 => Schema::Int,
        DataType::Int32 => Schema::Long,
        DataType::UInt32 => Schema::Long,
        DataType::Int64 => Schema::Long,
        DataType::UInt64 => Schema::String,
        DataType::Float32 => Schema::Float,
        DataType::Float64 => Schema::Double,
        DataType::Decimal(_) => Schema::String,
        DataType::JSON => Schema::String,
        DataType::Date => Schema::Date,
        DataType::Time => Schema::TimeMicros,
        DataType::DateTime => Schema::TimestampMicros,
        DataType::DateTimeWithTZ => Schema::TimestampMicros,
        DataType::Uuid => Schema::Uuid,
        DataType::Null => return Ok(Schema::Null),
    };

    if nullable {
        schema = Schema::Union(UnionSchema::new(vec![schema, Schema::Null])?);
    }

    Ok(schema)
}

pub fn from_arvo_value(val: ArvoValue) -> Result<DataValue> {
    let res = match val {
        ArvoValue::Null => DataValue::Null,
        ArvoValue::Boolean(b) => DataValue::Boolean(b),
        ArvoValue::Int(i) => DataValue::Int32(i),
        ArvoValue::Long(l) => DataValue::Int64(l),
        ArvoValue::Float(f) => DataValue::Float32(f),
        ArvoValue::Double(d) => DataValue::Float64(d),
        ArvoValue::Bytes(b) => DataValue::Binary(b),
        ArvoValue::String(s) => DataValue::Utf8String(s),
        ArvoValue::Fixed(_, b) => DataValue::Binary(b),
        ArvoValue::Enum(_, s) => DataValue::Utf8String(s),
        ArvoValue::Union(_, b) => from_arvo_value(*b)?,
        ArvoValue::Date(d) => {
            DataValue::Date(NaiveDate::from_ymd(1970, 1, 1) + Duration::days(d as _))
        }
        ArvoValue::TimeMillis(t) => DataValue::Time(NaiveTime::from_num_seconds_from_midnight(
            (t / 1000) as _,
            ((t % 1000) * 1000_000) as _,
        )),
        ArvoValue::TimeMicros(t) => DataValue::Time(NaiveTime::from_num_seconds_from_midnight(
            (t / 1000_000) as _,
            ((t % 1000_000) * 1000) as _,
        )),
        ArvoValue::TimestampMillis(t) => DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
            NaiveDateTime::from_timestamp((t / 1000) as _, ((t % 1000) * 1000_000) as _),
            Tz::UTC,
        )),
        ArvoValue::TimestampMicros(t) => DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
            NaiveDateTime::from_timestamp((t / 1000_000) as _, ((t % 1000_000) * 1000) as _),
            Tz::UTC,
        )),
        ArvoValue::Uuid(u) => DataValue::Uuid(u),
        _ => bail!("Unsupported arvo type: {:?}", val),
    };

    Ok(res)
}

pub fn into_arvo_value(val: DataValue) -> ArvoValue {
    match val {
        DataValue::Null => ArvoValue::Null,
        DataValue::Utf8String(s) => ArvoValue::String(s),
        DataValue::Binary(b) => ArvoValue::Bytes(b),
        DataValue::Boolean(b) => ArvoValue::Boolean(b),
        DataValue::Int8(i) => ArvoValue::Int(i as _),
        DataValue::UInt8(i) => ArvoValue::Int(i as _),
        DataValue::Int16(i) => ArvoValue::Int(i as _),
        DataValue::UInt16(i) => ArvoValue::Int(i as _),
        DataValue::Int32(i) => ArvoValue::Int(i as _),
        DataValue::UInt32(i) => ArvoValue::Long(i as _),
        DataValue::Int64(i) => ArvoValue::Long(i as _),
        DataValue::UInt64(i) => ArvoValue::String(i.to_string()),
        DataValue::Float32(f) => ArvoValue::Float(f),
        DataValue::Float64(f) => ArvoValue::Double(f),
        DataValue::Decimal(d) => ArvoValue::String(d.to_string()),
        DataValue::JSON(j) => ArvoValue::String(j),
        DataValue::Date(d) => ArvoValue::String(format!("{}", d.format("%Y-%m-%d"))),
        DataValue::Time(t) => ArvoValue::TimeMicros(
            t.num_seconds_from_midnight() as i64 * 1000_000 + t.nanosecond() as i64 / 1000,
        ),
        DataValue::DateTime(d) => ArvoValue::TimestampMicros(d.timestamp_micros()),
        DataValue::DateTimeWithTZ(d) => {
            ArvoValue::TimestampMicros(d.zoned().unwrap().timestamp_micros())
        }
        DataValue::Uuid(u) => ArvoValue::Uuid(u),
    }
}
