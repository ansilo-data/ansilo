use ansilo_core::{
    data::{
        chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike},
        DataType, DataValue, StringOptions,
    },
    err::{bail, Context, Result},
};
use apache_avro::{schema::UnionSchema, types::Value as AvroValue, Schema};

pub fn from_avro_type(schema: &Schema) -> Result<(DataType, bool)> {
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
        Schema::Uuid => DataType::Uuid,
        Schema::Date => DataType::Date,
        Schema::TimeMillis => DataType::Time,
        Schema::TimeMicros => DataType::Time,
        Schema::TimestampMillis => DataType::DateTime,
        Schema::TimestampMicros => DataType::DateTime,
        _ => bail!("Unsupported avro type: {:?}", schema),
    };

    Ok((r#type, nullable))
}

pub fn into_avro_type(r#type: &DataType, nullable: bool) -> Result<Schema> {
    let mut schema = match r#type {
        DataType::Utf8String(_) => Schema::String,
        DataType::Binary => Schema::Bytes,
        DataType::Boolean => Schema::Boolean,
        DataType::Int8 => Schema::Int,
        DataType::UInt8 => Schema::Int,
        DataType::Int16 => Schema::Int,
        DataType::UInt16 => Schema::Int,
        DataType::Int32 => Schema::Int,
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

pub fn from_avro_value(val: AvroValue) -> Result<DataValue> {
    let res = match val {
        AvroValue::Null => DataValue::Null,
        AvroValue::Boolean(b) => DataValue::Boolean(b),
        AvroValue::Int(i) => DataValue::Int32(i),
        AvroValue::Long(l) => DataValue::Int64(l),
        AvroValue::Float(f) => DataValue::Float32(f),
        AvroValue::Double(d) => DataValue::Float64(d),
        AvroValue::Bytes(b) => DataValue::Binary(b),
        AvroValue::String(s) => DataValue::Utf8String(s),
        AvroValue::Union(_, b) => from_avro_value(*b)?,
        AvroValue::Date(d) => {
            DataValue::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(d as _))
        }
        AvroValue::TimeMillis(t) => DataValue::Time(
            NaiveTime::from_num_seconds_from_midnight_opt(
                (t / 1000) as _,
                ((t % 1000) * 1000_000) as _,
            )
            .unwrap(),
        ),
        AvroValue::TimeMicros(t) => DataValue::Time(
            NaiveTime::from_num_seconds_from_midnight_opt(
                (t / 1000_000) as _,
                ((t % 1000_000) * 1000) as _,
            )
            .unwrap(),
        ),
        AvroValue::TimestampMillis(t) => DataValue::DateTime(
            NaiveDateTime::from_timestamp_opt((t / 1000) as _, ((t % 1000) * 1000_000) as _)
                .unwrap(),
        ),
        AvroValue::TimestampMicros(t) => DataValue::DateTime(
            NaiveDateTime::from_timestamp_opt((t / 1000_000) as _, ((t % 1000_000) * 1000) as _)
                .unwrap(),
        ),
        AvroValue::Uuid(u) => DataValue::Uuid(u),
        _ => bail!("Unsupported avro type: {:?}", val),
    };

    Ok(res)
}

pub fn into_avro_value(val: DataValue) -> AvroValue {
    match val {
        DataValue::Null => AvroValue::Null,
        DataValue::Utf8String(s) => AvroValue::String(s),
        DataValue::Binary(b) => AvroValue::Bytes(b),
        DataValue::Boolean(b) => AvroValue::Boolean(b),
        DataValue::Int8(i) => AvroValue::Int(i as _),
        DataValue::UInt8(i) => AvroValue::Int(i as _),
        DataValue::Int16(i) => AvroValue::Int(i as _),
        DataValue::UInt16(i) => AvroValue::Int(i as _),
        DataValue::Int32(i) => AvroValue::Int(i as _),
        DataValue::UInt32(i) => AvroValue::Long(i as _),
        DataValue::Int64(i) => AvroValue::Long(i as _),
        DataValue::UInt64(i) => AvroValue::String(i.to_string()),
        DataValue::Float32(f) => AvroValue::Float(f),
        DataValue::Float64(f) => AvroValue::Double(f),
        DataValue::Decimal(d) => AvroValue::String(d.to_string()),
        DataValue::JSON(j) => AvroValue::String(j),
        DataValue::Date(d) => AvroValue::Date(
            d.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as _,
        ),
        DataValue::Time(t) => AvroValue::TimeMicros(
            t.num_seconds_from_midnight() as i64 * 1000_000 + t.nanosecond() as i64 / 1000,
        ),
        DataValue::DateTime(d) => AvroValue::TimestampMicros(d.timestamp_micros()),
        DataValue::DateTimeWithTZ(d) => {
            AvroValue::TimestampMicros(d.zoned().unwrap().timestamp_micros())
        }
        DataValue::Uuid(u) => AvroValue::Uuid(u),
    }
}
