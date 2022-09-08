use ansilo_core::{
    data::{
        chrono::{DateTime, Utc},
        chrono_tz::Tz,
        DataType, DataValue, DateTimeWithTZ,
    },
    err::{bail, Result},
};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tokio_postgres::{
    types::{ToSql, Type},
    Row,
};

pub mod types;

/// Mapping between a DataType and postgres type
pub fn to_pg_type(r#type: &DataType) -> Type {
    match r#type {
        DataType::Utf8String(_) => Type::TEXT,
        DataType::Binary => Type::BYTEA,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::INT2,
        DataType::UInt8 => Type::INT2,
        DataType::Int16 => Type::INT2,
        DataType::UInt16 => Type::INT4,
        DataType::Int32 => Type::INT4,
        DataType::UInt32 => Type::INT8,
        DataType::Int64 => Type::INT8,
        DataType::UInt64 => Type::NUMERIC,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Decimal(_) => Type::NUMERIC,
        DataType::JSON => Type::JSON,
        DataType::Date => Type::DATE,
        DataType::Time => Type::TIME,
        DataType::DateTime => Type::TIMESTAMP,
        DataType::DateTimeWithTZ => Type::TIMESTAMPTZ,
        DataType::Uuid => Type::UUID,
        DataType::Null => Type::TEXT,
    }
}

/// Mapping from pg type to DataType
pub fn from_pg_type(r#type: &Type) -> Result<DataType> {
    Ok(match *r#type {
        Type::TEXT | Type::VARCHAR | Type::NAME | Type::BPCHAR | Type::CHAR => {
            DataType::Utf8String(Default::default())
        }
        Type::BYTEA | Type::VARBIT | Type::TID => DataType::Binary,
        Type::BOOL | Type::BIT => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::NUMERIC => DataType::Decimal(Default::default()),
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::JSON | Type::JSONB => DataType::JSON,
        Type::DATE => DataType::Date,
        Type::TIME => DataType::Time,
        Type::TIMESTAMP => DataType::DateTime,
        Type::TIMESTAMPTZ => DataType::DateTimeWithTZ,
        Type::UUID => DataType::Uuid,
        _ => bail!("Postgres type '{:?}' is not supported", r#type),
    })
}

/// Converts a DataValue into the supplied postgres type
pub fn to_pg(val: DataValue, r#type: &Type) -> Result<Box<dyn ToSql>> {
    // Coerce to the desired type if necessary
    let val = val.try_coerce_into(&from_pg_type(r#type)?)?;

    Ok(match val {
        DataValue::Utf8String(d) => Box::new(d),
        DataValue::Binary(d) => Box::new(types::Binary(d)),
        DataValue::Boolean(d) => Box::new(d),
        DataValue::Int8(d) => Box::new(d),
        DataValue::UInt8(d) => Box::new(d as i16),
        DataValue::Int16(d) => Box::new(d),
        DataValue::UInt16(d) => Box::new(d as i32),
        DataValue::Int32(d) => Box::new(d),
        DataValue::UInt32(d) => Box::new(d as i64),
        DataValue::Int64(d) => Box::new(d),
        DataValue::UInt64(d) => Box::new(Decimal::from_u64(d).unwrap()),
        DataValue::Float32(d) => Box::new(d),
        DataValue::Float64(d) => Box::new(d),
        DataValue::Decimal(d) => Box::new(d),
        DataValue::JSON(d) => Box::new(serde_json::from_str::<serde_json::Value>(&d)?),
        DataValue::Date(d) => Box::new(d),
        DataValue::Time(d) => Box::new(d),
        DataValue::DateTime(d) => Box::new(d),
        DataValue::DateTimeWithTZ(d) => Box::new(d.utc().unwrap()),
        DataValue::Uuid(d) => Box::new(d),
        DataValue::Null => Box::new(types::Null),
    })
}

/// Converts a DataValue into the supplied postgres type
pub fn from_pg(row: &Row, idx: usize, r#type: &Type) -> Result<DataValue> {
    let val = match from_pg_type(r#type)? {
        DataType::Utf8String(_) => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Utf8String(d)),
        DataType::Binary => row
            .try_get::<_, Option<types::Binary>>(idx)?
            .map(|d| DataValue::Binary(d.0)),
        DataType::Boolean => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Boolean(d)),
        DataType::Int8 => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Int8(d)),
        DataType::Int16 => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Int16(d)),
        DataType::Int32 => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Int32(d)),
        DataType::Int64 => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Int64(d)),
        DataType::Float32 => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Float32(d)),
        DataType::Float64 => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Float64(d)),
        DataType::Decimal(_) => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Decimal(d)),
        DataType::JSON => {
            let d = row.try_get::<_, Option<serde_json::Value>>(idx)?;
            if let Some(d) = d {
                Some(DataValue::JSON(serde_json::to_string(&d)?))
            } else {
                None
            }
        }
        DataType::Date => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Date(d)),
        DataType::Time => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Time(d)),
        DataType::DateTime => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::DateTime(d)),
        DataType::DateTimeWithTZ => row
            .try_get::<_, Option<DateTime<Utc>>>(idx)?
            .map(|d| DataValue::DateTimeWithTZ(DateTimeWithTZ::new(d.naive_utc(), Tz::UTC))),
        DataType::Uuid => row
            .try_get::<_, Option<_>>(idx)?
            .map(|d| DataValue::Uuid(d)),
        DataType::Null => Some(DataValue::Null),
        DataType::UInt8 => unreachable!(),
        DataType::UInt16 => unreachable!(),
        DataType::UInt32 => unreachable!(),
        DataType::UInt64 => unreachable!(),
    };

    Ok(val.unwrap_or_else(|| DataValue::Null))
}
