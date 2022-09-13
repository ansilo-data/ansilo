use ansilo_core::{
    data::{chrono::Utc, DataType, DataValue, StringOptions},
    err::{bail, Result},
};
use rusqlite::{
    types::{Type, Value},
    ToSql,
};

pub fn to_sqlite_type(r#type: &DataType) -> Type {
    match r#type {
        DataType::Utf8String(_) => Type::Text,
        DataType::Binary => Type::Blob,
        DataType::Boolean => Type::Integer,
        DataType::Int8 => Type::Integer,
        DataType::UInt8 => Type::Integer,
        DataType::Int16 => Type::Integer,
        DataType::UInt16 => Type::Integer,
        DataType::Int32 => Type::Integer,
        DataType::UInt32 => Type::Integer,
        DataType::Int64 => Type::Integer,
        DataType::UInt64 => Type::Text,
        DataType::Float32 => Type::Real,
        DataType::Float64 => Type::Real,
        DataType::Decimal(_) => Type::Text,
        DataType::JSON => Type::Text,
        DataType::Date => Type::Text,
        DataType::Time => Type::Text,
        DataType::DateTime => Type::Text,
        DataType::DateTimeWithTZ => Type::Text,
        DataType::Uuid => Type::Text,
        DataType::Null => Type::Null,
    }
}

pub fn from_sqlite_type(r#type: &str) -> Result<DataType> {
    // Use the column's type affinity
    // @see sqlite3AffinityType in sqlite source
    // https://github.com/sqlite/sqlite/blob/41ce47c4f4bcae3882fdccec18a6100a85f4bba5/src/build.c#L1654

    let r#type = r#type.to_uppercase();

    let affinity = {
        if r#type.contains("INT") {
            Type::Integer
        } else if r#type.contains("CHAR") || r#type.contains("CLOB") || r#type.contains("TEXT") {
            Type::Text
        } else if r#type.contains("BLOB") {
            Type::Blob
        } else if r#type.contains("REAL") || r#type.contains("FLOA") || r#type.contains("DOUB") {
            Type::Real
        } else {
            Type::Text
        }
    };

    Ok(match affinity {
        Type::Null => DataType::Null,
        Type::Integer => DataType::Int64,
        Type::Real => DataType::Float64,
        Type::Text => DataType::Utf8String(StringOptions::default()),
        Type::Blob => DataType::Binary,
    })
}

pub fn to_sqlite(val: DataValue) -> Result<Box<dyn ToSql>> {
    Ok(match val {
        DataValue::Null => Box::new(rusqlite::types::Null),
        DataValue::Utf8String(d) => Box::new(d),
        DataValue::Binary(d) => Box::new(d),
        DataValue::Boolean(d) => Box::new(d),
        DataValue::Int8(d) => Box::new(d),
        DataValue::UInt8(d) => Box::new(d),
        DataValue::Int16(d) => Box::new(d),
        DataValue::UInt16(d) => Box::new(d),
        DataValue::Int32(d) => Box::new(d),
        DataValue::UInt32(d) => Box::new(d),
        DataValue::Int64(d) => Box::new(d),
        DataValue::UInt64(d) => Box::new(d),
        DataValue::Float32(d) => Box::new(d),
        DataValue::Float64(d) => Box::new(d),
        DataValue::Decimal(d) => Box::new(d.to_string()),
        DataValue::JSON(d) => Box::new(d),
        DataValue::Date(d) => Box::new(d),
        DataValue::Time(d) => Box::new(d),
        DataValue::DateTime(d) => Box::new(d),
        DataValue::DateTimeWithTZ(d) => Box::new(d.zoned()?.with_timezone(&Utc)),
        DataValue::Uuid(d) => Box::new(d.to_string()),
    })
}

pub fn from_sqlite(val: Value, r#type: &DataType) -> Result<DataValue> {
    let val = match val {
        Value::Null => DataValue::Null,
        Value::Integer(d) => DataValue::Int64(d),
        Value::Real(d) => DataValue::Float64(d),
        Value::Text(d) => DataValue::Utf8String(d),
        Value::Blob(d) => DataValue::Binary(d),
    };

    // Since sqlite is dynamically typed we wont know the type
    // of the resultant columns unless it is declared in the table
    // In these cases we default to the Binary type as that can hold
    // every value. We convert the value to a string representation
    // before into binary so it is easier to handle.
    if r#type.is_binary() {
        return Ok(val
            .try_coerce_into(&DataType::Utf8String(StringOptions::default()))?
            .try_coerce_into(&DataType::Binary)?);
    }

    val.try_coerce_into(r#type)
}
