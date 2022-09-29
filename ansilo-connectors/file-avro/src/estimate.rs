use std::{fs, path::Path};

use ansilo_core::err::Result;
use apache_avro::Schema;

pub(crate) fn estimate_row_count(path: &Path) -> Result<u64> {
    let file = fs::OpenOptions::new().read(true).open(path)?;
    let total_len = file.metadata()?.len();
    let reader = apache_avro::Reader::new(file)?;
    let schema = reader.writer_schema();
    let row_len = estimate_bytes(schema);

    Ok(total_len / row_len)
}

fn estimate_bytes(schema: &Schema) -> u64 {
    match &schema {
        Schema::Null => 1,
        Schema::Boolean => 1,
        Schema::Int => 4,
        Schema::Long => 8,
        Schema::Float => 4,
        Schema::Double => 8,
        Schema::Bytes => 50,
        Schema::String => 50,
        Schema::Array(_) => 100,
        Schema::Map(_) => 200,
        Schema::Union(u) => u
            .variants()
            .iter()
            .map(|s| estimate_bytes(s))
            .max()
            .unwrap(),
        Schema::Record { fields, .. } => fields.iter().map(|f| estimate_bytes(&f.schema)).sum(),
        Schema::Enum { .. } => 20,
        Schema::Fixed { size, .. } => (*size) as _,
        Schema::Decimal { .. } => 10,
        Schema::Uuid => 16,
        Schema::Date => 12,
        Schema::TimeMillis => 8,
        Schema::TimeMicros => 8,
        Schema::TimestampMillis => 14,
        Schema::TimestampMicros => 18,
        Schema::Duration => 12,
        Schema::Ref { .. } => 10,
    }
}
