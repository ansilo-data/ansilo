use std::sync::Arc;

use ansilo_connectors_base::interface::{Connection, QueryHandle, ResultSet, RowStructure};
use ansilo_connectors_file_avro::{AvroConfig, AvroIO};
use ansilo_connectors_file_base::{FileConnection, FileQuery, FileQueryType, ReadColumnsQuery};
use ansilo_core::{
    config::{EntityConfig, EntitySourceConfig},
    data::{DataType, DataValue},
};
use pretty_assertions::assert_eq;

mod common;

// exmaple.arvo generated from https://github.com/apache/avro/blob/fcc4e2d/lang/rust/avro/examples/generate_interop_data.rs

#[test]
fn test_avro_read() {
    ansilo_logging::init_for_tests();
    let mut con =
        FileConnection::<AvroIO>::new(Arc::new(AvroConfig::new(current_dir!().join("data"))));

    let mut query = con
        .prepare(FileQuery::new(
            EntityConfig::minimal("unused", vec![], EntitySourceConfig::minimal("")),
            con.conf().path.join("example.avro"),
            FileQueryType::ReadColumns(ReadColumnsQuery::new(vec![
                ("int".into(), "intField".into()),
                ("long".into(), "longField".into()),
                ("string".into(), "stringField".into()),
            ])),
        ))
        .unwrap();

    let mut results = query.execute_query().unwrap().reader().unwrap();

    assert_eq!(
        results.get_structure(),
        &RowStructure::new(vec![
            ("int".into(), DataType::Int32),
            ("long".into(), DataType::Int64),
            ("string".into(), DataType::Utf8String(Default::default())),
        ])
    );

    assert_eq!(
        results.read_row_vec().unwrap(),
        Some(vec![
            DataValue::Int32(12),
            DataValue::Int64(15234324),
            DataValue::Utf8String("hey".into()),
        ])
    );
    assert_eq!(results.read_row_vec().unwrap(), None);
}
