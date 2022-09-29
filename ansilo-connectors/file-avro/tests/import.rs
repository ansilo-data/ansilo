use std::sync::Arc;

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};
use ansilo_connectors_file_avro::{AvroConfig, AvroIO};
use ansilo_connectors_file_base::{FileConnection, FileEntitySearcher, FileSourceConfig};
use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig},
    data::DataType,
};
use pretty_assertions::assert_eq;

mod common;

// exmaple.arvo generated from https://github.com/apache/avro/blob/fcc4e2d/lang/rust/avro/examples/generate_interop_data.rs

#[test]
fn test_avro_entity_import() {
    ansilo_logging::init_for_tests();

    let entities = FileEntitySearcher::<AvroIO>::discover(
        &mut FileConnection::new(Arc::new(AvroConfig::new(current_dir!().join("data")))),
        &Default::default(),
        EntityDiscoverOptions::new("example.avro", Default::default()),
    )
    .unwrap();

    assert_eq!(
        entities,
        vec![EntityConfig::new(
            "example.avro".into(),
            None,
            None,
            vec![],
            vec![
                EntityAttributeConfig::minimal("intField", DataType::Int32),
                EntityAttributeConfig::minimal("longField", DataType::Int64),
                EntityAttributeConfig::minimal(
                    "stringField",
                    DataType::Utf8String(Default::default())
                ),
                EntityAttributeConfig::minimal("boolField", DataType::Boolean),
                EntityAttributeConfig::minimal("floatField", DataType::Float32),
                EntityAttributeConfig::minimal("doubleField", DataType::Float64),
                EntityAttributeConfig::minimal("bytesField", DataType::Binary),
                EntityAttributeConfig::minimal("nullField", DataType::Null),
            ],
            vec![],
            EntitySourceConfig::from(FileSourceConfig::new("example.avro".into())).unwrap()
        )]
    )
}
