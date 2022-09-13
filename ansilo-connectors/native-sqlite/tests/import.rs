use std::collections::HashMap;

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use ansilo_connectors_native_sqlite::{
    SqliteEntitySearcher, SqliteEntitySourceConfig, SqliteTableOptions,
};
use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, StringOptions},
};
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

mod common;

#[test]
#[serial]
fn test_sqlite_discover_entities_default() {
    ansilo_logging::init_for_tests();
    let mut con = common::connect_to_sqlite();

    let entities = SqliteEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::default(),
    )
    .unwrap();

    assert_eq!(
        entities,
        Vec::<EntityConfig>::new(),
        "sqlite should return no tables on empty db"
    );
}

#[test]
#[serial]
fn test_sqlite_discover_entities_with_filter_wildcard() {
    ansilo_logging::init_for_tests();
    let mut con = common::connect_to_sqlite();

    con.execute_modify(
        "
        CREATE TABLE test_import_wildcard (
            x VARCHAR(255)
        );
        ",
        vec![],
    )
    .unwrap();

    let entities = SqliteEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("not_table%"),
    )
    .unwrap();

    assert_eq!(entities.len(), 0);

    let entities = SqliteEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%"),
    )
    .unwrap();

    assert!(entities.len() > 0);

    assert_eq!(
        entities.iter().map(|i| i.id.clone()).collect_vec(),
        vec!["test_import_wildcard"]
    )
}

#[test]
#[serial]
fn test_sqlite_discover_entities_varchar_type_mapping() {
    let mut con = common::connect_to_sqlite();

    con.execute_modify(
        r#"
        CREATE TABLE import_varchar_types (
            "VARCHAR" VARCHAR(255),
            "TEXT" TEXT
        )
        "#,
        vec![],
    )
    .unwrap();

    let entities = SqliteEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%import_varchar_types%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "import_varchar_types",
            vec![
                EntityAttributeConfig::nullable(
                    "VARCHAR",
                    DataType::Utf8String(StringOptions::new(None))
                ),
                EntityAttributeConfig::nullable(
                    "TEXT",
                    DataType::Utf8String(StringOptions::new(None))
                ),
            ],
            EntitySourceConfig::from(SqliteEntitySourceConfig::Table(SqliteTableOptions::new(
                "import_varchar_types".into(),
                HashMap::new()
            )))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_sqlite_discover_entities_number_type_mapping() {
    let mut con = common::connect_to_sqlite();

    con.execute_modify(
        r#"
        CREATE TABLE import_number_types (
            "INT16" SMALLINT,
            "INT32" INT,
            "INT64" BIGINT,
            "DEC1" DECIMAL(19),
            "DEC2" DECIMAL(5, 1),
            "FLOAT32" REAL,
            "FLOAT64" DOUBLE PRECISION
        )
        "#,
        vec![],
    )
    .unwrap();

    let entities = SqliteEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%import_number_types%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "import_number_types",
            vec![
                EntityAttributeConfig::nullable("INT16", DataType::Int64),
                EntityAttributeConfig::nullable("INT32", DataType::Int64),
                EntityAttributeConfig::nullable("INT64", DataType::Int64),
                EntityAttributeConfig::nullable(
                    "DEC1",
                    DataType::Utf8String(StringOptions::default())
                ),
                EntityAttributeConfig::nullable(
                    "DEC2",
                    DataType::Utf8String(StringOptions::default())
                ),
                EntityAttributeConfig::nullable("FLOAT32", DataType::Float64),
                EntityAttributeConfig::nullable("FLOAT64", DataType::Float64),
            ],
            EntitySourceConfig::from(SqliteEntitySourceConfig::Table(SqliteTableOptions::new(
                "import_number_types".into(),
                HashMap::new()
            )))
            .unwrap()
        )
    )
}
