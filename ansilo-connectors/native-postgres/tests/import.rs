use std::collections::HashMap;

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use ansilo_connectors_native_postgres::{
    PostgresEntitySearcher, PostgresEntitySourceConfig, PostgresTableOptions,
};
use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, DecimalOptions, StringOptions},
};
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

mod common;

#[test]
#[serial]
fn test_postgres_discover_entities_default() {
    let containers = common::start_postgres();
    let mut con = common::connect_to_postgres(&containers);

    let entities = PostgresEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::default(),
    )
    .unwrap();

    dbg!(entities.clone());
    assert!(
        entities.len() > 10,
        "postgres database should have many default tables"
    );
}

#[test]
#[serial]
fn test_postgres_discover_entities_with_filter_wildcard() {
    let containers = common::start_postgres();
    let mut con = common::connect_to_postgres(&containers);

    con.execute(
        "
        DROP TABLE IF EXISTS test_import_wildcard;
        ",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE test_import_wildcard (
            x VARCHAR(255)
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = PostgresEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%.test_import_wild%"),
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
fn test_postgres_discover_entities_varchar_type_mapping() {
    let containers = common::start_postgres();
    let mut con = common::connect_to_postgres(&containers);

    con.execute(
        "
        DROP TABLE IF EXISTS import_varchar_types;
        ",
        vec![],
    )
    .unwrap();

    con.execute(
        r#"
        CREATE TABLE import_varchar_types (
            "VARCHAR" VARCHAR(255),
            "TEXT" TEXT
        ) 
        "#,
        vec![],
    )
    .unwrap();

    let entities = PostgresEntitySearcher::discover(
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
                    DataType::Utf8String(StringOptions::new(Some(255)))
                ),
                EntityAttributeConfig::nullable(
                    "TEXT",
                    DataType::Utf8String(StringOptions::new(None))
                ),
            ],
            EntitySourceConfig::from(PostgresEntitySourceConfig::Table(
                PostgresTableOptions::new(
                    Some("public".into()),
                    "import_varchar_types".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_postgres_discover_entities_number_type_mapping() {
    let containers = common::start_postgres();
    let mut con = common::connect_to_postgres(&containers);

    con.execute(
        r#"
        DROP TABLE IF EXISTS import_number_types;
        "#,
        vec![],
    )
    .unwrap();

    con.execute(
        r#"
        CREATE TABLE import_number_types (
            "INT16" SMALLINT,
            "INT32" INT,
            "INT64" BIGINT,
            "SMALLSERIAL" SMALLSERIAL,
            "SERIAL" SERIAL,
            "BIGSERIAL" BIGSERIAL,
            "DEC1" DECIMAL(19),
            "DEC2" DECIMAL(5, 1),
            "FLOAT32" REAL,
            "FLOAT64" DOUBLE PRECISION
        ) 
        "#,
        vec![],
    )
    .unwrap();

    let entities = PostgresEntitySearcher::discover(
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
                EntityAttributeConfig::nullable("INT16", DataType::Int16),
                EntityAttributeConfig::nullable("INT32", DataType::Int32),
                EntityAttributeConfig::nullable("INT64", DataType::Int64),
                EntityAttributeConfig::minimal("SMALLSERIAL", DataType::Int16),
                EntityAttributeConfig::minimal("SERIAL", DataType::Int32),
                EntityAttributeConfig::minimal("BIGSERIAL", DataType::Int64),
                EntityAttributeConfig::nullable(
                    "DEC1",
                    DataType::Decimal(DecimalOptions::new(Some(19), Some(0)))
                ),
                EntityAttributeConfig::nullable(
                    "DEC2",
                    DataType::Decimal(DecimalOptions::new(Some(5), Some(1)))
                ),
                EntityAttributeConfig::nullable("FLOAT32", DataType::Float32),
                EntityAttributeConfig::nullable("FLOAT64", DataType::Float64),
            ],
            EntitySourceConfig::from(PostgresEntitySourceConfig::Table(
                PostgresTableOptions::new(
                    Some("public".into()),
                    "import_number_types".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}
