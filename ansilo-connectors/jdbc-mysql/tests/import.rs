use std::collections::HashMap;

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use ansilo_connectors_jdbc_mysql::{
    MysqlJdbcEntitySearcher, MysqlJdbcEntitySourceConfig, MysqlJdbcTableOptions,
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
fn test_mysql_jdbc_discover_entities() {
    let containers = common::start_mysql();
    let mut con = common::connect_to_mysql(&containers);

    let entities = MysqlJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::default(),
    )
    .unwrap();

    assert!(
        entities.len() > 100,
        "Mysql database should have many default tables"
    );
}

#[test]
#[serial]
fn test_mysql_jdbc_discover_entities_with_filter_wildcard() {
    let containers = common::start_mysql();
    let mut con = common::connect_to_mysql(&containers);

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

    let entities = MysqlJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%.test_import_wild%"),
    )
    .unwrap();

    assert!(entities.len() > 0);

    assert_eq!(
        entities.iter().map(|i| i.id.clone()).collect_vec(),
        vec!["db.test_import_wildcard"]
    )
}

#[test]
#[serial]
fn test_mysql_jdbc_discover_entities_varchar_type_mapping() {
    let containers = common::start_mysql();
    let mut con = common::connect_to_mysql(&containers);

    con.execute(
        "
        DROP TABLE IF EXISTS import_varchar_types;
        ",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE import_varchar_types (
            `VARCHAR` VARCHAR(255),
            `TEXT` TEXT,
            `LONG_TEXT` LONGTEXT
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = MysqlJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%import_varchar_types%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "db.import_varchar_types",
            vec![
                EntityAttributeConfig::nullable(
                    "VARCHAR",
                    DataType::Utf8String(StringOptions::new(Some(255)))
                ),
                EntityAttributeConfig::nullable(
                    "TEXT",
                    DataType::Utf8String(StringOptions::new(Some(65535)))
                ),
                EntityAttributeConfig::nullable(
                    "LONG_TEXT",
                    DataType::Utf8String(StringOptions::new(Some(4294967295)))
                ),
            ],
            EntitySourceConfig::from(MysqlJdbcEntitySourceConfig::Table(
                MysqlJdbcTableOptions::new(
                    Some("db".into()),
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
fn test_mysql_jdbc_discover_entities_number_type_mapping() {
    let containers = common::start_mysql();
    let mut con = common::connect_to_mysql(&containers);

    con.execute(
        "
        DROP TABLE IF EXISTS import_number_types;
        ",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE import_number_types (
            `INT8` TINYINT,
            `INT16` SMALLINT,
            `INT32` INT,
            `INT64` BIGINT,
            `UINT8` TINYINT UNSIGNED,
            `UINT16` SMALLINT UNSIGNED,
            `UINT32` INT UNSIGNED,
            `UINT64` BIGINT UNSIGNED,
            `DEC1` DECIMAL(19),
            `DEC2` DECIMAL(5, 1)
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = MysqlJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%import_number_types%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "db.import_number_types",
            vec![
                EntityAttributeConfig::nullable("INT8", DataType::Int8),
                EntityAttributeConfig::nullable("INT16", DataType::Int16),
                EntityAttributeConfig::nullable("INT32", DataType::Int32),
                EntityAttributeConfig::nullable("INT64", DataType::Int64),
                EntityAttributeConfig::nullable("UINT8", DataType::UInt8),
                EntityAttributeConfig::nullable("UINT16", DataType::UInt16),
                EntityAttributeConfig::nullable("UINT32", DataType::UInt32),
                EntityAttributeConfig::nullable("UINT64", DataType::UInt64),
                EntityAttributeConfig::nullable(
                    "DEC1",
                    DataType::Decimal(DecimalOptions::new(Some(19), Some(0)))
                ),
                EntityAttributeConfig::nullable(
                    "DEC2",
                    DataType::Decimal(DecimalOptions::new(Some(5), Some(1)))
                ),
            ],
            EntitySourceConfig::from(MysqlJdbcEntitySourceConfig::Table(
                MysqlJdbcTableOptions::new(
                    Some("db".into()),
                    "import_number_types".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}
