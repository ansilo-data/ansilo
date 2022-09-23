use std::collections::HashMap;

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use ansilo_connectors_jdbc_teradata::{
    TeradataJdbcEntitySearcher, TeradataJdbcEntitySourceConfig, TeradataJdbcTableOptions,
};
use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, DecimalOptions, StringOptions},
};
use pretty_assertions::assert_eq;
use serial_test::serial;

mod common;

#[test]
#[serial]

fn test_teradata_jdbc_discover_entities() {
    ansilo_logging::init_for_tests();
    common::start_teradata();
    let mut con = common::connect_to_teradata();

    let entities = TeradataJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::default(),
    )
    .unwrap();

    assert!(
        entities.len() > 100,
        "Teradata database should have many default tables"
    );
}

#[test]
#[serial]
fn test_teradata_jdbc_discover_entities_number_type_mapping() {
    ansilo_logging::init_for_tests();
    common::start_teradata();
    let mut con = common::connect_to_teradata();

    con.execute(
        "CALL testdb.DROP_IF_EXISTS('testdb', 'IMPORT_NUMBER_TYPES');",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE IMPORT_NUMBER_TYPES (
            INT8 BYTEINT,
            INT16 SMALLINT,
            INT32 INT,
            INT64 BIGINT,
            FLOAT64 FLOAT,
            DEC1 DECIMAL(19),
            DEC2 DECIMAL(5, 1)
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = TeradataJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%IMPORT_NUMBER_TYPES%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "IMPORT_NUMBER_TYPES",
            vec![
                EntityAttributeConfig::nullable("INT8", DataType::Int8),
                EntityAttributeConfig::nullable("INT16", DataType::Int16),
                EntityAttributeConfig::nullable("INT32", DataType::Int32),
                EntityAttributeConfig::nullable("INT64", DataType::Int64),
                EntityAttributeConfig::nullable("FLOAT64", DataType::Float64),
                EntityAttributeConfig::nullable(
                    "DEC1",
                    DataType::Decimal(DecimalOptions::new(Some(19), Some(0)),)
                ),
                EntityAttributeConfig::nullable(
                    "DEC2",
                    DataType::Decimal(DecimalOptions::new(Some(5), Some(1)),)
                ),
            ],
            EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
                TeradataJdbcTableOptions::new(
                    "testdb".into(),
                    "IMPORT_NUMBER_TYPES".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_teradata_jdbc_discover_entities_varchar_type_mapping() {
    ansilo_logging::init_for_tests();
    common::start_teradata();
    let mut con = common::connect_to_teradata();

    con.execute(
        "CALL testdb.DROP_IF_EXISTS('testdb', 'IMPORT_VARCHAR_TYPES');",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE IMPORT_VARCHAR_TYPES (
            VC VARCHAR(255),
            CH CHARACTER(5),
            CL CLOB
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = TeradataJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%IMPORT_VARCHAR_TYPES%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "IMPORT_VARCHAR_TYPES",
            vec![
                EntityAttributeConfig::nullable(
                    "VC",
                    DataType::Utf8String(StringOptions::new(Some(255)))
                ),
                EntityAttributeConfig::nullable(
                    "CH",
                    DataType::Utf8String(StringOptions::new(Some(5)))
                ),
                EntityAttributeConfig::nullable(
                    "CL",
                    DataType::Utf8String(StringOptions::new(Some(2097088000)))
                ),
            ],
            EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
                TeradataJdbcTableOptions::new(
                    "testdb".into(),
                    "IMPORT_VARCHAR_TYPES".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_teradata_jdbc_discover_entities_binary_type_mapping() {
    ansilo_logging::init_for_tests();
    common::start_teradata();
    let mut con = common::connect_to_teradata();

    con.execute(
        "CALL testdb.DROP_IF_EXISTS('testdb', 'IMPORT_BINARY_TYPES');",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE IMPORT_BINARY_TYPES (
            BYT BYTE(255),
            VBY VARBYTE(5),
            BLO BLOB
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = TeradataJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%IMPORT_BINARY_TYPES%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "IMPORT_BINARY_TYPES",
            vec![
                EntityAttributeConfig::nullable("BYT", DataType::Binary),
                EntityAttributeConfig::nullable("VBY", DataType::Binary),
                EntityAttributeConfig::nullable("BLO", DataType::Binary),
            ],
            EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
                TeradataJdbcTableOptions::new(
                    "testdb".into(),
                    "IMPORT_BINARY_TYPES".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_teradata_jdbc_discover_entities_date_time_type_mapping() {
    ansilo_logging::init_for_tests();
    common::start_teradata();
    let mut con = common::connect_to_teradata();

    con.execute(
        "CALL testdb.DROP_IF_EXISTS('testdb', 'IMPORT_DATE_TIME_TYPES');",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE IMPORT_DATE_TIME_TYPES (
            DAT DATE,
            TIM TIME,
            TS TIMESTAMP,
            TSZ TIMESTAMP WITH TIME ZONE
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = TeradataJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%IMPORT_DATE_TIME_TYPES%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "IMPORT_DATE_TIME_TYPES",
            vec![
                EntityAttributeConfig::nullable("DAT", DataType::Date),
                EntityAttributeConfig::nullable("TIM", DataType::Time),
                EntityAttributeConfig::nullable("TS", DataType::DateTime),
                EntityAttributeConfig::nullable("TSZ", DataType::DateTimeWithTZ),
            ],
            EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
                TeradataJdbcTableOptions::new(
                    "testdb".into(),
                    "IMPORT_DATE_TIME_TYPES".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_teradata_jdbc_discover_entities_not_null_type_mapping() {
    ansilo_logging::init_for_tests();
    common::start_teradata();
    let mut con = common::connect_to_teradata();

    con.execute(
        "CALL testdb.DROP_IF_EXISTS('testdb', 'IMPORT_NOT_NULL_TYPES');",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE IMPORT_NOT_NULL_TYPES (
            NN INT NOT NULL
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = TeradataJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%IMPORT_NOT_NULL_TYPES%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "IMPORT_NOT_NULL_TYPES",
            vec![EntityAttributeConfig::new(
                "NN".into(),
                None,
                DataType::Int32,
                false,
                false
            ),],
            EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
                TeradataJdbcTableOptions::new(
                    "testdb".into(),
                    "IMPORT_NOT_NULL_TYPES".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_teradata_jdbc_discover_entities_pk_type_mapping() {
    ansilo_logging::init_for_tests();
    common::start_teradata();
    let mut con = common::connect_to_teradata();

    con.execute(
        "CALL testdb.DROP_IF_EXISTS('testdb', 'IMPORT_PK_TYPES');",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE IMPORT_PK_TYPES (
            PK1 INT NOT NULL,
            PK2 INT NOT NULL,
            COL3 INT,
            PRIMARY KEY(PK1, PK2)
        )
        ",
        vec![],
    )
    .unwrap();

    let entities = TeradataJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%IMPORT_PK_TYPES%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "IMPORT_PK_TYPES",
            vec![
                EntityAttributeConfig::new("PK1".into(), None, DataType::Int32, true, false),
                EntityAttributeConfig::new("PK2".into(), None, DataType::Int32, true, false),
                EntityAttributeConfig::nullable("COL3", DataType::Int32)
            ],
            EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
                TeradataJdbcTableOptions::new(
                    "testdb".into(),
                    "IMPORT_PK_TYPES".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}
