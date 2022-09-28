use std::collections::HashMap;

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use ansilo_connectors_jdbc_mssql::{
    MssqlJdbcEntitySearcher, MssqlJdbcEntitySourceConfig, MssqlJdbcTableOptions,
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
fn test_mssql_jdbc_discover_entities_varchar_type_mapping() {
    let containers = common::start_mssql();
    let mut con = common::connect_to_mssql(&containers);

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
            [VARCHAR] VARCHAR(255),
            [NVARCHAR] NVARCHAR(123),
            [CHAR] CHAR,
            [NCHAR] NCHAR,
            [TEXT] TEXT,
            [NTEXT] NTEXT,
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = MssqlJdbcEntitySearcher::discover(
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
                    "NVARCHAR",
                    DataType::Utf8String(StringOptions::new(Some(123)))
                ),
                EntityAttributeConfig::nullable(
                    "CHAR",
                    DataType::Utf8String(StringOptions::new(Some(1)))
                ),
                EntityAttributeConfig::nullable(
                    "NCHAR",
                    DataType::Utf8String(StringOptions::new(Some(1)))
                ),
                EntityAttributeConfig::nullable(
                    "TEXT",
                    DataType::Utf8String(StringOptions::new(Some(2147483647)))
                ),
                EntityAttributeConfig::nullable(
                    "NTEXT",
                    DataType::Utf8String(StringOptions::new(Some(1073741823)))
                ),
            ],
            EntitySourceConfig::from(MssqlJdbcEntitySourceConfig::Table(
                MssqlJdbcTableOptions::new(
                    "dbo".into(),
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
fn test_mssql_jdbc_discover_entities_number_type_mapping() {
    let containers = common::start_mssql();
    let mut con = common::connect_to_mssql(&containers);

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
            [UINT8] TINYINT,
            [INT16] SMALLINT,
            [INT32] INT,
            [INT64] BIGINT,
            [FLOAT32] FLOAT(24),
            [FLOAT64] FLOAT(53),
            [DEC1] DECIMAL(19),
            [DEC2] DECIMAL(5, 1)
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = MssqlJdbcEntitySearcher::discover(
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
                EntityAttributeConfig::nullable("UINT8", DataType::UInt8),
                EntityAttributeConfig::nullable("INT16", DataType::Int16),
                EntityAttributeConfig::nullable("INT32", DataType::Int32),
                EntityAttributeConfig::nullable("INT64", DataType::Int64),
                EntityAttributeConfig::nullable("FLOAT32", DataType::Float32),
                EntityAttributeConfig::nullable("FLOAT64", DataType::Float64),
                EntityAttributeConfig::nullable(
                    "DEC1",
                    DataType::Decimal(DecimalOptions::new(Some(19), Some(0)))
                ),
                EntityAttributeConfig::nullable(
                    "DEC2",
                    DataType::Decimal(DecimalOptions::new(Some(5), Some(1)))
                ),
            ],
            EntitySourceConfig::from(MssqlJdbcEntitySourceConfig::Table(
                MssqlJdbcTableOptions::new(
                    "dbo".into(),
                    "import_number_types".into(),
                    HashMap::new()
                )
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_mssql_jdbc_discover_entities_with_pk() {
    let containers = common::start_mssql();
    let mut con = common::connect_to_mssql(&containers);

    con.execute(
        "
        DROP TABLE IF EXISTS import_pk_types;
        ",
        vec![],
    )
    .unwrap();

    con.execute(
        "
        CREATE TABLE import_pk_types (
            [id] INT PRIMARY KEY,
            [data] NVARCHAR(123),
        ) 
        ",
        vec![],
    )
    .unwrap();

    let entities = MssqlJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("%import_pk_types%"),
    )
    .unwrap();

    assert_eq!(
        entities[0].clone(),
        EntityConfig::minimal(
            "import_pk_types",
            vec![
                EntityAttributeConfig::new("id".into(), None, DataType::Int32, true, false),
                EntityAttributeConfig::nullable(
                    "data",
                    DataType::Utf8String(StringOptions::new(Some(123)))
                ),
            ],
            EntitySourceConfig::from(MssqlJdbcEntitySourceConfig::Table(
                MssqlJdbcTableOptions::new("dbo".into(), "import_pk_types".into(), HashMap::new())
            ))
            .unwrap()
        )
    )
}
