use std::collections::HashMap;

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use ansilo_connectors_jdbc_oracle::{
    OracleJdbcEntitySearcher, OracleJdbcEntitySourceConfig, OracleJdbcTableOptions,
};
use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, StringOptions},
};
use serial_test::serial;

mod common;

#[test]
#[serial]
fn test_oracle_jdbc_discover_entities() {
    let containers = common::start_oracle();
    let mut con = common::connect_to_oracle(&containers);

    let entities = OracleJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::default(),
    )
    .unwrap();

    assert!(
        entities.len() > 100,
        "Oracle database should have many default tables"
    );

    let dual = entities
        .iter()
        .find(|i| i.id == "SYS.DUAL")
        .unwrap()
        .clone();

    assert_eq!(
        dual,
        EntityConfig::minimal(
            "SYS.DUAL",
            vec![EntityAttributeConfig::new(
                "DUMMY".into(),
                None,
                DataType::Utf8String(StringOptions::new(Some(1))),
                false,
                true
            )],
            EntitySourceConfig::from(OracleJdbcEntitySourceConfig::Table(
                OracleJdbcTableOptions::new(Some("SYS".into()), "DUAL".into(), HashMap::new())
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_oracle_jdbc_discover_entities_with_filter_for_single_table() {
    let containers = common::start_oracle();
    let mut con = common::connect_to_oracle(&containers);

    let entities = OracleJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("SYS.DUAL"),
    )
    .unwrap();

    assert_eq!(
        entities.len(),
        1,
        "Remote schema filter should filter down to a specific table"
    );

    let dual = entities[0].clone();

    assert_eq!(
        dual,
        EntityConfig::minimal(
            "SYS.DUAL",
            vec![EntityAttributeConfig::new(
                "DUMMY".into(),
                None,
                DataType::Utf8String(StringOptions::new(Some(1))),
                false,
                true
            )],
            EntitySourceConfig::from(OracleJdbcEntitySourceConfig::Table(
                OracleJdbcTableOptions::new(Some("SYS".into()), "DUAL".into(), HashMap::new())
            ))
            .unwrap()
        )
    )
}

#[test]
#[serial]
fn test_oracle_jdbc_discover_entities_with_filter_wildcard() {
    let containers = common::start_oracle();
    let mut con = common::connect_to_oracle(&containers);

    let entities = OracleJdbcEntitySearcher::discover(
        &mut con,
        &NodeConfig::default(),
        EntityDiscoverOptions::schema("SYS.%"),
    )
    .unwrap();

    assert!(entities.len() > 0);

    assert!(entities.iter().all(|e| e.id.starts_with("SYS.")));
}
