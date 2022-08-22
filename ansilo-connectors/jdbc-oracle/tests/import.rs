use std::collections::HashMap;

use ansilo_connectors_base::interface::EntitySearcher;

use ansilo_connectors_jdbc_oracle::{
    OracleJdbcEntitySearcher, OracleJdbcEntitySourceConfig, OracleJdbcTableOptions,
};
use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::{DataType, StringOptions},
};

mod common;

#[test]
fn test_oracle_jdbc_discover_entities() {
    let containers = common::start_oracle();
    let mut con = common::connect_to_oracle(&containers);

    let entities = OracleJdbcEntitySearcher::discover(&mut con, &NodeConfig::default()).unwrap();

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
                false
            )],
            EntitySourceConfig::from(OracleJdbcEntitySourceConfig::Table(
                OracleJdbcTableOptions::new(Some("SYS".into()), "DUAL".into(), HashMap::new())
            ))
            .unwrap()
        )
    )
}
