// use std::collections::HashMap;

// use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

// use ansilo_connectors_jdbc_teradata::{
//     TeradataJdbcEntitySearcher, TeradataJdbcEntitySourceConfig, TeradataJdbcTableOptions,
// };
// use ansilo_core::{
//     config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
//     data::{DataType, DecimalOptions, StringOptions},
// };
// use pretty_assertions::assert_eq;
// use serial_test::serial;

// mod common;

// #[test]
// #[serial]

// fn test_teradata_jdbc_discover_entities() {
//     // This test takes an enourmous amount of time over gha
//     // (i think due to the slow network link)
//     if std::env::var("ANSILO_GHA_TESTS").is_ok() {
//         return;
//     }

//     ansilo_logging::init_for_tests();
//     let containers = common::start_teradata();
//     let mut con = common::connect_to_teradata(&containers);

//     let entities = TeradataJdbcEntitySearcher::discover(
//         &mut con,
//         &NodeConfig::default(),
//         EntityDiscoverOptions::default(),
//     )
//     .unwrap();

//     assert!(
//         entities.len() > 100,
//         "Teradata database should have many default tables"
//     );

//     let dual = entities.iter().find(|i| i.id == "DUAL").unwrap().clone();

//     assert_eq!(
//         dual,
//         EntityConfig::minimal(
//             "DUAL",
//             vec![EntityAttributeConfig::new(
//                 "DUMMY".into(),
//                 None,
//                 DataType::Utf8String(StringOptions::new(Some(1))),
//                 false,
//                 true
//             )],
//             EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
//                 TeradataJdbcTableOptions::new(Some("SYS".into()), "DUAL".into(), HashMap::new())
//             ))
//             .unwrap()
//         )
//     )
// }

// #[test]
// #[serial]
// fn test_teradata_jdbc_discover_entities_with_filter_for_single_table() {
//     ansilo_logging::init_for_tests();
//     let containers = common::start_teradata();
//     let mut con = common::connect_to_teradata(&containers);

//     let entities = TeradataJdbcEntitySearcher::discover(
//         &mut con,
//         &NodeConfig::default(),
//         EntityDiscoverOptions::schema("SYS.DUAL"),
//     )
//     .unwrap();

//     assert_eq!(
//         entities.len(),
//         1,
//         "Remote schema filter should filter down to a specific table"
//     );

//     let dual = entities[0].clone();

//     assert_eq!(
//         dual,
//         EntityConfig::minimal(
//             "DUAL",
//             vec![EntityAttributeConfig::new(
//                 "DUMMY".into(),
//                 None,
//                 DataType::Utf8String(StringOptions::new(Some(1))),
//                 false,
//                 true
//             )],
//             EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
//                 TeradataJdbcTableOptions::new(Some("SYS".into()), "DUAL".into(), HashMap::new())
//             ))
//             .unwrap()
//         )
//     )
// }

// #[test]
// #[serial]
// fn test_teradata_jdbc_discover_entities_with_filter_wildcard() {
//     ansilo_logging::init_for_tests();
//     let containers = common::start_teradata();
//     let mut con = common::connect_to_teradata(&containers);

//     let entities = TeradataJdbcEntitySearcher::discover(
//         &mut con,
//         &NodeConfig::default(),
//         EntityDiscoverOptions::schema("SYS.ALL_TAB%"),
//     )
//     .unwrap();

//     assert!(entities.len() > 0);

//     assert!(entities
//         .iter()
//         .all(|e| e.source.options.as_mapping().unwrap()["owner_name"]
//             == ansilo_core::config::Value::String("SYS".into())));
// }

// #[test]
// #[serial]
// fn test_teradata_jdbc_discover_entities_number_type_mapping() {
//     let containers = common::start_teradata();
//     let mut con = common::connect_to_teradata(&containers);

//     con.execute(
//         "
//         BEGIN
//         EXECUTE IMMEDIATE 'DROP TABLE IMPORT_NUMBER_TYPES';
//         EXCEPTION
//         WHEN OTHERS THEN NULL;
//         END;
//         ",
//         vec![],
//     )
//     .unwrap();

//     con.execute(
//         "
//         CREATE TABLE IMPORT_NUMBER_TYPES (
//             INT8 NUMBER(2),
//             INT16 NUMBER(4),
//             INT32 NUMBER(9),
//             INT64 NUMBER(18),
//             DEC1 NUMBER(19),
//             DEC2 NUMBER(5, 1)
//         ) 
//         ",
//         vec![],
//     )
//     .unwrap();

//     let entities = TeradataJdbcEntitySearcher::discover(
//         &mut con,
//         &NodeConfig::default(),
//         EntityDiscoverOptions::schema("%IMPORT_NUMBER_TYPES%"),
//     )
//     .unwrap();

//     assert_eq!(
//         entities[0].clone(),
//         EntityConfig::minimal(
//             "IMPORT_NUMBER_TYPES",
//             vec![
//                 EntityAttributeConfig::nullable("INT8", DataType::Int8),
//                 EntityAttributeConfig::nullable("INT16", DataType::Int16),
//                 EntityAttributeConfig::nullable("INT32", DataType::Int32),
//                 EntityAttributeConfig::nullable("INT64", DataType::Int64),
//                 EntityAttributeConfig::nullable(
//                     "DEC1",
//                     DataType::Decimal(DecimalOptions::new(Some(19), Some(0)),)
//                 ),
//                 EntityAttributeConfig::nullable(
//                     "DEC2",
//                     DataType::Decimal(DecimalOptions::new(Some(5), Some(1)),)
//                 ),
//             ],
//             EntitySourceConfig::from(TeradataJdbcEntitySourceConfig::Table(
//                 TeradataJdbcTableOptions::new(
//                     Some("ANSILO_ADMIN".into()),
//                     "IMPORT_NUMBER_TYPES".into(),
//                     HashMap::new()
//                 )
//             ))
//             .unwrap()
//         )
//     )
// }
