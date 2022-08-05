use pgx::*;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::{
        fs, iter,
        panic::{RefUnwindSafe, UnwindSafe},
        path::PathBuf,
        thread,
        time::Duration,
    };

    use super::*;

    use crate::{
        assert_query_plan_expected,
        fdw::test::{
            query::{execute_query, explain_query_verbose},
            server::start_fdw_server,
        },
        sqlil::test,
    };
    use ansilo_connectors::{
        common::entity::{ConnectorEntityConfig, EntitySource},
        interface::{container::ConnectionPools, Connector, OperationCost},
        memory::{
            MemoryConnectionConfig, MemoryConnectionPool, MemoryConnector,
            MemoryConnectorEntitySourceConfig,
        },
    };
    use ansilo_core::data::*;
    use ansilo_core::{
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig, NodeConfig},
        data::{DataType, DataValue},
        sqlil,
    };
    use ansilo_pg::fdw::server::FdwServer;
    use assert_json_diff::*;
    use serde::{de::DeserializeOwned, Serialize};
    use serde_json::json;

    fn create_memory_connection_pool() -> ConnectionPools {
        let mut conf = MemoryConnectionConfig::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::minimal(
            "people",
            EntityVersionConfig::minimal(
                "1.0",
                vec![
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                    EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            // We mock the tabel size to be large as the query optimizer
            // does not like pushing down queries on very small tables.
            MemoryConnectorEntitySourceConfig::new(Some(OperationCost::new(
                Some(1000),
                None,
                None,
                None,
            ))),
        ));

        entities.add(EntitySource::minimal(
            "pets",
            EntityVersionConfig::minimal(
                "1.0",
                vec![
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("owner_id", DataType::UInt32),
                    EntityAttributeConfig::minimal("pet_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::default(),
        ));

        entities.add(EntitySource::minimal(
            "large",
            EntityVersionConfig::minimal(
                "1.0",
                vec![EntityAttributeConfig::minimal("x", DataType::UInt32)],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::default(),
        ));

        conf.set_data(
            "people",
            "1.0",
            vec![
                vec![
                    DataValue::UInt32(1),
                    DataValue::from("Mary"),
                    DataValue::from("Jane"),
                ],
                vec![
                    DataValue::UInt32(2),
                    DataValue::from("John"),
                    DataValue::from("Smith"),
                ],
                vec![
                    DataValue::UInt32(3),
                    DataValue::from("Gary"),
                    DataValue::from("Gregson"),
                ],
                vec![
                    DataValue::UInt32(4),
                    DataValue::from("Mary"),
                    DataValue::from("Bennet"),
                ],
            ],
        );

        conf.set_data(
            "pets",
            "1.0",
            vec![
                vec![
                    DataValue::UInt32(1),
                    DataValue::UInt32(1),
                    DataValue::from("Pepper"),
                ],
                vec![
                    DataValue::UInt32(2),
                    DataValue::UInt32(1),
                    DataValue::from("Salt"),
                ],
                vec![
                    DataValue::UInt32(3),
                    DataValue::UInt32(3),
                    DataValue::from("Relish"),
                ],
                vec![
                    DataValue::UInt32(4),
                    DataValue::Null,
                    DataValue::from("Luna"),
                ],
            ],
        );

        conf.set_data(
            "large",
            "1.0",
            (0..1_000_000)
                .into_iter()
                .map(|x| vec![DataValue::UInt32(x)])
                .collect(),
        );

        let pool = MemoryConnector::create_connection_pool(conf, &NodeConfig::default(), &entities)
            .unwrap();

        ConnectionPools::Memory(pool, entities)
    }

    fn setup_db(socket_path: impl Into<String>) {
        let socket_path = socket_path.into();
        Spi::execute(|mut client| {
            client.update(
                format!(
                    r#"
                DROP SERVER IF EXISTS test_srv CASCADE;
                CREATE SERVER test_srv FOREIGN DATA WRAPPER ansilo_fdw OPTIONS (
                    socket '{socket_path}',
                    data_source 'memory'
                );

                CREATE FOREIGN TABLE "people:1.0" (
                    id BIGINT,
                    first_name VARCHAR,
                    last_name VARCHAR
                ) SERVER test_srv;

                CREATE FOREIGN TABLE "pets:1.0" (
                    id BIGINT,
                    owner_id BIGINT,
                    pet_name VARCHAR
                ) SERVER test_srv;

                CREATE FOREIGN TABLE "large:1.0" (
                    x BIGINT
                ) SERVER test_srv;
                "#
                )
                .as_str(),
                None,
                None,
            );
        });
    }

    fn setup_test(test_name: impl Into<String>) {
        let test_name = test_name.into();
        let sock_path = format!("/tmp/ansilo/fdw_server/{test_name}");
        start_fdw_server(create_memory_connection_pool(), sock_path.clone());
        setup_db(sock_path);
    }

    #[pg_test]
    fn test_fdw_insert_single_row() {
        setup_test("insert_single_row");

        let results = execute_query(
            r#"
            INSERT INTO "people:1.0" (id, first_name, last_name) 
            VALUES (123, 'Barry', 'Diploma');

            SELECT * FROM "people:1.0";
            "#,
            |i| {
                (
                    i["id"].value::<i64>().unwrap(),
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                (1, "Mary".into(), "Jane".into()),
                (2, "John".into(), "Smith".into()),
                (3, "Gary".into(), "Gregson".into()),
                (4, "Mary".into(), "Bennet".into()),
                (123, "Barry".into(), "Diploma".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_insert_single_row_explain() {
        assert_query_plan_expected!("test_cases/0001_insert_single_row.json");
    }

    #[pg_test]
    fn test_fdw_insert_multiple_rows() {
        setup_test("insert_multiple_rows");

        let results = execute_query(
            r#"
            INSERT INTO "people:1.0" (id, first_name, last_name) 
            VALUES (123, 'Barry', 'Diploma'), (456, 'Harry', 'Potter'), (789, 'Ron', 'Weasly');

            SELECT * FROM "people:1.0";
            "#,
            |i| {
                (
                    i["id"].value::<i64>().unwrap(),
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                (1, "Mary".into(), "Jane".into()),
                (2, "John".into(), "Smith".into()),
                (3, "Gary".into(), "Gregson".into()),
                (4, "Mary".into(), "Bennet".into()),
                (123, "Barry".into(), "Diploma".into()),
                (456, "Harry".into(), "Potter".into()),
                (789, "Ron".into(), "Weasly".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_insert_select() {
        setup_test("insert_select");

        let results = execute_query(
            r#"
            INSERT INTO "people:1.0" (id, first_name, last_name) 
            SELECT id + 10, last_name, first_name FROM "people:1.0";

            SELECT * FROM "people:1.0";
            "#,
            |i| {
                (
                    i["id"].value::<i64>().unwrap(),
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                (1, "Mary".into(), "Jane".into()),
                (2, "John".into(), "Smith".into()),
                (3, "Gary".into(), "Gregson".into()),
                (4, "Mary".into(), "Bennet".into()),
                (11, "Jane".into(), "Mary".into()),
                (12, "Smith".into(), "John".into()),
                (13, "Gregson".into(), "Gary".into()),
                (14, "Bennet".into(), "Mary".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_insert_select_explain() {
        assert_query_plan_expected!("test_cases/0002_insert_select.json");
    }
}
