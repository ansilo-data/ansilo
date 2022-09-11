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
    use ansilo_connectors_all::{ConnectionPools, ConnectorEntityConfigs};
    use ansilo_connectors_base::{
        common::entity::{ConnectorEntityConfig, EntitySource},
        interface::Connector,
    };
    use ansilo_connectors_memory::{
        MemoryConnector, MemoryConnectorEntitySourceConfig, MemoryDatabase,
    };
    use ansilo_core::data::*;
    use ansilo_core::{
        config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
        data::{DataType, DataValue},
        sqlil,
    };
    use ansilo_pg::fdw::{proto::OperationCost, server::FdwServer};
    use assert_json_diff::*;
    use pretty_assertions::assert_eq;
    use serde::{de::DeserializeOwned, Serialize};
    use serde_json::json;

    fn create_memory_connection_pool() -> (ConnectionPools, ConnectorEntityConfigs) {
        let mut conf = MemoryDatabase::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::new(
            EntityConfig::minimal(
                "people",
                vec![
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                    EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            // We mock the table size to be large as the query optimizer
            // does not like pushing down queries on very small tables.
            MemoryConnectorEntitySourceConfig::new(Some(OperationCost::new(
                Some(1000),
                None,
                None,
                None,
            ))),
        ));

        entities.add(EntitySource::new(
            EntityConfig::minimal(
                "pets",
                vec![
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("owner_id", DataType::UInt32),
                    EntityAttributeConfig::minimal("pet_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::default(),
        ));

        conf.set_data(
            "people",
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

        let pool = MemoryConnector::create_connection_pool(conf, &NodeConfig::default(), &entities)
            .unwrap();

        (
            ConnectionPools::Memory(pool),
            ConnectorEntityConfigs::Memory(entities),
        )
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

                IMPORT FOREIGN SCHEMA memory 
                FROM SERVER test_srv INTO public;
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
            INSERT INTO "people" (id, first_name, last_name) 
            VALUES (123, 'Barry', 'Diploma');

            SELECT * FROM "people";
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
            INSERT INTO "people" (id, first_name, last_name) 
            VALUES (123, 'Barry', 'Diploma'), (456, 'Harry', 'Potter'), (789, 'Ron', 'Weasly');

            SELECT * FROM "people";
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
    fn test_fdw_insert_multiple_batches() {
        setup_test("insert_multi_batch");

        let results = execute_query(
            r#"
            INSERT INTO "people" (id, first_name, last_name) 
            SELECT 100 + x, 'first_name_' || x::text, 'last_name_' || x::text
            FROM generate_series(1, 55) AS x;

            SELECT * FROM "people";
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
            [
                vec![
                    (1, "Mary".into(), "Jane".into()),
                    (2, "John".into(), "Smith".into()),
                    (3, "Gary".into(), "Gregson".into()),
                    (4, "Mary".into(), "Bennet".into()),
                ],
                (1..=55)
                    .into_iter()
                    .map(|x| (100 + x, format!("first_name_{x}"), format!("last_name_{x}")))
                    .collect()
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
        );
    }

    #[pg_test]
    fn test_fdw_insert_select() {
        setup_test("insert_select");

        let results = execute_query(
            r#"
            INSERT INTO "people" (id, first_name, last_name) 
            SELECT id + 10, last_name, first_name FROM "people";

            SELECT * FROM "people";
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

    #[pg_test]
    fn test_fdw_update_all_rows_local_set() {
        setup_test("update_all_rows_local_set");

        let results = execute_query(
            r#"
            UPDATE "people" SET first_name = 'Updated: ' || MD5(first_name);

            SELECT * FROM "people";
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
                (
                    1,
                    "Updated: e39e74fb4e80ba656f773669ed50315a".into(),
                    "Jane".into()
                ),
                (
                    2,
                    "Updated: 61409aa1fd47d4a5332de23cbf59a36f".into(),
                    "Smith".into()
                ),
                (
                    3,
                    "Updated: 01d4848202a3c7697ec037b02b4ee4e8".into(),
                    "Gregson".into()
                ),
                (
                    4,
                    "Updated: e39e74fb4e80ba656f773669ed50315a".into(),
                    "Bennet".into()
                ),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_update_all_rows_local_explain() {
        assert_query_plan_expected!("test_cases/0003_update_all_rows_local_set.json");
    }

    #[pg_test]
    fn test_fdw_update_where_local_set() {
        setup_test("update_where_local_set");

        let results = execute_query(
            r#"
            UPDATE "people" SET first_name = 'Updated: ' || MD5(first_name) WHERE id = 3;

            SELECT * FROM "people";
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
                (
                    3,
                    "Updated: 01d4848202a3c7697ec037b02b4ee4e8".into(),
                    "Gregson".into()
                ),
                (4, "Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_update_where_local_set_explain() {
        assert_query_plan_expected!("test_cases/0004_update_where_local_set.json");
    }

    #[pg_test]
    fn test_fdw_update_where_local_cond() {
        setup_test("update_where_local_cond");

        let results = execute_query(
            r#"
            UPDATE "people" SET first_name = 'Updated: ' || first_name WHERE MD5(id::text) = MD5('3');

            SELECT * FROM "people";
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
                (3, "Updated: Gary".into(), "Gregson".into()),
                (4, "Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_update_where_local_cond_explain() {
        assert_query_plan_expected!("test_cases/0005_update_where_local_cond.json");
    }

    #[pg_test]
    fn test_fdw_delete_all_rows() {
        setup_test("delete_all_rows");

        let results = execute_query(
            r#"
            DELETE FROM "people";

            SELECT * FROM "people";
            "#,
            |i| {
                (
                    i["id"].value::<i64>().unwrap(),
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(results, vec![]);
    }

    #[pg_test]
    fn test_fdw_delete_all_rows_explain() {
        assert_query_plan_expected!("test_cases/0006_delete_all_rows.json");
    }

    #[pg_test]
    fn test_fdw_delete_where_local_cond() {
        setup_test("delete_where_local_cond");

        let results = execute_query(
            r#"
            DELETE FROM "people" WHERE MD5(id::text) = MD5('3');

            SELECT * FROM "people";
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
                (4, "Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_delete_where_local_cond_explain() {
        assert_query_plan_expected!("test_cases/0007_delete_where_local_cond.json");
    }

    #[pg_test]
    fn test_fdw_update_all_rows_remote_set() {
        setup_test("update_all_rows_remote_set");

        let results = execute_query(
            r#"
            UPDATE "people" SET first_name = 'Updated: ' || first_name;

            SELECT * FROM "people";
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
                (1, "Updated: Mary".into(), "Jane".into()),
                (2, "Updated: John".into(), "Smith".into()),
                (3, "Updated: Gary".into(), "Gregson".into()),
                (4, "Updated: Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_update_all_rows_remote_set_explain() {
        assert_query_plan_expected!("test_cases/0008_update_all_rows_remote_set.json");
    }

    #[pg_test]
    fn test_fdw_update_remote_cond() {
        setup_test("update_remote_cond");

        let results = execute_query(
            r#"
            UPDATE "people" SET first_name = 'Updated: ' || first_name WHERE id = 4;

            SELECT * FROM "people";
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
                (4, "Updated: Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_update_remote_cond_explain() {
        assert_query_plan_expected!("test_cases/0009_update_remote_cond.json");
    }

    #[pg_test]
    fn test_fdw_delete_remote_cond() {
        setup_test("delete_remote_cond");

        let results = execute_query(
            r#"
            DELETE FROM "people" WHERE id = 4;

            SELECT * FROM "people";
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
            ]
        );
    }

    #[pg_test]
    fn test_fdw_delete_remote_cond_explain() {
        assert_query_plan_expected!("test_cases/0010_delete_remote_cond.json");
    }

    #[pg_test]
    fn test_fdw_insert_row_with_missing_cols() {
        setup_test("insert_row_with_missing_cols");

        let results = execute_query(
            r#"
            INSERT INTO "people" (id, first_name) 
            VALUES (123, 'Barry');

            SELECT * FROM "people";
            "#,
            |i| {
                (
                    i["id"].value::<i64>().unwrap(),
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                (1, "Mary".into(), Some("Jane".into())),
                (2, "John".into(), Some("Smith".into())),
                (3, "Gary".into(), Some("Gregson".into())),
                (4, "Mary".into(), Some("Bennet".into())),
                (123, "Barry".into(), None),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_insert_row_with_missing_cols_explain() {
        assert_query_plan_expected!("test_cases/0011_insert_row_with_missing_cols.json");
    }

    #[pg_test]
    fn test_fdw_modify_test_before_modify_function_is_called_if_specified() {
        setup_test("scan_before_modify_cb");

        execute_query(
            r#"
            CREATE TABLE invocations 
            AS SELECT 0 as cnt;

            CREATE FUNCTION before_modify_cb() RETURNS VOID
                AS 'UPDATE invocations SET cnt = cnt + 1;'
                LANGUAGE SQL;

            ALTER TABLE people OPTIONS (ADD before_modify 'before_modify_cb');
            "#,
            |_| (),
        );

        // Should be triggered on INSERT/UPDATE/DELETE
        let rows = execute_query(
            r#"
            INSERT INTO "people" (id, first_name) VALUES (123, 'Barry');

            SELECT cnt FROM invocations;
            "#,
            |i| i["cnt"].value::<i64>().unwrap(),
        );

        assert_eq!(rows, vec![1]);

        let rows = execute_query(
            r#"
            UPDATE "people" SET first_name = 'test' WHERE id = 1;

            SELECT cnt FROM invocations;
            "#,
            |i| i["cnt"].value::<i64>().unwrap(),
        );

        assert_eq!(rows, vec![2]);

        let rows = execute_query(
            r#"
            DELETE FROM people WHERE id = 1;

            SELECT cnt FROM invocations;
            "#,
            |i| i["cnt"].value::<i64>().unwrap(),
        );

        assert_eq!(rows, vec![3]);
    }

    #[pg_test]
    fn test_fdw_modify_test_before_insert_function_is_called_if_specified() {
        setup_test("scan_before_insert_cb");

        execute_query(
            r#"
            CREATE TABLE invocations 
            AS SELECT 0 as cnt;

            CREATE FUNCTION before_modify_cb() RETURNS VOID
                AS 'UPDATE invocations SET cnt = cnt + 1;'
                LANGUAGE SQL;

            ALTER TABLE people OPTIONS (ADD before_modify 'before_modify_cb');
            "#,
            |_| (),
        );

        let rows = execute_query(
            r#"
            INSERT INTO "people" (id, first_name) VALUES (123, 'Barry');

            SELECT cnt FROM invocations;
            "#,
            |i| i["cnt"].value::<i64>().unwrap(),
        );

        assert_eq!(rows, vec![1]);
    }

    #[pg_test]
    fn test_fdw_modify_test_before_update_function_is_called_if_specified() {
        setup_test("scan_before_update_cb");

        execute_query(
            r#"
            CREATE TABLE invocations 
            AS SELECT 0 as cnt;

            CREATE FUNCTION before_modify_cb() RETURNS VOID
                AS 'UPDATE invocations SET cnt = cnt + 1;'
                LANGUAGE SQL;

            ALTER TABLE people OPTIONS (ADD before_modify 'before_modify_cb');
            "#,
            |_| (),
        );

        let rows = execute_query(
            r#"
            UPDATE "people" SET first_name = 'test' WHERE id = 1;

            SELECT cnt FROM invocations;
            "#,
            |i| i["cnt"].value::<i64>().unwrap(),
        );

        assert_eq!(rows, vec![1]);

    }

    #[pg_test]
    fn test_fdw_modify_test_before_delete_function_is_called_if_specified() {
        setup_test("scan_before_delete_cb");

        execute_query(
            r#"
            CREATE TABLE invocations 
            AS SELECT 0 as cnt;

            CREATE FUNCTION before_modify_cb() RETURNS VOID
                AS 'UPDATE invocations SET cnt = cnt + 1;'
                LANGUAGE SQL;

            ALTER TABLE people OPTIONS (ADD before_modify 'before_modify_cb');
            "#,
            |_| (),
        );

        let rows = execute_query(
            r#"
            DELETE FROM people WHERE id = 1;

            SELECT cnt FROM invocations;
            "#,
            |i| i["cnt"].value::<i64>().unwrap(),
        );

        assert_eq!(rows, vec![1]);
    }
}
