use pgx::*;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::{
        fs,
        panic::{RefUnwindSafe, UnwindSafe},
        path::PathBuf,
        thread,
        time::Duration,
    };

    use super::*;

    use crate::sqlil::test;
    use ansilo_connectors::{
        common::entity::{ConnectorEntityConfig, EntitySource},
        interface::{container::ConnectionPools, Connector},
        memory::{MemoryConnectionConfig, MemoryConnectionPool, MemoryConnector},
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

    fn create_memory_connection_pool() -> (ConnectorEntityConfig<()>, MemoryConnectionPool) {
        let mut conf = MemoryConnectionConfig::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::minimal(
            "people",
            EntityVersionConfig::minimal(
                "1.0",
                vec![
                    EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                    EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            (),
        ));

        conf.set_data(
            "people",
            "1.0",
            vec![
                vec![DataValue::from("Mary"), DataValue::from("Jane")],
                vec![DataValue::from("John"), DataValue::from("Smith")],
                vec![DataValue::from("Gary"), DataValue::from("Gregson")],
                vec![DataValue::from("Mary"), DataValue::from("Bennet")],
            ],
        );

        let pool = MemoryConnector::create_connection_pool(conf, &NodeConfig::default(), &entities)
            .unwrap();

        (entities, pool)
    }

    fn start_fdw_server(socket_path: impl Into<String>) -> FdwServer {
        let (entities, pool) = create_memory_connection_pool();
        let pool = ConnectionPools::Memory(pool, entities);
        let path = PathBuf::from(socket_path.into());
        fs::create_dir_all(path.parent().unwrap().clone()).unwrap();

        let server =
            FdwServer::start(path, [("memory".to_string(), pool)].into_iter().collect()).unwrap();
        thread::sleep(Duration::from_millis(10));

        server
    }

    fn setup_db(socket_path: impl Into<String>) {
        let socket_path = socket_path.into();
        Spi::execute(|mut client| {
            client.update(
                format!(
                    r#"
                DROP FOREIGN TABLE IF EXISTS "people:1.0";
                DROP SERVER IF EXISTS test_srv;
                CREATE SERVER test_srv FOREIGN DATA WRAPPER ansilo_fdw OPTIONS (
                    socket '{socket_path}',
                    data_source 'memory'
                );

                CREATE FOREIGN TABLE "people:1.0" (
                    first_name VARCHAR,
                    last_name VARCHAR
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
        start_fdw_server(sock_path.clone());
        setup_db(sock_path);
    }

    fn execute_query<F: Fn(SpiHeapTupleData) -> R, R: DeserializeOwned + Serialize + Clone>(
        query: impl Into<String>,
        f: F,
    ) -> Vec<R> {
        let query = query.into();
        let json = Spi::connect(|mut client| {
            let res = client
                .select(query.as_str(), None, None)
                .map(f)
                .collect::<Vec<R>>();
            let res = serde_json::to_string(&res).unwrap();

            Ok(Some(res))
        })
        .unwrap();

        serde_json::from_str(json.as_str()).unwrap()
    }

    fn explain_query(query: impl Into<String>) -> serde_json::Value {
        let query = query.into();
        let json = Spi::connect(|mut client| {
            let table = client
                .update(
                    &format!("EXPLAIN (format json, verbose true) {}", query.as_str()),
                    None,
                    None,
                )
                .first();
            Ok(Some(
                table
                    .get_one::<Json>()
                    .expect("failed to get json EXPLAIN result"),
            ))
        })
        .unwrap();

        json.0.as_array().take().unwrap().get(0).unwrap().clone()
    }

    macro_rules! assert_query_plan_expected {
        ($path:expr) => {
            setup_test(format!("query_plan/{}", $path));
            assert_query_plan_expected_fn(include_str!($path));
        };
    }

    fn assert_query_plan_expected_fn(spec_json: &str) {
        let json = serde_json::from_str::<serde_json::Value>(spec_json).unwrap();

        let query = json["SQL"].as_str().unwrap().to_string();
        let expected_plan = json["Expected"].clone();

        let actual_plan = explain_query(query);

        assert_json_include!(actual: actual_plan, expected: expected_plan)
    }

    #[pg_test]
    fn test_fdw_scan_select_all() {
        setup_test("scan_select_all");

        let results = execute_query(r#"SELECT * FROM "people:1.0""#, |i| {
            (
                i["first_name"].value::<String>().unwrap(),
                i["last_name"].value::<String>().unwrap(),
            )
        });

        assert_eq!(
            results,
            vec![
                ("Mary".into(), "Jane".into()),
                ("John".into(), "Smith".into()),
                ("Gary".into(), "Gregson".into()),
                ("Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_all_explain() {
        assert_query_plan_expected!("test_cases/0001_select_all.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_all_where_remote_cond() {
        setup_test("scan_select_all_remote_cond");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" WHERE first_name = 'Mary'"#,
            |i| {
                (
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                ("Mary".into(), "Jane".into()),
                ("Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_all_explain_where_remote_cond() {
        assert_query_plan_expected!("test_cases/0002_select_all_where_remote_cond.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_all_where_local_cond() {
        setup_test("scan_select_all_local_cond");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" WHERE MD5(first_name) = MD5('John')"#,
            |i| {
                (
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(results, vec![("John".into(), "Smith".into()),]);
    }

    #[pg_test]
    fn test_fdw_scan_select_all_explain_where_local_cond() {
        assert_query_plan_expected!("test_cases/0003_select_all_where_local_cond.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_count_all() {
        setup_test("scan_select_count_all");

        let results = execute_query(r#"SELECT COUNT(*) as count FROM "people:1.0""#, |i| {
            (i["count"].value::<i64>().unwrap(),)
        });

        assert_eq!(results, vec![(4,),]);
    }

    #[pg_test]
    fn test_fdw_scan_select_count_all_explain() {
        assert_query_plan_expected!("test_cases/0004_select_count_all.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_group_by_name() {
        setup_test("scan_select_group_by_name");

        let results = execute_query(
            r#"SELECT first_name FROM "people:1.0" GROUP BY first_name"#,
            |i| (i["first_name"].value::<String>().unwrap(),),
        );

        assert_eq!(
            results,
            vec![("Mary".into(),), ("John".into(),), ("Gary".into(),),]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_group_by_name_explain() {
        assert_query_plan_expected!("test_cases/0005_select_group_by_name.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_group_by_name_with_count() {
        setup_test("scan_select_group_by_name_with_count");

        let results = execute_query(
            r#"SELECT first_name, COUNT(*) as count FROM "people:1.0" GROUP BY first_name"#,
            |i| {
                (
                    i["first_name"].value::<String>().unwrap(),
                    i["count"].value::<i64>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![("Mary".into(), 2), ("John".into(), 1), ("Gary".into(), 1),]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_group_by_name_with_count_explain() {
        assert_query_plan_expected!("test_cases/0006_select_group_by_name_with_count.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_group_by_local() {
        setup_test("scan_select_group_by_local");

        let results = execute_query(
            r#"SELECT MD5(first_name) as hash FROM "people:1.0" GROUP BY MD5(first_name)"#,
            |i| (i["hash"].value::<String>().unwrap(),),
        );

        assert_eq!(
            results,
            vec![
                ("01d4848202a3c7697ec037b02b4ee4e8".into(),),
                ("61409aa1fd47d4a5332de23cbf59a36f".into(),),
                ("e39e74fb4e80ba656f773669ed50315a".into(),),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_group_by_local_explain() {
        assert_query_plan_expected!("test_cases/0007_select_group_by_local.json");
    }
}
