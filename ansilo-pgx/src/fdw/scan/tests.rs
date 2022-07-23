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
                    EntityAttributeConfig::minimal("id", DataType::UInt32),
                    EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                    EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            (),
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
            (),
        ));

        entities.add(EntitySource::minimal(
            "large",
            EntityVersionConfig::minimal(
                "1.0",
                vec![
                    EntityAttributeConfig::minimal("x", DataType::UInt32),
                ],
                EntitySourceConfig::minimal(""),
            ),
            (),
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

    #[pg_test]
    fn test_fdw_scan_select_order_by_single_col() {
        setup_test("scan_select_order_by_single_col");

        let results = execute_query(r#"SELECT * FROM "people:1.0" ORDER BY first_name"#, |i| {
            (
                i["first_name"].value::<String>().unwrap(),
                i["last_name"].value::<String>().unwrap(),
            )
        });

        assert_eq!(
            results,
            vec![
                ("Gary".into(), "Gregson".into()),
                ("John".into(), "Smith".into()),
                ("Mary".into(), "Jane".into()),
                ("Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_all_order_by_single_col_explain() {
        assert_query_plan_expected!("test_cases/0008_select_order_by_single_col.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_order_by_single_col_desc() {
        setup_test("scan_select_order_by_single_col_desc");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" ORDER BY first_name DESC"#,
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
                ("John".into(), "Smith".into()),
                ("Gary".into(), "Gregson".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_all_order_by_single_col_desc_explain() {
        assert_query_plan_expected!("test_cases/0009_select_order_by_single_col_desc.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_order_by_multiple_cols() {
        setup_test("scan_select_order_by_multiple_cols");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" ORDER BY first_name, last_name DESC"#,
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
                ("Gary".into(), "Gregson".into()),
                ("John".into(), "Smith".into()),
                ("Mary".into(), "Jane".into()),
                ("Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_all_order_by_multiple_cols_explain() {
        assert_query_plan_expected!("test_cases/0010_select_order_by_multiple_cols.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_order_by_local() {
        setup_test("scan_select_order_by_local");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" ORDER BY MD5(first_name), last_name"#,
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
                ("Gary".into(), "Gregson".into()),
                ("John".into(), "Smith".into()),
                ("Mary".into(), "Bennet".into()),
                ("Mary".into(), "Jane".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_all_order_by_local_explain() {
        assert_query_plan_expected!("test_cases/0011_select_order_by_local.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_limit() {
        setup_test("scan_select_limit");

        let results = execute_query(r#"SELECT * FROM "people:1.0" LIMIT 2"#, |i| {
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
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_limit_explain() {
        assert_query_plan_expected!("test_cases/0012_select_limit.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_offset() {
        setup_test("scan_select_offset");

        let results = execute_query(r#"SELECT * FROM "people:1.0" OFFSET 2"#, |i| {
            (
                i["first_name"].value::<String>().unwrap(),
                i["last_name"].value::<String>().unwrap(),
            )
        });

        assert_eq!(
            results,
            vec![
                ("Gary".into(), "Gregson".into()),
                ("Mary".into(), "Bennet".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_offset_explain() {
        assert_query_plan_expected!("test_cases/0013_select_offset.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_limit_offset() {
        setup_test("scan_select_limit_offset");

        let results = execute_query(r#"SELECT * FROM "people:1.0" LIMIT 2 OFFSET 1"#, |i| {
            (
                i["first_name"].value::<String>().unwrap(),
                i["last_name"].value::<String>().unwrap(),
            )
        });

        assert_eq!(
            results,
            vec![
                ("John".into(), "Smith".into()),
                ("Gary".into(), "Gregson".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_offset_limit_explain() {
        assert_query_plan_expected!("test_cases/0014_select_limit_offset.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_single_col() {
        assert_query_plan_expected!("test_cases/0015_select_single_col.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_inner_join() {
        setup_test("scan_select_inner_join");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" p INNER JOIN "pets:1.0" pets ON pets.owner_id = p.id"#,
            |i| {
                (
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                    i["pet_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                ("Mary".into(), "Jane".into(), "Pepper".into()),
                ("Mary".into(), "Jane".into(), "Salt".into()),
                ("Gary".into(), "Gregson".into(), "Relish".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_inner_join_explain() {
        assert_query_plan_expected!("test_cases/0016_select_inner_join.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_left_join() {
        setup_test("scan_select_left_join");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" p LEFT JOIN "pets:1.0" pets ON pets.owner_id = p.id"#,
            |i| {
                (
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                    i["pet_name"].value::<String>(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                ("Mary".into(), "Jane".into(), Some("Pepper".into())),
                ("Mary".into(), "Jane".into(), Some("Salt".into())),
                ("Gary".into(), "Gregson".into(), Some("Relish".into())),
                ("John".into(), "Smith".into(), None),
                ("Mary".into(), "Bennet".into(), None),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_left_join_explain() {
        assert_query_plan_expected!("test_cases/0017_select_left_join.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_right_join() {
        setup_test("scan_select_right_join");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" p RIGHT JOIN "pets:1.0" pets ON pets.owner_id = p.id"#,
            |i| {
                (
                    i["first_name"].value::<String>(),
                    i["last_name"].value::<String>(),
                    i["pet_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                (Some("Mary".into()), Some("Jane".into()), "Pepper".into()),
                (Some("Mary".into()), Some("Jane".into()), "Salt".into()),
                (Some("Gary".into()), Some("Gregson".into()), "Relish".into()),
                (None, None, "Luna".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_right_join_explain() {
        assert_query_plan_expected!("test_cases/0018_select_right_join.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_full_join() {
        setup_test("scan_select_full_join");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" p FULL JOIN "pets:1.0" pets ON pets.owner_id = p.id"#,
            |i| {
                (
                    i["first_name"].value::<String>(),
                    i["last_name"].value::<String>(),
                    i["pet_name"].value::<String>(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                (
                    Some("Mary".into()),
                    Some("Jane".into()),
                    Some("Pepper".into())
                ),
                (
                    Some("Mary".into()),
                    Some("Jane".into()),
                    Some("Salt".into())
                ),
                (
                    Some("Gary".into()),
                    Some("Gregson".into()),
                    Some("Relish".into())
                ),
                (Some("John".into()), Some("Smith".into()), None),
                (Some("Mary".into()), Some("Bennet".into()), None),
                (None, None, Some("Luna".into())),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_full_join_explain() {
        assert_query_plan_expected!("test_cases/0019_select_full_join.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_inner_join_local() {
        setup_test("scan_select_inner_join_local");

        let results = execute_query(
            r#"SELECT * FROM "people:1.0" p INNER JOIN "pets:1.0" pets ON MD5(pets.owner_id::text) = MD5(p.id::text)"#,
            |i| {
                (
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                    i["pet_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                ("Mary".into(), "Jane".into(), "Salt".into()),
                ("Mary".into(), "Jane".into(), "Pepper".into()),
                ("Gary".into(), "Gregson".into(), "Relish".into()),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_inner_join_local_explain() {
        assert_query_plan_expected!("test_cases/0020_select_inner_join_local.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_join_where_group_order_limit() {
        setup_test("scan_select_join_where_group_order_limit");

        let results = execute_query(
            r#"
            SELECT p.first_name, p.last_name, COUNT(*) as pets 
            FROM "people:1.0" p 
            INNER JOIN "pets:1.0" pets ON pets.owner_id = p.id
            WHERE pets.pet_name != 'XXX'
            GROUP BY p.first_name, p.last_name
            ORDER BY pets DESC
            LIMIT 3
            "#,
            |i| {
                (
                    i["first_name"].value::<String>().unwrap(),
                    i["last_name"].value::<String>().unwrap(),
                    i["pets"].value::<i32>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                ("Mary".into(), "Jane".into(), 2),
                ("Gary".into(), "Gregson".into(), 1),
            ]
        );
    }

    #[pg_test]
    fn test_fdw_scan_select_join_where_group_order_limit_explain() {
        assert_query_plan_expected!("test_cases/0021_select_join_where_group_order_limit.json");
    }

    #[pg_test]
    fn test_fdw_scan_select_paramterized_sub_query() {
        setup_test("scan_select_paramterized_sub_query");

        let results = execute_query(
            r#"
            SELECT 
                (SELECT first_name FROM "people:1.0" WHERE id = x) as first_name
            FROM generate_series(1, 2)
            "#,
            |i| {
                (
                    i["first_name"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                ("Mary".into()),
                ("John".into()),
            ]
        );
    }
}
