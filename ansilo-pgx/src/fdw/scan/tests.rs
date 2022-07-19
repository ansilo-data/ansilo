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
    use ansilo_core::common::data::*;
    use ansilo_core::{
        common::data::{DataType, DataValue},
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig, NodeConfig},
        sqlil,
    };
    use ansilo_pg::fdw::server::FdwServer;
    use serde::{de::DeserializeOwned, Serialize};

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

    fn setup_test(test_name: &'static str) {
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

    fn explain_query(query: impl Into<String>) -> String {
        Spi::explain(query.into().as_str()).0.to_string()
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
            ]
        );
    }
}
