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
        fdw::test::{
            query::{explain_query, explain_query_verbose},
            server::start_fdw_server,
        },
        sqlil::test,
    };
    use ansilo_connectors::{
        common::entity::{ConnectorEntityConfig, EntitySource},
        interface::{container::ConnectionPools, Connector, OperationCost},
        memory::{
            MemoryDatabase, MemoryConnectionPool, MemoryConnector,
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
        let mut conf = MemoryDatabase::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::minimal(
            "dummy",
            EntityVersionConfig::minimal(
                "1.0",
                vec![EntityAttributeConfig::minimal("x", DataType::UInt32)],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::new(None),
        ));

        conf.set_data("dummy", "1.0", vec![vec![DataValue::UInt32(123)]]);

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

                CREATE FOREIGN TABLE "dummy:1.0" (
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
    fn test_fdw_explain_select() {
        setup_test("explain_select");

        let results = explain_query(r#"SELECT * FROM "dummy:1.0""#);

        assert_eq!(
            results["Plan"]["Remote Query"]["Select"]
                .as_object()
                .unwrap()
                .keys()
                .map(|i| i.as_str())
                .collect::<Vec<&str>>(),
            vec![
                "cols",
                "from",
                "group_bys",
                "joins",
                "order_bys",
                "row_limit",
                "row_lock",
                "row_skip",
                "where"
            ]
        );
    }

    #[pg_test]
    fn test_fdw_explain_verbose_select() {
        setup_test("explain_verbose_select");

        let results = explain_query_verbose(r#"SELECT * FROM "dummy:1.0""#);

        assert_eq!(
            results["Plan"]["Remote Query"]
                .as_object()
                .unwrap()
                .keys()
                .map(|i| i.as_str())
                .collect::<Vec<&str>>(),
            vec!["params", "query"]
        );
    }
}
