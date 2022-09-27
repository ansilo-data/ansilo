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
    use ansilo_pg::fdw::server::FdwServer;
    use assert_json_diff::*;
    use itertools::Itertools;
    use serde::{de::DeserializeOwned, Serialize};
    use serde_json::json;

    fn create_memory_connection_pool() -> (ConnectionPools, ConnectorEntityConfigs) {
        let mut conf = MemoryDatabase::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::new(
            EntityConfig::minimal(
                "dummy",
                vec![EntityAttributeConfig::minimal("x", DataType::UInt32)],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::new(None),
        ));

        conf.set_data("dummy", vec![vec![DataValue::UInt32(123)]]);

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
                    data_source 'mock'
                );

                CREATE FOREIGN TABLE "dummy" (
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

        let results = explain_query(r#"SELECT * FROM "dummy""#);

        assert_eq!(
            results["Plan"]["Remote Query"]["Select"]
                .as_object()
                .unwrap()
                .keys()
                .map(|i| i.as_str())
                .sorted()
                .collect_vec(),
            [
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
            .into_iter()
            .sorted()
            .collect_vec()
        );
    }

    #[pg_test]
    fn test_fdw_explain_verbose_select() {
        setup_test("explain_verbose_select");

        let results = explain_query_verbose(r#"SELECT * FROM "dummy""#);

        assert_eq!(
            results["Plan"]["Remote Query"]
                .as_object()
                .unwrap()
                .keys()
                .map(|i| i.as_str())
                .sorted()
                .collect_vec(),
            ["params", "query"].into_iter().sorted().collect_vec()
        );
    }
}
