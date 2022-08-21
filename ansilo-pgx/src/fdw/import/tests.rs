use pgx::*;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::{fs, iter, ptr, thread};

    use super::*;

    use crate::{
        fdw::{
            import::import_foreign_schema,
            test::{
                query::{execute_query, explain_query_verbose},
                server::start_fdw_server,
            },
        },
        sqlil::test,
        util::string::{parse_to_owned_utf8_string, to_pg_cstr},
    };
    use ansilo_connectors_all::{ConnectionPools, ConnectorEntityConfigs};
    use ansilo_connectors_base::{
        common::entity::{ConnectorEntityConfig, EntitySource},
        interface::Connector,
    };
    use ansilo_connectors_memory::{
        MemoryConnector, MemoryConnectorEntitySourceConfig, MemoryDatabase,
    };
    use ansilo_core::{
        config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
        data::{uuid::Uuid, DataType, DataValue, DecimalOptions, StringOptions},
        sqlil,
    };
    use ansilo_pg::fdw::proto::OperationCost;

    fn create_memory_connection_pool(
        confs: Vec<EntityConfig>,
    ) -> (ConnectionPools, ConnectorEntityConfigs) {
        let mut conf = MemoryDatabase::new();
        let mut entities = ConnectorEntityConfig::new();

        for entity in confs.into_iter() {
            entities.add(EntitySource::new(
                entity,
                MemoryConnectorEntitySourceConfig::new(Some(OperationCost::new(
                    None, None, None, None,
                ))),
            ));
        }

        let pool = MemoryConnector::create_connection_pool(conf, &NodeConfig::default(), &entities)
            .unwrap();

        (
            ConnectionPools::Memory(pool),
            ConnectorEntityConfigs::Memory(entities),
        )
    }

    fn run_import_foreign_schema(entities: Vec<EntityConfig>) -> Vec<String> {
        let sock_path = format!("/tmp/ansilo/fdw_server/{}", Uuid::new_v4().to_string());
        start_fdw_server(create_memory_connection_pool(entities), sock_path.clone());
        Spi::execute(|mut client| {
            client.update(
                format!(
                    r#"
                DROP SERVER IF EXISTS test_srv CASCADE;
                CREATE SERVER test_srv FOREIGN DATA WRAPPER ansilo_fdw OPTIONS (
                    socket '{sock_path}',
                    data_source 'memory'
                );
                 "#
                )
                .as_str(),
                None,
                None,
            );
        });

        unsafe {
            let server = pg_sys::GetForeignServerByName(to_pg_cstr("test_srv").unwrap(), false);
            let import_stmts = import_foreign_schema(ptr::null_mut(), (*server).serverid);
            let import_stmts = PgList::<i8>::from_pg(import_stmts);

            import_stmts
                .iter_ptr()
                .map(|stmt| parse_to_owned_utf8_string(stmt).unwrap())
                .collect::<Vec<_>>()
        }
    }

    #[pg_test]
    fn test_fdw_import_table_integer_types() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "tab",
            vec![
                EntityAttributeConfig::minimal("int8", DataType::Int8),
                EntityAttributeConfig::minimal("uint8", DataType::UInt8),
                EntityAttributeConfig::minimal("int16", DataType::Int16),
                EntityAttributeConfig::minimal("uint16", DataType::UInt16),
                EntityAttributeConfig::minimal("int32", DataType::Int32),
                EntityAttributeConfig::minimal("uint32", DataType::UInt32),
                EntityAttributeConfig::minimal("int64", DataType::Int64),
                EntityAttributeConfig::minimal("uint64", DataType::UInt64),
            ],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE tab (
    int8 SMALLINT NOT NULL,
    uint8 SMALLINT NOT NULL,
    int16 SMALLINT NOT NULL,
    uint16 INTEGER NOT NULL,
    int32 INTEGER NOT NULL,
    uint32 BIGINT NOT NULL,
    int64 BIGINT NOT NULL,
    uint64 NUMERIC NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }

    #[pg_test]
    fn test_fdw_import_table_char_types() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "tab",
            vec![
                EntityAttributeConfig::minimal(
                    "str",
                    DataType::Utf8String(StringOptions::default()),
                ),
                EntityAttributeConfig::minimal(
                    "str_max_len",
                    DataType::Utf8String(StringOptions::new(Some(255))),
                ),
            ],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE tab (
    str TEXT NOT NULL,
    str_max_len VARCHAR(255) NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }

    #[pg_test]
    fn test_fdw_import_table_byte_type() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "tab",
            vec![EntityAttributeConfig::minimal("binary", DataType::Binary)],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE tab (
    \"binary\" BYTEA NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }

    #[pg_test]
    fn test_fdw_import_table_boolean_type() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "tab",
            vec![EntityAttributeConfig::minimal("bool", DataType::Boolean)],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE tab (
    bool BOOLEAN NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }

    #[pg_test]
    fn test_fdw_import_table_numeric_types() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "tab",
            vec![
                EntityAttributeConfig::minimal("float32", DataType::Float32),
                EntityAttributeConfig::minimal("float64", DataType::Float64),
                EntityAttributeConfig::minimal(
                    "decimal",
                    DataType::Decimal(DecimalOptions::default()),
                ),
            ],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE tab (
    float32 REAL NOT NULL,
    float64 DOUBLE PRECISION NOT NULL,
    \"decimal\" NUMERIC NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }

    #[pg_test]
    fn test_fdw_import_table_date_time_types() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "tab",
            vec![
                EntityAttributeConfig::minimal("date", DataType::Date),
                EntityAttributeConfig::minimal("time", DataType::Time),
                EntityAttributeConfig::minimal("date_time", DataType::DateTime),
                EntityAttributeConfig::minimal("date_time_tz", DataType::DateTimeWithTZ),
            ],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE tab (
    date DATE NOT NULL,
    \"time\" TIME NOT NULL,
    date_time TIMESTAMP NOT NULL,
    date_time_tz TIMESTAMPTZ NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }

    #[pg_test]
    fn test_fdw_import_table_json_types() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "tab",
            vec![EntityAttributeConfig::minimal("json", DataType::JSON)],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE tab (
    json JSON NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }

    #[pg_test]
    fn test_fdw_import_table_uuid_types() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "tab",
            vec![EntityAttributeConfig::minimal("uuid", DataType::Uuid)],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE tab (
    uuid UUID NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }

    #[pg_test]
    fn test_fdw_import_table_quoted_table_name() {
        let stmts = run_import_foreign_schema(vec![EntityConfig::minimal(
            "some:name",
            vec![EntityAttributeConfig::minimal("foo:bar", DataType::Int8)],
            EntitySourceConfig::minimal(""),
        )]);

        assert_eq!(
            stmts,
            vec![
                "CREATE FOREIGN TABLE \"some:name\" (
    \"foo:bar\" SMALLINT NOT NULL
)
SERVER test_srv
OPTIONS (
    __config E'null\n'
)"
            ]
        )
    }
}
