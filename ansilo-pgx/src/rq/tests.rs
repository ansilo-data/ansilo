use pgx::*;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {

    use ansilo_connectors_all::{ConnectionPools, ConnectorEntityConfigs, SqliteConnector};
    use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::Connector};
    use ansilo_connectors_native_sqlite::SqliteConnectionConfig;
    use ansilo_core::config::NodeConfig;

    use crate::fdw::test::{query::execute_query, server::start_fdw_server};

    use super::*;

    fn create_sqlite_connection_pool() -> (ConnectionPools, ConnectorEntityConfigs) {
        let pool = <SqliteConnector as Connector>::create_connection_pool(
            SqliteConnectionConfig {
                path: ":memory:".into(),
                extensions: vec![],
            },
            &NodeConfig::default(),
            &ConnectorEntityConfig::new(),
        )
        .unwrap();

        (
            ConnectionPools::NativeSqlite(pool),
            ConnectorEntityConfigs::NativeSqlite(ConnectorEntityConfig::new()),
        )
    }

    fn setup_db(socket_path: impl Into<String>) {
        let socket_path = socket_path.into();
        Spi::execute(|mut client| {
            client.update(
                format!(
                    r#"
                DROP SERVER IF EXISTS sqlite_srv CASCADE;
                CREATE SERVER sqlite_srv FOREIGN DATA WRAPPER ansilo_fdw OPTIONS (
                    socket '{socket_path}',
                    data_source 'mock'
                );

                IMPORT FOREIGN SCHEMA sqlite 
                FROM SERVER sqlite_srv INTO public;
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
        let sock_path = format!("/tmp/ansilo/rq_fdw_server/{test_name}");
        start_fdw_server(create_sqlite_connection_pool(), sock_path.clone());
        setup_db(sock_path);
    }

    #[pg_test]
    fn test_remote_query_select_int() {
        setup_test("rq_select_int");

        let results = execute_query(
            r#"SELECT * FROM remote_query('sqlite_srv', 'SELECT 1') AS t(col INT)"#,
            |i| (i["col"].value::<i32>().unwrap(),),
        );

        assert_eq!(results, vec![(1,)]);
    }

    #[pg_test]
    fn test_remote_query_select_varchar() {
        setup_test("rq_select_varchar");

        let results = execute_query(
            r#"SELECT * FROM remote_query('sqlite_srv', 'SELECT ''abc123''') AS t(col TEXT)"#,
            |i| (i["col"].value::<String>().unwrap(),),
        );

        assert_eq!(results, vec![("abc123".into(),)]);
    }

    #[pg_test]
    fn test_remote_query_select_null() {
        setup_test("rq_select_null");

        let results = execute_query(
            r#"SELECT * FROM remote_query('sqlite_srv', 'SELECT NULL') AS t(col INT)"#,
            |i| (i["col"].value::<i32>(),),
        );

        assert_eq!(results, vec![(None,)]);
    }

    #[pg_test]
    fn test_remote_query_select_bool() {
        setup_test("rq_select_bool");

        let results = execute_query(
            r#"SELECT * FROM remote_query('sqlite_srv', 'SELECT TRUE') AS t(col BOOLEAN)"#,
            |i| (i["col"].value::<bool>().unwrap(),),
        );

        assert_eq!(results, vec![(true,)]);
    }

    #[pg_test]
    fn test_remote_query_select_numeric() {
        setup_test("rq_select_numeric");

        let results = execute_query(
            r#"SELECT * FROM remote_query('sqlite_srv', 'SELECT 123.456') AS t(col DECIMAL(6, 3))"#,
            |i| (i["col"].value::<pgx::Numeric>().unwrap().0,),
        );

        assert_eq!(results, vec![("123.456".into(),)]);
    }

    #[pg_test]
    fn test_remote_query_select_float8() {
        setup_test("rq_select_float8");

        let results = execute_query(
            r#"SELECT * FROM remote_query('sqlite_srv', 'SELECT 123.456') AS t(col FLOAT8)"#,
            |i| (i["col"].value::<f64>().unwrap(),),
        );

        assert_eq!(results, vec![(123.456f64,)]);
    }

    #[pg_test]
    fn test_remote_query_select_json() {
        setup_test("rq_select_json");

        let results = execute_query(
            r#"SELECT * FROM remote_query('sqlite_srv', 'SELECT ''{"foo": "bar"}''') AS t(col JSON)"#,
            |i| (i["col"].value::<pgx::Json>().unwrap().0,),
        );

        assert_eq!(results, vec![(serde_json::json!({"foo": "bar"}),)]);
    }

    #[pg_test]
    fn test_remote_query_select_timestamp() {
        setup_test("rq_select_timestamp");

        let results = execute_query(
            r#"SELECT * FROM remote_query('sqlite_srv', 'SELECT ''2010-09-08T01:02:03''') AS t(col TIMESTAMP)"#,
            |i| {
                ({
                    let ts: time::PrimitiveDateTime = i["col"]
                        .value::<pgx::Timestamp>()
                        .unwrap()
                        .try_into()
                        .unwrap();

                    ts.to_string()
                },)
            },
        );

        assert_eq!(results, vec![("2010-09-08 1:02:03.0".into(),)]);
    }

    #[pg_test]
    fn test_remote_query_select_1000_rows_generate_series() {
        setup_test("rq_select_1000_rows_generate_series");

        let results = execute_query(
            r#"
            SELECT * FROM 
            remote_query(
                'sqlite_srv',
                'WITH RECURSIVE generate_series AS (
                    SELECT 1 as value
                    UNION ALL
                    SELECT value+1 FROM generate_series
                     WHERE value+1<=1000
                  ) SELECT value FROM generate_series'
            ) AS t(col INT)
            "#,
            |i| i["col"].value::<i32>().unwrap(),
        );

        assert_eq!(results, (1..=1000).into_iter().collect::<Vec<_>>());
    }

    #[pg_test]
    fn test_remote_query_select_multiple_cols_and_rows() {
        setup_test("rq_select_multiple_cols_and_rows");

        let results = execute_query(
            r#"
            SELECT * FROM 
            remote_query(
                'sqlite_srv',
                'SELECT 1, ''abc'', NULL
                UNION ALL
                SELECT 2, ''def'', TRUE
                UNION ALL
                SELECT NULL, ''ghi'', FALSE'
            ) AS t(c1 INT, c2 TEXT, c3 BOOLEAN)
            "#,
            |i| {
                (
                    i["c1"].value::<i32>(),
                    i["c2"].value::<String>(),
                    i["c3"].value::<bool>(),
                )
            },
        );

        assert_eq!(
            results,
            vec![
                (Some(1), Some("abc".into()), None),
                (Some(2), Some("def".into()), Some(true)),
                (None, Some("ghi".into()), Some(false)),
            ]
        );
    }

    #[pg_test]
    fn test_remote_query_select_text_query_param() {
        setup_test("rq_select_text_query_param");

        let results = execute_query(
            r#"
            SELECT * FROM 
            remote_query('sqlite_srv', 'SELECT ?', 'test-param') AS t(col TEXT)
            "#,
            |i| i["col"].value::<String>().unwrap(),
        );

        assert_eq!(results, vec!["test-param".to_string()]);
    }

    #[pg_test]
    fn test_remote_execute_create_table() {
        setup_test("rq_exec_create_table");

        execute_query(
            r#"
            SELECT remote_execute('sqlite_srv', 'CREATE TABLE test (col TEXT)')
            "#,
            |_| (),
        );

        assert_eq!(crate::rq::get_prepared_queries_count(), 1);

        let results = execute_query(
            r#"
            SELECT * FROM remote_query('sqlite_srv', 'SELECT * FROM test') AS t(col TEXT)
            "#,
            |i| i["col"].value::<String>().unwrap(),
        );

        assert_eq!(results, Vec::<String>::new());
        assert_eq!(crate::rq::get_prepared_queries_count(), 2);

        execute_query(
            r#"
            SELECT remote_execute('sqlite_srv', 'INSERT INTO test VALUES (?)', 'abc')
            "#,
            |_| (),
        );

        let results = execute_query(
            r#"
            SELECT * FROM remote_query('sqlite_srv', 'SELECT * FROM test') AS t(col TEXT)
            "#,
            |i| i["col"].value::<String>().unwrap(),
        );

        assert_eq!(results, vec!["abc".to_string()]);
        assert_eq!(crate::rq::get_prepared_queries_count(), 3);
    }

    #[pg_test]
    fn test_remote_execute_insert_from_pg_select() {
        setup_test("rq_exec_insert_from_pg_select");

        execute_query(
            r#"
            SELECT remote_execute('sqlite_srv', 'CREATE TABLE test (id INT, data TEXT)')
            "#,
            |_| (),
        );

        assert_eq!(crate::rq::get_prepared_queries_count(), 1);

        execute_query(
            r#"
            SELECT remote_execute('sqlite_srv', 'INSERT INTO test VALUES (?, ?)', x, 'str-' || x)
            FROM generate_series(1, 1000) x;
            "#,
            |_| (),
        );

        assert_eq!(crate::rq::get_prepared_queries_count(), 2);

        let results = execute_query(
            r#"
            SELECT * FROM remote_query('sqlite_srv', 'SELECT * FROM test') AS t(id INT, data TEXT)
            "#,
            |i| {
                (
                    i["id"].value::<i32>().unwrap(),
                    i["data"].value::<String>().unwrap(),
                )
            },
        );

        assert_eq!(
            results,
            (1..=1000)
                .into_iter()
                .map(|x| (x, format!("str-{x}")))
                .collect::<Vec<_>>()
        );
        // Should reuse the same prepared query for all rows
        assert_eq!(crate::rq::get_prepared_queries_count(), 3);
    }

    #[pg_test]
    fn test_remote_query_select_column_count_mismatch() {
        setup_test("rq_select_column_count_mismatch");

        std::panic::catch_unwind(|| {
            Spi::connect(|client| {
                client.select(
                    r#"
                SELECT * FROM 
                remote_query(
                    'sqlite_srv',
                    'SELECT 1, ''abc'', NULL'
                ) AS t(c1 INT, c2 TEXT)
                "#,
                    None,
                    None,
                );
                Ok(None::<()>)
            });
        })
        .unwrap_err();
    }
}
