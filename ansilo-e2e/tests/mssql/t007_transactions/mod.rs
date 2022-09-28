use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_transaction_commit() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mssql::start_mssql();
    let mut mssql =
        ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO "t007__test_tab" (data) VALUES ('value');
        COMMIT;
        "#,
        )
        .unwrap();

    // Check data received on mssql end
    let results = mssql
        .execute("SELECT * FROM t007__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![
            vec![("data".to_string(), DataValue::Utf8String("value".into()))]
                .into_iter()
                .collect(),
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mssql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mssql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO [dbo].[t007__test_tab] "#,
                        r#"([data]) VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mssql".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

#[test]
#[serial]
fn test_transaction_rollback() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mssql::start_mssql();
    let mut mssql =
        ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO "t007__test_tab" (data) VALUES ('value');
        ROLLBACK;
        "#,
        )
        .unwrap();

    // Check rolled back on mssql side
    let results = mssql
        .execute("SELECT * FROM t007__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(results, vec![]);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mssql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mssql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO [dbo].[t007__test_tab] "#,
                        r#"([data]) VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mssql".to_string(), LoggedQuery::new_query("ROLLBACK")),
        ]
    );
}

#[test]
#[serial]
fn test_transaction_rollback_due_to_error() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mssql::start_mssql();
    let mut mssql =
        ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;

        INSERT INTO "t007__test_tab" (data) VALUES ('value');

        DO $$BEGIN
            RAISE EXCEPTION "An error occurred!";
        END$$;
        "#,
        )
        .unwrap_err();

    // After the error the rollback occurs asynchronously from this thread
    // drop the client to ensure we wait for the connection to finish
    // processing and the rollback to be issued
    drop(client);

    // Check rolled back on mssql side
    let results = mssql
        .execute("SELECT * FROM t007__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(results, vec![]);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mssql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mssql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO [dbo].[t007__test_tab] "#,
                        r#"([data]) VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mssql".to_string(), LoggedQuery::new_query("ROLLBACK")),
        ]
    );
}
