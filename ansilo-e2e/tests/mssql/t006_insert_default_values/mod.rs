use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_identity() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mssql::start_mssql();
    let mut mssql =
        ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t006__identity" 
            (data) VALUES ('value'), ('another')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on mssql end
    let results = mssql
        .execute("SELECT * FROM t006__identity", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![
            vec![
                ("id".to_string(), DataValue::Int32(1)),
                ("data".to_string(), DataValue::Utf8String("value".into())),
            ]
            .into_iter()
            .collect(),
            vec![
                ("id".to_string(), DataValue::Int32(2)),
                ("data".to_string(), DataValue::Utf8String("another".into())),
            ]
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
                        r#"INSERT INTO [dbo].[t006__identity] "#,
                        r#"([data]) VALUES (?), (?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setNString, value=value]".into(),
                        "LoggedParam [index=2, method=setNString, value=another]".into()
                    ],
                    Some(
                        [("affected".into(), "Some(2)".into())]
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
fn test_default() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mssql::start_mssql();
    let mut mssql =
        ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
            INSERT INTO "t006__default" 
            (data) VALUES ('first');

            INSERT INTO "t006__default" 
            (id, data) VALUES (123, 'second');
        "#,
        )
        .unwrap();

    // Check data received on mssql end
    let results = mssql
        .execute("SELECT * FROM t006__default", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![
            vec![
                ("id".to_string(), DataValue::Int32(-1)),
                ("data".to_string(), DataValue::Utf8String("first".into())),
            ]
            .into_iter()
            .collect(),
            vec![
                ("id".to_string(), DataValue::Int32(123)),
                ("data".to_string(), DataValue::Utf8String("second".into())),
            ]
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
                        r#"INSERT INTO [dbo].[t006__default] "#,
                        r#"([data]) VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=first]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            (
                "mssql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO [dbo].[t006__default] "#,
                        r#"([id], [data]) VALUES (?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setInt, value=123]".into(),
                        "LoggedParam [index=2, method=setNString, value=second]".into()
                    ],
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
