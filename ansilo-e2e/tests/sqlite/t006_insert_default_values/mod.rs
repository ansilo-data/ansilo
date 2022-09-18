use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_auto_increment() {
    ansilo_logging::init_for_tests();
    let (mut sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .execute(
            r#"
            INSERT INTO "t006__auto_increment" 
            (data) VALUES ('value'), ('another')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on sqlite end
    let results = sqlite
        .execute("SELECT * FROM t006__auto_increment", vec![])
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
                ("id".to_string(), DataValue::Int64(1)),
                ("data".to_string(), DataValue::Utf8String("value".into())),
            ]
            .into_iter()
            .collect(),
            vec![
                ("id".to_string(), DataValue::Int64(2)),
                ("data".to_string(), DataValue::Utf8String("another".into())),
            ]
            .into_iter()
            .collect(),
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("sqlite".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "sqlite".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "t006__auto_increment" "#,
                        r#"("data") VALUES (?1), (?2)"#
                    ]
                    .join(""),
                    vec![
                        "value=Utf8String(\"value\")".into(),
                        "value=Utf8String(\"another\")".into()
                    ],
                    Some(
                        [("affected".into(), "Some(2)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("sqlite".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

#[test]
#[serial]
fn test_default() {
    ansilo_logging::init_for_tests();
    let (mut sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

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

    // Check data received on sqlite end
    let results = sqlite
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
                ("id".to_string(), DataValue::Int64(-1)),
                ("data".to_string(), DataValue::Utf8String("first".into())),
            ]
            .into_iter()
            .collect(),
            vec![
                ("id".to_string(), DataValue::Int64(123)),
                ("data".to_string(), DataValue::Utf8String("second".into())),
            ]
            .into_iter()
            .collect(),
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("sqlite".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "sqlite".to_string(),
                LoggedQuery::new(
                    [r#"INSERT INTO "t006__default" "#, r#"("data") VALUES (?1)"#].join(""),
                    vec!["value=Utf8String(\"first\")".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            (
                "sqlite".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "t006__default" "#,
                        r#"("id", "data") VALUES (?1, ?2)"#
                    ]
                    .join(""),
                    vec![
                        "value=Int64(123)".into(),
                        "value=Utf8String(\"second\")".into()
                    ],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("sqlite".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}
