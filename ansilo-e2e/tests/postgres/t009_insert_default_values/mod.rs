use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_serial() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::postgres::start_postgres();
    let mut postgres = ansilo_e2e::postgres::init_postgres_sql(
        &containers,
        current_dir!().join("postgres-sql/*.sql"),
    );

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t009__serial" 
            (data) VALUES ('value'), ('another')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on postgres end
    let results = postgres
        .execute("SELECT * FROM t009__serial", vec![])
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
            ("postgres".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "postgres".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "public"."t009__serial" "#,
                        r#"("data") VALUES ($1), ($2)"#
                    ]
                    .join(""),
                    vec![
                        "value=Utf8String(\"value\") type=varchar".into(),
                        "value=Utf8String(\"another\") type=varchar".into(),
                    ],
                    Some(
                        [("affected".into(), "Some(2)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("postgres".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

#[test]
#[serial]
fn test_default() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::postgres::start_postgres();
    let mut postgres = ansilo_e2e::postgres::init_postgres_sql(
        &containers,
        current_dir!().join("postgres-sql/*.sql"),
    );

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
            INSERT INTO "t009__default" 
            (data) VALUES ('first');

            INSERT INTO "t009__default" 
            (id, data) VALUES (123, 'second');
        "#,
        )
        .unwrap();

    // Check data received on postgres end
    let results = postgres
        .execute("SELECT * FROM t009__default", vec![])
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
            ("postgres".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "postgres".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "public"."t009__default" "#,
                        r#"("data") VALUES ($1)"#
                    ]
                    .join(""),
                    vec!["value=Utf8String(\"first\") type=varchar".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            (
                "postgres".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "public"."t009__default" "#,
                        r#"("id", "data") VALUES ($1, $2)"#
                    ]
                    .join(""),
                    vec![
                        "value=Int32(123) type=int4".into(),
                        "value=Utf8String(\"second\") type=varchar".into()
                    ],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("postgres".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}
