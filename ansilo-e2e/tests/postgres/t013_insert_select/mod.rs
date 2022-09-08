use std::{env, str::FromStr};

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use chrono::NaiveDateTime;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_insert_select_local_values() {
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
            INSERT INTO "t013__test_target" (
                "id", "name", "source", "created_at"
            )
            SELECT 1, 'Jerry', 'SELECT', TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 -5:00'
            UNION ALL
            SELECT 2, 'George', 'SELECT', TIMESTAMP WITH TIME ZONE '2000-01-15 11:00:00 -5:00'
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on postgres end
    let results = postgres
        .execute("SELECT * FROM t013__test_target", vec![])
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
                ("name".to_string(), DataValue::Utf8String("Jerry".into())),
                ("source".to_string(), DataValue::Utf8String("SELECT".into())),
                (
                    "created_at".to_string(),
                    DataValue::DateTime(NaiveDateTime::from_str("1999-01-15T16:00:00").unwrap()),
                ),
            ]
            .into_iter()
            .collect(),
            vec![
                ("id".to_string(), DataValue::Int32(2)),
                ("name".to_string(), DataValue::Utf8String("George".into())),
                ("source".to_string(), DataValue::Utf8String("SELECT".into())),
                (
                    "created_at".to_string(),
                    DataValue::DateTime(NaiveDateTime::from_str("2000-01-15T16:00:00").unwrap()),
                ),
            ]
            .into_iter()
            .collect(),
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "postgres".to_string(),
            LoggedQuery::new(
                [
                    r#"INSERT INTO "public"."t013__test_target" "#,
                    r#"("id", "name", "source", "created_at") VALUES "#,
                    r#"($1, $2, $3, $4), ($5, $6, $7, $8)"#
                ]
                .join(""),
                vec![
                    "value=Int32(1) type=int4".into(),
                    "value=Utf8String(\"Jerry\") type=varchar".into(),
                    "value=Utf8String(\"SELECT\") type=varchar".into(),
                    "value=DateTime(1999-01-15T16:00:00) type=timestamp".into(),
                    //
                    "value=Int32(2) type=int4".into(),
                    "value=Utf8String(\"George\") type=varchar".into(),
                    "value=Utf8String(\"SELECT\") type=varchar".into(),
                    "value=DateTime(2000-01-15T16:00:00) type=timestamp".into(),
                ],
                Some(
                    [("affected".into(), "Some(2)".into())]
                        .into_iter()
                        .collect()
                )
            )
        )]
    );
}

#[test]
#[serial]
fn test_insert_select_from_remote_table() {
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
            INSERT INTO "t013__test_target" (
                "id", "name", "source", "created_at"
            )
            SELECT "id", "name", 'remote', TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 +00:00'
            FROM "t013__test_source"
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 3);

    // Check data received on postgres end
    let results = postgres
        .execute("SELECT * FROM t013__test_target", vec![])
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
                ("name".to_string(), DataValue::Utf8String("John".into())),
                ("source".to_string(), DataValue::Utf8String("remote".into())),
                (
                    "created_at".to_string(),
                    DataValue::DateTime(NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap()),
                ),
            ]
            .into_iter()
            .collect(),
            vec![
                ("id".to_string(), DataValue::Int32(2)),
                ("name".to_string(), DataValue::Utf8String("Emma".into())),
                ("source".to_string(), DataValue::Utf8String("remote".into())),
                (
                    "created_at".to_string(),
                    DataValue::DateTime(NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap()),
                ),
            ]
            .into_iter()
            .collect(),
            vec![
                ("id".to_string(), DataValue::Int32(3)),
                ("name".to_string(), DataValue::Utf8String("Jane".into())),
                ("source".to_string(), DataValue::Utf8String("remote".into())),
                (
                    "created_at".to_string(),
                    DataValue::DateTime(NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap()),
                ),
            ]
            .into_iter()
            .collect(),
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            (
                "postgres".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."id" AS "c0", "t1"."name" AS "c1" "#,
                        r#"FROM "public"."t013__test_source" AS "t1""#
                    ]
                    .join(""),
                    vec![],
                    None
                ),
            ),
            (
                "postgres".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "public"."t013__test_target" "#,
                        r#"("id", "name", "source", "created_at") VALUES "#,
                        r#"($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)"#
                    ]
                    .join(""),
                    vec![
                        "value=Int32(1) type=int4".into(),
                        "value=Utf8String(\"John\") type=varchar".into(),
                        "value=Utf8String(\"remote\") type=varchar".into(),
                        "value=DateTime(1999-01-15T11:00:00) type=timestamp".into(),
                        "value=Int32(2) type=int4".into(),
                        "value=Utf8String(\"Emma\") type=varchar".into(),
                        "value=Utf8String(\"remote\") type=varchar".into(),
                        "value=DateTime(1999-01-15T11:00:00) type=timestamp".into(),
                        "value=Int32(3) type=int4".into(),
                        "value=Utf8String(\"Jane\") type=varchar".into(),
                        "value=Utf8String(\"remote\") type=varchar".into(),
                        "value=DateTime(1999-01-15T11:00:00) type=timestamp".into(),
                    ],
                    Some(
                        [("affected".into(), "Some(3)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            )
        ]
    );
}
