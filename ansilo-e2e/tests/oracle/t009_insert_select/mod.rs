use std::{env, str::FromStr};

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{
    data::{chrono_tz::Tz, DataValue, DateTimeWithTZ},
    err::Result,
};
use chrono::NaiveDateTime;
use pretty_assertions::assert_eq;
use serial_test::serial;

use crate::assert::assert_rows_equal;

#[test]
#[serial]
fn test_insert_select_local_values() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "ANSILO_ADMIN.T009__TEST_TARGET" (
                "ID", "NAME", "SOURCE", "CREATED_AT"
            )
            SELECT 1, 'Jerry', 'SELECT', TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 -5:00'
            UNION ALL
            SELECT 2, 'George', 'SELECT', TIMESTAMP WITH TIME ZONE '2000-01-15 11:00:00 -5:00'
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T009__TEST_TARGET", vec![])
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
                ("ID".to_string(), DataValue::Decimal(1.into())),
                ("NAME".to_string(), DataValue::Utf8String("Jerry".into())),
                ("SOURCE".to_string(), DataValue::Utf8String("SELECT".into())),
                (
                    "CREATED_AT".to_string(),
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::from_str("1999-01-15T16:00:00").unwrap(),
                        Tz::UTC,
                    )),
                ),
            ]
            .into_iter()
            .collect(),
            vec![
                ("ID".to_string(), DataValue::Decimal(2.into())),
                ("NAME".to_string(), DataValue::Utf8String("George".into())),
                ("SOURCE".to_string(), DataValue::Utf8String("SELECT".into())),
                (
                    "CREATED_AT".to_string(),
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::from_str("2000-01-15T16:00:00").unwrap(),
                        Tz::UTC,
                    )),
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
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T009__TEST_TARGET" "#,
                        r#"("ID", "NAME", "SOURCE", "CREATED_AT")"#,
                        r#" VALUES "#,
                        r#"(?, ?, ?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setBigDecimal, value=1]".into(),
                        "LoggedParam [index=2, method=setNString, value=Jerry]".into(),
                        "LoggedParam [index=3, method=setNString, value=SELECT]".into(),
                        "LoggedParam [index=4, method=setTimestamp, value=1999-01-15 16:00:00.0]"
                            .into(),
                    ],
                    None
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T009__TEST_TARGET" "#,
                        r#"("ID", "NAME", "SOURCE", "CREATED_AT")"#,
                        r#" VALUES "#,
                        r#"(?, ?, ?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setBigDecimal, value=2]".into(),
                        "LoggedParam [index=2, method=setNString, value=George]".into(),
                        "LoggedParam [index=3, method=setNString, value=SELECT]".into(),
                        "LoggedParam [index=4, method=setTimestamp, value=2000-01-15 16:00:00.0]"
                            .into(),
                    ],
                    None
                )
            )
        ]
    );
}

#[test]
#[serial]
fn test_insert_select_from_remote_table() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "ANSILO_ADMIN.T009__TEST_TARGET" (
                "ID", "NAME", "SOURCE", "CREATED_AT"
            )
            SELECT "ID", "NAME", 'REMOTE', TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 +00:00'
            FROM "ANSILO_ADMIN.T009__TEST_SOURCE"
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 3);

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T009__TEST_TARGET", vec![])
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
                ("ID".to_string(), DataValue::Decimal(1.into())),
                ("NAME".to_string(), DataValue::Utf8String("John".into())),
                ("SOURCE".to_string(), DataValue::Utf8String("REMOTE".into())),
                (
                    "CREATED_AT".to_string(),
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap(),
                        Tz::UTC,
                    )),
                ),
            ]
            .into_iter()
            .collect(),
            vec![
                ("ID".to_string(), DataValue::Decimal(2.into())),
                ("NAME".to_string(), DataValue::Utf8String("Emma".into())),
                ("SOURCE".to_string(), DataValue::Utf8String("REMOTE".into())),
                (
                    "CREATED_AT".to_string(),
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap(),
                        Tz::UTC,
                    )),
                ),
            ]
            .into_iter()
            .collect(),
            vec![
                ("ID".to_string(), DataValue::Decimal(3.into())),
                ("NAME".to_string(), DataValue::Utf8String("Jane".into())),
                ("SOURCE".to_string(), DataValue::Utf8String("REMOTE".into())),
                (
                    "CREATED_AT".to_string(),
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap(),
                        Tz::UTC,
                    )),
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
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."ID" AS "c0", "t1"."NAME" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T009__TEST_SOURCE" "t1""#
                    ]
                    .join(""),
                    vec![],
                    None
                ),
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T009__TEST_TARGET" "#,
                        r#"("ID", "NAME", "SOURCE", "CREATED_AT")"#,
                        r#" VALUES "#,
                        r#"(?, ?, ?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setBigDecimal, value=1]".into(),
                        "LoggedParam [index=2, method=setNString, value=John]".into(),
                        "LoggedParam [index=3, method=setNString, value=REMOTE]".into(),
                        "LoggedParam [index=4, method=setTimestamp, value=1999-01-15 11:00:00.0]"
                            .into(),
                    ],
                    None
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T009__TEST_TARGET" "#,
                        r#"("ID", "NAME", "SOURCE", "CREATED_AT")"#,
                        r#" VALUES "#,
                        r#"(?, ?, ?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setBigDecimal, value=2]".into(),
                        "LoggedParam [index=2, method=setNString, value=Emma]".into(),
                        "LoggedParam [index=3, method=setNString, value=REMOTE]".into(),
                        "LoggedParam [index=4, method=setTimestamp, value=1999-01-15 11:00:00.0]"
                            .into(),
                    ],
                    None
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T009__TEST_TARGET" "#,
                        r#"("ID", "NAME", "SOURCE", "CREATED_AT")"#,
                        r#" VALUES "#,
                        r#"(?, ?, ?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setBigDecimal, value=3]".into(),
                        "LoggedParam [index=2, method=setNString, value=Jane]".into(),
                        "LoggedParam [index=3, method=setNString, value=REMOTE]".into(),
                        "LoggedParam [index=4, method=setTimestamp, value=1999-01-15 11:00:00.0]"
                            .into(),
                    ],
                    None
                )
            )
        ]
    );
}
