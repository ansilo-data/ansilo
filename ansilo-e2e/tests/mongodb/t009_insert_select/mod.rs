use std::{env, str::FromStr};

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{
    data::{chrono_tz::Tz, DataValue, DateTimeWithTZ},
    err::Result,
};
use ansilo_e2e::current_dir;
use chrono::NaiveDateTime;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_insert_select_local_values() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t009__test_target" (
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

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t009__test_target", vec![])
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
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::from_str("1999-01-15T16:00:00").unwrap(),
                        Tz::UTC,
                    )),
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
            ("mysql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO `db`.`t009__test_target` "#,
                        r#"(`id`, `name`, `source`, `created_at`) VALUES "#,
                        r#"(?, ?, ?, ?), (?, ?, ?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setInt, value=1]".into(),
                        "LoggedParam [index=2, method=setString, value=Jerry]".into(),
                        "LoggedParam [index=3, method=setString, value=SELECT]".into(),
                        "LoggedParam [index=4, method=setTimestamp, value=1999-01-15 16:00:00.0]"
                            .into(),
                        "LoggedParam [index=5, method=setInt, value=2]".into(),
                        "LoggedParam [index=6, method=setString, value=George]".into(),
                        "LoggedParam [index=7, method=setString, value=SELECT]".into(),
                        "LoggedParam [index=8, method=setTimestamp, value=2000-01-15 16:00:00.0]"
                            .into(),
                    ],
                    Some(
                        [("affected".into(), "Some(2)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mysql".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

#[test]
#[serial]
fn test_insert_select_from_remote_table() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t009__test_target" (
                "id", "name", "source", "created_at"
            )
            SELECT "id", "name", 'remote', TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 +00:00'
            FROM "t009__test_source"
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 3);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t009__test_target", vec![])
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
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap(),
                        Tz::UTC,
                    )),
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
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::from_str("1999-01-15T11:00:00").unwrap(),
                        Tz::UTC,
                    )),
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
            ("mysql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT `t1`.`id` AS `c0`, `t1`.`name` AS `c1` "#,
                        r#"FROM `db`.`t009__test_source` AS `t1`"#
                    ]
                    .join(""),
                    vec![],
                    None
                ),
            ),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO `db`.`t009__test_target` "#,
                        r#"(`id`, `name`, `source`, `created_at`) VALUES "#,
                        r#"(?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setInt, value=1]".into(),
                        "LoggedParam [index=2, method=setString, value=John]".into(),
                        "LoggedParam [index=3, method=setString, value=remote]".into(),
                        "LoggedParam [index=4, method=setTimestamp, value=1999-01-15 11:00:00.0]"
                            .into(),
                        "LoggedParam [index=5, method=setInt, value=2]".into(),
                        "LoggedParam [index=6, method=setString, value=Emma]".into(),
                        "LoggedParam [index=7, method=setString, value=remote]".into(),
                        "LoggedParam [index=8, method=setTimestamp, value=1999-01-15 11:00:00.0]"
                            .into(),
                        "LoggedParam [index=9, method=setInt, value=3]".into(),
                        "LoggedParam [index=10, method=setString, value=Jane]".into(),
                        "LoggedParam [index=11, method=setString, value=remote]".into(),
                        "LoggedParam [index=12, method=setTimestamp, value=1999-01-15 11:00:00.0]"
                            .into(),
                    ],
                    Some(
                        [("affected".into(), "Some(3)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mysql".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}
