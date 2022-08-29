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
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "db.t006__auto_increment" 
            (data) VALUES ('value'), ('another')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on mysql end
    let results = mysql
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
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO `db`.`t006__auto_increment` "#,
                        r#"(`data`) VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setString, value=value]".into()],
                    None
                )
            ),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO `db`.`t006__auto_increment` "#,
                        r#"(`data`) VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setString, value=another]".into()],
                    None
                )
            )
        ]
    );
}

#[test]
#[serial]
fn test_default() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
            INSERT INTO "db.t006__default" 
            (data) VALUES ('first');

            INSERT INTO "db.t006__default" 
            (id, data) VALUES (123, 'second');
        "#,
        )
        .unwrap();

    // Check data received on mysql end
    let results = mysql
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
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO `db`.`t006__default` "#,
                        r#"(`data`) VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setString, value=first]".into()],
                    None
                )
            ),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO `db`.`t006__default` "#,
                        r#"(`id`, `data`) VALUES (?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setInt, value=123]".into(),
                        "LoggedParam [index=2, method=setString, value=second]".into()
                    ],
                    None
                )
            )
        ]
    );
}
