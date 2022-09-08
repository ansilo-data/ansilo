use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::err::Result;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_update_where_remote() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE t010__test_tab
            SET name = 'Jannet'
            WHERE id = 2
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t010__test_tab ORDER BY id", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        results
            .into_iter()
            .map(|r| (
                r["id"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![
            (1, "John".to_string()),
            (2, "Jannet".to_string()),
            (3, "Mary".to_string()),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "mysql".to_string(),
            LoggedQuery::new(
                [
                    r#"UPDATE `db`.`t010__test_tab` SET `name` = ? "#,
                    r#"WHERE ((`t010__test_tab`.`id`) = (?))"#,
                ]
                .join(""),
                vec![
                    "LoggedParam [index=1, method=setString, value=Jannet]".into(),
                    "LoggedParam [index=2, method=setInt, value=2]".into(),
                ],
                Some(
                    [("affected".into(), "Some(1)".into())]
                        .into_iter()
                        .collect()
                )
            )
        )]
    );
}

#[test]
#[serial]
fn test_update_where_local() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE t010__test_tab
            SET name = 'Johnny'
            WHERE MD5(id::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t010__test_tab ORDER BY id", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        results
            .into_iter()
            .map(|r| (
                r["id"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![
            (1, "Johnny".to_string()),
            (2, "Jane".to_string()),
            (3, "Mary".to_string()),
        ]
    );

    let query_log = instance.log().get_from_memory().unwrap();

    // Update with local eval should lock remote rows using FOR UPDATE first
    assert_eq!(
        query_log,
        vec![
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT `t1`.`id` AS `i0`, `t1`.`id` AS `c0`, `t1`.`name` AS `c1` "#,
                        r#"FROM `db`.`t010__test_tab` AS `t1` "#,
                        r#"FOR UPDATE"#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"UPDATE `db`.`t010__test_tab` SET `name` = ? "#,
                        r#"WHERE ((`t010__test_tab`.`id`) = (?))"#,
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setString, value=Johnny]".into(),
                        "LoggedParam [index=2, method=setInt, value=1]".into()
                    ],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            )
        ]
    );
}

#[test]
#[serial]
fn test_update_where_remote_with_no_pk() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE t010__test_tab_no_pk
            SET name = 'Jannet'
            WHERE id = 2
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t010__test_tab_no_pk ORDER BY id", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        results
            .into_iter()
            .map(|r| (
                r["id"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![
            (1, "John".to_string()),
            (2, "Jannet".to_string()),
            (3, "Mary".to_string()),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "mysql".to_string(),
            LoggedQuery::new(
                [
                    r#"UPDATE `db`.`t010__test_tab_no_pk` SET `name` = ? "#,
                    r#"WHERE ((`t010__test_tab_no_pk`.`id`) = (?))"#,
                ]
                .join(""),
                vec![
                    "LoggedParam [index=1, method=setString, value=Jannet]".into(),
                    "LoggedParam [index=2, method=setInt, value=2]".into(),
                ],
                Some(
                    [("affected".into(), "Some(1)".into())]
                        .into_iter()
                        .collect()
                )
            )
        )]
    );
}

#[test]
#[serial]
fn test_update_where_local_with_no_pk_fails() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let _mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute(
            r#"
            UPDATE t010__test_tab_no_pk
            SET name = 'Johnny'
            WHERE MD5(id::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap_err();

    dbg!(err.to_string());
    assert!(err
        .to_string()
        .contains("Cannot perform operation on table without primary keys"));
}
