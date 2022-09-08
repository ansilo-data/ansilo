use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::err::Result;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_delete_where_remote() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let _rows = client
        .execute(
            r#"
            DELETE FROM t011__test_tab
            WHERE id = 2
        "#,
            &[],
        )
        .unwrap();

    // TODO: implement row count reporting for update / delete
    // assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t011__test_tab ORDER BY id", vec![])
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
        vec![(1, "John".to_string()), (3, "Mary".to_string()),]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "mysql".to_string(),
            LoggedQuery::new(
                [
                    r#"DELETE FROM `db`.`t011__test_tab` "#,
                    r#"WHERE ((`t011__test_tab`.`id`) = (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setInt, value=2]".into(),],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_delete_where_remote_with_no_pk() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let _rows = client
        .execute(
            r#"
            DELETE FROM t011__test_tab_no_pk
            WHERE id = 2
        "#,
            &[],
        )
        .unwrap();

    // TODO: implement row count reporting for update / delete
    // assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t011__test_tab_no_pk ORDER BY id", vec![])
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
        vec![(1, "John".to_string()), (3, "Mary".to_string()),]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "mysql".to_string(),
            LoggedQuery::new(
                [
                    r#"DELETE FROM `db`.`t011__test_tab_no_pk` "#,
                    r#"WHERE ((`t011__test_tab_no_pk`.`id`) = (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setInt, value=2]".into(),],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_delete_where_local() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let _rows = client
        .execute(
            r#"
            DELETE FROM t011__test_tab
            WHERE MD5(id::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap();

    // TODO: implement row count reporting for update / delete
    // assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t011__test_tab ORDER BY id", vec![])
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
        vec![(2, "Jane".to_string()), (3, "Mary".to_string()),]
    );

    let query_log = instance.log().get_from_memory().unwrap();

    // Delete with local eval should lock remote rows using FOR UPDATE first
    assert_eq!(
        query_log,
        vec![
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT `t1`.`id` AS `i0`, `t1`.`id` AS `c0` "#,
                        r#"FROM `db`.`t011__test_tab` AS `t1` "#,
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
                        r#"DELETE FROM `db`.`t011__test_tab` "#,
                        r#"WHERE ((`t011__test_tab`.`id`) = (?))"#,
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setInt, value=1]".into()],
                    None
                )
            )
        ]
    );
}

#[test]
#[serial]
fn test_delete_where_local_with_no_pk_fails() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let _mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute(
            r#"
            DELETE FROM t011__test_tab_no_pk
            WHERE MD5(id::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap_err();

    dbg!(err.to_string());
    assert!(err.to_string().contains("Cannot perform operation on table without primary keys"));
}
