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
            UPDATE t013__test_tab
            SET name = 'Jannet'
            WHERE id1 = 1 AND id2 = 2
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t013__test_tab ORDER BY id1, id2", vec![])
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
                r["id1"].as_int32().unwrap().clone(),
                r["id2"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![
            (1, 1, "John".to_string()),
            (1, 2, "Jannet".to_string()),
            (1, 3, "Mary".to_string()),
            (2, 1, "Jack".to_string()),
            (2, 2, "Jen".to_string()),
            (2, 3, "Gerald".to_string()),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mysql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"UPDATE `db`.`t013__test_tab` SET `name` = ? "#,
                        r#"WHERE ((`t013__test_tab`.`id1`) = (?)) AND ((`t013__test_tab`.`id2`) = (?))"#,
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setString, value=Jannet]".into(),
                        "LoggedParam [index=2, method=setInt, value=1]".into(),
                        "LoggedParam [index=3, method=setInt, value=2]".into(),
                    ],
                    Some(
                        [("affected".into(), "Some(1)".into())]
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
            UPDATE t013__test_tab
            SET name = 'Jacky'
            WHERE MD5(id1::text) = MD5('2') AND MD5(id2::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t013__test_tab ORDER BY id1, id2", vec![])
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
                r["id1"].as_int32().unwrap().clone(),
                r["id2"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![
            (1, 1, "John".to_string()),
            (1, 2, "Jane".to_string()),
            (1, 3, "Mary".to_string()),
            (2, 1, "Jacky".to_string()),
            (2, 2, "Jen".to_string()),
            (2, 3, "Gerald".to_string()),
        ]
    );

    let query_log = instance.log().get_from_memory().unwrap();

    // Update with local eval should lock remote rows using FOR UPDATE first
    assert_eq!(
        query_log,
        vec![
            ("mysql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT `t1`.`id1` AS `i0`, `t1`.`id2` AS `i1`, `t1`.`id1` AS `c0`, `t1`.`id2` AS `c1`, `t1`.`name` AS `c2` "#,
                        r#"FROM `db`.`t013__test_tab` AS `t1` "#,
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
                        r#"UPDATE `db`.`t013__test_tab` SET `name` = ? "#,
                        r#"WHERE ((`t013__test_tab`.`id1`) = (?)) AND ((`t013__test_tab`.`id2`) = (?))"#,
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setString, value=Jacky]".into(),
                        "LoggedParam [index=2, method=setInt, value=2]".into(),
                        "LoggedParam [index=3, method=setInt, value=1]".into(),
                    ],
                    Some(
                        [("affected".into(), "Some(1)".into())]
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
fn test_delete_where_remote() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            DELETE FROM t013__test_tab
            WHERE id1 = 1 AND id2 = 2
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t013__test_tab ORDER BY id1, id2", vec![])
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
                r["id1"].as_int32().unwrap().clone(),
                r["id2"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![
            (1, 1, "John".to_string()),
            (1, 3, "Mary".to_string()),
            (2, 1, "Jack".to_string()),
            (2, 2, "Jen".to_string()),
            (2, 3, "Gerald".to_string()),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("mysql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"DELETE FROM `db`.`t013__test_tab` "#,
                        r#"WHERE ((`t013__test_tab`.`id1`) = (?)) AND ((`t013__test_tab`.`id2`) = (?))"#,
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setInt, value=1]".into(),
                        "LoggedParam [index=2, method=setInt, value=2]".into(),
                    ],
                    Some(
                        [("affected".into(), "Some(1)".into())]
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
fn test_delete_where_local() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            DELETE FROM t013__test_tab
            WHERE MD5(id1::text) = MD5('2') AND MD5(id2::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t013__test_tab ORDER BY id1, id2", vec![])
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
                r["id1"].as_int32().unwrap().clone(),
                r["id2"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![
            (1, 1, "John".to_string()),
            (1, 2, "Jane".to_string()),
            (1, 3, "Mary".to_string()),
            (2, 2, "Jen".to_string()),
            (2, 3, "Gerald".to_string()),
        ]
    );

    let query_log = instance.log().get_from_memory().unwrap();

    // Update with local eval should lock remote rows using FOR UPDATE first
    assert_eq!(
        query_log,
        vec![
            ("mysql".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "mysql".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT `t1`.`id1` AS `i0`, `t1`.`id2` AS `i1`, `t1`.`id1` AS `c0`, `t1`.`id2` AS `c1` "#,
                        r#"FROM `db`.`t013__test_tab` AS `t1` "#,
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
                        r#"DELETE FROM `db`.`t013__test_tab` "#,
                        r#"WHERE ((`t013__test_tab`.`id1`) = (?)) AND ((`t013__test_tab`.`id2`) = (?))"#,
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setInt, value=2]".into(),
                        "LoggedParam [index=2, method=setInt, value=1]".into(),
                    ],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("mysql".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}
