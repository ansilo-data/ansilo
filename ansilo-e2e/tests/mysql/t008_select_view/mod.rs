use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT * FROM \"db.t008__test_view\"", &[])
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .columns()
            .into_iter()
            .map(|c| c.name())
            .collect_vec(),
        vec!["col"]
    );
    assert_eq!(rows[0].get::<_, String>(0), "data".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "mysql".to_string(),
            LoggedQuery::new_query(
                [
                    r#"SELECT `t1`.`col` AS `c0` "#,
                    r#"FROM `db`.`t008__test_view` AS `t1`"#,
                ]
                .join("")
            )
        )]
    );
}

#[test]
#[serial]
fn test_cannot_insert() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("INSERT INTO \"db.t008__test_view\" VALUES ('test')", &[])
        .err()
        .unwrap();

    assert!(err
        .to_string()
        .contains("The target table t008__test_view of the INSERT is not insertable-into"));
}

#[test]
#[serial]
fn test_cannot_update() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("UPDATE \"db.t008__test_view\" SET col = 'test'", &[])
        .err()
        .unwrap();

    assert!(err
        .to_string()
        .contains("The target table t008__test_view of the UPDATE is not updatable"));
}

#[test]
#[serial]
fn test_cannot_delete() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("DELETE FROM \"db.t008__test_view\"", &[])
        .err()
        .unwrap();

    assert!(err
        .to_string()
        .contains("The target table t008__test_view of the DELETE is not updatable"));
}
