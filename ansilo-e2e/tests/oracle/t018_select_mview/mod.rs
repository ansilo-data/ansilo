use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT * FROM \"T018__TEST_MVIEW\"", &[])
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .columns()
            .into_iter()
            .map(|c| c.name())
            .collect::<Vec<_>>(),
        vec!["COL"]
    );
    assert_eq!(rows[0].get::<_, String>(0), "data".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new_query(r#"SELECT "t1"."COL" AS "c0" FROM "ANSILO_ADMIN"."T018__TEST_MVIEW" "t1""#)
        )]
    )
}

#[test]
#[serial]
fn test_cannot_insert() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("INSERT INTO \"T018__TEST_MVIEW\" VALUES ('test')", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err.to_string().contains("ORA-01732: data manipulation operation not legal on this view"));
}

#[test]
#[serial]
fn test_cannot_update() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("UPDATE \"T018__TEST_MVIEW\" SET \"COL\" = 'test'", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err.to_string().contains("ORA-01732: data manipulation operation not legal on this view"));
}

#[test]
#[serial]
fn test_cannot_delete() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("DELETE FROM \"T018__TEST_MVIEW\"", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err.to_string().contains("ORA-01732: data manipulation operation not legal on this view"));
}
