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
    let containers = ansilo_e2e::postgres::start_postgres();
    ansilo_e2e::postgres::init_postgres_sql(&containers, current_dir!().join("postgres-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT * FROM \"t011__test_view\"", &[])
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
            "postgres".to_string(),
            LoggedQuery::new_query(
                [
                    r#"SELECT "t1"."col" AS "c0" "#,
                    r#"FROM "public"."t011__test_view" AS "t1""#,
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
    let containers = ansilo_e2e::postgres::start_postgres();
    ansilo_e2e::postgres::init_postgres_sql(&containers, current_dir!().join("postgres-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("INSERT INTO \"t011__test_view\" VALUES ('test')", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err
        .to_string()
        .contains("cannot insert into view"));
}

#[test]
#[serial]
fn test_cannot_update() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::postgres::start_postgres();
    ansilo_e2e::postgres::init_postgres_sql(&containers, current_dir!().join("postgres-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("UPDATE \"t011__test_view\" SET col = 'test'", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err
        .to_string()
        .contains("cannot update view"));
}

#[test]
#[serial]
fn test_cannot_delete() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::postgres::start_postgres();
    ansilo_e2e::postgres::init_postgres_sql(&containers, current_dir!().join("postgres-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("DELETE FROM \"t011__test_view\"", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err
        .to_string()
        .contains("cannot delete from view"));
}
