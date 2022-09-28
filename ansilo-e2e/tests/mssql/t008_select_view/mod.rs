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
    let containers = ansilo_e2e::mssql::start_mssql();
    ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT * FROM \"t008__test_view\"", &[])
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
            "mssql".to_string(),
            LoggedQuery::new_query(
                [
                    r#"SELECT [t1].[col] AS [c0] "#,
                    r#"FROM [dbo].[t008__test_view] AS [t1]"#,
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
    let containers = ansilo_e2e::mssql::start_mssql();
    ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("INSERT INTO \"t008__test_view\" VALUES ('test')", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err
        .to_string()
        .contains("Update or insert of view or function 'dbo.t008__test_view' failed"));
}

#[test]
#[serial]
fn test_cannot_update() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mssql::start_mssql();
    ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("UPDATE \"t008__test_view\" SET col = 'test'", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err
        .to_string()
        .contains("Update or insert of view or function 'dbo.t008__test_view' failed"));
}

#[test]
#[serial]
fn test_cannot_delete() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mssql::start_mssql();
    ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let err = client
        .execute("DELETE FROM \"t008__test_view\"", &[])
        .err()
        .unwrap();

    dbg!(err.to_string());
    assert!(err
        .to_string()
        .contains("Update or insert of view or function 'dbo.t008__test_view' failed"));
}
