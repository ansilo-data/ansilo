use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_select_where_constant_string() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let _teradata = ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT * FROM "t005__test_tab"
            WHERE "name" = 'John'
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "John".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "teradata".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."name" AS "c0" "#,
                    r#"FROM "testdb"."t005__test_tab" AS "t1" "#,
                    r#"WHERE (("t1"."name") = (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setString, value=John]".into(),],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_select_where_constant_string_none_matching() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let _teradata = ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT * FROM "t005__test_tab"
            WHERE "name" = 'Unknown...'
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 0);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "teradata".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."name" AS "c0" "#,
                    r#"FROM "testdb"."t005__test_tab" AS "t1" "#,
                    r#"WHERE (("t1"."name") = (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setString, value=Unknown...]".into(),],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_select_where_param_prepared_statement() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let _teradata = ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let statement = client
        .prepare(
            r#"
            SELECT * FROM "t005__test_tab"
            WHERE "name" = $1
            "#,
        )
        .unwrap();

    let names = ["Mary", "John"];

    for name in names.iter() {
        let rows = client.query(&statement, &[name]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, String>(0), name.to_string());
    }

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        names
            .iter()
            .map(|name| (
                "teradata".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."name" AS "c0" "#,
                        r#"FROM "testdb"."t005__test_tab" AS "t1" "#,
                        r#"WHERE (("t1"."name") = (?))"#,
                    ]
                    .join(""),
                    vec![format!(
                        "LoggedParam [index=1, method=setString, value={}]",
                        name
                    )],
                    None
                )
            ))
            .collect_vec()
    );
}

#[test]
#[serial]
fn test_select_where_local_condition() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let _teradata = ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT * FROM "t005__test_tab"
            WHERE MD5("name") = MD5('Jane')
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "Jane".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "teradata".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."name" AS "c0" "#,
                    r#"FROM "testdb"."t005__test_tab" AS "t1""#,
                ]
                .join(""),
                vec![],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_select_where_remote_and_local_condition() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let _teradata = ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT * FROM "t005__test_tab"
            WHERE "name" != 'John'
            AND MD5("name") != MD5('Mary')
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "Jane".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "teradata".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."name" AS "c0" "#,
                    r#"FROM "testdb"."t005__test_tab" AS "t1" "#,
                    r#"WHERE (("t1"."name") <> (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setString, value=John]".into()],
                None
            )
        )]
    );
}
