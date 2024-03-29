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
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT * FROM "T006__TEST_TAB"
            WHERE "NAME" = 'John'
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "John".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."NAME" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T006__TEST_TAB" "t1" "#,
                    r#"WHERE (("t1"."NAME") = (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setNString, value=John]".into(),],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_select_where_constant_string_none_matching() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT * FROM "T006__TEST_TAB"
            WHERE "NAME" = 'Unknown...'
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 0);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."NAME" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T006__TEST_TAB" "t1" "#,
                    r#"WHERE (("t1"."NAME") = (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setNString, value=Unknown...]".into(),],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_select_where_param_prepared_statement() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let statement = client
        .prepare(
            r#"
            SELECT * FROM "T006__TEST_TAB"
            WHERE "NAME" = $1
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
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0" "#,
                        r#"FROM "ANSILO_ADMIN"."T006__TEST_TAB" "t1" "#,
                        r#"WHERE (("t1"."NAME") = (?))"#,
                    ]
                    .join(""),
                    vec![format!(
                        "LoggedParam [index=1, method=setNString, value={}]",
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
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT * FROM "T006__TEST_TAB"
            WHERE MD5("NAME") = MD5('Jane')
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "Jane".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."NAME" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T006__TEST_TAB" "t1""#,
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
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT * FROM "T006__TEST_TAB"
            WHERE "NAME" != 'John'
            AND MD5("NAME") != MD5('Mary')
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "Jane".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."NAME" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T006__TEST_TAB" "t1" "#,
                    r#"WHERE (("t1"."NAME") != (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setNString, value=John]".into()],
                None
            )
        )]
    );
}
