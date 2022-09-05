use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_select_remote_limit() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT "ID"::int
            FROM "T012__TEST_TAB"
            LIMIT 3
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, i32>(0),))
            .collect_vec(),
        vec![(1,), (2,), (3,),]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."ID" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T012__TEST_TAB" "t1" "#,
                    r#"FETCH FIRST 3 ROWS ONLY"#,
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
fn test_select_remote_offset() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT "ID"::int
            FROM "T012__TEST_TAB"
            OFFSET 4
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, i32>(0),))
            .collect_vec(),
        vec![(5,), (6,)]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."ID" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T012__TEST_TAB" "t1" "#,
                    r#"OFFSET 4 ROWS"#,
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
fn test_select_limit_and_offset() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT "ID"::int
            FROM "T012__TEST_TAB"
            LIMIT 2 OFFSET 3
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, i32>(0),))
            .collect_vec(),
        vec![(4,), (5,)]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."ID" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T012__TEST_TAB" "t1" "#,
                    r#"OFFSET 3 ROWS FETCH FIRST 2 ROWS ONLY"#,
                ]
                .join(""),
                vec![],
                None
            )
        )]
    );
}
