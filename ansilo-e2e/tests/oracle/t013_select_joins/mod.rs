use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_select_remote_inner_join() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", p."NAME"
            FROM "ANSILO_ADMIN.T013__PEOPLE" h
            INNER JOIN "ANSILO_ADMIN.T013__PETS" p ON h."ID" = p."OWNER_ID"
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .sorted()
            .collect_vec(),
        vec![
            ("Jane".into(), "Pepper".into(),),
            ("John".into(), "Luna".into(),),
            ("John".into(), "Salt".into(),),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."NAME" AS "c0", "t2"."NAME" AS "c1" "#,
                    r#"FROM "ANSILO_ADMIN"."T013__PEOPLE" "t1" "#,
                    r#"INNER JOIN "ANSILO_ADMIN"."T013__PETS" "t2" ON (("t1"."ID") = ("t2"."OWNER_ID"))"#,
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
fn test_select_local_inner_join() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", p."NAME"
            FROM "ANSILO_ADMIN.T013__PEOPLE" h
            INNER JOIN "ANSILO_ADMIN.T013__PETS" p ON MD5(h."ID"::text) = MD5(p."OWNER_ID"::text)
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .sorted()
            .collect_vec(),
        vec![
            ("Jane".into(), "Pepper".into(),),
            ("John".into(), "Luna".into(),),
            ("John".into(), "Salt".into(),),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0", "t1"."ID" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T013__PEOPLE" "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0", "t1"."OWNER_ID" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T013__PETS" "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            )
        ]
    );
}

#[test]
#[serial]
fn test_select_remote_left_join() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", p."NAME"
            FROM "ANSILO_ADMIN.T013__PEOPLE" h
            LEFT JOIN "ANSILO_ADMIN.T013__PETS" p ON h."ID" = p."OWNER_ID"
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
            .sorted()
            .collect_vec(),
        vec![
            ("Jane".into(), Some("Pepper".into()),),
            ("John".into(), Some("Luna".into()),),
            ("John".into(), Some("Salt".into()),),
            ("Mary".into(), None,),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."NAME" AS "c0", "t2"."NAME" AS "c1" "#,
                    r#"FROM "ANSILO_ADMIN"."T013__PEOPLE" "t1" "#,
                    r#"LEFT JOIN "ANSILO_ADMIN"."T013__PETS" "t2" ON (("t1"."ID") = ("t2"."OWNER_ID"))"#,
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
fn test_select_local_left_join() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", p."NAME"
            FROM "ANSILO_ADMIN.T013__PEOPLE" h
            LEFT JOIN "ANSILO_ADMIN.T013__PETS" p ON MD5(h."ID"::text) = MD5(p."OWNER_ID"::text)
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
            .sorted()
            .collect_vec(),
        vec![
            ("Jane".into(), Some("Pepper".into()),),
            ("John".into(), Some("Luna".into()),),
            ("John".into(), Some("Salt".into()),),
            ("Mary".into(), None,),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0", "t1"."ID" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T013__PEOPLE" "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0", "t1"."OWNER_ID" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T013__PETS" "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            )
        ]
    );
}

#[test]
#[serial]
fn test_select_remote_right_join() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", p."NAME"
            FROM "ANSILO_ADMIN.T013__PEOPLE" h
            RIGHT JOIN "ANSILO_ADMIN.T013__PETS" p ON h."ID" = p."OWNER_ID"
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, Option<String>>(0), r.get::<_, String>(1)))
            .sorted()
            .collect_vec(),
        vec![
            (None, "Morris".into(),),
            (Some("Jane".into()), "Pepper".into(),),
            (Some("John".into()), "Luna".into(),),
            (Some("John".into()), "Salt".into(),),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t2"."NAME" AS "c0", "t1"."NAME" AS "c1" "#,
                    r#"FROM "ANSILO_ADMIN"."T013__PETS" "t1" "#,
                    r#"LEFT JOIN "ANSILO_ADMIN"."T013__PEOPLE" "t2" ON (("t2"."ID") = ("t1"."OWNER_ID"))"#,
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
fn test_select_local_right_join() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", p."NAME"
            FROM "ANSILO_ADMIN.T013__PEOPLE" h
            RIGHT JOIN "ANSILO_ADMIN.T013__PETS" p ON MD5(h."ID"::text) = MD5(p."OWNER_ID"::text)
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, Option<String>>(0), r.get::<_, String>(1)))
            .sorted()
            .collect_vec(),
        vec![
            (None, "Morris".into(),),
            (Some("Jane".into()), "Pepper".into(),),
            (Some("John".into()), "Luna".into(),),
            (Some("John".into()), "Salt".into(),),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0", "t1"."OWNER_ID" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T013__PETS" "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0", "t1"."ID" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T013__PEOPLE" "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
        ]
    );
}

#[test]
#[serial]
fn test_select_remote_full_join() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", p."NAME"
            FROM "ANSILO_ADMIN.T013__PEOPLE" h
            FULL JOIN "ANSILO_ADMIN.T013__PETS" p ON h."ID" = p."OWNER_ID"
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, Option<String>>(0), r.get::<_, Option<String>>(1)))
            .sorted()
            .collect_vec(),
        vec![
            (None, Some("Morris".into()),),
            (Some("Jane".into()), Some("Pepper".into()),),
            (Some("John".into()), Some("Luna".into()),),
            (Some("John".into()), Some("Salt".into()),),
            (Some("Mary".into()), None),
        ]
    );
    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."NAME" AS "c0", "t2"."NAME" AS "c1" "#,
                    r#"FROM "ANSILO_ADMIN"."T013__PEOPLE" "t1" "#,
                    r#"FULL JOIN "ANSILO_ADMIN"."T013__PETS" "t2" ON (("t1"."ID") = ("t2"."OWNER_ID"))"#,
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
fn test_select_local_full_join() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", p."NAME"
            FROM "ANSILO_ADMIN.T013__PEOPLE" h
            FULL JOIN "ANSILO_ADMIN.T013__PETS" p ON MD5(h."ID"::text) = MD5(p."OWNER_ID"::text)
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, Option<String>>(0), r.get::<_, Option<String>>(1)))
            .sorted()
            .collect_vec(),
        vec![
            (None, Some("Morris".into()),),
            (Some("Jane".into()), Some("Pepper".into()),),
            (Some("John".into()), Some("Luna".into()),),
            (Some("John".into()), Some("Salt".into()),),
            (Some("Mary".into()), None),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0", "t1"."ID" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T013__PEOPLE" "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"SELECT "t1"."NAME" AS "c0", "t1"."OWNER_ID" AS "c1" "#,
                        r#"FROM "ANSILO_ADMIN"."T013__PETS" "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
        ]
    );
}
