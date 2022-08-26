use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_select_remote_order_by() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT "ID"::int, "COUNTRY", "NAME"
            FROM "ANSILO_ADMIN.T011__TEST_TAB"
            ORDER BY "COUNTRY", "NAME" DESC
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (
                r.get::<_, i32>(0),
                r.get::<_, String>(1),
                r.get::<_, String>(2)
            ))
            .collect_vec(),
        vec![
            (6, "AU".into(), "Will".into(),),
            (4, "AU".into(), "Sam".into(),),
            (1, "AU".into(), "John".into(),),
            (5, "NZ".into(), "Tom".into(),),
            (2, "NZ".into(), "Mary".into(),),
            (3, "US".into(), "Jane".into(),),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."ID" AS "c0", "t1"."COUNTRY" AS "c1", "t1"."NAME" AS "c2" "#,
                    r#"FROM "ANSILO_ADMIN"."T011__TEST_TAB" "t1" "#,
                    r#"ORDER BY "t1"."COUNTRY" ASC, "t1"."NAME" DESC"#,
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
fn test_select_local_order_by() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT "ID"::int, REVERSE("COUNTRY"), REVERSE("NAME")
            FROM "ANSILO_ADMIN.T011__TEST_TAB"
            ORDER BY REVERSE("COUNTRY"), REVERSE("NAME") DESC
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (
                r.get::<_, i32>(0),
                r.get::<_, String>(1),
                r.get::<_, String>(2)
            ))
            .collect_vec(),
        vec![
            (3, "SU".into(), "enaJ".into(),),
            (1, "UA".into(), "nhoJ".into(),),
            (4, "UA".into(), "maS".into(),),
            (6, "UA".into(), "lliW".into(),),
            (2, "ZN".into(), "yraM".into(),),
            (5, "ZN".into(), "moT".into(),),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."ID" AS "c0", "t1"."COUNTRY" AS "c1", "t1"."NAME" AS "c2" "#,
                    r#"FROM "ANSILO_ADMIN"."T011__TEST_TAB" "t1""#,
                ]
                .join(""),
                vec![],
                None
            )
        )]
    );
}