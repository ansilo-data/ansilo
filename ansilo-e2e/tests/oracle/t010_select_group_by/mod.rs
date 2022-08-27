use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_select_remote_group_by_with_count_aggregation() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT "COUNTRY", COUNT(*) 
            FROM "ANSILO_ADMIN.T010__TEST_TAB"
            GROUP BY "COUNTRY"
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
            .sorted()
            .collect_vec(),
        vec![("AU".into(), 3), ("NZ".into(), 2), ("US".into(), 1),]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."COUNTRY" AS "c0", COUNT(*) AS "c1" "#,
                    r#"FROM "ANSILO_ADMIN"."T010__TEST_TAB" "t1" "#,
                    r#"GROUP BY "t1"."COUNTRY""#,
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
fn test_select_local_group_by_with_count_aggregation() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT REVERSE("COUNTRY"), COUNT(*) 
            FROM "ANSILO_ADMIN.T010__TEST_TAB"
            GROUP BY REVERSE("COUNTRY")
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
            .sorted()
            .collect_vec(),
        vec![("SU".into(), 1), ("UA".into(), 3), ("ZN".into(), 2),]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."COUNTRY" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T010__TEST_TAB" "t1""#,
                ]
                .join(""),
                vec![],
                None
            )
        )]
    );
}
