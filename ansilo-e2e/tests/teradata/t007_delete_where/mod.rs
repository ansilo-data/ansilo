use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::err::Result;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_delete_where_remote() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let mut teradata =
        ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            DELETE FROM "t007__test_tab"
            WHERE "id" = 2
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on teradata end
    let results = teradata
        .execute("SELECT * FROM t007__test_tab ORDER BY id", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        results
            .into_iter()
            .map(|r| (
                r["id"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![(1, "John".to_string()), (3, "Mary".to_string()),]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("teradata".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "teradata".to_string(),
                LoggedQuery::new(
                    [
                        r#"DELETE FROM "testdb"."t007__test_tab" "#,
                        r#"WHERE (("t007__test_tab"."id") = (?))"#,
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setInt, value=2]".into(),],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("teradata".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

#[test]
#[serial]
fn test_delete_where_local() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let mut teradata =
        ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            DELETE FROM "t007__test_tab"
            WHERE MD5("id"::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on teradata end
    let results = teradata
        .execute("SELECT * FROM t007__test_tab ORDER BY id", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        results
            .into_iter()
            .map(|r| (
                r["id"].as_int32().unwrap().clone(),
                r["name"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![(2, "Jane".to_string()), (3, "Mary".to_string()),]
    );

    let query_log = instance.log().get_from_memory().unwrap();

    // Delete with local eval should lock remote rows using lock first
    assert_eq!(
        query_log,
        vec![
            ("teradata".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "teradata".to_string(),
                LoggedQuery::new(
                    [
                        r#"LOCKING ROW FOR WRITE "#,
                        r#"SELECT "t1"."id" AS "i0", "t1"."id" AS "c0" "#,
                        r#"FROM "testdb"."t007__test_tab" AS "t1""#,
                    ]
                    .join(""),
                    vec![],
                    None
                )
            ),
            (
                "teradata".to_string(),
                LoggedQuery::new(
                    [
                        r#"DELETE FROM "testdb"."t007__test_tab" "#,
                        r#"WHERE (("t007__test_tab"."id") = (?))"#,
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setInt, value=1]".into(),],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("teradata".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}
