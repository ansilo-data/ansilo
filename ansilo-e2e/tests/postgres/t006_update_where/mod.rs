use std::{collections::HashMap, env};

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::err::Result;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_update_where_remote() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::postgres::start_postgres();
    let mut postgres = ansilo_e2e::postgres::init_postgres_sql(
        &containers,
        current_dir!().join("postgres-sql/*.sql"),
    );

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE "t006__test_tab"
            SET "name" = 'Jannet'
            WHERE "id" = 2
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on postgres end
    let results = postgres
        .execute("SELECT * FROM t006__test_tab ORDER BY id", vec![])
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
        vec![
            (1, "John".to_string()),
            (2, "Jannet".to_string()),
            (3, "Mary".to_string()),
        ]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "postgres".to_string(),
            LoggedQuery::new(
                [
                    r#"UPDATE "public"."t006__test_tab" SET "name" = $1 "#,
                    r#"WHERE (("t006__test_tab"."id") = ($2))"#,
                ]
                .join(""),
                vec![
                    "value=Utf8String(\"Jannet\") type=varchar".into(),
                    "value=Int32(2) type=int4".into(),
                ],
                Some(
                    [("affected".into(), "Some(1)".into())]
                        .into_iter()
                        .collect()
                )
            )
        )]
    );
}

#[test]
#[serial]
fn test_update_where_local() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::postgres::start_postgres();
    let mut postgres = ansilo_e2e::postgres::init_postgres_sql(
        &containers,
        current_dir!().join("postgres-sql/*.sql"),
    );

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            UPDATE "t006__test_tab"
            SET "name" = 'Johnny'
            WHERE MD5("id"::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on postgres end
    let results = postgres
        .execute("SELECT * FROM t006__test_tab ORDER BY id", vec![])
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
        vec![
            (1, "Johnny".to_string()),
            (2, "Jane".to_string()),
            (3, "Mary".to_string()),
        ]
    );

    let query_log = instance.log().get_from_memory().unwrap();

    // Update with local eval should lock remote rows using FOR UPDATE first
    assert_eq!(
        query_log[0],
        (
            "postgres".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."ctid" AS "i0", "t1"."id" AS "c0", "t1"."name" AS "c1" "#,
                    r#"FROM "public"."t006__test_tab" AS "t1" "#,
                    r#"FOR UPDATE"#,
                ]
                .join(""),
                vec![],
                None
            )
        )
    );
    assert_eq!(query_log[1].0, "postgres".to_string());
    assert_eq!(
        query_log[1].1.query(),
        [
            r#"UPDATE "public"."t006__test_tab" SET "name" = $1 "#,
            r#"WHERE (("t006__test_tab"."ctid") = ($2))"#,
        ]
        .join("")
        .as_str(),
    );
    assert_eq!(
        query_log[1].1.params()[0].as_str(),
        "value=Utf8String(\"Johnny\") type=varchar"
    );
    assert!(query_log[1].1.params()[1]
        .as_str()
        .starts_with("value=Utf8String("));
    assert_eq!(
        query_log[1].1.other(),
        &[("affected".into(), "Some(1)".into())]
            .into_iter()
            .collect::<HashMap<String, String>>()
    );
}
