use std::{collections::HashMap, env};

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::err::Result;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use rust_decimal::prelude::ToPrimitive;
use serial_test::serial;

#[test]
#[serial]
fn test_delete_where_remote() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            DELETE FROM "T008__TEST_TAB"
            WHERE "ID" = 2
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T008__TEST_TAB ORDER BY ID", vec![])
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
                r["ID"].as_decimal().unwrap().to_i64().unwrap().clone(),
                r["NAME"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![(1, "John".to_string()), (3, "Mary".to_string()),]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"DELETE FROM "ANSILO_ADMIN"."T008__TEST_TAB" "#,
                    r#"WHERE (("T008__TEST_TAB"."ID") = (?))"#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setBigDecimal, value=2]".into(),],
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
fn test_delete_where_local() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            DELETE FROM "T008__TEST_TAB"
            WHERE MD5("ID"::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T008__TEST_TAB ORDER BY ID", vec![])
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
                r["ID"].as_decimal().unwrap().to_i64().unwrap().clone(),
                r["NAME"].as_utf8_string().unwrap().clone()
            ))
            .collect_vec(),
        vec![(2, "Jane".to_string()), (3, "Mary".to_string()),]
    );

    let query_log = instance.log().get_from_memory().unwrap();

    // Delete with local eval should lock remote rows using FOR UPDATE first
    assert_eq!(
        query_log[0],
        (
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."ROWID" AS "i0", "t1"."ID" AS "c0" "#,
                    r#"FROM "ANSILO_ADMIN"."T008__TEST_TAB" "t1" "#,
                    r#"FOR UPDATE"#,
                ]
                .join(""),
                vec![],
                None
            )
        )
    );
    assert_eq!(query_log[1].0, "oracle".to_string());
    assert_eq!(
        query_log[1].1.query(),
        [
            r#"DELETE FROM "ANSILO_ADMIN"."T008__TEST_TAB" "#,
            r#"WHERE (("T008__TEST_TAB"."ROWID") = (?))"#,
        ]
        .join("")
        .as_str(),
    );
    assert!(query_log[1].1.params()[0]
        .as_str()
        .starts_with("LoggedParam [index=1, method=setNString, value="));
    assert_eq!(
        query_log[1].1.other(),
        &[("affected".into(), "Some(1)".into())]
            .into_iter()
            .collect::<HashMap<String, String>>()
    );
}
