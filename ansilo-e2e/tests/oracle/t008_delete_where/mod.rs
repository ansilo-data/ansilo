use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::err::Result;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use rust_decimal::prelude::ToPrimitive;
use serial_test::serial;

#[test]
#[serial]
fn test_delete_where_remote() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    let _rows = client
        .execute(
            r#"
            DELETE FROM "ANSILO_ADMIN.T008__TEST_TAB"
            WHERE "ID" = 2
        "#,
            &[],
        )
        .unwrap();

    // TODO: implement row count reporting for update / delete
    // assert_eq!(rows, 1);

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
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_delete_where_local() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    let _rows = client
        .execute(
            r#"
            DELETE FROM "ANSILO_ADMIN.T008__TEST_TAB"
            WHERE MD5("ID"::text) = MD5('1')
        "#,
            &[],
        )
        .unwrap();

    // TODO: implement row count reporting for update / delete
    // assert_eq!(rows, 1);

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
                    r#"SELECT "t1"."ROWID" AS "c0", "t1"."ID" AS "c1" "#,
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
        .starts_with("LoggedParam [index=1, method=setNString, value="))
}
