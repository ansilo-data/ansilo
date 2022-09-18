use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_transaction_commit() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO "T016__TEST_TAB" ("DATA") VALUES ('value');
        COMMIT;
        "#,
        )
        .unwrap();

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T016__TEST_TAB", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![
            vec![("DATA".to_string(), DataValue::Utf8String("value".into()))]
                .into_iter()
                .collect(),
        ],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("oracle".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T016__TEST_TAB" "#,
                        r#"("DATA") VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("oracle".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}

#[test]
#[serial]
fn test_transaction_rollback() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO "T016__TEST_TAB" ("DATA") VALUES ('value');
        ROLLBACK;
        "#,
        )
        .unwrap();

    // Check rolled back on oracle side
    let results = oracle
        .execute("SELECT * FROM T016__TEST_TAB", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(results, vec![]);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("oracle".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T016__TEST_TAB" "#,
                        r#"("DATA") VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("oracle".to_string(), LoggedQuery::new_query("ROLLBACK")),
        ]
    );
}

#[test]
#[serial]
fn test_transaction_rollback_due_to_error() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;

        INSERT INTO "T016__TEST_TAB" ("DATA") VALUES ('value');

        DO $$BEGIN
            RAISE EXCEPTION "An error occurred!";
        END$$;
        "#,
        )
        .unwrap_err();

    // Check rolled back on oracle side
    let results = oracle
        .execute("SELECT * FROM T016__TEST_TAB", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(results, vec![]);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("oracle".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T016__TEST_TAB" "#,
                        r#"("DATA") VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("oracle".to_string(), LoggedQuery::new_query("ROLLBACK")),
        ]
    );
}
