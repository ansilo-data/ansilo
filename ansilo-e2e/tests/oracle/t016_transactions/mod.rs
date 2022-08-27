use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use pretty_assertions::assert_eq;
use serial_test::serial;

use crate::assert::assert_rows_equal;

#[test]
#[serial]
fn test_transaction_commit() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO "ANSILO_ADMIN.T016__TEST_TAB" ("DATA") VALUES ('value');
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
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"INSERT INTO "ANSILO_ADMIN"."T016__TEST_TAB" "#,
                    r#"("DATA") VALUES (?)"#
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_transaction_rollback() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO "ANSILO_ADMIN.T016__TEST_TAB" ("DATA") VALUES ('value');
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
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"INSERT INTO "ANSILO_ADMIN"."T016__TEST_TAB" "#,
                    r#"("DATA") VALUES (?)"#
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                None
            )
        )]
    );
}

#[test]
#[serial]
fn test_transaction_rollback_due_to_error() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
        BEGIN;

        INSERT INTO "ANSILO_ADMIN.T016__TEST_TAB" ("DATA") VALUES ('value');

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
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"INSERT INTO "ANSILO_ADMIN"."T016__TEST_TAB" "#,
                    r#"("DATA") VALUES (?)"#
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setNString, value=value]".into()],
                None
            )
        )]
    );
}