use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_generated_always() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "T015__GENERATED_ALWAYS" (
                "DATA"
            ) VALUES (
                'value'
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T015__GENERATED_ALWAYS", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![vec![
            ("ID".to_string(), DataValue::Decimal(1.into())),
            ("DATA".to_string(), DataValue::Utf8String("value".into())),
        ]
        .into_iter()
        .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("oracle".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T015__GENERATED_ALWAYS" "#,
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
fn test_generated_default() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
            INSERT INTO "T015__GENERATED_DEFAULT" 
            ("DATA") VALUES ('first');

            INSERT INTO "T015__GENERATED_DEFAULT" 
            ("ID", "DATA") VALUES (123, 'second');
        "#,
        )
        .unwrap();

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T015__GENERATED_DEFAULT", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![
            vec![
                ("ID".to_string(), DataValue::Decimal(1.into())),
                ("DATA".to_string(), DataValue::Utf8String("first".into())),
            ]
            .into_iter()
            .collect(),
            vec![
                ("ID".to_string(), DataValue::Decimal(123.into())),
                ("DATA".to_string(), DataValue::Utf8String("second".into())),
            ]
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
                        r#"INSERT INTO "ANSILO_ADMIN"."T015__GENERATED_DEFAULT" "#,
                        r#"("DATA") VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=first]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T015__GENERATED_DEFAULT" "#,
                        r#"("ID", "DATA") VALUES (?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setBigDecimal, value=123]".into(),
                        "LoggedParam [index=2, method=setNString, value=second]".into()
                    ],
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
fn test_default() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    client
        .batch_execute(
            r#"
            INSERT INTO "T015__DEFAULT" 
            ("DATA") VALUES ('first');

            INSERT INTO "T015__DEFAULT" 
            ("ID", "DATA") VALUES (123, 'second');
        "#,
        )
        .unwrap();

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T015__DEFAULT", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![
            vec![
                ("ID".to_string(), DataValue::Decimal((-1).into())),
                ("DATA".to_string(), DataValue::Utf8String("first".into())),
            ]
            .into_iter()
            .collect(),
            vec![
                ("ID".to_string(), DataValue::Decimal(123.into())),
                ("DATA".to_string(), DataValue::Utf8String("second".into())),
            ]
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
                        r#"INSERT INTO "ANSILO_ADMIN"."T015__DEFAULT" "#,
                        r#"("DATA") VALUES (?)"#
                    ]
                    .join(""),
                    vec!["LoggedParam [index=1, method=setNString, value=first]".into()],
                    Some(
                        [("affected".into(), "Some(1)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            (
                "oracle".to_string(),
                LoggedQuery::new(
                    [
                        r#"INSERT INTO "ANSILO_ADMIN"."T015__DEFAULT" "#,
                        r#"("ID", "DATA") VALUES (?, ?)"#
                    ]
                    .join(""),
                    vec![
                        "LoggedParam [index=1, method=setBigDecimal, value=123]".into(),
                        "LoggedParam [index=2, method=setNString, value=second]".into()
                    ],
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
