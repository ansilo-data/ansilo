use std::env;

use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test_ignores_table_with_no_supported_columns() {
    // table should not be imported
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let row = client
        .query_one(
            r#"
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public' 
            AND table_name = 'T019__NO_SUPPORTED_COLS'
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(row.get::<_, i64>(0), 0);
}

#[test]
#[serial]
fn test_ignores_unsupported_column_type() {
    // table should not be imported
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = 'T019__ONE_SUPPORTED_COLS'
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect_vec(),
        vec!["STR"]
    );
}
