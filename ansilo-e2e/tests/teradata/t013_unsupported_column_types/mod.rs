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
    ansilo_e2e::teradata::start_teradata();
    let _teradata = ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let row = client
        .query_one(
            r#"
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public' 
            AND table_name = 't013__no_supported_cols'
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
    ansilo_e2e::teradata::start_teradata();
    let _teradata = ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = 't013__one_supported_cols'
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect_vec(),
        vec!["str"]
    );
}
