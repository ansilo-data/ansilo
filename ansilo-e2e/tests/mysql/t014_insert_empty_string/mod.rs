use std::env;

use ansilo_connectors_base::interface::ResultSet;
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_insert_select_local_values() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "t014__test_target" (data) VALUES ('')
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on mysql end
    let results = mysql
        .execute("SELECT * FROM t014__test_target", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![vec![("data".to_string(), DataValue::Utf8String("".into()))]
            .into_iter()
            .collect()],
    );
}
