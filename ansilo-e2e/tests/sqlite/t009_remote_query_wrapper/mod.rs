use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::{data::DataValue, err::Result};
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ansilo_e2e::util::assert::assert_rows_equal;

#[test]
#[serial]
fn test_remote_select() {
    ansilo_logging::init_for_tests();
    let (mut sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    client
        .batch_execute(r#"SELECT remote_increment_counter();"#)
        .unwrap();

    // Check query executed on sqlite
    let results = sqlite
        .execute("SELECT cnt FROM t009__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_rows_equal(
        results,
        vec![vec![("cnt".to_string(), DataValue::Int64(1))]
            .into_iter()
            .collect()],
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "sqlite".to_string(),
            LoggedQuery::new(
                [r#"UPDATE t009__test_tab SET cnt = cnt + 1"#].join(""),
                vec![],
                Some(
                    [("affected".into(), "Some(1)".into())]
                        .into_iter()
                        .collect()
                )
            )
        ),]
    );
}
