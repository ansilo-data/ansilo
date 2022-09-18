use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::data::DataValue;

use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let (mut sqlite, sqlite_path) =
        ansilo_e2e::sqlite::init_sqlite_sql(current_dir!().join("sqlite-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance_args(
        current_dir!().join("config.yml"),
        &[("SQLITE_PATH", sqlite_path.to_string_lossy())],
    );

    let rows = client
        .execute(r#"DELETE FROM t004__test_tab"#, &[])
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on sqlite end
    let count = sqlite
        .execute("SELECT COUNT(*) FROM t004__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .read_data_value()
        .unwrap()
        .unwrap();

    // Will be coerced to binary due to unknown type
    assert_eq!(count, DataValue::Binary(b"0".to_vec()));

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("sqlite".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "sqlite".to_string(),
                LoggedQuery::new(
                    r#"DELETE FROM "t004__test_tab""#,
                    vec![],
                    Some(
                        [("affected".into(), "Some(2)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("sqlite".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}
