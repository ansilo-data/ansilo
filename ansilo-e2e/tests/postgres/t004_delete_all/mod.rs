use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::data::DataValue;

use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::postgres::start_postgres();
    let mut postgres = ansilo_e2e::postgres::init_postgres_sql(
        &containers,
        current_dir!().join("postgres-sql/*.sql"),
    );

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(r#"DELETE FROM "t004__test_tab""#, &[])
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on postgres end
    let count = postgres
        .execute("SELECT COUNT(*) FROM t004__test_tab", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .read_data_value()
        .unwrap()
        .unwrap();

    assert_eq!(count, DataValue::Int64(0));

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![
            ("postgres".to_string(), LoggedQuery::new_query("BEGIN")),
            (
                "postgres".to_string(),
                LoggedQuery::new(
                    r#"DELETE FROM "public"."t004__test_tab""#,
                    vec![],
                    Some(
                        [("affected".into(), "Some(2)".into())]
                            .into_iter()
                            .collect()
                    )
                )
            ),
            ("postgres".to_string(), LoggedQuery::new_query("COMMIT")),
        ]
    );
}
