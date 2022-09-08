use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::data::DataValue;

use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    let mut oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(r#"DELETE FROM "T005__TEST_TAB""#, &[])
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on oracle end
    let count = oracle
        .execute("SELECT COUNT(*) FROM T005__TEST_TAB", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .read_data_value()
        .unwrap()
        .unwrap();

    assert_eq!(count, DataValue::Decimal(0.into()));

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                r#"DELETE FROM "ANSILO_ADMIN"."T005__TEST_TAB""#,
                vec![],
                Some(
                    [("affected".into(), "Some(2)".into())]
                        .into_iter()
                        .collect()
                )
            )
        )]
    );
}
