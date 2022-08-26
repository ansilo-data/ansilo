use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::data::DataValue;

use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::main::run_instance(crate::current_dir!().join("config.yml"));

    let _rows = client
        .execute(r#"DELETE FROM "ANSILO_ADMIN.T005__TEST_TAB""#, &[])
        .unwrap();

    // TODO: implement row count reporting for update / delete
    // assert_eq!(rows, 1);

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
                None
            )
        )]
    );
}
