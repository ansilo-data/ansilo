use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::data::DataValue;

use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::mysql::start_mysql();
    let mut mysql =
        ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .execute(r#"DELETE FROM "t004__test_tab""#, &[])
        .unwrap();

    assert_eq!(rows, 2);

    // Check data received on mysql end
    let count = mysql
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
        vec![(
            "mysql".to_string(),
            LoggedQuery::new(
                r#"DELETE FROM `db`.`t004__test_tab`"#,
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
