use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    ansilo_e2e::teradata::start_teradata();
    let _teradata =
        ansilo_e2e::teradata::init_teradata_sql(current_dir!().join("teradata-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query("SELECT 1234 as col FROM \"t014__test_tab\"", &[])
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .columns()
            .into_iter()
            .map(|c| c.name())
            .collect_vec(),
        vec!["col"]
    );
    assert_eq!(rows[0].get::<_, i32>(0), 1234);

    // When no source columns are required
    // it should use a constant NULL value to form a valid query
    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "teradata".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT ? AS "c0" "#,
                    r#"FROM "testdb"."t014__test_tab" AS "t1""#,
                ]
                .join(""),
                vec!["LoggedParam [index=1, method=setNull, value=null]".into()],
                None
            )
        )]
    );
}
