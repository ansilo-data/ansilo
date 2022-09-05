use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let _containers = ansilo_e2e::oracle::start_oracle();

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client.query("SELECT * FROM \"DUAL\"", &[]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .columns()
            .into_iter()
            .map(|c| c.name())
            .collect::<Vec<_>>(),
        vec!["DUMMY"]
    );
    assert_eq!(rows[0].get::<_, String>(0), "X".to_string());

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new_query(r#"SELECT "t1"."DUMMY" AS "c0" FROM "SYS"."DUAL" "t1""#)
        )]
    )
}
