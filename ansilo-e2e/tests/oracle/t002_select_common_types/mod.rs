use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_main::{
    args::{Args, Command},
    Ansilo, RemoteQueryLog,
};

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let instance = Ansilo::start(
        Command::Run(Args::testing(crate::current_dir!().join("config.yml"))),
        Some(RemoteQueryLog::store_in_memory()),
    )
    .unwrap();

    let mut client = crate::common::connect(65432);

    let rows = client.query("SELECT * FROM \"ANSILO_ADMIN.T002__TEST_TAB\"", &[]).unwrap();

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
            LoggedQuery::query(r#"SELECT "t1"."DUMMY" AS "c0" FROM "SYS"."DUAL" "t1""#)
        )]
    )
}
