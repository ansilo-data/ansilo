use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    #[allow(unused)]
    let [(peer_instance, mut peer_client), (main_instance, mut main_client)] =
        ansilo_e2e::peer::run_instances([
            ("PEER", current_dir!().join("peer-config.yml")),
            ("MAIN", current_dir!().join("main-config.yml")),
        ]);

    let rows = main_client
        .query(
            r#"
            SELECT * FROM people;
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Elizabeth");
    assert_eq!(rows[0].get::<_, i32>("age"), 20);

    assert_eq!(
        main_instance.log().get_from_memory().unwrap(),
        vec![(
            "peer".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."name" AS "c0", "t1"."age" AS "c1" "#,
                    r#"FROM "public"."people" AS "t1""#,
                ]
                .join(""),
                vec![],
                None
            )
        )]
    );
}
